param(
    [int]$BasePort = 18090
)

$ErrorActionPreference = "Stop"
$projectRoot = if ([string]::IsNullOrWhiteSpace($PSScriptRoot)) {
    (Get-Location).Path
} else {
    Split-Path -Parent $PSScriptRoot
}
$runID = [guid]::NewGuid().ToString("N").Substring(0, 10)
$realmName = "token-review-$runID"
$databaseName = "jungle_token_review_$runID"
$applicationName = "jungle-token-review-$runID"
$inputQueue = "token-review-$runID.fifo"
$outboxQueue = "token-outbox-review-$runID.fifo"
$databaseCreated = $false
$applicationCreated = $false
$realmCreated = $false
$queuesCreated = @()
$postgresContainer = $null
$localstackContainer = $null
$keycloakAdminAuthenticated = $false

function Get-ClientCredentialsToken {
    param([string]$ClientID, [string]$ClientSecret)

    $response = docker compose --profile tools run --rm -T curl -fsS -X POST `
        "http://keycloak:8080/realms/$realmName/protocol/openid-connect/token" `
        -u "${ClientID}:$ClientSecret" -d grant_type=client_credentials
    if ($LASTEXITCODE -ne 0) {
        throw "could not obtain OIDC token for $ClientID from temporary realm"
    }
    $token = ($response | ConvertFrom-Json).access_token
    if (-not $token) {
        throw "Keycloak returned an empty token for $ClientID"
    }
    return $token
}

function Get-HTTPStatus {
    param([scriptblock]$Request)

    try {
        & $Request | Out-Null
        return 200
    } catch {
        if ($_.Exception.Response) {
            return [int]$_.Exception.Response.StatusCode
        }
        throw
    }
}

Push-Location $projectRoot
try {
    $compose = docker compose config --format json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose config failed: $LASTEXITCODE"
    }
    $postgres = $compose.services.postgres
    $postgresUser = [string]$postgres.environment.POSTGRES_USER
    $postgresPassword = [uri]::EscapeDataString([string]$postgres.environment.POSTGRES_PASSWORD)
    $databaseUser = [uri]::EscapeDataString($postgresUser)
    $keycloak = $compose.services.keycloak
    $adminUser = [string]$keycloak.environment.KC_BOOTSTRAP_ADMIN_USERNAME
    $adminPassword = [string]$keycloak.environment.KC_BOOTSTRAP_ADMIN_PASSWORD

    docker compose up --detach --wait postgres keycloak localstack
    if ($LASTEXITCODE -ne 0) {
        throw "required dependencies failed to start: $LASTEXITCODE"
    }
    $postgresContainer = (docker compose ps -q postgres).Trim()
    $localstackContainer = (docker compose ps -q localstack).Trim()

    docker compose build app migrate
    if ($LASTEXITCODE -ne 0) {
        throw "application images failed to build: $LASTEXITCODE"
    }
    docker exec $postgresContainer psql -U $postgresUser -d postgres -v ON_ERROR_STOP=1 `
        -c "CREATE DATABASE $databaseName" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "could not create isolated database $databaseName"
    }
    $databaseCreated = $true
    $databaseURL = "postgres://${databaseUser}:${postgresPassword}@postgres:5432/${databaseName}?sslmode=disable"
    docker compose run --rm --no-deps -e "DATABASE_URL=$databaseURL" migrate up
    if ($LASTEXITCODE -ne 0) {
        throw "migrations failed for $databaseName"
    }

    foreach ($queueName in @($inputQueue, $outboxQueue)) {
        docker exec $localstackContainer awslocal sqs create-queue --queue-name $queueName `
            --attributes "FifoQueue=true,ContentBasedDeduplication=false" | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "could not create isolated queue $queueName"
        }
        $queuesCreated += $queueName
    }

    docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh config credentials `
        --server http://localhost:8080 --realm master --user $adminUser --password $adminPassword | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "could not authenticate the local Keycloak admin CLI"
    }
    $keycloakAdminAuthenticated = $true
    $realm = Get-Content (Join-Path $projectRoot "deploy\keycloak\jungle-realm.json") -Raw |
        ConvertFrom-Json
    $realm.realm = $realmName
    $realm.displayName = "Temporary OIDC expiry verification"
    $realm | Add-Member -MemberType NoteProperty -Name accessTokenLifespan -Value 3
    $realmJSON = $realm | ConvertTo-Json -Depth 20 -Compress
    $encodedRealm = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($realmJSON))
    docker compose exec -T keycloak sh -c `
        "printf '%s' $encodedRealm | base64 -d > /tmp/$realmName.json"
    if ($LASTEXITCODE -ne 0) {
        throw "could not stage the temporary realm definition"
    }
    docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh `
        create realms -f "/tmp/$realmName.json" | Out-Null
    $createRealmExitCode = $LASTEXITCODE
    docker compose exec -T keycloak rm -f "/tmp/$realmName.json" | Out-Null
    if ($createRealmExitCode -ne 0) {
        throw "could not create the isolated Keycloak realm"
    }
    $realmCreated = $true

    docker compose run --detach --no-deps --name $applicationName `
        --publish ("{0}:8080" -f $BasePort) `
        -e "DATABASE_URL=$databaseURL" `
        -e "WAGER_QUEUE_URL=http://localstack:4566/000000000000/$inputQueue" `
        -e "OUTBOX_QUEUE_URL=http://localstack:4566/000000000000/$outboxQueue" `
        -e "OIDC_ISSUER_URL=http://keycloak:8080/realms/$realmName" app | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "could not start isolated API process"
    }
    $applicationCreated = $true
    $ready = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        try {
            $response = Invoke-WebRequest -Uri "http://localhost:$BasePort/health/ready" `
                -UseBasicParsing -TimeoutSec 2
            if ($response.StatusCode -eq 200) {
                $ready = $true
                break
            }
        } catch {
            Start-Sleep -Milliseconds 500
        }
    }
    if (-not $ready) {
        throw "isolated API did not become ready"
    }

    $providerToken = Get-ClientCredentialsToken "provider-a" "provider-a-local-secret"
    $lookupURI = "http://localhost:$BasePort/providers/provider-a/wagering/transactions/not-found"
    $validTokenStatus = Get-HTTPStatus {
        Invoke-WebRequest -Uri $lookupURI -Headers @{ Authorization = "Bearer $providerToken" } `
            -UseBasicParsing
    }
    if ($validTokenStatus -ne 404) {
        throw "valid unexpired token returned $validTokenStatus, expected 404 from the missing transaction"
    }

    $invalidTokenStatus = Get-HTTPStatus {
        Invoke-WebRequest -Uri $lookupURI -Headers @{ Authorization = "Bearer not.a.valid.jwt" } `
            -UseBasicParsing
    }
    if ($invalidTokenStatus -ne 401) {
        throw "invalid token returned $invalidTokenStatus, expected 401"
    }

    Start-Sleep -Seconds 5
    $expiredTokenStatus = Get-HTTPStatus {
        Invoke-WebRequest -Uri $lookupURI -Headers @{ Authorization = "Bearer $providerToken" } `
            -UseBasicParsing
    }
    if ($expiredTokenStatus -ne 401) {
        throw "expired signed OIDC token returned $expiredTokenStatus, expected 401"
    }

    Write-Output "VALID_TOKEN_ACCEPTED_BEFORE_EXPIRY=PASS"
    Write-Output "INVALID_TOKEN_REJECTED=PASS"
    Write-Output "EXPIRED_SIGNED_TOKEN_REJECTED=PASS"
} finally {
    if ($applicationCreated) {
        docker rm -f $applicationName 2>$null | Out-Null
    }
    if ($realmCreated -and $keycloakAdminAuthenticated) {
        docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh `
            delete "realms/$realmName" 2>$null | Out-Null
    }
    if ($queuesCreated.Count -gt 0 -and $localstackContainer) {
        foreach ($queueName in $queuesCreated) {
            docker exec $localstackContainer awslocal sqs delete-queue `
                --queue-url "http://localhost:4566/000000000000/$queueName" 2>$null | Out-Null
        }
    }
    if ($databaseCreated -and $postgresContainer) {
        docker exec $postgresContainer psql -U $postgresUser -d postgres -v ON_ERROR_STOP=1 `
            -c "DROP DATABASE $databaseName WITH (FORCE)" 2>$null | Out-Null
    }
    Pop-Location
}
