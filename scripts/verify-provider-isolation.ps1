param(
    [int]$BasePort = 18085
)

$ErrorActionPreference = "Stop"
$projectRoot = if ([string]::IsNullOrWhiteSpace($PSScriptRoot)) {
    (Get-Location).Path
} else {
    Split-Path -Parent $PSScriptRoot
}
$runID = [guid]::NewGuid().ToString("N").Substring(0, 10)
$databaseName = "jungle_provider_review_$runID"
$applicationName = "jungle-provider-review-$runID"
$inputQueue = "provider-review-$runID.fifo"
$outboxQueue = "provider-outbox-review-$runID.fifo"
$databaseCreated = $false
$applicationCreated = $false
$queuesCreated = @()
$postgresContainer = $null
$localstackContainer = $null
$keycloakAdminAuthenticated = $false
$providerClientID = $null
$providerClientName = "provider-b-review-$runID"

function Get-ClientCredentialsToken {
    param(
        [string]$ClientID,
        [string]$ClientSecret
    )

    $response = docker compose --profile tools run --rm -T curl -fsS -X POST `
        http://keycloak:8080/realms/jungle/protocol/openid-connect/token `
        -u "${ClientID}:$ClientSecret" -d grant_type=client_credentials
    if ($LASTEXITCODE -ne 0) {
        throw "could not obtain OIDC token for $ClientID"
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

    docker compose run --detach --no-deps --name $applicationName `
        --publish ("{0}:8080" -f $BasePort) `
        -e "DATABASE_URL=$databaseURL" `
        -e "WAGER_QUEUE_URL=http://localstack:4566/000000000000/$inputQueue" `
        -e "OUTBOX_QUEUE_URL=http://localstack:4566/000000000000/$outboxQueue" app | Out-Null
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

    $internalToken = Get-ClientCredentialsToken "internal-service" "internal-service-local-secret"
    $providerAToken = Get-ClientCredentialsToken "provider-a" "provider-a-local-secret"

    $keycloak = $compose.services.keycloak
    $adminUser = [string]$keycloak.environment.KC_BOOTSTRAP_ADMIN_USERNAME
    $adminPassword = [string]$keycloak.environment.KC_BOOTSTRAP_ADMIN_PASSWORD
    docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh config credentials `
        --server http://localhost:8080 --realm master --user $adminUser --password $adminPassword | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "could not authenticate the local Keycloak admin CLI"
    }
    $keycloakAdminAuthenticated = $true
    $providerClients = docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh `
        get clients -r jungle -q "clientId=$providerClientName"
    if ($LASTEXITCODE -ne 0) {
        throw "could not inspect provider-b test client"
    }
    if ($providerClients.Trim() -match '^\[\s*\]$') {
        $realm = Get-Content (Join-Path $projectRoot "deploy\keycloak\jungle-realm.json") -Raw |
            ConvertFrom-Json
        $providerClient = $realm.clients | Where-Object { $_.clientId -eq "provider-b" }
        if (-not $providerClient) {
            throw "provider-b test identity is missing from the local realm definition"
        }
        $providerClient.clientId = $providerClientName
        $providerClient.name = "Temporary provider isolation acceptance client"
        $clientJSON = $providerClient | ConvertTo-Json -Depth 15 -Compress
        $encodedClient = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($clientJSON))
        docker compose exec -T keycloak sh -c `
            "printf '%s' $encodedClient | base64 -d > /tmp/$providerClientName.json"
        if ($LASTEXITCODE -ne 0) {
            throw "could not stage the provider-b client representation"
        }
        docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh `
            create clients -r jungle -f "/tmp/$providerClientName.json" | Out-Null
        $createClientExitCode = $LASTEXITCODE
        docker compose exec -T keycloak rm -f "/tmp/$providerClientName.json" | Out-Null
        if ($createClientExitCode -ne 0) {
            throw "could not create the provider-b test client"
        }
    }
    $providerClients = docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh `
        get clients -r jungle -q "clientId=$providerClientName" | ConvertFrom-Json
    $providerClientID = $providerClients[0].id
    if (-not $providerClientID) {
        throw "temporary provider-b client ID was not returned by Keycloak"
    }
    docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh `
        update "clients/$providerClientID" -r jungle -s publicClient=false | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "could not enforce confidential client authentication for provider-b"
    }
    docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh `
        create "clients/$providerClientID/client-secret" -r jungle | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "could not provision a temporary provider-b client secret"
    }
    $secretResponse = docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh `
        get "clients/$providerClientID/client-secret" -r jungle | ConvertFrom-Json
    $providerBToken = Get-ClientCredentialsToken $providerClientName $secretResponse.value

    $runSuffix = [guid]::NewGuid().ToString("N").Substring(0, 10)
    $player = "provider-isolation-$runSuffix"
    $externalID = "shared-external-$runSuffix"
    $idempotencyKey = "shared-key-$runSuffix"
    $walletBody = @{
        playerId = $player
        initialBalance = @{ amount = "100.00"; currency = "BRL" }
    } | ConvertTo-Json -Depth 4 -Compress
    $wallet = Invoke-RestMethod -Method Post -Uri "http://localhost:$BasePort/wallets" `
        -Headers @{ Authorization = "Bearer $internalToken" } `
        -ContentType application/json -Body $walletBody

    $bodyA = @{
        providerId = "provider-a"; externalTransactionId = $externalID
        playerId = $player; walletId = $wallet.id; roundId = "shared-round"
        gameId = "isolation"; kind = "BET"; money = @{ amount = "1.00"; currency = "BRL" }
    } | ConvertTo-Json -Depth 5 -Compress
    $bodyB = @{
        providerId = "provider-b"; externalTransactionId = $externalID
        playerId = $player; walletId = $wallet.id; roundId = "shared-round"
        gameId = "isolation"; kind = "BET"; money = @{ amount = "1.00"; currency = "BRL" }
    } | ConvertTo-Json -Depth 5 -Compress
    $transactionA = Invoke-RestMethod -Method Post -Uri "http://localhost:$BasePort/wagering/transactions" `
        -Headers @{ Authorization = "Bearer $providerAToken"; "Idempotency-Key" = $idempotencyKey } `
        -ContentType application/json -Body $bodyA
    $transactionB = Invoke-RestMethod -Method Post -Uri "http://localhost:$BasePort/wagering/transactions" `
        -Headers @{ Authorization = "Bearer $providerBToken"; "Idempotency-Key" = $idempotencyKey } `
        -ContentType application/json -Body $bodyB
    if ($transactionA.idempotentReplay -or $transactionB.idempotentReplay -or
        $transactionA.transactionId -eq $transactionB.transactionId) {
        throw "the same external ID and idempotency key were not isolated per provider"
    }

    foreach ($test in @(
        @{ Token = $providerAToken; Provider = "provider-a"; ExpectedID = $transactionA.transactionId },
        @{ Token = $providerBToken; Provider = "provider-b"; ExpectedID = $transactionB.transactionId }
    )) {
        $owned = Invoke-RestMethod -Uri `
            "http://localhost:$BasePort/providers/$($test.Provider)/wagering/transactions/$externalID" `
            -Headers @{ Authorization = "Bearer $($test.Token)" }
        if ($owned.transactionId -ne $test.ExpectedID) {
            throw "$($test.Provider) did not retrieve its own transaction"
        }
    }

    $crossProviderPathStatus = Get-HTTPStatus {
        Invoke-WebRequest -Uri `
            "http://localhost:$BasePort/providers/provider-a/wagering/transactions/$externalID" `
            -Headers @{ Authorization = "Bearer $providerBToken" } -UseBasicParsing
    }
    $crossProviderIDStatus = Get-HTTPStatus {
        Invoke-WebRequest -Uri `
            "http://localhost:$BasePort/wagering/transactions/$($transactionA.transactionId)" `
            -Headers @{ Authorization = "Bearer $providerBToken" } -UseBasicParsing
    }
    if ($crossProviderPathStatus -ne 403 -or $crossProviderIDStatus -ne 404) {
        throw "cross-provider lookup statuses were $crossProviderPathStatus/$crossProviderIDStatus, expected 403/404"
    }

    $forgedProviderStatus = Get-HTTPStatus {
        Invoke-WebRequest -Method Post -Uri "http://localhost:$BasePort/wagering/transactions" `
            -Headers @{ Authorization = "Bearer $providerAToken"; "Idempotency-Key" = "forged-$runSuffix" } `
            -ContentType application/json -Body ($bodyB | ConvertFrom-Json | ConvertTo-Json -Depth 5 -Compress) `
            -UseBasicParsing
    }
    if ($forgedProviderStatus -ne 403) {
        throw "provider-a spoofing provider-b in the request body returned $forgedProviderStatus, expected 403"
    }

    $finalWallet = Invoke-RestMethod -Uri "http://localhost:$BasePort/wallets/$($wallet.id)" `
        -Headers @{ Authorization = "Bearer $internalToken" }
    if ($finalWallet.balance.amount -ne "98.00") {
        throw "provider-scoped transactions left balance $($finalWallet.balance.amount), expected 98.00"
    }
    Write-Output "PROVIDER_SCOPED_IDEMPOTENCY=PASS"
    Write-Output "OWN_TRANSACTION_LOOKUPS=PASS"
    Write-Output "CROSS_PROVIDER_LOOKUPS_REJECTED=PASS"
    Write-Output "FORGED_PROVIDER_IDENTITY_REJECTED=PASS"
    Write-Output "BALANCE=$($finalWallet.balance.amount) DISTINCT_TRANSACTIONS=2"
} finally {
    if ($applicationCreated) {
        docker rm -f $applicationName 2>$null | Out-Null
    }
    if ($providerClientID -and $keycloakAdminAuthenticated) {
        docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh `
            delete "clients/$providerClientID" -r jungle 2>$null | Out-Null
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
