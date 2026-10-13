param(
    [int]$BasePort = 18082
)

$previousErrorActionPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$projectRoot = if ([string]::IsNullOrWhiteSpace($PSScriptRoot)) {
    (Get-Location).Path
} else {
    Split-Path -Parent $PSScriptRoot
}
$runID = [guid]::NewGuid().ToString("N").Substring(0, 10)
$databaseName = "jungle_acceptance_$runID"
$applicationNames = @(1..3 | ForEach-Object { "jungle-acceptance-$runID-api-$_" })
$databaseCreated = $false
$postgresContainer = $null

function Wait-APIReady {
    param([string]$Uri)

    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        try {
            $response = Invoke-WebRequest -Uri $Uri -UseBasicParsing -TimeoutSec 2
            if ($response.StatusCode -eq 200) {
                return
            }
        } catch {
        }
    }
    throw "API did not become ready: $Uri"
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
    $postgresContainer = (docker compose ps -q postgres).Trim()

    docker compose up --detach --wait postgres keycloak localstack
    if ($LASTEXITCODE -ne 0) {
        throw "required dependencies failed to start: $LASTEXITCODE"
    }
    $postgresContainer = (docker compose ps -q postgres).Trim()

    docker compose build app migrate
    if ($LASTEXITCODE -ne 0) {
        throw "application images failed to build: $LASTEXITCODE"
    }

    docker exec $postgresContainer psql -U $postgresUser -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $databaseName"
    if ($LASTEXITCODE -ne 0) {
        throw "could not create isolated database $databaseName"
    }
    $databaseCreated = $true
    $databaseURL = "postgres://$($databaseUser):$($postgresPassword)@postgres:5432/$($databaseName)?sslmode=disable"

    docker compose run --rm --no-deps -e "DATABASE_URL=$databaseURL" migrate up
    if ($LASTEXITCODE -ne 0) {
        throw "migrations failed for $databaseName"
    }

    for ($index = 0; $index -lt 3; $index++) {
        $port = $BasePort + $index
        $name = $applicationNames[$index]
        docker compose run --detach --no-deps --name $name --publish ("{0}:8080" -f $port) -e "DATABASE_URL=$databaseURL" app | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "could not start API process $($index + 1)"
        }
    }

    $ports = @(0..2 | ForEach-Object { $BasePort + $_ })
    foreach ($port in $ports) {
        Wait-APIReady "http://localhost:$port/health/ready"
    }

    $internalJson = docker compose --profile tools run --rm -T curl -fsS -X POST `
        http://keycloak:8080/realms/jungle/protocol/openid-connect/token `
        -u internal-service:internal-service-local-secret -d grant_type=client_credentials
    if ($LASTEXITCODE -ne 0) {
        throw "could not obtain internal Keycloak token: $LASTEXITCODE"
    }
    $providerJson = docker compose --profile tools run --rm -T curl -fsS -X POST `
        http://keycloak:8080/realms/jungle/protocol/openid-connect/token `
        -u provider-a:provider-a-local-secret -d grant_type=client_credentials
    if ($LASTEXITCODE -ne 0) {
        throw "could not obtain provider Keycloak token: $LASTEXITCODE"
    }
    $internalToken = ($internalJson | ConvertFrom-Json).access_token
    $providerToken = ($providerJson | ConvertFrom-Json).access_token
    if (-not $internalToken -or -not $providerToken) {
        throw "Keycloak returned an empty access token"
    }

    $anonymousStatus = 0
    try {
        Invoke-WebRequest -Method Post -Uri "http://localhost:$BasePort/wallets" -ContentType application/json -Body "{}" -UseBasicParsing | Out-Null
        $anonymousStatus = 200
    } catch {
        if ($_.Exception.Response) { $anonymousStatus = [int]$_.Exception.Response.StatusCode }
    }
    if ($anonymousStatus -ne 401) {
        throw "anonymous business request returned $anonymousStatus, expected 401"
    }

    $providerWalletStatus = 0
    try {
        Invoke-WebRequest -Uri "http://localhost:$BasePort/wallets/00000000-0000-0000-0000-000000000000" `
            -Headers @{ Authorization = "Bearer $providerToken" } -UseBasicParsing | Out-Null
        $providerWalletStatus = 200
    } catch {
        if ($_.Exception.Response) { $providerWalletStatus = [int]$_.Exception.Response.StatusCode }
    }
    if ($providerWalletStatus -ne 403) {
        throw "provider wallet request returned $providerWalletStatus, expected 403"
    }

    $internalWagerStatus = 0
    try {
        Invoke-WebRequest -Method Post -Uri "http://localhost:$BasePort/wagering/transactions" `
            -Headers @{ Authorization = "Bearer $internalToken" } -ContentType application/json -Body "{}" -UseBasicParsing | Out-Null
        $internalWagerStatus = 200
    } catch {
        if ($_.Exception.Response) { $internalWagerStatus = [int]$_.Exception.Response.StatusCode }
    }
    if ($internalWagerStatus -ne 403) {
        throw "internal client wager request returned $internalWagerStatus, expected 403"
    }

    $playerID = "multi-process-$runID"
    $walletBody = @{ playerId = $playerID; initialBalance = @{ amount = "100.00"; currency = "BRL" } } |
        ConvertTo-Json -Depth 4 -Compress
    $wallet = Invoke-RestMethod -Method Post -Uri "http://localhost:$BasePort/wallets" `
        -Headers @{ Authorization = "Bearer $internalToken" } -ContentType application/json -Body $walletBody

    $keyA = "process-a-$runID"
    $keyB = "process-b-$runID"
    $keyC = "process-c-$runID"
    $externalA = "external-a-$runID"
    $externalB = "external-b-$runID"
    $externalC = "external-c-$runID"
    $bodyA = @{
        providerId = "provider-a"; externalTransactionId = $externalA; playerId = $playerID
        walletId = $wallet.id; roundId = "multi-process-$runID"; gameId = "acceptance"
        kind = "BET"; money = @{ amount = "80.00"; currency = "BRL" }
    } | ConvertTo-Json -Depth 5 -Compress
    $bodyB = @{
        providerId = "provider-a"; externalTransactionId = $externalB; playerId = $playerID
        walletId = $wallet.id; roundId = "multi-process-$runID"; gameId = "acceptance"
        kind = "BET"; money = @{ amount = "80.00"; currency = "BRL" }
    } | ConvertTo-Json -Depth 5 -Compress
    $bodyC = @{
        providerId = "provider-a"; externalTransactionId = $externalC; playerId = $playerID
        walletId = $wallet.id; roundId = "multi-process-$runID"; gameId = "acceptance"
        kind = "BET"; money = @{ amount = "80.00"; currency = "BRL" }
    } | ConvertTo-Json -Depth 5 -Compress

    $sendWager = {
        param($Uri, $Token, $Key, $Body)
        try {
            $response = Invoke-WebRequest -Method Post -Uri $Uri `
                -Headers @{ Authorization = "Bearer $Token"; "Idempotency-Key" = $Key } `
                -ContentType application/json -Body $Body -UseBasicParsing -TimeoutSec 20
            [pscustomobject]@{ Status = [int]$response.StatusCode; Body = $response.Content; Key = $Key }
        } catch {
            $status = 0
            if ($_.Exception.Response) { $status = [int]$_.Exception.Response.StatusCode }
            [pscustomobject]@{ Status = $status; Body = $_.ErrorDetails.Message; Key = $Key }
        }
    }
    $jobA = Start-Job -ScriptBlock $sendWager -ArgumentList @("http://localhost:$($ports[0])/wagering/transactions", $providerToken, $keyA, $bodyA)
    $jobB = Start-Job -ScriptBlock $sendWager -ArgumentList @("http://localhost:$($ports[1])/wagering/transactions", $providerToken, $keyB, $bodyB)
    $jobC = Start-Job -ScriptBlock $sendWager -ArgumentList @("http://localhost:$($ports[2])/wagering/transactions", $providerToken, $keyC, $bodyC)
    $jobs = @($jobA, $jobB, $jobC)
    $finished = Wait-Job -Job $jobs -Timeout 30
    if (@($finished).Count -ne 3) {
        Stop-Job $jobs -ErrorAction SilentlyContinue
        throw "concurrent HTTP requests did not finish"
    }
    $results = @($jobs | ForEach-Object { Receive-Job $_ })
    Remove-Job $jobs
    $processed = @($results | Where-Object { $_.Status -eq 200 })
    $rejected = @($results | Where-Object { $_.Status -eq 422 })
    if ($processed.Count -ne 1 -or $rejected.Count -ne 2) {
        $results | ForEach-Object { Write-Output "status=$($_.Status) body=$($_.Body)" }
        throw "expected one processed and two insufficient-funds wagers across three processes"
    }
    $processedResult = $processed[0].Body | ConvertFrom-Json
    $rejectedResult = $rejected[0].Body | ConvertFrom-Json
    if ($processedResult.status -ne "PROCESSED" -or $rejectedResult.failureCode -ne "INSUFFICIENT_FUNDS") {
        throw "unexpected concurrent wager outcomes"
    }

    $winnerBody = if ($processed[0].Key -eq $keyA) { $bodyA } else { $bodyB }
    if ($processed[0].Key -eq $keyC) { $winnerBody = $bodyC }
    $winnerKey = $processed[0].Key
    $replayPort = $ports[2]
    if ($processed[0].Key -eq $keyC) { $replayPort = $ports[0] }
    $replay = Invoke-RestMethod -Method Post -Uri "http://localhost:$replayPort/wagering/transactions" `
        -Headers @{ Authorization = "Bearer $providerToken"; "Idempotency-Key" = $winnerKey } `
        -ContentType application/json -Body $winnerBody
    if (-not $replay.idempotentReplay -or $replay.transactionId -ne $processedResult.transactionId) {
        throw "cross-process replay did not return the original transaction"
    }

    $finalWallet = Invoke-RestMethod -Uri "http://localhost:$BasePort/wallets/$($wallet.id)" `
        -Headers @{ Authorization = "Bearer $internalToken" }
    $ledger = Invoke-RestMethod -Uri "http://localhost:$BasePort/wallets/$($wallet.id)/ledger?limit=20" `
        -Headers @{ Authorization = "Bearer $internalToken" }
    $debits = @($ledger.entries | Where-Object { $_.direction -eq "DEBIT" })
    if ($finalWallet.balance.amount -ne "20.00" -or $debits.Count -ne 1) {
        throw "expected balance 20.00 and one ledger debit; got $($finalWallet.balance.amount) and $($debits.Count)"
    }

    docker stop --time 15 $applicationNames[0] | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "graceful API shutdown failed"
    }
    $exitCode = [int](docker inspect --format "{{.State.ExitCode}}" $applicationNames[0])
    if ($exitCode -ne 0) {
        throw "API process exited with code $exitCode during graceful shutdown"
    }

    Write-Output "THREE_PROCESS_CONCURRENCY=PASS"
    Write-Output "ALL_THREE_PROCESSES_PARTICIPATED=PASS"
    Write-Output "CROSS_PROCESS_REPLAY=PASS"
    Write-Output "AUTHORIZATION_NEGATIVES=PASS"
    Write-Output "BALANCE=$($finalWallet.balance.amount) LEDGER_DEBITS=$($debits.Count)"
    Write-Output "GRACEFUL_SHUTDOWN=PASS"
} finally {
    foreach ($name in $applicationNames) {
        docker rm -f $name 2>$null | Out-Null
    }
    if ($databaseCreated -and $postgresContainer) {
        docker exec $postgresContainer psql -U $postgresUser -d postgres -v ON_ERROR_STOP=1 `
            -c "DROP DATABASE $databaseName WITH (FORCE)" 2>$null | Out-Null
    }
    Pop-Location
    $ErrorActionPreference = $previousErrorActionPreference
}