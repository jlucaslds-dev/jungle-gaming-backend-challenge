# Jungle Gaming — processamento de operações de wagering

Serviço Go para abertura e movimentação de carteiras, com persistência PostgreSQL, API autenticada por OAuth 2.0/OIDC, consumo at-least-once via SQS e publicação de eventos por transactional outbox.

## Componentes

- Go `1.27.2`, Uber Fx, `net/http` com Chi e pgx.
- PostgreSQL com migrations SQL up/down, locks por carteira, optimistic version check e ledger append-only.
- Keycloak local com clients de serviço `provider-a` e `internal-service`.
- LocalStack com `wager-transactions.fifo`, `wager-transactions-dlq.fifo` e `outbox-events.fifo`.
- Inbox por consumidor e `messageId`, com hash, recebimento e conclusão; idempotência persistente por provedor, chave e hash canônico do conteúdo.
- Worker de outbox com disputa `SKIP LOCKED`, lease, retry e deduplicação FIFO pelo `eventId`.
- Worker que retenta referências pendentes com backoff e até 10 tentativas.

## Executar localmente

Pré-requisitos: Docker Desktop com Docker Compose, Go `1.27.2` para executar os comandos Go no host e PowerShell no Windows.

```powershell
Copy-Item .env.example .env
docker compose up --build
```

O arquivo `.env.example` acompanha o projeto e contém somente valores demonstrativos para desenvolvimento local. Copie-o para `.env` antes de subir o Compose. O `.env` local é excluído do contexto Docker; não o versione nem inclua credenciais reais no projeto. A configuração e os clients importados são exclusivamente para desenvolvimento local. Substitua todos os segredos e configure credenciais/políticas apropriadas antes de qualquer uso fora da máquina local.

O provisionamento SQS define `MessageGroupId` como o agregado (ID da carteira para mudanças de carteira) e `MessageDeduplicationId` como o `eventId`. A fila de entrada manda mensagens para a DLQ após cinco recebimentos sem confirmação.

### Tokens de teste

Os tokens usam `client_credentials`. A configuração local coloca o `provider_id` no token de `provider-a`; a API deriva a identidade desse claim e não confia no `providerId` enviado pelo corpo.

```powershell
$providerToken = (docker compose --profile tools run --rm curl -fsS `
  -X POST http://keycloak:8080/realms/jungle/protocol/openid-connect/token `
  -u provider-a:provider-a-local-secret -d grant_type=client_credentials | ConvertFrom-Json).access_token

$internalToken = (docker compose --profile tools run --rm curl -fsS `
  -X POST http://keycloak:8080/realms/jungle/protocol/openid-connect/token `
  -u internal-service:internal-service-local-secret -d grant_type=client_credentials | ConvertFrom-Json).access_token
```

### Abrir carteira

Operações de carteira exigem token de um client listado em `INTERNAL_CLIENT_IDS`.

```powershell
$headers = @{ Authorization = "Bearer $internalToken" }
$wallet = Invoke-RestMethod -Method Post -Uri http://localhost:8080/wallets `
  -Headers $headers -ContentType application/json -Body (@{
    playerId = "player-001"
    initialBalance = @{ amount = "100.00"; currency = "BRL" }
  } | ConvertTo-Json -Depth 4)
$wallet
```

Saldo inicial positivo cria `OPENING`, crédito no ledger e os dois eventos financeiros na mesma transação. Saldo inicial zero cria apenas a carteira. A unicidade de `(playerId, currency)` retorna conflito.

### Enviar uma aposta

```powershell
$headers = @{
  Authorization = "Bearer $providerToken"
  "Idempotency-Key" = "provider-a:transaction-001"
}
$body = @{
  providerId = "provider-a"
  externalTransactionId = "transaction-001"
  playerId = "player-001"
  walletId = $wallet.id
  roundId = "round-001"
  gameId = "fortune-chimp"
  kind = "BET"
  money = @{ amount = "25.00"; currency = "BRL" }
} | ConvertTo-Json -Depth 4
Invoke-RestMethod -Method Post -Uri http://localhost:8080/wagering/transactions `
  -Headers $headers -ContentType application/json -Body $body
```

Reenvie a mesma chave e o mesmo conteúdo para obter o resultado original com `idempotentReplay: true`. A mesma chave com payload diferente, ou a mesma transação externa com outra chave, resulta em `409 IDEMPOTENCY_CONFLICT`.

Um `WIN` pode incluir `referenceExternalTransactionId`. Quando informado, deve apontar para uma `BET` processada do mesmo provedor, jogador, carteira, moeda e rodada. Se a aposta ainda não chegou, o ganho fica em `PENDING_REFERENCE` e o worker o processa depois que a referência estiver disponível. `REFUND` e `ROLLBACK` também exigem uma referência processada no mesmo escopo; reembolsos revertem apostas, e rollbacks revertem `BET`, `WIN` ou `REFUND`.

### Rotas

| Rota | Acesso | Uso |
| --- | --- | --- |
| `GET /health/live` | público | Liveness |
| `GET /health/ready` | público | PostgreSQL e SQS disponíveis |
| `GET /metrics` | público | Métricas Prometheus |
| `POST /wallets` | client interno | Abrir carteira |
| `GET /wallets/{walletId}` | client interno | Consultar carteira |
| `GET /wallets/{walletId}/ledger?cursor=...&limit=50` | client interno | Ledger paginado |
| `POST /wallets/{walletId}/reconciliation` | client interno | Recalcular saldo a partir do ledger |
| `POST /wagering/transactions` | provedor autenticado | Processar `BET`, `WIN`, `LOSS`, `REFUND` ou `ROLLBACK` |
| `GET /wagering/transactions/{transactionId}` | provedor dono | Consultar pelo ID interno |
| `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}` | provedor dono | Consultar pela transação externa |

Erros são JSON `{ "code": "...", "message": "..." }`. A API usa `400` para entrada inválida, `401/403` para autenticação/autorização, `404` para recurso ausente, `409` para conflito, `422` para rejeição de negócio, `202` para referência pendente e `503` para falha transitória de infraestrutura.

## Migrations

O serviço `migrate` aplica, em ordem, todas as migrations `.up.sql` ainda pendentes. `down` reverte somente a última migration aplicada; execute novamente para voltar mais versões:

```powershell
docker compose run --rm --entrypoint /usr/local/bin/migrate migrate up
docker compose run --rm --entrypoint /usr/local/bin/migrate migrate down
```

Cada migration só é registrada ou removida do histórico depois do commit de todo o script. O rollback da versão inicial remove o schema deste desafio; faça backup antes de reverter qualquer banco com dados que devam ser preservados.

## Testes e verificações

```powershell
go test ./...
go test -race ./...
go vet ./...
```

Os testes unitários de domínio não precisam de serviços externos. Os testes de integração criam e removem schemas isolados em um PostgreSQL real; para executá-los, suba o Compose e defina:

```powershell
$env:TEST_DATABASE_URL = "postgres://postgres:postgres@localhost:5432/jungle_gaming?sslmode=disable"
go test -race ./internal/usecase
Remove-Item Env:TEST_DATABASE_URL
```

No Windows sem CGO/GCC, execute a suíte completa com race detector em um container Linux. O comando abaixo também ativa os testes reais de PostgreSQL e LocalStack, conectando o container aos serviços publicados no host:

```powershell
$databaseUrl = ((docker compose config --format json | ConvertFrom-Json).services.app.environment.DATABASE_URL) `
  -replace "@postgres:", "@host.docker.internal:"
docker run --rm -v "${PWD}:/src" -w /src `
  -e "TEST_DATABASE_URL=$databaseUrl" `
  -e "TEST_SQS_ENDPOINT_URL=http://host.docker.internal:4566" `
  golang:1.27.2-alpine sh -c "apk add --no-cache build-base >/dev/null && go test -race ./..."
```

Os testes PostgreSQL validam a disputa de duas apostas de `80.00` sobre `100.00`, replay paralelo, conflito de chave, um único débito no ledger, concorrência em carteiras distintas, `WIN` com referência (imediata e pendente), `WIN` seguido de `REFUND` da aposta referenciada, rejeição de alterações diretas de saldo sem lançamento no ledger, reversões, inbox, estado `FAILED`, abertura única por carteira, imutabilidade e ledger, disputa entre publishers, recuperação de lease/retry e round-trip das migrations.

Com PostgreSQL e LocalStack reais em execução, os testes do worker injetam falha de ACK depois do commit e validam redelivery sem débito duplicado, redrive de mensagem inválida à DLQ e interrupção após `SendMessage` antes de `MarkPublished`, incluindo takeover do lease e preservação do `eventId`:

```powershell
$env:TEST_DATABASE_URL = "******localhost:5432/jungle_gaming?sslmode=disable"
$env:TEST_SQS_ENDPOINT_URL = "http://localhost:4566"
go test ./internal/delivery/worker -run 'Test(SQSCommitBeforeAckRedelivery|SQSRedriveMovesPoisonMessageToDLQ|OutboxRecoversAfterSendBeforeMarkPublished)$' -count=1 -v
Remove-Item Env:TEST_DATABASE_URL
Remove-Item Env:TEST_SQS_ENDPOINT_URL
```

Para provar coordenação com **três processos API independentes**, Keycloak e serviços PostgreSQL/SQS reais, execute no Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify-three-processes.ps1
```

O script cria um banco temporário, aplica as migrations, inicia três containers API em portas `18082` a `18084`, verifica chamadas sem permissão, disputa três apostas de `80.00` simultaneamente — uma por processo —, faz replay numa instância diferente, confere saldo/ledger e testa encerramento gracioso. Também processa uma aposta por HTTP e reenvia a mesma operação pelo SQS; consulta o PostgreSQL para confirmar saldo `75.00`, uma transação, um débito no ledger e uma mensagem concluída na inbox. Ao terminar, remove os containers e o banco temporários; os serviços base do Compose permanecem ativos. Use `-BasePort` para escolher outra porta inicial.

Para validar isolamento entre provedores com PostgreSQL e Keycloak reais, incluindo o mesmo ID externo e chave idempotente nos dois provedores, consultas cruzadas e tentativa de falsificar o `providerId`:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify-provider-isolation.ps1
```

O script cria um client OIDC temporário com identidade `provider-b` a partir do modelo local em `deploy/keycloak/jungle-realm.json`, remove esse client e remove o banco, as filas e o container temporários ao terminar.

Para verificar a aceitação de token válido e a rejeição de token inválido e token OIDC assinado já expirado pelo Keycloak:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify-oidc-token-rejection.ps1
```

O teste cria um realm temporário com access token de três segundos, executa a API contra esse issuer, confirma que o token funciona antes da expiração e espera `401` para token adulterado e token expirado. O realm, o banco, as filas e o container temporários são removidos ao final.

Os testes de integração do worker injetam falha nos limites commit/ACK e `SendMessage`/`MarkPublished`; também exercitam redrive real à DLQ e takeover de lease. Eles verificam que a inbox impede débito duplicado e que a outbox recuperada preserva o mesmo `eventId`. A execução com `-race` deve incluir PostgreSQL e LocalStack usando o comando Linux acima.

## Configuração

Use `.env.example` como referência para portas, credenciais demonstrativas locais e região AWS. O Compose define `DATABASE_URL`, `WAGER_QUEUE_URL`, `OUTBOX_QUEUE_URL`, `OIDC_ISSUER_URL`, `OIDC_AUDIENCE` e `INTERNAL_CLIENT_IDS` para o ambiente local; fora dele, configure essas variáveis explicitamente. O processo falha na inicialização se OIDC ou os recursos obrigatórios não estiverem configurados.

Para enviar uma mensagem de teste, publique um envelope `WagerTransactionRequested` na fila FIFO de entrada, incluindo um `messageId` estável, `idempotencyKey` e o mesmo corpo de negócio aceito pela API. O consumidor só remove a mensagem depois do commit do caso de uso; falhas transitórias recebem nova visibilidade com backoff e, após o limite configurado, são redirecionadas à DLQ.
