# Decisões de arquitetura e limites conhecidos

## Limites e composição

O domínio não importa Fx, HTTP, SQS ou pgx. `domain` contém `Money`, carteira, transação, ledger e estruturas tipadas dos eventos. `usecase` coordena regras e transações; `repository/postgres` contém SQL; `delivery/http` e `delivery/worker` adaptam os contratos HTTP e SQS. Uber Fx monta as dependências por construtores e conecta servidor e workers a `fx.Lifecycle`.

`ProvideDB` e a descoberta OIDC têm timeout de inicialização. HTTP usa `net.Listen` dentro de `OnStart`, para falha de bind impedir uma inicialização aparentemente saudável. No encerramento, Fx chama os hooks na ordem inversa: o servidor interrompe novas entradas, workers cancelam long polling e aguardam sua goroutine, e o pool PostgreSQL fecha depois. Mensagens SQS em processamento cuja transação foi cancelada têm a visibilidade liberada; se a confirmação SQS falhar depois do commit, inbox e idempotência permitem a reentrega.

## Dinheiro e carteira

`Money` guarda `int64` em centavos. O parsing usa formato decimal com exatamente duas casas, valida moeda ISO 4217, rejeita sinal negativo na entrada externa, expoente, escala excedente e overflow. Não há conversão monetária para float. A serialização JSON usa sempre strings de duas casas. Valores negativos só aparecem em diferenças internas e lançamentos assinados do ledger.

O limite representável é `int64` centavos: `92,233,720,368,547,758.07` unidades monetárias. Soma, subtração e negação verificam overflow. Moedas distintas não podem ser comparadas ou somadas. A escala fixa de duas casas significa que moedas ISO sem duas casas decimais não são aceitas neste contrato; o fluxo principal do desafio usa BRL.

A carteira começa na versão `1`; `Debit`/`Credit` rejeitam valor zero, valor negativo, moeda incompatível e saldo insuficiente. Operações sem mudança, como `LOSS`, não incrementam a versão. A criação de saldo inicial positivo registra a abertura na versão `1`.

## Banco, transação e concorrência

Uma operação processada lê a carteira com `SELECT ... FOR UPDATE`, calcula a transição no agregado e, na mesma transação pgx, grava carteira, wager, ledger e outbox. A inbox SQS participa da mesma transação de entrada. Um update adicional exige `version = versão_nova - 1`, além da constraint `balance >= 0`; assim, o banco também impede lost update se algum escritor deixar de adquirir o lock.

Cada carteira é bloqueada independentemente; não existe mutex ou lock global na aplicação. IDs e constraints do banco coordenam processos com pools separados. A disputa de duas apostas de `80.00` sobre `100.00` deve resultar em uma aposta processada, uma rejeitada (`INSUFFICIENT_FUNDS`), saldo `20.00` e um débito.

`ledger_entries.amount` é assinado: débito negativo, crédito positivo. O schema exige moeda igual à da carteira, unicidade de `transaction_id`, não negatividade dos saldos e `balance_after = balance_before + amount`. Triggers recusam `UPDATE`, `DELETE` e `TRUNCATE`; um constraint trigger diferido também exige, no commit, um lançamento processado correspondente a cada abertura positiva ou alteração de saldo, impedindo atualizar `wallets.balance` diretamente sem ledger na mesma transação. Correções são novos lançamentos, nunca edição do histórico. Em produção, configure o role da aplicação sem privilégios de proprietário/superusuário e conceda somente `SELECT`/`INSERT` no ledger; os triggers são proteção adicional.

`POST /wallets/{id}/reconciliation` usa snapshot PostgreSQL repeatable-read/read-only, soma o ledger desde zero (incluindo `OPENING`), compara ao saldo e retorna a diferença sem reparar nem modificar dados. Divergência gera log JSON e métrica.

## Idempotência, inbox e estados

O hash é SHA-256 de JSON determinístico de struct Go (ordem estável), contendo `providerId`, `externalTransactionId`, `playerId`, `walletId`, `roundId`, `gameId`, `kind`, amount normalizado, currency e referência de reversão. `Idempotency-Key`, timestamps, IDs SQS e demais dados de transporte são excluídos. Entradas externas exigem duas casas, então não há arredondamento. Zeros à esquerda no inteiro são aceitos e normalizados pelo valor em centavos antes do hash.

O banco garante unicidade de `(provider_id, idempotency_key)` e `(provider_id, external_id)`. Reuso com conteúdo ou chave divergente é `409`; replay equivalente recupera transação e `result_balance` persistidos. O saldo devolvido é o observado no processamento original, não um saldo mais recente.

Para SQS, `(consumerName, messageId)` do envelope é a identidade inbox durável e SHA-256 do corpo bruto detecta conteúdo divergente no mesmo identificador. Recebimento, conclusão e operação financeira confirmam juntas para mensagens novas; um registro incompleto não fica visível após rollback. Em replay HTTP/SQS de operação já persistida, a inbox é registrada e concluída em uma transação curta antes do ACK.

Estados: `PENDING`, `PENDING_REFERENCE`, `PROCESSED`, `REJECTED` e `FAILED`. `PENDING` não é confirmado isoladamente: operações sem dependência são concluídas na mesma transação. `FAILED` registra uma falha de infraestrutura permanente após cinco recebimentos SQS quando o PostgreSQL volta a aceitar gravações; o registro e a conclusão da inbox são atômicos e não criam lançamento financeiro. Se o registro da falha também não puder ser persistido, a mensagem permanece sem ACK e o redrive a envia à DLQ. Rejeições financeiras são persistidas como `REJECTED`, têm `failureCode` estável e não criam débito/crédito no ledger. Códigos principais: `INSUFFICIENT_FUNDS`, `REVERSAL_INSUFFICIENT_FUNDS`, `WALLET_BALANCE_OVERFLOW`, `REFERENCE_NOT_FOUND`, `REFERENCE_NOT_PROCESSED`, `REFERENCE_SCOPE_MISMATCH`, `REFERENCE_AMOUNT_MISMATCH`, `REFERENCE_HAS_NO_FINANCIAL_EFFECT` e `REFERENCE_ALREADY_REVERSED`; falhas de infraestrutura esgotadas usam `INFRASTRUCTURE_RETRIES_EXHAUSTED`.

## Referências e reversões

`WIN` pode opcionalmente referenciar uma `BET` processada da mesma rodada, carteira, jogador e moeda; uma referência ausente ou ainda pendente deixa o ganho em `PENDING_REFERENCE` para resolução durável. `REFUND` exige referência processada de `BET`; `ROLLBACK` inverte o efeito de `BET`, `WIN` ou `REFUND`. Referências de reversões precisam pertencer à mesma rodada, carteira, jogador e moeda; valor e efeito financeiro também precisam corresponder. Uma segunda reversão processada para a mesma origem é impedida por índice único parcial. Se a referência estiver ausente ou pendente, persiste-se `PENDING_REFERENCE` e publica-se `WagerTransactionPendingReference`. O worker busca referências vencidas com coordenação transacional por carteira, aplica backoff exponencial de até cinco minutos e encerra após 10 tentativas (`REFERENCE_NOT_FOUND` se ausente; `REFERENCE_NOT_PROCESSED` se terminou rejeitada ou não concluiu). Pendências sobrevivem ao reinício porque horário e tentativas estão no banco.

Se uma reversão não puder debitar sem deixar saldo negativo, persiste-se `REJECTED` com `REVERSAL_INSUFFICIENT_FUNDS`, sem lançamento. `WIN` referenciado não conta como reversão e não impede um `REFUND` ou `ROLLBACK` posterior da aposta. O índice único parcial e a consulta de reversões consideram apenas `REFUND` e `ROLLBACK` processados. `REFUND` de algo que não seja `BET`, referência de outra carteira/jogador e operações sem efeito financeiro são rejeitadas e auditáveis.

## HTTP, identidade e autorização

Keycloak é o IdP OIDC local; a aplicação valida assinatura, issuer, expiração e audience de tokens Bearer via discovery/JWKS. O claim `provider_id` estabelece o provedor autorizado e é comparado com o path/body quando fornecido. Consultas por ID interno e externo limitam dados ao provedor autenticado, inclusive replays. Clients em `INTERNAL_CLIENT_IDS` têm acesso às operações de carteira, ledger e reconciliação; um token de provedor não pode chamar essas rotas. Cadastro de senhas e emissão própria de tokens não são implementados.

No Compose, `provider-a` e `internal-service` usam `client_credentials` e secrets demonstrativos. Em produção, importe/configure os clients no IdP real, inclua a audience `jungle-api` e o mapper de claim do provedor, restrinja os scopes e externalize os secrets. A mensageria requer credenciais e ACLs próprias no broker; LocalStack usa credenciais de teste e não representa política de produção.

## SQS e transactional outbox

A fila FIFO de entrada assume entrega at-least-once. Apenas um commit durável permite ACK; rejeições de negócio persistidas são terminais. Erros transitórios mantêm/reduzem visibilidade com backoff. No quinto recebimento, uma operação válida cujo erro de infraestrutura persista é registrada como `FAILED` se o banco estiver disponível; falhas de entrada não persistidas e indisponibilidade continuada seguem para a DLQ pelo redrive. `SIGTERM` cancela long polling, espera trabalho em andamento e libera mensagem não confirmada. HTTP e SQS compartilham `ProcessWager`.

Eventos são snapshots imutáveis criados dentro do commit financeiro. Um publisher separado usa `FOR UPDATE SKIP LOCKED`, lease de 30 segundos e retry exponencial até cinco minutos. Publica para `outbox-events.fifo` com `MessageGroupId=aggregateId` e `MessageDeduplicationId=eventId`; confirma a outbox somente após resposta de envio. Falha depois do envio mas antes da confirmação pode publicar de novo, preservando `eventId`; consumidores de eventos devem deduplicar por esse identificador.

Eventos definidos: `WagerTransactionProcessed`, `WagerTransactionRejected`, `WagerTransactionPendingReference` e `WalletBalanceChanged`. O envelope define tipo, versão, IDs de evento/agregado/correlação, timestamp UTC e payload tipado; payload monetário permanece string decimal.

## Observabilidade e verificação

Logs emitidos pela aplicação usam JSON e evitam registrar tokens e payloads financeiros. Métricas Prometheus incluem resultados por status, replay/conflito, retries/DLQ, erros e atraso de outbox, latência agregada e divergência de reconciliação. Liveness é independente; readiness confirma conexão com PostgreSQL e as duas filas SQS.

Testes unitários cobrem parsing/aritmética de `Money`, moeda, overflow, invariantes da carteira e transições. Os testes PostgreSQL reais são habilitados por `TEST_DATABASE_URL`: aplicam migrations em schemas isolados, verificam ciclo up/down repetido, unicidade de abertura, concorrência, replay 50x em pools independentes, `FAILED`, imutabilidade do ledger, inbox e regras de referência/reversão. Dois publishers com pools distintos disputam 20 eventos; leases abandonados e retries preservam `eventId`. `scripts/verify-three-processes.ps1` sobe três containers API com conexões próprias e exercita OIDC, disputa HTTP, replay entre processos, saldo/ledger e shutdown. Os testes de recuperação com PostgreSQL e LocalStack reais injetam falha após o commit financeiro e antes do ACK SQS e entre o envio do evento e a confirmação da outbox; validam redelivery sem débito duplicado, redrive à DLQ, retomada por outro publisher e preservação do `eventId`. ACLs de um broker de produção não são verificadas pelo ambiente local e devem ser configuradas e validadas na implantação.

## Limitações e trabalho não concluído

- O Compose usa Keycloak em modo de desenvolvimento, secrets fixos de exemplo, LocalStack e credenciais AWS de teste; isso não é configuração apropriada para produção.
- Integração com um IdP e um broker de produção, incluindo gestão de secrets, TLS, permissões/ACLs e alarmes operacionais, precisa ser configurada e validada no ambiente de implantação. Os testes locais não validam essas políticas de produção.
- O contrato monetário usa escala fixa de duas casas e, portanto, não atende moedas cuja escala legal seja diferente.
- Tracing distribuído e dashboards não estão implementados; são diferenciais opcionais do desafio.
