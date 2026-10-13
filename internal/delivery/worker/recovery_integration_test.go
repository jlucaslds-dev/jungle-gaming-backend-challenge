package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/domain"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/repository/postgres"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/usecase"
)

type recoveryTestRig struct {
	pool     *pgxpool.Pool
	admin    *pgxpool.Pool
	schema   string
	sqsAdmin *sqs.Client
	client   *Client
	walletUC *usecase.WalletUseCase
	wagerUC  *usecase.WageringUseCase
}

type failFirstDeleteAPI struct {
	SQSAPI
	failed atomic.Bool
}

func (api *failFirstDeleteAPI) DeleteMessage(
	ctx context.Context,
	input *sqs.DeleteMessageInput,
	options ...func(*sqs.Options),
) (*sqs.DeleteMessageOutput, error) {
	if api.failed.CompareAndSwap(false, true) {
		return nil, errors.New("injected failure after financial commit, before SQS acknowledgement")
	}
	return api.SQSAPI.DeleteMessage(ctx, input, options...)
}

type immediateVisibilityAPI struct {
	SQSAPI
}

func (api immediateVisibilityAPI) ChangeMessageVisibility(
	ctx context.Context,
	input *sqs.ChangeMessageVisibilityInput,
	options ...func(*sqs.Options),
) (*sqs.ChangeMessageVisibilityOutput, error) {
	copy := *input
	copy.VisibilityTimeout = 0
	return api.SQSAPI.ChangeMessageVisibility(ctx, &copy, options...)
}

func TestSQSCommitBeforeAckRedelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rig := newRecoveryTestRig(t, ctx)
	wallet, err := rig.walletUC.CreateWallet(ctx, "sqs-redelivery-player", domain.NewMoneyFromInt64(10000, "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	queueURL := createRecoveryQueue(t, ctx, rig.sqsAdmin, "commit-before-ack")
	defer deleteRecoveryQueue(t, ctx, rig.sqsAdmin, queueURL)

	externalID := "redelivery-" + uuid.NewString()
	messageID := "message-" + uuid.NewString()
	body, err := json.Marshal(map[string]any{
		"messageId": messageID,
		"type":      "WagerTransactionRequested",
		"data": map[string]any{
			"providerId": "provider-a", "externalTransactionId": externalID,
			"idempotencyKey": "provider-a:" + externalID,
			"playerId":       wallet.PlayerID(), "walletId": wallet.ID(),
			"roundId": "redelivery-round", "gameId": "integration",
			"kind": "BET", "money": map[string]string{"amount": "25.00", "currency": "BRL"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rig.client.API.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: aws.String(queueURL), MessageBody: aws.String(string(body)),
		MessageGroupId: aws.String(wallet.ID()), MessageDeduplicationId: aws.String(messageID),
	}); err != nil {
		t.Fatalf("send first delivery: %v", err)
	}

	api := &failFirstDeleteAPI{SQSAPI: rig.client.API}
	consumer := &WagerConsumer{
		client: &Client{API: api, WagerQueueURL: queueURL},
		uc:     rig.wagerUC,
	}
	first := receiveRecoveryMessage(t, ctx, rig.client.API, queueURL, 1)
	consumer.handle(ctx, first)
	if !api.failed.Load() {
		t.Fatal("failure injection did not interrupt the acknowledgement")
	}

	time.Sleep(1200 * time.Millisecond)
	second := receiveRecoveryMessage(t, ctx, rig.client.API, queueURL, 5)
	consumer.handle(ctx, second)

	var balance int64
	if err := rig.pool.QueryRow(ctx, `SELECT balance FROM wallets WHERE id = $1`, wallet.ID()).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != 7500 {
		t.Fatalf("wallet balance after redelivery = %d minor units, want 7500", balance)
	}
	var debitCount, inboxCount int
	if err := rig.pool.QueryRow(ctx, `
		SELECT count(*) FROM ledger_entries entry
		JOIN wager_transactions wager ON wager.id = entry.transaction_id
		WHERE wager.external_id = $1 AND entry.amount = -2500
	`, externalID).Scan(&debitCount); err != nil {
		t.Fatal(err)
	}
	if err := rig.pool.QueryRow(ctx, `
		SELECT count(*) FROM consumer_inbox
		WHERE consumer_name = $1 AND message_id = $2 AND completed_at IS NOT NULL
	`, postgres.WagerTransactionsConsumer, messageID).Scan(&inboxCount); err != nil {
		t.Fatal(err)
	}
	if debitCount != 1 || inboxCount != 1 {
		t.Fatalf("redelivery evidence: ledger debits=%d, completed inbox rows=%d, want 1/1",
			debitCount, inboxCount)
	}
}

func TestSQSRedriveMovesPoisonMessageToDLQ(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rig := newRecoveryTestRig(t, ctx)
	dlqURL := createRecoveryQueue(t, ctx, rig.sqsAdmin, "poison-dlq")
	defer deleteRecoveryQueue(t, ctx, rig.sqsAdmin, dlqURL)
	dlqAttributes, err := rig.client.API.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(dlqURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatal(err)
	}
	redrivePolicy, err := json.Marshal(map[string]string{
		"deadLetterTargetArn": dlqAttributes.Attributes[string(types.QueueAttributeNameQueueArn)],
		"maxReceiveCount":     "5",
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceURL := createRecoveryQueueWithAttributes(t, ctx, rig.sqsAdmin, "poison-source", map[string]string{
		string(types.QueueAttributeNameRedrivePolicy): string(redrivePolicy),
	})
	defer deleteRecoveryQueue(t, ctx, rig.sqsAdmin, sourceURL)

	if _, err := rig.client.API.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: aws.String(sourceURL), MessageBody: aws.String("{invalid-json"),
		MessageGroupId: aws.String("poison-test"), MessageDeduplicationId: aws.String(uuid.NewString()),
	}); err != nil {
		t.Fatalf("send poison message: %v", err)
	}
	consumer := &WagerConsumer{
		client: &Client{API: immediateVisibilityAPI{SQSAPI: rig.client.API}, WagerQueueURL: sourceURL},
		uc:     rig.wagerUC,
	}
	for attempt := 0; attempt < 10; attempt++ {
		output, err := rig.client.API.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(sourceURL), MaxNumberOfMessages: 1, WaitTimeSeconds: 1,
			VisibilityTimeout: 1,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
			},
		})
		if err != nil {
			t.Fatalf("receive poison message attempt %d: %v", attempt+1, err)
		}
		for _, message := range output.Messages {
			consumer.handle(ctx, message)
		}
		attributes, err := rig.client.API.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(dlqURL),
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameApproximateNumberOfMessages,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if attributes.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)] == "1" {
			deadLetter, err := rig.client.API.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl: aws.String(dlqURL), MaxNumberOfMessages: 1, WaitTimeSeconds: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(deadLetter.Messages) != 1 || aws.ToString(deadLetter.Messages[0].Body) != "{invalid-json" {
				t.Fatalf("DLQ did not preserve the poison message: %#v", deadLetter.Messages)
			}
			return
		}
	}
	t.Fatal("poison message did not reach the configured DLQ after redrive attempts")
}

func TestOutboxRecoversAfterSendBeforeMarkPublished(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rig := newRecoveryTestRig(t, ctx)
	queueURL := createRecoveryQueue(t, ctx, rig.sqsAdmin, "outbox-recovery")
	defer deleteRecoveryQueue(t, ctx, rig.sqsAdmin, queueURL)

	eventID, aggregateID := uuid.NewString(), uuid.NewString()
	payload, err := json.Marshal(map[string]any{
		"eventId": eventID, "eventType": "WagerTransactionProcessed",
		"aggregateId": aggregateID, "correlationId": "outbox-recovery", "version": 1,
		"data": map[string]any{"transactionId": uuid.NewString()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rig.pool.Exec(ctx, `
		INSERT INTO outbox_events (event_id, event_type, aggregate_id, correlation_id, version, payload)
		VALUES ($1, 'WagerTransactionProcessed', $2, 'outbox-recovery', 1, $3)
	`, eventID, aggregateID, payload); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.pool.Exec(ctx, `
		CREATE FUNCTION fail_first_outbox_confirmation() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF OLD.attempts = 1 AND NEW.published_at IS NOT NULL THEN
				RAISE EXCEPTION 'injected crash after SQS send and before MarkPublished';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER fail_first_outbox_confirmation
			BEFORE UPDATE ON outbox_events
			FOR EACH ROW EXECUTE FUNCTION fail_first_outbox_confirmation();
	`, pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("install one-shot outbox failure injection: %v", err)
	}

	repository := postgres.NewOutboxRepository(&postgres.DB{Pool: rig.pool})
	claimed, err := repository.Claim(ctx, 1, 30*time.Second)
	if err != nil || len(claimed) != 1 || claimed[0].EventID != eventID {
		t.Fatalf("first publisher claim = %#v, error %v", claimed, err)
	}
	publisher := &OutboxPublisher{
		client: &Client{API: rig.client.API, OutboxQueueURL: queueURL},
		repo:   repository,
	}
	publisher.publish(ctx, claimed[0])

	var publishedAt *time.Time
	var attempts int
	if err := rig.pool.QueryRow(ctx, `
		SELECT published_at, attempts FROM outbox_events WHERE event_id = $1
	`, eventID).Scan(&publishedAt, &attempts); err != nil {
		t.Fatal(err)
	}
	if publishedAt != nil || attempts != 1 {
		t.Fatalf("after injected lost confirmation publishedAt=%v attempts=%d, want nil/1", publishedAt, attempts)
	}
	if _, err := rig.pool.Exec(ctx, `UPDATE outbox_events SET locked_until = now() WHERE event_id = $1`, eventID); err != nil {
		t.Fatal(err)
	}
	recovered, err := repository.Claim(ctx, 1, 30*time.Second)
	if err != nil || len(recovered) != 1 || recovered[0].EventID != eventID || recovered[0].Attempts != 2 {
		t.Fatalf("recovery publisher claim = %#v, error %v", recovered, err)
	}
	publisher.publish(ctx, recovered[0])

	if err := rig.pool.QueryRow(ctx, `
		SELECT published_at, attempts FROM outbox_events WHERE event_id = $1
	`, eventID).Scan(&publishedAt, &attempts); err != nil {
		t.Fatal(err)
	}
	if publishedAt == nil || attempts != 2 {
		t.Fatalf("after recovery publishedAt=%v attempts=%d, want non-nil/2", publishedAt, attempts)
	}
	message, err := rig.client.API.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 1, WaitTimeSeconds: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(message.Messages) != 1 {
		t.Fatalf("published event was not available in SQS: %#v", message.Messages)
	}
	var received struct {
		EventID string `json:"eventId"`
	}
	if err := json.Unmarshal([]byte(aws.ToString(message.Messages[0].Body)), &received); err != nil {
		t.Fatal(err)
	}
	if received.EventID != eventID {
		t.Fatalf("recovered eventId=%q, want stable ID %q", received.EventID, eventID)
	}
}

func newRecoveryTestRig(t *testing.T, ctx context.Context) *recoveryTestRig {
	t.Helper()
	connectionString := os.Getenv("TEST_DATABASE_URL")
	endpoint := strings.TrimSpace(os.Getenv("TEST_SQS_ENDPOINT_URL"))
	if connectionString == "" || endpoint == "" {
		t.Skip("set TEST_DATABASE_URL and TEST_SQS_ENDPOINT_URL to run PostgreSQL/LocalStack recovery tests")
	}
	admin, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		t.Fatal(err)
	}
	schema := "worker_recovery_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	for _, name := range []string{
		"001_initial_schema", "002_reference_and_inbox",
		"003_failed_state_and_opening_once", "004_reversal_index_and_wallet_ledger_guard",
	} {
		migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", name+".up.sql"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(migration), pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}
	awsConfig, err := configAWS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sqsClient := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) {
		options.BaseEndpoint = aws.String(endpoint)
	})
	db := &postgres.DB{Pool: pool}
	walletRepo := postgres.NewWalletRepository(db)
	transactionRepo := postgres.NewTransactionRepository(db)
	ledgerRepo := postgres.NewLedgerRepository(db)
	outboxRepo := postgres.NewOutboxRepository(db)
	txManager := postgres.NewTxManager(db)
	walletUC := usecase.NewWalletUseCase(db, txManager, walletRepo, transactionRepo, ledgerRepo, outboxRepo)
	wagerUC := usecase.NewWageringUseCase(
		txManager, walletRepo, transactionRepo, ledgerRepo, outboxRepo, postgres.NewInboxRepository(),
	)
	return &recoveryTestRig{
		pool: pool, admin: admin, schema: schema,
		sqsAdmin: sqsClient,
		client:   &Client{API: sqsClient}, walletUC: walletUC, wagerUC: wagerUC,
	}
}

func configAWS(ctx context.Context) (aws.Config, error) {
	return config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
}

func createRecoveryQueue(t *testing.T, ctx context.Context, api *sqs.Client, suffix string) string {
	t.Helper()
	return createRecoveryQueueWithAttributes(t, ctx, api, suffix, nil)
}

func createRecoveryQueueWithAttributes(
	t *testing.T,
	ctx context.Context,
	api *sqs.Client,
	suffix string,
	extra map[string]string,
) string {
	t.Helper()
	attributes := map[string]string{
		string(types.QueueAttributeNameFifoQueue):                 "true",
		string(types.QueueAttributeNameContentBasedDeduplication): "false",
		string(types.QueueAttributeNameVisibilityTimeout):         "1",
	}
	for key, value := range extra {
		attributes[key] = value
	}
	name := "review-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16] + "-" + suffix + ".fifo"
	output, err := api.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(name), Attributes: attributes,
	})
	if err != nil {
		t.Fatalf("create SQS queue %s: %v", name, err)
	}
	return aws.ToString(output.QueueUrl)
}

func deleteRecoveryQueue(t *testing.T, ctx context.Context, api *sqs.Client, queueURL string) {
	t.Helper()
	if _, err := api.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(queueURL)}); err != nil {
		t.Errorf("delete temporary SQS queue: %v", err)
	}
}

func receiveRecoveryMessage(
	t *testing.T,
	ctx context.Context,
	api SQSAPI,
	queueURL string,
	visibility int32,
) types.Message {
	t.Helper()
	output, err := api.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 1,
		WaitTimeSeconds: 1, VisibilityTimeout: visibility,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
		},
	})
	if err != nil {
		t.Fatal(fmt.Errorf("receive SQS message: %w", err))
	}
	if len(output.Messages) != 1 {
		t.Fatalf("expected one SQS message, got %d", len(output.Messages))
	}
	return output.Messages[0]
}

var _ SQSAPI = (*failFirstDeleteAPI)(nil)
var _ SQSAPI = immediateVisibilityAPI{}
