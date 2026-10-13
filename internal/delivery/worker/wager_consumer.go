package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/domain"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/observability"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/repository/postgres"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/usecase"
	"go.uber.org/fx"
)

type WagerConsumer struct {
	client *Client
	uc     *usecase.WageringUseCase
	cancel context.CancelFunc
	done   chan struct{}
}

func NewWagerConsumer(lc fx.Lifecycle, client *Client, uc *usecase.WageringUseCase) *WagerConsumer {
	consumer := &WagerConsumer{client: client, uc: uc}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ctx, cancel := context.WithCancel(context.Background())
			consumer.cancel = cancel
			consumer.done = make(chan struct{})
			go consumer.run(ctx)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if consumer.cancel == nil {
				return nil
			}
			consumer.cancel()
			select {
			case <-consumer.done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	return consumer
}

type wagerEnvelope struct {
	MessageID string           `json:"messageId"`
	Type      string           `json:"type"`
	Data      wagerMessageData `json:"data"`
}

type wagerMessageData struct {
	ProviderID     string       `json:"providerId"`
	ExternalID     string       `json:"externalTransactionId"`
	IdempotencyKey string       `json:"idempotencyKey"`
	PlayerID       string       `json:"playerId"`
	WalletID       string       `json:"walletId"`
	RoundID        string       `json:"roundId"`
	GameID         string       `json:"gameId"`
	Type           string       `json:"kind"`
	Money          domain.Money `json:"money"`
	ReferenceID    string       `json:"referenceExternalTransactionId,omitempty"`
}

func (c *WagerConsumer) run(ctx context.Context) {
	defer close(c.done)
	for ctx.Err() == nil {
		output, err := c.client.API.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(c.client.WagerQueueURL), MaxNumberOfMessages: 10,
			WaitTimeSeconds: 20, VisibilityTimeout: 60,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("SQS receive failed", "error", err)
			waitContext(ctx, time.Second)
			continue
		}
		for _, message := range output.Messages {
			if ctx.Err() != nil {
				c.release(message)
				return
			}
			c.handle(ctx, message)
		}
	}
}

func (c *WagerConsumer) handle(ctx context.Context, message types.Message) {
	if ctx.Err() != nil {
		c.release(message)
		return
	}
	var envelope wagerEnvelope
	if err := json.Unmarshal([]byte(aws.ToString(message.Body)), &envelope); err != nil ||
		envelope.Type != "WagerTransactionRequested" || envelope.MessageID == "" {
		slog.Warn("invalid SQS wager message; allowing redrive", "sqsMessageId", aws.ToString(message.MessageId))
		c.retry(ctx, message)
		return
	}
	body := []byte(aws.ToString(message.Body))
	payloadHash := sha256.Sum256(body)
	input := usecase.ProcessWagerInput{
		ProviderID: envelope.Data.ProviderID, IdempotencyKey: envelope.Data.IdempotencyKey,
		PlayerID: envelope.Data.PlayerID, WalletID: envelope.Data.WalletID,
		ExternalID: envelope.Data.ExternalID, RoundID: envelope.Data.RoundID, GameID: envelope.Data.GameID,
		Amount: envelope.Data.Money.String(), Currency: envelope.Data.Money.Currency(),
		Type: domain.TransactionType(envelope.Data.Type), ReferenceID: envelope.Data.ReferenceID,
		MessageID: envelope.MessageID, MessagePayloadHash: hex.EncodeToString(payloadHash[:]),
	}
	started := time.Now()
	_, err := c.uc.ProcessWager(ctx, input)
	if err != nil {
		if ctx.Err() != nil {
			c.release(message)
			return
		}
		if errors.Is(err, usecase.ErrIdempotencyConflict) {
			slog.Warn("terminal SQS idempotency conflict", "messageId", envelope.MessageID)
			c.delete(ctx, message)
			return
		}
		receiveCount, _ := strconv.Atoi(message.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
		if receiveCount >= 5 && !isTerminalWagerError(err) {
			if failErr := c.uc.RecordPermanentFailure(ctx, input, "INFRASTRUCTURE_RETRIES_EXHAUSTED"); failErr == nil {
				observability.RecordWager(string(domain.StatusFailed), time.Since(started))
				slog.Error("SQS operation permanently failed after retries", "messageId", envelope.MessageID,
					"failureCode", "INFRASTRUCTURE_RETRIES_EXHAUSTED")
				c.delete(ctx, message)
				return
			} else {
				slog.Error("could not persist terminal SQS failure; allowing redrive", "messageId", envelope.MessageID, "error", failErr)
			}
		}
		slog.Error("SQS wager processing failed", "messageId", envelope.MessageID, "error", err)
		c.retry(ctx, message)
		return
	}
	c.delete(ctx, message)
}

func isTerminalWagerError(err error) bool {
	return errors.Is(err, usecase.ErrInvalidWager) ||
		errors.Is(err, domain.ErrInvalidMoney) ||
		errors.Is(err, postgres.ErrWalletNotFound) ||
		errors.Is(err, postgres.ErrInboxHashConflict)
}

func (c *WagerConsumer) delete(ctx context.Context, message types.Message) {
	ackCtx := ctx
	cancel := func() {}
	if ctx.Err() != nil {
		ackCtx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	}
	defer cancel()
	if _, err := c.client.API.DeleteMessage(ackCtx, &sqs.DeleteMessageInput{
		QueueUrl: aws.String(c.client.WagerQueueURL), ReceiptHandle: message.ReceiptHandle,
	}); err != nil {
		slog.Error("SQS delete failed; message will be redelivered", "sqsMessageId", aws.ToString(message.MessageId), "error", err)
	}
}

func (c *WagerConsumer) retry(ctx context.Context, message types.Message) {
	if ctx.Err() != nil {
		c.release(message)
		return
	}
	receiveCount, _ := strconv.Atoi(message.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	observability.RecordSQSRetry()
	if receiveCount >= 5 {
		observability.RecordSQSDeadLetter()
	}
	delay := 1 << min(receiveCount, 6)
	if delay > 120 {
		delay = 120
	}
	if _, err := c.client.API.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.client.WagerQueueURL), ReceiptHandle: message.ReceiptHandle,
		VisibilityTimeout: int32(delay),
	}); err != nil && ctx.Err() == nil {
		slog.Error("SQS visibility update failed", "sqsMessageId", aws.ToString(message.MessageId), "error", err)
	}
}

func (c *WagerConsumer) release(message types.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.client.API.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.client.WagerQueueURL), ReceiptHandle: message.ReceiptHandle,
		VisibilityTimeout: 0,
	}); err != nil {
		slog.Error("SQS shutdown release failed; message will become visible after visibility timeout",
			"sqsMessageId", aws.ToString(message.MessageId), "error", err)
	}
}

func waitContext(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
