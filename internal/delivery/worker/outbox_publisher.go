package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/observability"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/repository/postgres"
	"go.uber.org/fx"
)

type OutboxPublisher struct {
	client *Client
	repo   *postgres.OutboxRepository
	cancel context.CancelFunc
	done   chan struct{}
}

func NewOutboxPublisher(lc fx.Lifecycle, client *Client, repo *postgres.OutboxRepository) *OutboxPublisher {
	publisher := &OutboxPublisher{client: client, repo: repo}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ctx, cancel := context.WithCancel(context.Background())
			publisher.cancel = cancel
			publisher.done = make(chan struct{})
			go publisher.run(ctx)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if publisher.cancel == nil {
				return nil
			}
			publisher.cancel()
			select {
			case <-publisher.done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	return publisher
}

func (p *OutboxPublisher) run(ctx context.Context) {
	defer close(p.done)
	for ctx.Err() == nil {
		records, err := p.repo.Claim(ctx, 10, 30*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("outbox claim failed", "error", err)
			waitContext(ctx, time.Second)
			continue
		}
		if len(records) == 0 {
			waitContext(ctx, time.Second)
			continue
		}
		for _, record := range records {
			if ctx.Err() != nil {
				return
			}
			p.publish(ctx, record)
		}
	}
}

func (p *OutboxPublisher) publish(ctx context.Context, record postgres.OutboxRecord) {
	observability.ObserveOutboxLag(time.Since(record.CreatedAt))
	var envelope struct {
		AggregateID string `json:"aggregateId"`
	}
	if err := json.Unmarshal(record.Payload, &envelope); err != nil {
		p.retry(ctx, record, err)
		return
	}
	_, err := p.client.API.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(p.client.OutboxQueueURL),
		MessageBody:            aws.String(string(record.Payload)),
		MessageGroupId:         aws.String(envelope.AggregateID),
		MessageDeduplicationId: aws.String(record.EventID),
	})
	if err != nil {
		p.retry(ctx, record, err)
		return
	}
	if err := p.repo.MarkPublished(ctx, record.EventID); err != nil {
		slog.Error("outbox publish confirmation failed; event will be retried", "eventId", record.EventID, "error", err)
	}
}

func (p *OutboxPublisher) retry(ctx context.Context, record postgres.OutboxRecord, cause error) {
	observability.RecordOutboxFailure()
	delaySeconds := 1 << min(record.Attempts, 8)
	delay := time.Duration(delaySeconds) * time.Second
	if delay > 5*time.Minute {
		delay = 5 * time.Minute
	}
	if err := p.repo.Retry(ctx, record.EventID, delay, cause); err != nil && ctx.Err() == nil {
		slog.Error("outbox retry scheduling failed", "eventId", record.EventID, "error", err)
	}
	slog.Error("outbox publish failed", "eventId", record.EventID, "error", cause)
}
