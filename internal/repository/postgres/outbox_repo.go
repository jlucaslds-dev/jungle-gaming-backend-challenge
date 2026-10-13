package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/domain"
)

type OutboxRepository struct {
	db *DB
}

func NewOutboxRepository(db *DB) *OutboxRepository {
	return &OutboxRepository{db: db}
}

func (r *OutboxRepository) Save(ctx context.Context, tx pgx.Tx, event domain.IntegrationEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("serializar evento de outbox: %w", err)
	}
	const query = `
		INSERT INTO outbox_events (event_id, event_type, aggregate_id, correlation_id, causation_id, version, payload)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err = tx.Exec(ctx, query, event.EventID, event.EventType, event.AggregateID, event.CorrelationID,
		nullableString(event.CausationID), event.Version, payload)
	if err != nil {
		return fmt.Errorf("salvar evento de outbox: %w", err)
	}
	return nil
}

type OutboxRecord struct {
	EventID   string
	EventType string
	Payload   []byte
	Attempts  int
	CreatedAt time.Time
}

func (r *OutboxRepository) Claim(ctx context.Context, limit int, lease time.Duration) ([]OutboxRecord, error) {
	tx, err := r.db.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("iniciar disputa da outbox: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		UPDATE outbox_events
		SET locked_until = now() + $2::interval, attempts = attempts + 1
		WHERE event_id IN (
			SELECT event_id FROM outbox_events
			WHERE published_at IS NULL AND next_attempt_at <= now()
				AND (locked_until IS NULL OR locked_until < now())
			ORDER BY created_at, event_id
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		RETURNING event_id, event_type, payload, attempts, created_at
	`, limit, lease.String())
	if err != nil {
		return nil, fmt.Errorf("reivindicar eventos da outbox: %w", err)
	}
	var records []OutboxRecord
	for rows.Next() {
		var record OutboxRecord
		if err := rows.Scan(&record.EventID, &record.EventType, &record.Payload, &record.Attempts, &record.CreatedAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("ler evento da outbox: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterar eventos da outbox: %w", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("confirmar disputa da outbox: %w", err)
	}
	return records, nil
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, eventID string) error {
	tag, err := r.db.Pool.Exec(ctx, `
		UPDATE outbox_events SET published_at = now(), locked_until = NULL, last_error = NULL
		WHERE event_id = $1 AND published_at IS NULL
	`, eventID)
	if err != nil {
		return fmt.Errorf("confirmar publicação da outbox: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("evento de outbox %s não está pendente", eventID)
	}
	return nil
}

func (r *OutboxRepository) Retry(ctx context.Context, eventID string, delay time.Duration, cause error) error {
	_, err := r.db.Pool.Exec(ctx, `
		UPDATE outbox_events
		SET locked_until = NULL, next_attempt_at = now() + $2::interval, last_error = $3
		WHERE event_id = $1 AND published_at IS NULL
	`, eventID, delay.String(), cause.Error())
	if err != nil {
		return fmt.Errorf("agendar retry da outbox: %w", err)
	}
	return nil
}

var _ pgx.Tx
