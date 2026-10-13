package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

var ErrInboxHashConflict = errors.New("message id reused with different content")

const WagerTransactionsConsumer = "wager-transactions"

type InboxRepository struct{}

func NewInboxRepository() *InboxRepository {
	return &InboxRepository{}
}

func (r *InboxRepository) Register(ctx context.Context, tx pgx.Tx, consumerName, messageID, payloadHash string) (bool, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO consumer_inbox (consumer_name, message_id, payload_hash)
		VALUES ($1, $2, $3)
		ON CONFLICT (consumer_name, message_id) DO NOTHING
	`, consumerName, messageID, payloadHash)
	if err != nil {
		return false, fmt.Errorf("registrar mensagem na inbox: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}
	var existingHash string
	if err := tx.QueryRow(ctx, `
		SELECT payload_hash FROM consumer_inbox
		WHERE consumer_name = $1 AND message_id = $2
	`, consumerName, messageID).Scan(&existingHash); err != nil {
		return false, fmt.Errorf("verificar mensagem da inbox: %w", err)
	}
	if existingHash != payloadHash {
		return false, ErrInboxHashConflict
	}
	return false, nil
}

func (r *InboxRepository) Complete(ctx context.Context, tx pgx.Tx, consumerName, messageID string) error {
	tag, err := tx.Exec(ctx, `
		UPDATE consumer_inbox
		SET completed_at = COALESCE(completed_at, clock_timestamp())
		WHERE consumer_name = $1 AND message_id = $2
	`, consumerName, messageID)
	if err != nil {
		return fmt.Errorf("concluir mensagem na inbox: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mensagem %s não encontrada na inbox do consumidor %s", messageID, consumerName)
	}
	return nil
}
