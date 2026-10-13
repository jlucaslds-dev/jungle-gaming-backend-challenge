package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/domain"
)

type TransactionRepository struct {
	db *DB
}

func NewTransactionRepository(db *DB) *TransactionRepository {
	return &TransactionRepository{db: db}
}

func pendingAttemptAt(t domain.WagerTransaction) any {
	if t.Status() == domain.StatusPendingReference {
		return time.Now().UTC().Add(2 * time.Second)
	}
	return nil
}

func (r *TransactionRepository) Save(ctx context.Context, tx pgx.Tx, t domain.WagerTransaction) error {
	const query = `
		INSERT INTO wager_transactions (
			id, wallet_id, currency, external_id, provider_id, idempotency_key,
			payload_hash, player_id, round_id, game_id, reference_external_id,
			amount, tx_type, status, failure_code, result_balance, created_at, updated_at, next_attempt_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
	`
	_, err := tx.Exec(ctx, query,
		t.ID(), t.WalletID(), t.Amount().Currency(), nullableString(t.ExternalID()), nullableString(t.ProviderID()),
		nullableString(t.IdempotencyKey()), nullableString(t.PayloadHash()), t.PlayerID(), nullableString(t.RoundID()), nullableString(t.GameID()),
		nullableString(t.ReferenceID()), t.Amount().Amount(), t.Type(), t.Status(),
		nullableString(t.FailureCode()), t.ResultBalance().Amount(), t.CreatedAt(), t.UpdatedAt(),
		pendingAttemptAt(t),
	)
	if err != nil {
		return fmt.Errorf("salvar transação: %w", err)
	}
	return nil
}

func (r *TransactionRepository) FindByProviderExternal(ctx context.Context, providerID, externalID string) (domain.WagerTransaction, error) {
	return scanTransaction(r.db.Pool.QueryRow(ctx, transactionSelect+`
		WHERE provider_id = $1 AND external_id = $2`, providerID, externalID))
}

func (r *TransactionRepository) FindByIdempotencyKey(ctx context.Context, providerID, key string) (domain.WagerTransaction, error) {
	return scanTransaction(r.db.Pool.QueryRow(ctx, transactionSelect+`
		WHERE provider_id = $1 AND idempotency_key = $2`, providerID, key))
}

func (r *TransactionRepository) FindByID(ctx context.Context, id string) (domain.WagerTransaction, error) {
	return scanTransaction(r.db.Pool.QueryRow(ctx, transactionSelect+`
		WHERE id = $1`, id))
}

func (r *TransactionRepository) FindByIDTx(ctx context.Context, tx pgx.Tx, id string, lock bool) (domain.WagerTransaction, error) {
	query := transactionSelect + ` WHERE id = $1`
	if lock {
		query += ` FOR UPDATE`
	}
	return scanTransaction(tx.QueryRow(ctx, query, id))
}

func (r *TransactionRepository) ListDuePendingReferenceIDs(ctx context.Context, limit int) ([]string, error) {
	rows, err := r.db.Pool.Query(ctx, `
		SELECT id FROM wager_transactions
		WHERE status = 'PENDING_REFERENCE' AND COALESCE(next_attempt_at, now()) <= now()
		ORDER BY next_attempt_at, created_at
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("listar referências pendentes: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("ler referência pendente: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterar referências pendentes: %w", err)
	}
	return ids, nil
}

func (r *TransactionRepository) Attempts(ctx context.Context, tx pgx.Tx, id string) (int, error) {
	var attempts int
	if err := tx.QueryRow(ctx, `SELECT attempts FROM wager_transactions WHERE id = $1`, id).Scan(&attempts); err != nil {
		return 0, fmt.Errorf("ler tentativas da transação: %w", err)
	}
	return attempts, nil
}

func (r *TransactionRepository) PendingScheduleTx(ctx context.Context, tx pgx.Tx, id string) (int, *time.Time, error) {
	var attempts int
	var next *time.Time
	if err := tx.QueryRow(ctx, `
		SELECT attempts, next_attempt_at
		FROM wager_transactions
		WHERE id = $1 AND status = 'PENDING_REFERENCE'
		FOR UPDATE
	`, id).Scan(&attempts, &next); err != nil {
		return 0, nil, fmt.Errorf("lock de retry de referência: %w", err)
	}
	return attempts, next, nil
}

func (r *TransactionRepository) UpdatePending(ctx context.Context, tx pgx.Tx, t domain.WagerTransaction, attempts int, nextAttempt *time.Time) error {
	tag, err := tx.Exec(ctx, `
		UPDATE wager_transactions
		SET status = $2, failure_code = $3, result_balance = $4, updated_at = now(),
			attempts = $5, next_attempt_at = $6
		WHERE id = $1 AND status = 'PENDING_REFERENCE'
	`, t.ID(), t.Status(), nullableString(t.FailureCode()), t.ResultBalance().Amount(),
		attempts, nextAttempt)
	if err != nil {
		return fmt.Errorf("atualizar transação pendente: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("transação %s não está mais pendente", t.ID())
	}
	return nil
}

func (r *TransactionRepository) FindByProviderExternalTx(ctx context.Context, tx pgx.Tx, providerID, externalID string) (domain.WagerTransaction, error) {
	return scanTransaction(tx.QueryRow(ctx, transactionSelect+`
		WHERE provider_id = $1 AND external_id = $2`, providerID, externalID))
}

func (r *TransactionRepository) FindByIdempotencyKeyTx(ctx context.Context, tx pgx.Tx, providerID, key string) (domain.WagerTransaction, error) {
	return scanTransaction(tx.QueryRow(ctx, transactionSelect+`
		WHERE provider_id = $1 AND idempotency_key = $2`, providerID, key))
}

func (r *TransactionRepository) HasProcessedReversalTx(ctx context.Context, tx pgx.Tx, providerID, referenceID string) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM wager_transactions
			WHERE provider_id = $1 AND reference_external_id = $2
				AND tx_type IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED'
		)
	`, providerID, referenceID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("verificar reversão anterior: %w", err)
	}
	return exists, nil
}

const transactionSelect = `
	SELECT id, wallet_id, COALESCE(external_id, ''), COALESCE(provider_id, ''), COALESCE(idempotency_key, ''),
		COALESCE(payload_hash, ''), player_id, COALESCE(round_id, ''), COALESCE(game_id, ''),
		COALESCE(reference_external_id, ''), amount, currency, tx_type, status,
		COALESCE(failure_code, ''), COALESCE(result_balance, 0), created_at, updated_at
	FROM wager_transactions`

type transactionRow interface {
	Scan(dest ...any) error
}

func scanTransaction(row transactionRow) (domain.WagerTransaction, error) {
	var (
		id, walletID, externalID, providerID, idempotencyKey, payloadHash string
		playerID, roundID, gameID, referenceID, failureCode               string
		amount, resultBalance                                             int64
		currency                                                          string
		txType                                                            domain.TransactionType
		status                                                            domain.TransactionStatus
		createdAt, updatedAt                                              time.Time
		err                                                               error
	)
	if err := row.Scan(
		&id, &walletID, &externalID, &providerID, &idempotencyKey, &payloadHash,
		&playerID, &roundID, &gameID, &referenceID, &amount, &currency,
		&txType, &status, &failureCode, &resultBalance, &createdAt, &updatedAt,
	); err != nil {
		return domain.WagerTransaction{}, fmt.Errorf("buscar transação: %w", err)
	}
	money := domain.NewMoneyFromInt64(amount, currency)
	var t domain.WagerTransaction
	if providerID != "" {
		t, err = domain.RehydrateExternalWagerTransaction(id, walletID, externalID, money, txType, status, domain.WagerMetadata{
			ProviderID: providerID, IdempotencyKey: idempotencyKey, PayloadHash: payloadHash,
			PlayerID: playerID, RoundID: roundID, GameID: gameID, ReferenceID: referenceID,
		}, failureCode, domain.NewMoneyFromInt64(resultBalance, currency), createdAt, updatedAt)
	} else {
		t = domain.RehydrateWagerTransaction(id, walletID, externalID, money, txType, status, createdAt, updatedAt)
		t.SetFailureCode(failureCode)
		t.SetResultBalance(domain.NewMoneyFromInt64(resultBalance, currency))
	}
	if err != nil {
		return domain.WagerTransaction{}, fmt.Errorf("rehidratar transação: %w", err)
	}
	return t, nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
