package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/domain"
)

type LedgerRepository struct {
	db *DB
}

func (r *LedgerRepository) FindByTransactionTx(ctx context.Context, tx pgx.Tx, transactionID string) (domain.LedgerEntry, error) {
	const query = `
		SELECT id, wallet_id, transaction_id, amount, currency, balance_before, balance_after, created_at
		FROM ledger_entries
		WHERE transaction_id = $1
	`
	var (
		id, walletID, txID, currency string
		amount, before, after        int64
		createdAt                    time.Time
	)
	if err := tx.QueryRow(ctx, query, transactionID).Scan(&id, &walletID, &txID, &amount, &currency, &before, &after, &createdAt); err != nil {
		return domain.LedgerEntry{}, fmt.Errorf("buscar lançamento por transação: %w", err)
	}
	return domain.RehydrateLedgerEntry(id, walletID, txID, domain.NewMoneyFromInt64(amount, currency),
		domain.NewMoneyFromInt64(before, currency), domain.NewMoneyFromInt64(after, currency), createdAt), nil
}

func NewLedgerRepository(db *DB) *LedgerRepository {
	return &LedgerRepository{db: db}
}

func (r *LedgerRepository) ListByWallet(ctx context.Context, walletID string, beforeTime *time.Time, beforeID string, limit int) ([]domain.LedgerEntry, error) {
	const query = `
		SELECT id, wallet_id, transaction_id, amount, currency, balance_before, balance_after, created_at
		FROM ledger_entries
		WHERE wallet_id = $1 AND ($2::timestamptz IS NULL OR (created_at, id) < ($2, $3::uuid))
		ORDER BY created_at DESC, id DESC
		LIMIT $4
	`
	rows, err := r.db.Pool.Query(ctx, query, walletID, beforeTime, nullableString(beforeID), limit)
	if err != nil {
		return nil, fmt.Errorf("listar ledger da carteira: %w", err)
	}
	defer rows.Close()
	var entries []domain.LedgerEntry
	for rows.Next() {
		var id, wallet, transaction, currency string
		var amount, before, after int64
		var created time.Time
		if err := rows.Scan(&id, &wallet, &transaction, &amount, &currency, &before, &after, &created); err != nil {
			return nil, fmt.Errorf("ler entrada do ledger: %w", err)
		}
		entries = append(entries, domain.RehydrateLedgerEntry(id, wallet, transaction,
			domain.NewMoneyFromInt64(amount, currency), domain.NewMoneyFromInt64(before, currency),
			domain.NewMoneyFromInt64(after, currency), created))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterar entradas do ledger: %w", err)
	}
	return entries, nil
}

func (r *LedgerRepository) Save(ctx context.Context, tx pgx.Tx, entry domain.LedgerEntry) error {
	query := `
		INSERT INTO ledger_entries (id, wallet_id, currency, transaction_id, amount, balance_before, balance_after, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := tx.Exec(ctx, query,
		entry.ID(),
		entry.WalletID(),
		entry.Amount().Currency(),
		entry.TransactionID(),
		entry.Amount().Amount(),
		entry.BalanceBefore().Amount(),
		entry.BalanceAfter().Amount(),
		entry.CreatedAt(),
	)
	if err != nil {
		return fmt.Errorf("falha ao salvar entrada no ledger: %w", err)
	}
	return nil
}
