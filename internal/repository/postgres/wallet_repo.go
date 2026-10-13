package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/domain"
)

type WalletRepository struct {
	db *DB
}

var ErrWalletNotFound = errors.New("carteira não encontrada")
var ErrWalletVersionConflict = errors.New("wallet version conflict")

func NewWalletRepository(db *DB) *WalletRepository {
	return &WalletRepository{db: db}
}

func (r *WalletRepository) Create(ctx context.Context, tx pgx.Tx, wallet domain.Wallet) error {
	query := `
		INSERT INTO wallets (id, player_id, balance, currency, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err := tx.Exec(ctx, query,
		wallet.ID(),
		wallet.PlayerID(),
		wallet.Balance().Amount(),
		wallet.Balance().Currency(),
		wallet.Version(),
		wallet.CreatedAt(),
		wallet.UpdatedAt(),
	)
	if err != nil {
		return fmt.Errorf("falha ao salvar carteira: %w", err)
	}
	return nil
}

func (r *WalletRepository) Save(ctx context.Context, tx pgx.Tx, wallet domain.Wallet) error {
	tag, err := tx.Exec(ctx, `
		UPDATE wallets
		SET balance = $3, version = $5, updated_at = $6
		WHERE id = $1 AND player_id = $2 AND currency = $4 AND version = $5 - 1
	`, wallet.ID(), wallet.PlayerID(), wallet.Balance().Amount(), wallet.Balance().Currency(),
		wallet.Version(), wallet.UpdatedAt())
	if err != nil {
		return fmt.Errorf("falha ao atualizar carteira: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrWalletVersionConflict
	}
	return nil
}

func (r *WalletRepository) FindByID(ctx context.Context, tx pgx.Tx, id string) (domain.Wallet, error) {
	query := `
		SELECT id, player_id, balance, currency, version, created_at, updated_at
		FROM wallets
		WHERE id = $1
	`
	var (
		walletID, playerID, currency string
		balanceAmt                   int64
		version                      int
		createdAt, updatedAt         time.Time
	)

	var err error
	if tx != nil {
		err = tx.QueryRow(ctx, query, id).Scan(&walletID, &playerID, &balanceAmt, &currency, &version, &createdAt, &updatedAt)
	} else {
		err = r.db.Pool.QueryRow(ctx, query, id).Scan(&walletID, &playerID, &balanceAmt, &currency, &version, &createdAt, &updatedAt)
	}

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Wallet{}, ErrWalletNotFound
		}
		return domain.Wallet{}, fmt.Errorf("falha ao buscar carteira: %w", err)
	}

	money := domain.NewMoneyFromInt64(balanceAmt, currency)
	return domain.RehydrateWallet(walletID, playerID, money, version, createdAt, updatedAt), nil
}

func (r *WalletRepository) FindByIDWithLock(ctx context.Context, tx pgx.Tx, id string) (domain.Wallet, error) {
	query := `
		SELECT id, player_id, balance, currency, version, created_at, updated_at
		FROM wallets
		WHERE id = $1
		FOR UPDATE
	`
	var (
		walletID, playerID, currency string
		balanceAmt                   int64
		version                      int
		createdAt, updatedAt         time.Time
	)

	err := tx.QueryRow(ctx, query, id).Scan(&walletID, &playerID, &balanceAmt, &currency, &version, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Wallet{}, ErrWalletNotFound
		}
		return domain.Wallet{}, fmt.Errorf("falha ao buscar carteira com lock: %w", err)
	}

	money := domain.NewMoneyFromInt64(balanceAmt, currency)
	return domain.RehydrateWallet(walletID, playerID, money, version, createdAt, updatedAt), nil
}
