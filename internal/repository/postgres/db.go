package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Tx define o alias para a transação do pgx, usado nos repositórios e use cases
type Tx pgx.Tx

type DB struct {
	Pool *pgxpool.Pool
}

func NewDB(ctx context.Context, connString string) (*DB, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("falha ao criar pool de conexões do postgres: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("falha ao testar conexão com o postgres: %w", err)
	}

	return &DB{Pool: pool}, nil
}

func (db *DB) Close() {
	db.Pool.Close()
}

type TxManager struct {
	db *DB
}

func NewTxManager(db *DB) *TxManager {
	return &TxManager{db: db}
}

func (tm *TxManager) RunInTx(ctx context.Context, fn func(tx Tx) error) error {
	pgxTx, err := tm.db.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("falha ao iniciar transação: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = pgxTx.Rollback(rollbackCtx)
			panic(p)
		}
	}()

	if err := fn(pgxTx); err != nil {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rbErr := pgxTx.Rollback(rollbackCtx)
		cancel()
		if rbErr != nil {
			return fmt.Errorf("falha ao fazer rollback (erro original: %v): %w", err, rbErr)
		}
		return err
	}

	if err := pgxTx.Commit(ctx); err != nil {
		return fmt.Errorf("falha ao fazer commit da transação: %w", err)
	}

	return nil
}
