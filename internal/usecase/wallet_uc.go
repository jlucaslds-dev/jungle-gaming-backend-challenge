package usecase

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/domain"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/observability"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/repository/postgres"
)

type WalletUseCase struct {
	db         *postgres.DB
	txManager  *postgres.TxManager
	walletRepo *postgres.WalletRepository
	transRepo  *postgres.TransactionRepository
	ledgerRepo *postgres.LedgerRepository
	outboxRepo *postgres.OutboxRepository
}

type ReconciliationResult struct {
	WalletID          string
	StoredBalance     domain.Money
	CalculatedBalance domain.Money
	Difference        domain.Money
	Consistent        bool
	CheckedEntries    int64
}

func (uc *WalletUseCase) GetWallet(ctx context.Context, walletID string) (domain.Wallet, error) {
	return uc.walletRepo.FindByID(ctx, nil, walletID)
}

func (uc *WalletUseCase) ListLedger(ctx context.Context, walletID string, beforeTime *time.Time, beforeID string, limit int) ([]domain.LedgerEntry, error) {
	return uc.ledgerRepo.ListByWallet(ctx, walletID, beforeTime, beforeID, limit)
}

func NewWalletUseCase(
	db *postgres.DB,
	txManager *postgres.TxManager,
	walletRepo *postgres.WalletRepository,
	transRepo *postgres.TransactionRepository,
	ledgerRepo *postgres.LedgerRepository,
	outboxRepo *postgres.OutboxRepository,
) *WalletUseCase {
	return &WalletUseCase{
		db:        db,
		txManager: txManager, walletRepo: walletRepo, transRepo: transRepo,
		ledgerRepo: ledgerRepo, outboxRepo: outboxRepo,
	}
}

func (uc *WalletUseCase) Reconcile(ctx context.Context, walletID string) (ReconciliationResult, error) {
	tx, err := uc.db.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf("iniciar reconciliação: %w", err)
	}
	defer tx.Rollback(ctx)
	wallet, err := uc.walletRepo.FindByID(ctx, tx, walletID)
	if err != nil {
		return ReconciliationResult{}, err
	}
	var totalText string
	var entries int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount), 0)::text, COUNT(*)
		FROM ledger_entries WHERE wallet_id = $1
	`, walletID).Scan(&totalText, &entries); err != nil {
		return ReconciliationResult{}, fmt.Errorf("somar ledger na reconciliação: %w", err)
	}
	total, err := strconv.ParseInt(totalText, 10, 64)
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf("saldo reconstruído fora do limite de int64: %w", err)
	}
	calculated := domain.NewMoneyFromInt64(total, wallet.Balance().Currency())
	difference, err := wallet.Balance().Subtract(calculated)
	if err != nil {
		return ReconciliationResult{}, err
	}
	if !difference.IsZero() {
		observability.RecordReconciliationIssue()
	}
	return ReconciliationResult{
		WalletID: walletID, StoredBalance: wallet.Balance(), CalculatedBalance: calculated,
		Difference: difference, Consistent: difference.IsZero(), CheckedEntries: entries,
	}, nil
}

func (uc *WalletUseCase) CreateWallet(ctx context.Context, playerID string, initialBalance domain.Money) (domain.Wallet, error) {
	wallet, err := domain.NewWallet(uuid.NewString(), playerID, initialBalance)
	if err != nil {
		return domain.Wallet{}, fmt.Errorf("validar carteira: %w", err)
	}
	err = uc.txManager.RunInTx(ctx, func(tx postgres.Tx) error {
		if err := uc.walletRepo.Create(ctx, tx, wallet); err != nil {
			return err
		}
		if initialBalance.IsZero() {
			return nil
		}

		opening, err := domain.NewOpeningTransaction(uuid.NewString(), wallet.ID(), playerID, initialBalance)
		if err != nil {
			return err
		}
		if err := opening.Complete(); err != nil {
			return err
		}
		opening.SetResultBalance(initialBalance)
		if err := uc.transRepo.Save(ctx, tx, opening); err != nil {
			return err
		}
		zero := domain.NewMoneyFromInt64(0, initialBalance.Currency())
		entry, err := domain.NewLedgerEntry(uuid.NewString(), wallet.ID(), opening.ID(), initialBalance, zero, initialBalance)
		if err != nil {
			return err
		}
		if err := uc.ledgerRepo.Save(ctx, tx, entry); err != nil {
			return err
		}
		processed := domain.NewWagerTransactionProcessedEvent(wallet.ID(), opening.ID(), "", domain.WagerTransactionEventData{
			TransactionID: opening.ID(), WalletID: wallet.ID(), Status: opening.Status(), Balance: initialBalance,
		})
		if err := uc.outboxRepo.Save(ctx, tx, processed); err != nil {
			return err
		}
		changed := domain.NewWalletBalanceChangedEvent(wallet.ID(), opening.ID(), "", domain.WalletBalanceChangedData{
			WalletID: wallet.ID(), TransactionID: opening.ID(), Direction: "CREDIT",
			Money: initialBalance, BalanceBefore: zero, BalanceAfter: initialBalance,
			WalletVersion: wallet.Version(),
		})
		return uc.outboxRepo.Save(ctx, tx, changed)
	})
	if err != nil {
		return domain.Wallet{}, fmt.Errorf("persistir carteira: %w", err)
	}
	return wallet, nil
}
