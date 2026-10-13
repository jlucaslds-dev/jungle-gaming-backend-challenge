package usecase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/domain"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/observability"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/repository/postgres"
)

var (
	ErrIdempotencyConflict = errors.New("idempotency key or external transaction reused with different content")
	ErrInvalidWager        = errors.New("invalid wager")
	ErrReferenceAmount     = errors.New("reversal amount does not match reference")
	ErrReferenceNoEffect   = errors.New("referenced transaction has no financial effect")
	ErrReferenceType       = errors.New("referenced transaction type is invalid")
	errWagerReplay         = errors.New("wager replay")
	errInboxReplay         = errors.New("inbox replay")
)

type WageringUseCase struct {
	txManager  *postgres.TxManager
	walletRepo *postgres.WalletRepository
	transRepo  *postgres.TransactionRepository
	ledgerRepo *postgres.LedgerRepository
	outboxRepo *postgres.OutboxRepository
	inboxRepo  *postgres.InboxRepository
}

func NewWageringUseCase(
	txManager *postgres.TxManager,
	walletRepo *postgres.WalletRepository,
	transRepo *postgres.TransactionRepository,
	ledgerRepo *postgres.LedgerRepository,
	outboxRepo *postgres.OutboxRepository,
	inboxRepo *postgres.InboxRepository,
) *WageringUseCase {
	return &WageringUseCase{
		txManager: txManager, walletRepo: walletRepo, transRepo: transRepo,
		ledgerRepo: ledgerRepo, outboxRepo: outboxRepo, inboxRepo: inboxRepo,
	}
}

type ProcessWagerInput struct {
	ProviderID         string
	IdempotencyKey     string
	PlayerID           string
	WalletID           string
	ExternalID         string
	RoundID            string
	GameID             string
	Amount             string
	Currency           string
	Type               domain.TransactionType
	ReferenceID        string
	MessageID          string
	MessagePayloadHash string
}

type ProcessWagerResult struct {
	Transaction      domain.WagerTransaction
	Balance          domain.Money
	IdempotentReplay bool
}

type canonicalWager struct {
	ProviderID  string                 `json:"providerId"`
	ExternalID  string                 `json:"externalTransactionId"`
	PlayerID    string                 `json:"playerId"`
	WalletID    string                 `json:"walletId"`
	RoundID     string                 `json:"roundId"`
	GameID      string                 `json:"gameId"`
	Type        domain.TransactionType `json:"kind"`
	Amount      string                 `json:"amount"`
	Currency    string                 `json:"currency"`
	ReferenceID string                 `json:"referenceExternalTransactionId,omitempty"`
}

func (uc *WageringUseCase) ProcessWager(ctx context.Context, input ProcessWagerInput) (ProcessWagerResult, error) {
	started := time.Now()
	result, err := uc.processWager(ctx, input)
	if err == nil {
		observability.RecordWager(string(result.Transaction.Status()), time.Since(started))
		if result.IdempotentReplay {
			observability.RecordReplay()
		}
	} else if errors.Is(err, ErrIdempotencyConflict) {
		observability.RecordConflict()
	} else if errors.Is(err, postgres.ErrWalletVersionConflict) {
		observability.RecordConcurrencyConflict()
	} else {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			observability.RecordConcurrencyConflict()
		}
	}
	return result, err
}

func (uc *WageringUseCase) processWager(ctx context.Context, input ProcessWagerInput) (ProcessWagerResult, error) {
	input, money, hash, err := prepareWagerInput(input)
	if err != nil {
		return ProcessWagerResult{}, err
	}
	if result, found, err := uc.findReplay(ctx, input, hash); err != nil || found {
		if found && input.MessageID != "" {
			if err := uc.registerInboxReplay(ctx, input); err != nil {
				return ProcessWagerResult{}, err
			}
		}
		return result, err
	}

	metadata := domain.WagerMetadata{
		ProviderID: input.ProviderID, IdempotencyKey: input.IdempotencyKey, PayloadHash: hash,
		PlayerID: input.PlayerID, RoundID: input.RoundID, GameID: input.GameID, ReferenceID: input.ReferenceID,
	}
	var result ProcessWagerResult
	err = uc.txManager.RunInTx(ctx, func(tx postgres.Tx) error {
		completeInbox := func() error {
			if input.MessageID == "" {
				return nil
			}
			return uc.inboxRepo.Complete(ctx, tx, postgres.WagerTransactionsConsumer, input.MessageID)
		}
		if input.MessageID != "" {
			registered, err := uc.inboxRepo.Register(ctx, tx, postgres.WagerTransactionsConsumer, input.MessageID, input.MessagePayloadHash)
			if err != nil {
				return err
			}
			if !registered {
				return errInboxReplay
			}
		}

		wallet, err := uc.walletRepo.FindByIDWithLock(ctx, tx, input.WalletID)
		if err != nil {
			return err
		}
		if wallet.PlayerID() != input.PlayerID {
			return fmt.Errorf("%w: wallet does not belong to player", ErrInvalidWager)
		}
		if wallet.Balance().Currency() != input.Currency {
			return fmt.Errorf("%w: wallet currency mismatch", ErrInvalidWager)
		}

		if existing, err := uc.transRepo.FindByProviderExternalTx(ctx, tx, input.ProviderID, input.ExternalID); err == nil {
			if err := verifyReplay(existing, input, hash); err != nil {
				return err
			}
			return errWagerReplay
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if existing, err := uc.transRepo.FindByIdempotencyKeyTx(ctx, tx, input.ProviderID, input.IdempotencyKey); err == nil {
			if err := verifyReplay(existing, input, hash); err != nil {
				return err
			}
			return errWagerReplay
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		wager, err := domain.NewExternalWagerTransaction(uuid.NewString(), input.WalletID, input.ExternalID, money, input.Type, metadata)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidWager, err)
		}
		before := wallet.Balance()
		changed := false
		var ledgerAmount domain.Money

		if isReversal(input.Type) || (input.Type == domain.TypeWin && input.ReferenceID != "") {
			if isReversal(input.Type) {
				reversed, err := uc.transRepo.HasProcessedReversalTx(ctx, tx, input.ProviderID, input.ReferenceID)
				if err != nil {
					return err
				}
				if reversed {
					if err := wager.Reject("REFERENCE_ALREADY_REVERSED"); err != nil {
						return err
					}
					wager.SetResultBalance(before)
					if err := uc.transRepo.Save(ctx, tx, wager); err != nil {
						return err
					}
					result = ProcessWagerResult{Transaction: wager, Balance: before}
					if err := uc.saveWagerEvent(ctx, tx, wager, before, ""); err != nil {
						return err
					}
					return completeInbox()
				}
			}
			reference, err := uc.transRepo.FindByProviderExternalTx(ctx, tx, input.ProviderID, input.ReferenceID)
			if errors.Is(err, pgx.ErrNoRows) || (err == nil && reference.Status() == domain.StatusPendingReference) {
				if err := wager.MarkPendingReference(); err != nil {
					return err
				}
				wager.SetResultBalance(before)
				if err := uc.transRepo.Save(ctx, tx, wager); err != nil {
					return err
				}
				result = ProcessWagerResult{Transaction: wager, Balance: before}
				if err := uc.saveWagerEvent(ctx, tx, wager, before, "WagerTransactionPendingReference"); err != nil {
					return err
				}
				return completeInbox()
			}
			if err != nil {
				return err
			}
			switch reference.Status() {
			case domain.StatusRejected:
				if err := wager.Reject("REFERENCE_NOT_PROCESSED"); err != nil {
					return err
				}
			case domain.StatusProcessed:
				if referenceScopeMismatch(reference, input.WalletID, input.PlayerID, input.RoundID, input.Currency) {
					if err := wager.Reject("REFERENCE_SCOPE_MISMATCH"); err != nil {
						return err
					}
				} else if input.Type == domain.TypeWin {
					if reference.Type() != domain.TypeBet {
						if err := wager.Reject("REFERENCE_NOT_BET"); err != nil {
							return err
						}
					} else if err := wallet.Credit(money); err != nil {
						code := "INVALID_REFERENCE"
						if errors.Is(err, domain.ErrMoneyOverflow) {
							code = "WALLET_BALANCE_OVERFLOW"
						}
						if rejectErr := wager.Reject(code); rejectErr != nil {
							return rejectErr
						}
					} else {
						ledgerAmount = money
						changed = true
					}
				} else {
					referenceLedger, ledgerErr := uc.ledgerRepo.FindByTransactionTx(ctx, tx, reference.ID())
					if errors.Is(ledgerErr, pgx.ErrNoRows) {
						ledgerErr = ErrReferenceNoEffect
					}
					if ledgerErr != nil && !errors.Is(ledgerErr, ErrReferenceNoEffect) {
						return ledgerErr
					}
					if ledgerErr != nil {
						err = ledgerErr
					} else {
						err = applyReversal(&wallet, reference, input.Type, money, referenceLedger.Amount())
					}
					if err == nil {
						changed = true
					} else {
						code := "INVALID_REFERENCE"
						if errors.Is(err, domain.ErrInsufficientFunds) {
							code = "REVERSAL_INSUFFICIENT_FUNDS"
						} else if errors.Is(err, ErrReferenceAmount) {
							code = "REFERENCE_AMOUNT_MISMATCH"
						} else if errors.Is(err, ErrReferenceType) {
							code = "REFERENCE_TYPE_INVALID"
						} else if errors.Is(err, ErrReferenceNoEffect) {
							code = "REFERENCE_HAS_NO_FINANCIAL_EFFECT"
						}
						if rejectErr := wager.Reject(code); rejectErr != nil {
							return rejectErr
						}
					}
				}
			default:
				if err := wager.MarkPendingReference(); err != nil {
					return err
				}
				wager.SetResultBalance(before)
				if err := uc.transRepo.Save(ctx, tx, wager); err != nil {
					return err
				}
				result = ProcessWagerResult{Transaction: wager, Balance: before}
				if err := uc.saveWagerEvent(ctx, tx, wager, before, "WagerTransactionPendingReference"); err != nil {
					return err
				}
				return completeInbox()
			}
		} else {
			switch input.Type {
			case domain.TypeBet:
				err = wallet.Debit(money)
				ledgerAmount, _ = money.Negate()
				changed = err == nil
			case domain.TypeWin:
				err = wallet.Credit(money)
				ledgerAmount = money
				changed = err == nil
			case domain.TypeLoss:
				changed = false
			default:
				return fmt.Errorf("%w: unsupported transaction type", ErrInvalidWager)
			}
			if err != nil {
				code := "INSUFFICIENT_FUNDS"
				if errors.Is(err, domain.ErrMoneyOverflow) && input.Type == domain.TypeWin {
					code = "WALLET_BALANCE_OVERFLOW"
				} else if !errors.Is(err, domain.ErrInsufficientFunds) {
					return err
				}
				if rejectErr := wager.Reject(code); rejectErr != nil {
					return rejectErr
				}
			}
		}

		after := wallet.Balance()
		if wager.Status() == domain.StatusPending {
			if err := wager.Complete(); err != nil {
				return err
			}
		}
		wager.SetResultBalance(after)
		if err := uc.transRepo.Save(ctx, tx, wager); err != nil {
			return err
		}
		result = ProcessWagerResult{Transaction: wager, Balance: after}
		if changed {
			if err := uc.walletRepo.Save(ctx, tx, wallet); err != nil {
				return err
			}
			if input.Type == domain.TypeRefund || input.Type == domain.TypeRollback {
				ledgerAmount, err = after.Subtract(before)
				if err != nil {
					return err
				}
			}
			entry, err := domain.NewLedgerEntry(uuid.NewString(), input.WalletID, wager.ID(), ledgerAmount, before, after)
			if err != nil {
				return err
			}
			if err := uc.ledgerRepo.Save(ctx, tx, entry); err != nil {
				return err
			}
		}
		if err := uc.saveWagerEvent(ctx, tx, wager, after, ""); err != nil {
			return err
		}
		if changed {
			direction := "CREDIT"
			if ledgerAmount.IsNegative() {
				direction = "DEBIT"
			}
			event := domain.NewWalletBalanceChangedEvent(input.WalletID, wager.ID(), "", domain.WalletBalanceChangedData{
				WalletID: input.WalletID, TransactionID: wager.ID(), Direction: direction, Money: money,
				BalanceBefore: before, BalanceAfter: after, WalletVersion: wallet.Version(),
			})
			if err := uc.outboxRepo.Save(ctx, tx, event); err != nil {
				return err
			}
		}
		return completeInbox()
	})
	if errors.Is(err, errWagerReplay) || errors.Is(err, errInboxReplay) {
		replay, found, replayErr := uc.findReplay(ctx, input, hash)
		if replayErr != nil || found {
			if found && input.MessageID != "" {
				if inboxErr := uc.registerInboxReplay(ctx, input); inboxErr != nil {
					return ProcessWagerResult{}, inboxErr
				}
			}
			return replay, replayErr
		}
		return ProcessWagerResult{}, fmt.Errorf("mensagem SQS já foi confirmada sem transação associada")
	}
	if err != nil {
		if replay, found, replayErr := uc.findReplay(ctx, input, hash); replayErr != nil || found {
			if found && input.MessageID != "" {
				if inboxErr := uc.registerInboxReplay(ctx, input); inboxErr != nil {
					return ProcessWagerResult{}, inboxErr
				}
			}
			return replay, replayErr
		}
		return ProcessWagerResult{}, fmt.Errorf("processar operação financeira: %w", err)
	}
	return result, nil
}

func prepareWagerInput(input ProcessWagerInput) (ProcessWagerInput, domain.Money, string, error) {
	money, err := parseWagerMoney(input)
	if err != nil {
		return ProcessWagerInput{}, domain.Money{}, "", err
	}
	if input.ProviderID == "" || len(input.ProviderID) > 200 ||
		input.IdempotencyKey == "" || len(input.IdempotencyKey) > 255 ||
		input.PlayerID == "" || len(input.PlayerID) > 100 ||
		input.WalletID == "" || len(input.WalletID) > 36 ||
		input.ExternalID == "" || len(input.ExternalID) > 200 ||
		input.RoundID == "" || len(input.RoundID) > 200 ||
		input.GameID == "" || len(input.GameID) > 200 || len(input.ReferenceID) > 200 {
		return ProcessWagerInput{}, domain.Money{}, "", fmt.Errorf("%w: required field missing or exceeds its limit", ErrInvalidWager)
	}
	walletID, err := uuid.Parse(input.WalletID)
	if err != nil {
		return ProcessWagerInput{}, domain.Money{}, "", ErrInvalidWager
	}
	input.WalletID = walletID.String()
	if domain.ValidateCurrency(input.Currency) != nil {
		return ProcessWagerInput{}, domain.Money{}, "", fmt.Errorf("%w: currency must be a three-letter uppercase ISO code", ErrInvalidWager)
	}
	switch input.Type {
	case domain.TypeBet, domain.TypeWin, domain.TypeLoss, domain.TypeRefund, domain.TypeRollback:
	default:
		return ProcessWagerInput{}, domain.Money{}, "", fmt.Errorf("%w: unsupported external transaction type", ErrInvalidWager)
	}
	if isReversal(input.Type) && input.ReferenceID == "" {
		return ProcessWagerInput{}, domain.Money{}, "", fmt.Errorf("%w: reversal reference is required", ErrInvalidWager)
	}
	if !isReversal(input.Type) && input.Type != domain.TypeWin && input.ReferenceID != "" {
		return ProcessWagerInput{}, domain.Money{}, "", fmt.Errorf("%w: only WIN and reversals may include a reference", ErrInvalidWager)
	}
	hash, err := hashWager(input, money)
	if err != nil {
		return ProcessWagerInput{}, domain.Money{}, "", fmt.Errorf("hash wager payload: %w", err)
	}
	return input, money, hash, nil
}

func (uc *WageringUseCase) RecordPermanentFailure(ctx context.Context, input ProcessWagerInput, code string) error {
	if code == "" {
		return errors.New("código de falha permanente obrigatório")
	}
	input, money, hash, err := prepareWagerInput(input)
	if err != nil {
		return err
	}
	metadata := domain.WagerMetadata{
		ProviderID: input.ProviderID, IdempotencyKey: input.IdempotencyKey, PayloadHash: hash,
		PlayerID: input.PlayerID, RoundID: input.RoundID, GameID: input.GameID, ReferenceID: input.ReferenceID,
	}
	err = uc.txManager.RunInTx(ctx, func(tx postgres.Tx) error {
		if input.MessageID != "" {
			registered, err := uc.inboxRepo.Register(ctx, tx, postgres.WagerTransactionsConsumer, input.MessageID, input.MessagePayloadHash)
			if err != nil {
				return err
			}
			if !registered {
				return errInboxReplay
			}
		}
		wallet, err := uc.walletRepo.FindByIDWithLock(ctx, tx, input.WalletID)
		if err != nil {
			return err
		}
		if wallet.PlayerID() != input.PlayerID || wallet.Balance().Currency() != input.Currency {
			return fmt.Errorf("%w: wallet scope mismatch", ErrInvalidWager)
		}
		if existing, err := uc.transRepo.FindByProviderExternalTx(ctx, tx, input.ProviderID, input.ExternalID); err == nil {
			if err := verifyReplay(existing, input, hash); err != nil {
				return err
			}
			return errWagerReplay
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if existing, err := uc.transRepo.FindByIdempotencyKeyTx(ctx, tx, input.ProviderID, input.IdempotencyKey); err == nil {
			if err := verifyReplay(existing, input, hash); err != nil {
				return err
			}
			return errWagerReplay
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		wager, err := domain.NewExternalWagerTransaction(uuid.NewString(), input.WalletID, input.ExternalID, money, input.Type, metadata)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidWager, err)
		}
		if err := wager.Fail(code); err != nil {
			return err
		}
		wager.SetResultBalance(wallet.Balance())
		if err := uc.transRepo.Save(ctx, tx, wager); err != nil {
			return err
		}
		if input.MessageID != "" {
			return uc.inboxRepo.Complete(ctx, tx, postgres.WagerTransactionsConsumer, input.MessageID)
		}
		return nil
	})
	if err == nil {
		return nil
	}
	if errors.Is(err, errWagerReplay) || errors.Is(err, errInboxReplay) {
		_, found, replayErr := uc.findReplay(ctx, input, hash)
		if replayErr != nil {
			return replayErr
		}
		if !found {
			return errors.New("mensagem SQS concluída sem uma transação durável")
		}
		if input.MessageID != "" {
			return uc.registerInboxReplay(ctx, input)
		}
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		_, found, replayErr := uc.findReplay(ctx, input, hash)
		if replayErr != nil {
			return replayErr
		}
		if found {
			if input.MessageID != "" {
				return uc.registerInboxReplay(ctx, input)
			}
			return nil
		}
	}
	return err
}

func (uc *WageringUseCase) registerInboxReplay(ctx context.Context, input ProcessWagerInput) error {
	return uc.txManager.RunInTx(ctx, func(tx postgres.Tx) error {
		_, err := uc.inboxRepo.Register(ctx, tx, postgres.WagerTransactionsConsumer, input.MessageID, input.MessagePayloadHash)
		if err != nil {
			return err
		}
		return uc.inboxRepo.Complete(ctx, tx, postgres.WagerTransactionsConsumer, input.MessageID)
	})
}

func (uc *WageringUseCase) GetTransaction(ctx context.Context, transactionID string) (domain.WagerTransaction, error) {
	return uc.transRepo.FindByID(ctx, transactionID)
}

func (uc *WageringUseCase) GetProviderTransaction(ctx context.Context, providerID, externalID string) (domain.WagerTransaction, error) {
	return uc.transRepo.FindByProviderExternal(ctx, providerID, externalID)
}

func (uc *WageringUseCase) ResolvePendingReferences(ctx context.Context) error {
	ids, err := uc.transRepo.ListDuePendingReferenceIDs(ctx, 50)
	if err != nil {
		return err
	}
	var failures []error
	for _, id := range ids {
		if err := uc.resolvePendingReference(ctx, id); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			failures = append(failures, fmt.Errorf("resolve pending transaction %s: %w", id, err))
		}
	}
	return errors.Join(failures...)
}

func (uc *WageringUseCase) resolvePendingReference(ctx context.Context, id string) error {
	return uc.txManager.RunInTx(ctx, func(tx postgres.Tx) error {
		initial, err := uc.transRepo.FindByIDTx(ctx, tx, id, false)
		if err != nil {
			return err
		}
		if initial.Status() != domain.StatusPendingReference {
			return nil
		}
		wallet, err := uc.walletRepo.FindByIDWithLock(ctx, tx, initial.WalletID())
		if err != nil {
			return err
		}
		wager, err := uc.transRepo.FindByIDTx(ctx, tx, id, true)
		if err != nil {
			return err
		}
		if wager.Status() != domain.StatusPendingReference {
			return nil
		}
		attempts, nextAttempt, err := uc.transRepo.PendingScheduleTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if nextAttempt != nil && nextAttempt.After(time.Now().UTC()) {
			return nil
		}
		attempts++
		before := wallet.Balance()
		reference, referenceErr := uc.transRepo.FindByProviderExternalTx(ctx, tx, wager.ProviderID(), wager.ReferenceID())
		if errors.Is(referenceErr, pgx.ErrNoRows) {
			if attempts >= 10 {
				if err := wager.Reject("REFERENCE_NOT_FOUND"); err != nil {
					return err
				}
				wager.SetResultBalance(before)
				if err := uc.transRepo.UpdatePending(ctx, tx, wager, attempts, nil); err != nil {
					return err
				}
				return uc.saveWagerEvent(ctx, tx, wager, before, "")
			}
			return uc.transRepo.UpdatePending(ctx, tx, wager, attempts, referenceRetryAt(attempts))
		}
		if referenceErr != nil {
			return referenceErr
		}
		switch reference.Status() {
		case domain.StatusPendingReference, domain.StatusPending:
			if attempts >= 10 {
				if err := wager.Reject("REFERENCE_NOT_PROCESSED"); err != nil {
					return err
				}
				wager.SetResultBalance(before)
				if err := uc.transRepo.UpdatePending(ctx, tx, wager, attempts, nil); err != nil {
					return err
				}
				return uc.saveWagerEvent(ctx, tx, wager, before, "")
			}
			return uc.transRepo.UpdatePending(ctx, tx, wager, attempts, referenceRetryAt(attempts))
		case domain.StatusRejected:
			if err := wager.Reject("REFERENCE_NOT_PROCESSED"); err != nil {
				return err
			}
		case domain.StatusProcessed:
			alreadyReversed := false
			if isReversal(wager.Type()) {
				alreadyReversed, err = uc.transRepo.HasProcessedReversalTx(ctx, tx, wager.ProviderID(), wager.ReferenceID())
				if err != nil {
					return err
				}
			}
			switch {
			case alreadyReversed:
				if err := wager.Reject("REFERENCE_ALREADY_REVERSED"); err != nil {
					return err
				}
			case referenceScopeMismatch(reference, wager.WalletID(), wager.PlayerID(), wager.RoundID(), wager.Amount().Currency()):
				if err := wager.Reject("REFERENCE_SCOPE_MISMATCH"); err != nil {
					return err
				}
			case wager.Type() == domain.TypeWin && reference.Type() != domain.TypeBet:
				if err := wager.Reject("REFERENCE_NOT_BET"); err != nil {
					return err
				}
			default:
				var ledgerErr error
				if wager.Type() == domain.TypeWin {
					ledgerErr = wallet.Credit(wager.Amount())
				} else {
					referenceLedger, lookupErr := uc.ledgerRepo.FindByTransactionTx(ctx, tx, reference.ID())
					if errors.Is(lookupErr, pgx.ErrNoRows) {
						lookupErr = ErrReferenceNoEffect
					}
					if lookupErr != nil && !errors.Is(lookupErr, ErrReferenceNoEffect) {
						return lookupErr
					}
					if lookupErr != nil {
						ledgerErr = lookupErr
					} else {
						ledgerErr = applyReversal(&wallet, reference, wager.Type(), wager.Amount(), referenceLedger.Amount())
					}
				}
				if ledgerErr != nil {
					code := "INVALID_REFERENCE"
					if errors.Is(ledgerErr, domain.ErrInsufficientFunds) {
						code = "REVERSAL_INSUFFICIENT_FUNDS"
					} else if errors.Is(ledgerErr, domain.ErrMoneyOverflow) && wager.Type() == domain.TypeWin {
						code = "WALLET_BALANCE_OVERFLOW"
					} else if errors.Is(ledgerErr, ErrReferenceAmount) {
						code = "REFERENCE_AMOUNT_MISMATCH"
					} else if errors.Is(ledgerErr, ErrReferenceType) {
						code = "REFERENCE_TYPE_INVALID"
					} else if errors.Is(ledgerErr, ErrReferenceNoEffect) {
						code = "REFERENCE_HAS_NO_FINANCIAL_EFFECT"
					}
					if err := wager.Reject(code); err != nil {
						return err
					}
				} else {
					after := wallet.Balance()
					delta, err := after.Subtract(before)
					if err != nil {
						return err
					}
					if err := wager.Complete(); err != nil {
						return err
					}
					wager.SetResultBalance(after)
					if err := uc.walletRepo.Save(ctx, tx, wallet); err != nil {
						return err
					}
					if err := uc.transRepo.UpdatePending(ctx, tx, wager, attempts, nil); err != nil {
						return err
					}
					entry, err := domain.NewLedgerEntry(uuid.NewString(), wager.WalletID(), wager.ID(), delta, before, after)
					if err != nil {
						return err
					}
					if err := uc.ledgerRepo.Save(ctx, tx, entry); err != nil {
						return err
					}
					if err := uc.saveWagerEvent(ctx, tx, wager, after, ""); err != nil {
						return err
					}
					direction := "CREDIT"
					if delta.IsNegative() {
						direction = "DEBIT"
					}
					return uc.outboxRepo.Save(ctx, tx, domain.NewWalletBalanceChangedEvent(
						wager.WalletID(), wager.ID(), "", domain.WalletBalanceChangedData{
							WalletID: wager.WalletID(), TransactionID: wager.ID(), Direction: direction,
							Money: wager.Amount(), BalanceBefore: before, BalanceAfter: after, WalletVersion: wallet.Version(),
						}))
				}
			}
		default:
			if attempts >= 10 {
				if err := wager.Reject("REFERENCE_NOT_PROCESSED"); err != nil {
					return err
				}
			} else {
				return uc.transRepo.UpdatePending(ctx, tx, wager, attempts, referenceRetryAt(attempts))
			}
		}
		wager.SetResultBalance(before)
		if err := uc.transRepo.UpdatePending(ctx, tx, wager, attempts, nil); err != nil {
			return err
		}
		return uc.saveWagerEvent(ctx, tx, wager, before, "")
	})
}

func referenceRetryAt(attempt int) *time.Time {
	observability.RecordReferenceRetry()
	delay := time.Second * time.Duration(1<<min(attempt, 8))
	if delay > 5*time.Minute {
		delay = 5 * time.Minute
	}
	next := time.Now().UTC().Add(delay)
	return &next
}

func parseWagerMoney(input ProcessWagerInput) (domain.Money, error) {
	if input.Type == domain.TypeLoss {
		if input.Amount != "0.00" {
			return domain.Money{}, fmt.Errorf("%w: LOSS amount must be 0.00", ErrInvalidWager)
		}
		return domain.NewMoneyFromInt64(0, input.Currency), nil
	}
	money, err := domain.NewMoney(input.Amount, input.Currency)
	if err != nil {
		return domain.Money{}, fmt.Errorf("%w: %v", ErrInvalidWager, err)
	}
	if money.IsZero() {
		return domain.Money{}, fmt.Errorf("%w: wager amount must be positive", ErrInvalidWager)
	}
	return money, nil
}

func hashWager(input ProcessWagerInput, money domain.Money) (string, error) {
	body, err := json.Marshal(canonicalWager{
		ProviderID: input.ProviderID, ExternalID: input.ExternalID, PlayerID: input.PlayerID,
		WalletID: input.WalletID, RoundID: input.RoundID, GameID: input.GameID, Type: input.Type,
		Amount: money.String(), Currency: money.Currency(), ReferenceID: input.ReferenceID,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func (uc *WageringUseCase) findReplay(ctx context.Context, input ProcessWagerInput, hash string) (ProcessWagerResult, bool, error) {
	existing, err := uc.transRepo.FindByProviderExternal(ctx, input.ProviderID, input.ExternalID)
	if err == nil {
		if err := verifyReplay(existing, input, hash); err != nil {
			return ProcessWagerResult{}, false, err
		}
		return replayResult(existing), true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ProcessWagerResult{}, false, err
	}
	existing, err = uc.transRepo.FindByIdempotencyKey(ctx, input.ProviderID, input.IdempotencyKey)
	if err == nil {
		if err := verifyReplay(existing, input, hash); err != nil {
			return ProcessWagerResult{}, false, err
		}
		return replayResult(existing), true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ProcessWagerResult{}, false, err
	}
	return ProcessWagerResult{}, false, nil
}

func verifyReplay(existing domain.WagerTransaction, input ProcessWagerInput, hash string) error {
	if existing.ExternalID() != input.ExternalID || existing.IdempotencyKey() != input.IdempotencyKey || existing.PayloadHash() != hash {
		return ErrIdempotencyConflict
	}
	return nil
}

func replayResult(transaction domain.WagerTransaction) ProcessWagerResult {
	return ProcessWagerResult{Transaction: transaction, Balance: transaction.ResultBalance(), IdempotentReplay: true}
}

func isReversal(kind domain.TransactionType) bool {
	return kind == domain.TypeRefund || kind == domain.TypeRollback
}

func referenceScopeMismatch(reference domain.WagerTransaction, walletID, playerID, roundID, currency string) bool {
	return reference.WalletID() != walletID ||
		reference.PlayerID() != playerID ||
		reference.RoundID() != roundID ||
		reference.Amount().Currency() != currency
}

func applyReversal(wallet *domain.Wallet, reference domain.WagerTransaction, kind domain.TransactionType, amount, referenceLedgerAmount domain.Money) error {
	if amount.Amount() != reference.Amount().Amount() || amount.Currency() != reference.Amount().Currency() {
		return ErrReferenceAmount
	}
	if referenceLedgerAmount.IsZero() {
		return ErrReferenceNoEffect
	}
	if kind == domain.TypeRefund && reference.Type() != domain.TypeBet {
		return ErrReferenceType
	}
	if kind == domain.TypeRollback && reference.Type() != domain.TypeBet &&
		reference.Type() != domain.TypeWin && reference.Type() != domain.TypeRefund {
		return ErrReferenceType
	}
	if referenceLedgerAmount.IsNegative() {
		return wallet.Credit(amount)
	}
	return wallet.Debit(amount)
}

func (uc *WageringUseCase) saveWagerEvent(ctx context.Context, tx postgres.Tx, wager domain.WagerTransaction, balance domain.Money, eventType string) error {
	if eventType == "" {
		switch wager.Status() {
		case domain.StatusProcessed:
			eventType = "WagerTransactionProcessed"
		case domain.StatusRejected:
			eventType = "WagerTransactionRejected"
		default:
			return nil
		}
	}
	data := domain.WagerTransactionEventData{
		TransactionID: wager.ID(), ProviderID: wager.ProviderID(), ExternalID: wager.ExternalID(),
		WalletID: wager.WalletID(), Status: wager.Status(), FailureCode: wager.FailureCode(), Balance: balance,
	}
	var event domain.IntegrationEvent
	switch eventType {
	case "WagerTransactionPendingReference":
		event = domain.NewWagerTransactionPendingReferenceEvent(wager.WalletID(), wager.ID(), "", data)
	case "WagerTransactionRejected":
		event = domain.NewWagerTransactionRejectedEvent(wager.WalletID(), wager.ID(), "", data)
	default:
		event = domain.NewWagerTransactionProcessedEvent(wager.WalletID(), wager.ID(), "", data)
	}
	return uc.outboxRepo.Save(ctx, tx, event)
}
