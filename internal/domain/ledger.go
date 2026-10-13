package domain

import (
	"errors"
	"time"
)

type LedgerEntry struct {
	id            string
	walletId      string
	transactionId string
	amount        Money
	balanceBefore Money
	balanceAfter  Money
	createdAt     time.Time
}

func NewLedgerEntry(id, walletId, transactionId string, amount, balanceBefore, balanceAfter Money) (LedgerEntry, error) {
	if id == "" || walletId == "" || transactionId == "" {
		return LedgerEntry{}, errors.New("ids da entrada do ledger são obrigatórios")
	}
	if amount.IsZero() || amount.Validate() != nil || balanceBefore.Validate() != nil || balanceAfter.Validate() != nil ||
		amount.Currency() != balanceBefore.Currency() || amount.Currency() != balanceAfter.Currency() {
		return LedgerEntry{}, errors.New("valores da entrada do ledger inválidos ou com moedas diferentes")
	}
	expected, err := balanceBefore.Add(amount)
	if err != nil {
		return LedgerEntry{}, err
	}
	if expected.Amount() != balanceAfter.Amount() || balanceBefore.IsNegative() || balanceAfter.IsNegative() {
		return LedgerEntry{}, errors.New("saldos da entrada do ledger inconsistentes")
	}

	return LedgerEntry{
		id:            id,
		walletId:      walletId,
		transactionId: transactionId,
		amount:        amount,
		balanceBefore: balanceBefore,
		balanceAfter:  balanceAfter,
		createdAt:     time.Now().UTC(),
	}, nil
}

func RehydrateLedgerEntry(id, walletId, transactionId string, amount, balanceBefore, balanceAfter Money, createdAt time.Time) LedgerEntry {
	return LedgerEntry{
		id:            id,
		walletId:      walletId,
		transactionId: transactionId,
		amount:        amount,
		balanceBefore: balanceBefore,
		balanceAfter:  balanceAfter,
		createdAt:     createdAt,
	}
}

func (l LedgerEntry) ID() string            { return l.id }
func (l LedgerEntry) WalletID() string      { return l.walletId }
func (l LedgerEntry) TransactionID() string { return l.transactionId }
func (l LedgerEntry) Amount() Money         { return l.amount }
func (l LedgerEntry) BalanceBefore() Money  { return l.balanceBefore }
func (l LedgerEntry) BalanceAfter() Money   { return l.balanceAfter }
func (l LedgerEntry) CreatedAt() time.Time  { return l.createdAt }
