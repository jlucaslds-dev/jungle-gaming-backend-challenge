package domain

import (
	"errors"
	"math"
	"testing"
)

func TestWalletMaintainsFinancialInvariants(t *testing.T) {
	balance := NewMoneyFromInt64(10000, "BRL")
	wallet, err := NewWallet("wallet-1", "player-1", balance)
	if err != nil {
		t.Fatal(err)
	}
	if err := wallet.Debit(NewMoneyFromInt64(8000, "BRL")); err != nil {
		t.Fatal(err)
	}
	if wallet.Balance().Amount() != 2000 || wallet.Version() != 2 {
		t.Fatalf("balance/version = %d/%d, want 2000/2", wallet.Balance().Amount(), wallet.Version())
	}
	if err := wallet.Debit(NewMoneyFromInt64(2001, "BRL")); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("insufficient debit error = %v", err)
	}
	if err := wallet.Credit(NewMoneyFromInt64(1, "USD")); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("currency mismatch error = %v", err)
	}
	if wallet.Balance().Amount() != 2000 || wallet.Version() != 2 {
		t.Fatalf("rejected operations changed wallet: balance/version = %d/%d", wallet.Balance().Amount(), wallet.Version())
	}
}

func TestNewWalletRejectsNegativeInitialBalance(t *testing.T) {
	if _, err := NewWallet("wallet-1", "player-1", NewMoneyFromInt64(-1, "BRL")); err == nil {
		t.Fatal("NewWallet() accepted negative balance")
	}
}

func TestWalletCreditOverflowDoesNotMutateBalance(t *testing.T) {
	wallet, err := NewWallet("wallet-overflow", "player", NewMoneyFromInt64(math.MaxInt64, "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	if err := wallet.Credit(NewMoneyFromInt64(1, "BRL")); !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("Credit() error = %v, want ErrMoneyOverflow", err)
	}
	if wallet.Balance().Amount() != math.MaxInt64 || wallet.Version() != 1 {
		t.Fatalf("wallet changed after overflow: amount/version = %d/%d", wallet.Balance().Amount(), wallet.Version())
	}
}
