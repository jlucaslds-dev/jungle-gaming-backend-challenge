package usecase

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jlucaslds-dev/backend-challenge-go/internal/domain"
)

func TestProcessWagerValidatesDatabaseBoundFields(t *testing.T) {
	uc := &WageringUseCase{}
	input := ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "key", PlayerID: "player",
		WalletID: "not-a-uuid", ExternalID: "tx", RoundID: "round", GameID: "game",
		Amount: "1.00", Currency: "BRL", Type: domain.TypeBet,
	}
	if _, err := uc.processWager(context.Background(), input); !errors.Is(err, ErrInvalidWager) {
		t.Fatalf("invalid wallet ID error = %v, want ErrInvalidWager", err)
	}

	input.WalletID = "00000000-0000-4000-8000-000000000000"
	input.IdempotencyKey = strings.Repeat("x", 256)
	if _, err := uc.processWager(context.Background(), input); !errors.Is(err, ErrInvalidWager) {
		t.Fatalf("oversized idempotency key error = %v, want ErrInvalidWager", err)
	}
}

func TestWagerHashIsDeterministicAndExcludesTransportKey(t *testing.T) {
	input := ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "key-one", PlayerID: "player",
		WalletID: "wallet", ExternalID: "tx", RoundID: "round", GameID: "game",
		Amount: "25.00", Currency: "BRL", Type: domain.TypeBet,
	}
	money, err := parseWagerMoney(input)
	if err != nil {
		t.Fatal(err)
	}
	first, err := hashWager(input, money)
	if err != nil {
		t.Fatal(err)
	}
	input.IdempotencyKey = "key-two"
	second, err := hashWager(input, money)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("hash changed with transport idempotency key: %s != %s", first, second)
	}
	input.Amount = "25.01"
	otherMoney, err := parseWagerMoney(input)
	if err != nil {
		t.Fatal(err)
	}
	third, err := hashWager(input, otherMoney)
	if err != nil {
		t.Fatal(err)
	}
	if first == third {
		t.Fatal("hash did not change when business amount changed")
	}
}

func TestWagerMoneyPolicies(t *testing.T) {
	loss, err := parseWagerMoney(ProcessWagerInput{Type: domain.TypeLoss, Amount: "0.00", Currency: "BRL"})
	if err != nil || !loss.IsZero() {
		t.Fatalf("zero LOSS parse = %s, %v", loss.String(), err)
	}
	for _, test := range []struct {
		kind   domain.TransactionType
		amount string
	}{
		{kind: domain.TypeBet, amount: "0.00"},
		{kind: domain.TypeBet, amount: "-1.00"},
		{kind: domain.TypeBet, amount: "1.001"},
		{kind: domain.TypeLoss, amount: "0.01"},
	} {
		if _, err := parseWagerMoney(ProcessWagerInput{Type: test.kind, Amount: test.amount, Currency: "BRL"}); err == nil {
			t.Errorf("%s accepted amount %q", test.kind, test.amount)
		}
	}
}
