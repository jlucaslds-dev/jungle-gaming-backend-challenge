package domain

import (
	"testing"
)

func TestTransactionTypeAndStatusInvariants(t *testing.T) {
	amount := NewMoneyFromInt64(100, "BRL")
	metadata := WagerMetadata{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:txn-1",
		PayloadHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		PlayerID:    "player-1", RoundID: "round-1", GameID: "game-1",
	}
	tx, err := NewExternalWagerTransaction("id", "wallet", "external", amount, TypeBet, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if tx.Status() != StatusPending {
		t.Fatalf("new transaction status = %s", tx.Status())
	}
	if err := tx.Complete(); err != nil {
		t.Fatal(err)
	}
	if tx.Status() != StatusProcessed {
		t.Fatalf("completed status = %s", tx.Status())
	}
	if err := tx.Reject("TEST"); err == nil {
		t.Fatal("processed transaction was allowed to reject")
	}
	if _, err := NewWagerTransaction("id", "wallet", "external", amount, TypeLoss); err == nil {
		t.Fatal("LOSS with a non-zero amount was accepted")
	}
	if _, err := NewWagerTransaction("id", "wallet", "external", NewMoneyFromInt64(0, "BRL"), TypeLoss); err != nil {
		t.Fatalf("LOSS with zero amount was rejected: %v", err)
	}
	for _, kind := range []TransactionType{TypeWin, TypeRefund, TypeRollback} {
		if _, err := NewWagerTransaction("id", "wallet", "external", amount, kind); err != nil {
			t.Errorf("%s with a positive amount was rejected: %v", kind, err)
		}
	}
	pending, err := NewExternalWagerTransaction("id2", "wallet", "external2", amount, TypeRefund, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := pending.MarkPendingReference(); err != nil {
		t.Fatal(err)
	}
	if err := pending.Reject("REFERENCE_NOT_FOUND"); err != nil {
		t.Fatal(err)
	}
	if pending.Status() != StatusRejected || pending.FailureCode() != "REFERENCE_NOT_FOUND" {
		t.Fatalf("reference rejection status/code = %s/%s", pending.Status(), pending.FailureCode())
	}
	if err := pending.Complete(); err == nil {
		t.Fatal("rejected transaction was allowed to complete")
	}
}

func TestTransactionCanFailPermanentlyOnlyWhilePending(t *testing.T) {
	amount := NewMoneyFromInt64(100, "BRL")
	metadata := WagerMetadata{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:txn-failed",
		PayloadHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		PlayerID:    "player-1", RoundID: "round-1", GameID: "game-1",
	}
	tx, err := NewExternalWagerTransaction("id-failed", "wallet", "external-failed", amount, TypeBet, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Fail(""); err == nil {
		t.Fatal("permanent failure without a code was accepted")
	}
	if err := tx.Fail("INFRASTRUCTURE_FAILURE"); err != nil {
		t.Fatal(err)
	}
	if tx.Status() != StatusFailed || tx.FailureCode() != "INFRASTRUCTURE_FAILURE" {
		t.Fatalf("failed status/code = %s/%s", tx.Status(), tx.FailureCode())
	}
	if err := tx.Complete(); err == nil {
		t.Fatal("failed transaction was allowed to complete")
	}
	if err := tx.Reject("BUSINESS_REJECTION"); err == nil {
		t.Fatal("failed transaction was allowed to become rejected")
	}
}
