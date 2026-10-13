package usecase

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/domain"
	"github.com/jlucaslds-dev/backend-challenge-go/internal/repository/postgres"
)

func TestPostgresWagerConcurrencyIdempotencyAndLedger(t *testing.T) {
	connectionString := os.Getenv("TEST_DATABASE_URL")
	if connectionString == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	admin, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		t.Fatal(err)
	}
	schema := "test_" + uuid.NewString()[:8]
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+identifier+" CASCADE")
		admin.Close()
		t.Fatal(err)
	}
	defer func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+identifier+" CASCADE")
		admin.Close()
	}()
	migrationNames := []string{
		"001_initial_schema",
		"002_reference_and_inbox",
		"003_failed_state_and_opening_once",
		"004_reversal_index_and_wallet_ledger_guard",
	}
	for _, name := range migrationNames {
		migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", name+".up.sql"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(migration), pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}

	db := &postgres.DB{Pool: pool}
	txManager := postgres.NewTxManager(db)
	walletRepo := postgres.NewWalletRepository(db)
	transactionRepo := postgres.NewTransactionRepository(db)
	ledgerRepo := postgres.NewLedgerRepository(db)
	outboxRepo := postgres.NewOutboxRepository(db)
	inboxRepo := postgres.NewInboxRepository()
	walletUC := NewWalletUseCase(db, txManager, walletRepo, transactionRepo, ledgerRepo, outboxRepo)
	wagerUC := NewWageringUseCase(txManager, walletRepo, transactionRepo, ledgerRepo, outboxRepo, inboxRepo)
	newWagerInstance := func(instancePool *pgxpool.Pool) *WageringUseCase {
		instanceDB := &postgres.DB{Pool: instancePool}
		return NewWageringUseCase(
			postgres.NewTxManager(instanceDB),
			postgres.NewWalletRepository(instanceDB),
			postgres.NewTransactionRepository(instanceDB),
			postgres.NewLedgerRepository(instanceDB),
			postgres.NewOutboxRepository(instanceDB),
			postgres.NewInboxRepository(),
		)
	}
	wagerInstances := []*WageringUseCase{wagerUC}
	var instancePools []*pgxpool.Pool
	for range 2 {
		instanceConfig, err := pgxpool.ParseConfig(connectionString)
		if err != nil {
			t.Fatal(err)
		}
		instanceConfig.ConnConfig.RuntimeParams["search_path"] = schema
		instancePool, err := pgxpool.NewWithConfig(ctx, instanceConfig)
		if err != nil {
			t.Fatal(err)
		}
		instancePools = append(instancePools, instancePool)
		wagerInstances = append(wagerInstances, newWagerInstance(instancePool))
	}
	defer func() {
		for _, instancePool := range instancePools {
			instancePool.Close()
		}
	}()

	wallet, err := walletUC.CreateWallet(ctx, "player-main", domain.NewMoneyFromInt64(10000, "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	unfundedWallet, err := walletUC.CreateWallet(ctx, "player-unfunded", domain.NewMoneyFromInt64(0, "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE wallets SET balance = balance + 1 WHERE id = $1
	`, unfundedWallet.ID()); err == nil {
		t.Fatal("direct wallet balance update without a ledger entry succeeded")
	}
	var unchangedBalance int64
	if err := pool.QueryRow(ctx, `SELECT balance FROM wallets WHERE id = $1`, unfundedWallet.ID()).
		Scan(&unchangedBalance); err != nil {
		t.Fatalf("read wallet balance after rejected update: %v", err)
	}
	if unchangedBalance != 0 {
		t.Fatalf("wallet balance after rejected direct update = %d, want 0", unchangedBalance)
	}
	ledgerGuardWallet, err := walletUC.CreateWallet(ctx, "player-ledger-guard", domain.NewMoneyFromInt64(1000, "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	for _, wager := range []struct {
		externalID string
		kind       domain.TransactionType
	}{
		{externalID: "ledger-guard-bet", kind: domain.TypeBet},
		{externalID: "ledger-guard-win", kind: domain.TypeWin},
	} {
		_, err := wagerUC.ProcessWager(ctx, ProcessWagerInput{
			ProviderID: "provider-a", IdempotencyKey: "provider-a:" + wager.externalID,
			PlayerID: "player-ledger-guard", WalletID: ledgerGuardWallet.ID(),
			ExternalID: wager.externalID, RoundID: "ledger-guard", GameID: "game",
			Amount: "5.00", Currency: "BRL", Type: wager.kind,
		})
		if err != nil {
			t.Fatalf("process %s for ledger guard: %v", wager.kind, err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE wallets SET balance = 500 WHERE id = $1`, ledgerGuardWallet.ID()); err == nil {
		t.Fatal("wallet balance update reused a ledger entry from an earlier transaction")
	}
	if err := pool.QueryRow(ctx, `SELECT balance FROM wallets WHERE id = $1`, ledgerGuardWallet.ID()).
		Scan(&unchangedBalance); err != nil {
		t.Fatalf("read wallet balance after stale-ledger update: %v", err)
	}
	if unchangedBalance != 1000 {
		t.Fatalf("wallet balance after rejected stale-ledger update = %d, want 1000", unchangedBalance)
	}
	var openingID string
	if err := pool.QueryRow(ctx, `SELECT id FROM wager_transactions WHERE wallet_id = $1 AND tx_type = 'OPENING'`, wallet.ID()).Scan(&openingID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, wallet_id, currency, player_id, amount, tx_type, status, result_balance
		) VALUES ($1, $2, 'BRL', 'player-main', 10000, 'OPENING', 'PROCESSED', 10000)
	`, uuid.NewString(), wallet.ID()); err == nil {
		t.Fatal("database allowed a second OPENING transaction for one wallet")
	}
	failedInput := ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:permanent-failure",
		PlayerID: "player-main", WalletID: wallet.ID(), ExternalID: "permanent-failure",
		RoundID: "failure-round", GameID: "game", Amount: "1.00", Currency: "BRL", Type: domain.TypeBet,
	}
	if err := wagerUC.RecordPermanentFailure(ctx, failedInput, "INFRASTRUCTURE_RETRIES_EXHAUSTED"); err != nil {
		t.Fatalf("persist FAILED transaction: %v", err)
	}
	reloadedFailure, err := wagerUC.GetProviderTransaction(ctx, "provider-a", "permanent-failure")
	if err != nil || reloadedFailure.Status() != domain.StatusFailed || reloadedFailure.FailureCode() != "INFRASTRUCTURE_RETRIES_EXHAUSTED" {
		t.Fatalf("rehydrated FAILED transaction = %s/%s, error %v", reloadedFailure.Status(), reloadedFailure.FailureCode(), err)
	}
	failedReplay, err := wagerUC.ProcessWager(ctx, failedInput)
	if err != nil || !failedReplay.IdempotentReplay || failedReplay.Transaction.Status() != domain.StatusFailed {
		t.Fatalf("FAILED replay = status %s, replay %t, error %v", failedReplay.Transaction.Status(), failedReplay.IdempotentReplay, err)
	}
	start := make(chan struct{})
	results := make(chan ProcessWagerResult, 2)
	failures := make(chan error, 2)
	for index, externalID := range []string{"race-bet-1", "race-bet-2"} {
		externalID := externalID
		instance := wagerInstances[index%len(wagerInstances)]
		go func() {
			<-start
			result, err := instance.ProcessWager(ctx, ProcessWagerInput{
				ProviderID: "provider-a", IdempotencyKey: "provider-a:" + externalID,
				PlayerID: "player-main", WalletID: wallet.ID(), ExternalID: externalID,
				RoundID: "round-race", GameID: "game", Amount: "80.00", Currency: "BRL", Type: domain.TypeBet,
			})
			results <- result
			failures <- err
		}()
	}
	close(start)
	var processed, rejected int
	var processedID string
	var processedExternalID string
	for range 2 {
		if err := <-failures; err != nil {
			t.Fatalf("concurrent wager failed: %v", err)
		}
		result := <-results
		switch result.Transaction.Status() {
		case domain.StatusProcessed:
			processed++
			processedID = result.Transaction.ID()
			processedExternalID = result.Transaction.ExternalID()
		case domain.StatusRejected:
			rejected++
			if result.Transaction.FailureCode() != "INSUFFICIENT_FUNDS" {
				t.Fatalf("rejection code = %q", result.Transaction.FailureCode())
			}
		default:
			t.Fatalf("unexpected wager status %s", result.Transaction.Status())
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("processed/rejected = %d/%d, want 1/1", processed, rejected)
	}
	updatedWallet, err := walletUC.GetWallet(ctx, wallet.ID())
	if err != nil {
		t.Fatal(err)
	}
	if updatedWallet.Balance().Amount() != 2000 {
		t.Fatalf("balance = %s, want 20.00", updatedWallet.Balance().String())
	}
	var debitCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM ledger_entries WHERE wallet_id = $1 AND amount < 0
	`, wallet.ID()).Scan(&debitCount); err != nil {
		t.Fatal(err)
	}
	if debitCount != 1 {
		t.Fatalf("debit entries = %d, want 1", debitCount)
	}
	for _, mutation := range []struct {
		name  string
		query string
		args  []any
	}{
		{name: "update", query: `UPDATE ledger_entries SET amount = amount WHERE transaction_id = $1`, args: []any{processedID}},
		{name: "delete", query: `DELETE FROM ledger_entries WHERE transaction_id = $1`, args: []any{processedID}},
		{name: "truncate", query: `TRUNCATE ledger_entries`},
	} {
		t.Run("ledger_is_immutable_"+mutation.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, mutation.query, mutation.args...); err == nil {
				t.Fatalf("ledger %s unexpectedly succeeded", mutation.name)
			}
		})
	}

	pendingEvent := domain.NewWalletBalanceChangedEvent(wallet.ID(), processedID, processedID, domain.WalletBalanceChangedData{
		WalletID: wallet.ID(), TransactionID: processedID, Direction: "DEBIT",
		Money:         domain.NewMoneyFromInt64(8000, "BRL"),
		BalanceBefore: domain.NewMoneyFromInt64(10000, "BRL"),
		BalanceAfter:  domain.NewMoneyFromInt64(2000, "BRL"), WalletVersion: 2,
	})
	pendingEventTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pendingEventTx.Rollback(ctx)
	if err := outboxRepo.Save(ctx, pendingEventTx, pendingEvent); err != nil {
		t.Fatal(err)
	}
	claimed, err := outboxRepo.Claim(ctx, 1000, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	containsPendingEvent := func(records []postgres.OutboxRecord) bool {
		for _, record := range records {
			if record.EventID == pendingEvent.EventID {
				return true
			}
		}
		return false
	}
	if containsPendingEvent(claimed) {
		t.Fatal("outbox exposed an event before its transaction committed")
	}
	if err := pendingEventTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	claimed, err = outboxRepo.Claim(ctx, 1000, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !containsPendingEvent(claimed) {
		t.Fatal("committed outbox event was not claimable")
	}
	if err := outboxRepo.MarkPublished(ctx, pendingEvent.EventID); err != nil {
		t.Fatal(err)
	}

	var outboxEventIDs []string
	if err := txManager.RunInTx(ctx, func(tx postgres.Tx) error {
		for range 20 {
			event := domain.NewWagerTransactionProcessedEvent(wallet.ID(), processedID, processedID,
				domain.WagerTransactionEventData{
					TransactionID: processedID, WalletID: wallet.ID(), Status: domain.StatusProcessed,
					Balance: domain.NewMoneyFromInt64(2000, "BRL"),
				})
			outboxEventIDs = append(outboxEventIDs, event.EventID)
			if err := outboxRepo.Save(ctx, tx, event); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	outboxReplicas := []*postgres.OutboxRepository{
		postgres.NewOutboxRepository(&postgres.DB{Pool: instancePools[0]}),
		postgres.NewOutboxRepository(&postgres.DB{Pool: instancePools[1]}),
	}
	claimStart := make(chan struct{})
	claimResults := make(chan []postgres.OutboxRecord, len(outboxReplicas))
	claimErrors := make(chan error, len(outboxReplicas))
	for _, repository := range outboxReplicas {
		repository := repository
		go func() {
			<-claimStart
			records, err := repository.Claim(ctx, 10, 30*time.Second)
			claimResults <- records
			claimErrors <- err
		}()
	}
	close(claimStart)
	claimedEventIDs := make(map[string]struct{}, len(outboxEventIDs))
	for range outboxReplicas {
		if err := <-claimErrors; err != nil {
			t.Fatal(err)
		}
		for _, record := range <-claimResults {
			if _, duplicate := claimedEventIDs[record.EventID]; duplicate {
				t.Fatalf("two publishers claimed event %s", record.EventID)
			}
			claimedEventIDs[record.EventID] = struct{}{}
		}
	}
	if len(claimedEventIDs) != len(outboxEventIDs) {
		t.Fatalf("publishers claimed %d/%d events", len(claimedEventIDs), len(outboxEventIDs))
	}

	recoveredEventID := outboxEventIDs[0]
	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET locked_until = now() - interval '1 second' WHERE event_id = $1`, recoveredEventID); err != nil {
		t.Fatal(err)
	}
	recovered, err := outboxReplicas[1].Claim(ctx, 1, 30*time.Second)
	if err != nil || len(recovered) != 1 || recovered[0].EventID != recoveredEventID {
		t.Fatalf("abandoned event lease was not recovered: records=%v, error=%v", recovered, err)
	}
	if err := outboxReplicas[1].Retry(ctx, recoveredEventID, -time.Second, errors.New("simulated publish interruption")); err != nil {
		t.Fatal(err)
	}
	retried, err := outboxReplicas[0].Claim(ctx, 1, 30*time.Second)
	if err != nil || len(retried) != 1 || retried[0].EventID != recoveredEventID || retried[0].Attempts < 3 {
		t.Fatalf("outbox retry did not preserve event identity: records=%v, error=%v", retried, err)
	}
	if err := outboxReplicas[0].MarkPublished(ctx, recoveredEventID); err != nil {
		t.Fatal(err)
	}

	replayInput := ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:" + processedExternalID,
		PlayerID: "player-main", WalletID: wallet.ID(), ExternalID: processedExternalID,
		RoundID: "round-race", GameID: "game", Amount: "80.00", Currency: "BRL", Type: domain.TypeBet,
	}
	var wg sync.WaitGroup
	for index := range 50 {
		wg.Add(1)
		instance := wagerInstances[index%len(wagerInstances)]
		go func(instance *WageringUseCase) {
			defer wg.Done()
			result, err := instance.ProcessWager(ctx, replayInput)
			if err != nil {
				t.Errorf("parallel replay failed: %v", err)
				return
			}
			if !result.IdempotentReplay || result.Transaction.ID() != processedID || result.Balance.Amount() != 2000 {
				t.Errorf("unexpected replay result: replay=%t id=%s balance=%s",
					result.IdempotentReplay, result.Transaction.ID(), result.Balance.String())
			}
		}(instance)
	}
	wg.Wait()
	if _, err := wagerUC.ProcessWager(ctx, ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:race-bet-1",
		PlayerID: "player-main", WalletID: wallet.ID(), ExternalID: "race-bet-1",
		RoundID: "round-race", GameID: "game", Amount: "1.00", Currency: "BRL", Type: domain.TypeBet,
	}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("reused idempotency key error = %v, want conflict", err)
	}

	walletA, err := walletUC.CreateWallet(ctx, "player-a", domain.NewMoneyFromInt64(0, "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	walletB, err := walletUC.CreateWallet(ctx, "player-b", domain.NewMoneyFromInt64(0, "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		player string
		wallet domain.Wallet
		ext    string
	}{{"player-a", walletA, "parallel-wallet-a"}, {"player-b", walletB, "parallel-wallet-b"}} {
		test := test
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := wagerUC.ProcessWager(ctx, ProcessWagerInput{
				ProviderID: "provider-a", IdempotencyKey: "provider-a:" + test.ext,
				PlayerID: test.player, WalletID: test.wallet.ID(), ExternalID: test.ext,
				RoundID: "parallel", GameID: "game", Amount: "1.00", Currency: "BRL", Type: domain.TypeBet,
			})
			if err != nil {
				t.Errorf("independent wallet processing failed: %v", err)
			}
		}()
	}
	wg.Wait()

	winWallet, err := walletUC.CreateWallet(ctx, "player-wins", domain.NewMoneyFromInt64(10000, "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	referenceBet, err := wagerUC.ProcessWager(ctx, ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:win-reference-bet",
		PlayerID: "player-wins", WalletID: winWallet.ID(), ExternalID: "win-reference-bet",
		RoundID: "win-round", GameID: "game", Amount: "10.00", Currency: "BRL", Type: domain.TypeBet,
	})
	if err != nil || referenceBet.Transaction.Status() != domain.StatusProcessed {
		t.Fatalf("reference BET result = %s, %v", referenceBet.Transaction.Status(), err)
	}
	winResult, err := wagerUC.ProcessWager(ctx, ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:win-referenced",
		PlayerID: "player-wins", WalletID: winWallet.ID(), ExternalID: "win-referenced",
		RoundID: "win-round", GameID: "game", Amount: "5.00", Currency: "BRL",
		Type: domain.TypeWin, ReferenceID: "win-reference-bet",
	})
	if err != nil || winResult.Transaction.Status() != domain.StatusProcessed || winResult.Balance.Amount() != 9500 {
		t.Fatalf("referenced WIN result = status %s, balance %s, error %v",
			winResult.Transaction.Status(), winResult.Balance.String(), err)
	}
	mismatchedRound, err := wagerUC.ProcessWager(ctx, ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:win-wrong-round",
		PlayerID: "player-wins", WalletID: winWallet.ID(), ExternalID: "win-wrong-round",
		RoundID: "different-round", GameID: "game", Amount: "1.00", Currency: "BRL",
		Type: domain.TypeWin, ReferenceID: "win-reference-bet",
	})
	if err != nil || mismatchedRound.Transaction.Status() != domain.StatusRejected ||
		mismatchedRound.Transaction.FailureCode() != "REFERENCE_SCOPE_MISMATCH" {
		t.Fatalf("WIN with mismatched round = status %s, failure %s, error %v",
			mismatchedRound.Transaction.Status(), mismatchedRound.Transaction.FailureCode(), err)
	}

	pendingWin, err := wagerUC.ProcessWager(ctx, ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:win-before-bet",
		PlayerID: "player-wins", WalletID: winWallet.ID(), ExternalID: "win-before-bet",
		RoundID: "future-round", GameID: "game", Amount: "7.00", Currency: "BRL",
		Type: domain.TypeWin, ReferenceID: "future-reference-bet",
	})
	if err != nil || pendingWin.Transaction.Status() != domain.StatusPendingReference {
		t.Fatalf("WIN before reference = status %s, error %v", pendingWin.Transaction.Status(), err)
	}
	futureBet, err := wagerUC.ProcessWager(ctx, ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:future-reference-bet",
		PlayerID: "player-wins", WalletID: winWallet.ID(), ExternalID: "future-reference-bet",
		RoundID: "future-round", GameID: "game", Amount: "7.00", Currency: "BRL", Type: domain.TypeBet,
	})
	if err != nil || futureBet.Transaction.Status() != domain.StatusProcessed {
		t.Fatalf("future reference BET = status %s, error %v", futureBet.Transaction.Status(), err)
	}
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET next_attempt_at = now() WHERE id = $1`, pendingWin.Transaction.ID()); err != nil {
		t.Fatal(err)
	}
	if err := wagerUC.ResolvePendingReferences(ctx); err != nil {
		t.Fatal(err)
	}
	resolvedWin, err := wagerUC.GetProviderTransaction(ctx, "provider-a", "win-before-bet")
	if err != nil || resolvedWin.Status() != domain.StatusProcessed || resolvedWin.ResultBalance().Amount() != 9500 {
		t.Fatalf("resolved WIN = status %s, result balance %s, error %v",
			resolvedWin.Status(), resolvedWin.ResultBalance().String(), err)
	}

	inboxInput := ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:inbox-bet",
		PlayerID: "player-wins", WalletID: winWallet.ID(), ExternalID: "inbox-bet",
		RoundID: "inbox-round", GameID: "game", Amount: "1.00", Currency: "BRL",
		Type: domain.TypeBet, MessageID: "message-1", MessagePayloadHash: strings.Repeat("a", 64),
	}
	if _, err := wagerUC.ProcessWager(ctx, inboxInput); err != nil {
		t.Fatalf("first inbox delivery: %v", err)
	}
	var inboxReceivedAt time.Time
	var inboxCompletedAt *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT received_at, completed_at FROM consumer_inbox
		WHERE consumer_name = $1 AND message_id = $2
	`, postgres.WagerTransactionsConsumer, inboxInput.MessageID).Scan(&inboxReceivedAt, &inboxCompletedAt); err != nil {
		t.Fatal(err)
	}
	if inboxCompletedAt == nil || inboxCompletedAt.Before(inboxReceivedAt) {
		t.Fatalf("inbox timestamps are incomplete or out of order: received=%s completed=%v",
			inboxReceivedAt, inboxCompletedAt)
	}
	if result, err := wagerUC.ProcessWager(ctx, inboxInput); err != nil || !result.IdempotentReplay {
		t.Fatalf("inbox replay = replay %t, error %v", result.IdempotentReplay, err)
	}
	conflictingInbox := inboxInput
	conflictingInbox.MessagePayloadHash = strings.Repeat("b", 64)
	if _, err := wagerUC.ProcessWager(ctx, conflictingInbox); !errors.Is(err, postgres.ErrInboxHashConflict) {
		t.Fatalf("inbox payload conflict = %v, want ErrInboxHashConflict", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO consumer_inbox (consumer_name, message_id, payload_hash)
		VALUES ('independent-consumer', 'message-1', $1)
	`, strings.Repeat("c", 64)); err != nil {
		t.Fatalf("same message id for another consumer: %v", err)
	}

	refundBet, err := wagerUC.ProcessWager(ctx, ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:refund-reference",
		PlayerID: "player-wins", WalletID: winWallet.ID(), ExternalID: "refund-reference",
		RoundID: "refund-round", GameID: "game", Amount: "2.00", Currency: "BRL", Type: domain.TypeBet,
	})
	if err != nil || refundBet.Transaction.Status() != domain.StatusProcessed {
		t.Fatalf("refund reference BET = status %s, error %v", refundBet.Transaction.Status(), err)
	}
	referencedWin, err := wagerUC.ProcessWager(ctx, ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:refund-reference-win",
		PlayerID: "player-wins", WalletID: winWallet.ID(), ExternalID: "refund-reference-win",
		RoundID: "refund-round", GameID: "game", Amount: "0.50", Currency: "BRL",
		Type: domain.TypeWin, ReferenceID: "refund-reference",
	})
	if err != nil || referencedWin.Transaction.Status() != domain.StatusProcessed {
		t.Fatalf("WIN referencing refund BET = status %s, error %v",
			referencedWin.Transaction.Status(), err)
	}
	mismatchedRefund, err := wagerUC.ProcessWager(ctx, ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:refund-wrong-round",
		PlayerID: "player-wins", WalletID: winWallet.ID(), ExternalID: "refund-wrong-round",
		RoundID: "different-round", GameID: "game", Amount: "2.00", Currency: "BRL",
		Type: domain.TypeRefund, ReferenceID: "refund-reference",
	})
	if err != nil || mismatchedRefund.Transaction.FailureCode() != "REFERENCE_SCOPE_MISMATCH" {
		t.Fatalf("REFUND with mismatched round = failure %s, error %v",
			mismatchedRefund.Transaction.FailureCode(), err)
	}
	refund, err := wagerUC.ProcessWager(ctx, ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:refund",
		PlayerID: "player-wins", WalletID: winWallet.ID(), ExternalID: "refund",
		RoundID: "refund-round", GameID: "game", Amount: "2.00", Currency: "BRL",
		Type: domain.TypeRefund, ReferenceID: "refund-reference",
	})
	if err != nil || refund.Transaction.Status() != domain.StatusProcessed {
		t.Fatalf("REFUND result = status %s, error %v", refund.Transaction.Status(), err)
	}
	doubleRefund, err := wagerUC.ProcessWager(ctx, ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:refund-again",
		PlayerID: "player-wins", WalletID: winWallet.ID(), ExternalID: "refund-again",
		RoundID: "refund-round", GameID: "game", Amount: "2.00", Currency: "BRL",
		Type: domain.TypeRefund, ReferenceID: "refund-reference",
	})
	if err != nil || doubleRefund.Transaction.FailureCode() != "REFERENCE_ALREADY_REVERSED" {
		t.Fatalf("second REFUND = failure %s, error %v", doubleRefund.Transaction.FailureCode(), err)
	}
	rollbackWin, err := wagerUC.ProcessWager(ctx, ProcessWagerInput{
		ProviderID: "provider-a", IdempotencyKey: "provider-a:rollback-win",
		PlayerID: "player-wins", WalletID: winWallet.ID(), ExternalID: "rollback-win",
		RoundID: "win-round", GameID: "game", Amount: "5.00", Currency: "BRL",
		Type: domain.TypeRollback, ReferenceID: "win-referenced",
	})
	if err != nil || rollbackWin.Transaction.Status() != domain.StatusProcessed {
		t.Fatalf("ROLLBACK of WIN = status %s, error %v", rollbackWin.Transaction.Status(), err)
	}
}

func TestPostgresMigrationsRoundTrip(t *testing.T) {
	connectionString := os.Getenv("TEST_DATABASE_URL")
	if connectionString == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL migration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "migration_" + uuid.NewString()[:8]
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+identifier+" CASCADE")
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+identifier+" CASCADE")
		t.Fatal(err)
	}
	defer func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+identifier+" CASCADE")
	}()

	migrationNames := []string{
		"001_initial_schema",
		"002_reference_and_inbox",
		"003_failed_state_and_opening_once",
		"004_reversal_index_and_wallet_ledger_guard",
	}
	for cycle := 0; cycle < 2; cycle++ {
		for _, name := range migrationNames {
			migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", name+".up.sql"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, string(migration), pgx.QueryExecModeSimpleProtocol); err != nil {
				t.Fatalf("cycle %d apply migration %s: %v", cycle+1, name, err)
			}
		}
		for index := len(migrationNames) - 1; index >= 0; index-- {
			name := migrationNames[index]
			migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", name+".down.sql"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, string(migration), pgx.QueryExecModeSimpleProtocol); err != nil {
				t.Fatalf("cycle %d rollback migration %s: %v", cycle+1, name, err)
			}
		}
	}
}
