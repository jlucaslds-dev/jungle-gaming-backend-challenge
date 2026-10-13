package observability

import (
	"fmt"
	"net/http"
	"sort"
	"sync/atomic"
	"time"
)

var (
	wagerStatuses = map[string]*atomic.Uint64{
		"PROCESSED": {}, "REJECTED": {}, "PENDING_REFERENCE": {}, "FAILED": {},
	}
	replays               atomic.Uint64
	conflicts             atomic.Uint64
	concurrencyConflicts  atomic.Uint64
	sqsRetries            atomic.Uint64
	referenceRetries      atomic.Uint64
	sqsDeadLetters        atomic.Uint64
	outboxPublishFailures atomic.Uint64
	reconciliationIssues  atomic.Uint64
	processingCount       atomic.Uint64
	processingNanos       atomic.Uint64
	outboxLagNanos        atomic.Uint64
)

var latencyBounds = []time.Duration{
	5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond,
	100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, time.Second,
	2500 * time.Millisecond, 5 * time.Second, 10 * time.Second,
}

var latencyBuckets [11]atomic.Uint64

func RecordWager(status string, elapsed time.Duration) {
	if counter, ok := wagerStatuses[status]; ok {
		counter.Add(1)
	}
	processingCount.Add(1)
	processingNanos.Add(uint64(elapsed))
	for i, bound := range latencyBounds {
		if elapsed <= bound {
			latencyBuckets[i].Add(1)
		}
	}
}

func RecordReplay()              { replays.Add(1) }
func RecordConflict()            { conflicts.Add(1) }
func RecordConcurrencyConflict() { concurrencyConflicts.Add(1) }
func RecordSQSRetry()            { sqsRetries.Add(1) }
func RecordReferenceRetry()      { referenceRetries.Add(1) }
func RecordSQSDeadLetter()       { sqsDeadLetters.Add(1) }
func RecordOutboxFailure()       { outboxPublishFailures.Add(1) }
func RecordReconciliationIssue() { reconciliationIssues.Add(1) }

func ObserveOutboxLag(lag time.Duration) {
	outboxLagNanos.Store(uint64(lag))
}

func Handler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	statuses := make([]string, 0, len(wagerStatuses))
	for status := range wagerStatuses {
		statuses = append(statuses, status)
	}
	sort.Strings(statuses)
	for _, status := range statuses {
		_, _ = fmt.Fprintf(w, "wager_results_total{status=%q} %d\n", status, wagerStatuses[status].Load())
	}
	_, _ = fmt.Fprintf(w, "wager_idempotent_replays_total %d\n", replays.Load())
	_, _ = fmt.Fprintf(w, "wager_idempotency_conflicts_total %d\n", conflicts.Load())
	_, _ = fmt.Fprintf(w, "wallet_concurrency_conflicts_total %d\n", concurrencyConflicts.Load())
	_, _ = fmt.Fprintf(w, "sqs_retries_total %d\n", sqsRetries.Load())
	_, _ = fmt.Fprintf(w, "wager_reference_retries_total %d\n", referenceRetries.Load())
	_, _ = fmt.Fprintf(w, "sqs_dlq_total %d\n", sqsDeadLetters.Load())
	_, _ = fmt.Fprintf(w, "outbox_publish_failures_total %d\n", outboxPublishFailures.Load())
	_, _ = fmt.Fprintf(w, "wallet_reconciliation_divergences_total %d\n", reconciliationIssues.Load())
	count := processingCount.Load()
	for i, bound := range latencyBounds {
		_, _ = fmt.Fprintf(w, "wager_processing_duration_seconds_bucket{le=%q} %d\n",
			fmt.Sprintf("%g", bound.Seconds()), latencyBuckets[i].Load())
	}
	_, _ = fmt.Fprintf(w, "wager_processing_duration_seconds_bucket{le=\"+Inf\"} %d\n", count)
	_, _ = fmt.Fprintf(w, "wager_processing_duration_seconds_count %d\n", count)
	_, _ = fmt.Fprintf(w, "wager_processing_duration_seconds_sum %f\n", float64(processingNanos.Load())/float64(time.Second))
	_, _ = fmt.Fprintf(w, "outbox_last_publish_lag_seconds %f\n", float64(outboxLagNanos.Load())/float64(time.Second))
}
