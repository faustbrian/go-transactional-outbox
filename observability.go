package outbox

import (
	"context"
	"time"
)

// Operation identifies a bounded outbox lifecycle action.
type Operation string

const (
	// OperationClaim identifies claim acquisition.
	OperationClaim Operation = "claim"
	// OperationPublish identifies a publisher attempt, not consumer handling.
	OperationPublish Operation = "publish"
	// OperationDeliver identifies durable delivery-mark transition.
	OperationDeliver Operation = "deliver"
	// OperationRetry identifies retry scheduling.
	OperationRetry Operation = "retry"
	// OperationDeadLetter identifies terminal-failure transition.
	OperationDeadLetter Operation = "dead_letter"
	// OperationRelease identifies claim release.
	OperationRelease Operation = "release"
	// OperationExtendLease identifies lease renewal.
	OperationExtendLease Operation = "extend_lease"
	// OperationReplay identifies duplicate-producing administrative replay.
	OperationReplay Operation = "replay"
	// OperationPrune identifies terminal deletion.
	OperationPrune Operation = "prune"
	// OperationArchive identifies archive-before-delete.
	OperationArchive Operation = "archive"
)

// Outcome identifies whether an operation completed successfully.
type Outcome string

const (
	// OutcomeSuccess classifies successful completion of the named operation.
	OutcomeSuccess Outcome = "success"
	// OutcomeFailure classifies failure of the named operation.
	OutcomeFailure Outcome = "failure"
)

// Event contains payload-safe diagnostics for one outbox operation. It never
// includes payloads, metadata, or error text.
type Event struct {
	// Operation Bounded lifecycle operation name when produced by this package.
	Operation Operation
	// Outcome Operation result classification, not end-consumer processing state.
	Outcome Outcome
	// Count Operation-specific record count; not a uniform delivered-message total.
	Count int
	// MessageID Caller-provided identity; potentially sensitive/high-cardinality.
	MessageID string
	// Topic Caller-provided routing label; potentially sensitive/high-cardinality.
	Topic string
	// Attempts Claim count carried with an envelope, not delivery confirmations.
	Attempts int
	// Duration Diagnostic elapsed time; produced relay/store durations clamp negatives.
	Duration time.Duration
}

// BacklogStats summarizes operationally relevant message states.
type BacklogStats struct {
	// Pending Current pending row count, including future-scheduled rows.
	Pending int64
	// Leased Current leased row count; expiry does not automatically change stored state.
	Leased int64
	// Dead Current dead-letter row count.
	Dead int64
	// OldestPendingAt Optional oldest pending availability time, not creation time.
	OldestPendingAt *time.Time
}

// Observer receives structured lifecycle events.
type Observer interface {
	// Observe runs synchronously and may be concurrent across store/relay calls.
	// Implementations own latency and privacy; library panic containment cannot
	// interrupt a blocked callback. IDs/topics may contain sensitive values.
	Observe(context.Context, Event)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(context.Context, Event)

// Observe forwards the event to the wrapped function.
func (observe ObserverFunc) Observe(ctx context.Context, event Event) {
	observe(ctx, event)
}
