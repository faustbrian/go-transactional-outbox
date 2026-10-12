// Package relay publishes claimed outbox envelopes with bounded concurrency.
package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/faustbrian/go-transactional-outbox/v2"
	"github.com/faustbrian/go-transactional-outbox/v2/postgres"
)

const (
	defaultBatchSize         = 100
	defaultLeaseDuration     = 30 * time.Second
	defaultMaxAttempts       = 10
	defaultPollInterval      = time.Second
	defaultTransitionTimeout = 5 * time.Second
	maximumBackoff           = time.Minute
	maximumBatchSize         = 1000
	maximumWorkers           = 256
	maximumAttempts          = 10_000
	maximumLeaseDuration     = 24 * time.Hour
)

var (
	// ErrStoreRequired requires a persistence interface.
	ErrStoreRequired = errors.New("outbox/relay: store is required")
	// ErrPublisherRequired requires a publication interface.
	ErrPublisherRequired = errors.New("outbox/relay: publisher is required")
	// ErrPublisherPanic contains publisher panic without its value.
	ErrPublisherPanic = errors.New("outbox/relay: publisher panicked")
	// ErrClassifierPanic reports contained classifier panic with transient fallback.
	ErrClassifierPanic = errors.New("outbox/relay: error classifier panicked")
	// ErrInvalidErrorClass reports unknown classification with transient fallback.
	ErrInvalidErrorClass = errors.New("outbox/relay: error classifier returned an invalid class")
	// ErrBackoffPanic reports contained delay callback panic with zero fallback.
	ErrBackoffPanic = errors.New("outbox/relay: backoff function panicked")
	// ErrInvalidBackoff reports negative/over-minute delay after clamping.
	ErrInvalidBackoff = errors.New("outbox/relay: backoff result is outside bounds")
	// ErrHeartbeatPanic contains heartbeat panic at its worker boundary.
	ErrHeartbeatPanic = errors.New("outbox/relay: heartbeat panicked")
	// ErrClaimBatchOverflow refuses a store result larger than requested without processing it.
	ErrClaimBatchOverflow = errors.New("outbox/relay: store returned more claims than requested")
	// ErrOwnerRequired requires a nonempty claim-owner label.
	ErrOwnerRequired = errors.New("outbox/relay: owner is required")
	// ErrInvalidConfig refuses unsupported relay bounds or serialization.
	ErrInvalidConfig = errors.New("outbox/relay: configuration values must be positive")
)

// ErrorClass controls whether a failed publication is retried or terminated.
type ErrorClass uint8

const (
	// ErrorTransient permits later publication, which may duplicate accepted work.
	ErrorTransient ErrorClass = iota
	// ErrorPermanent terminates the failed envelope according to relay policy.
	ErrorPermanent
)

// Publisher accepts an envelope. Returning nil means the publisher accepted
// it, not that exactly-once delivery has been achieved.
type Publisher interface {
	// Publish may overlap other calls; cooperate with cancellation. The relay
	// joins it and cannot force completion or interrupt arbitrary blocking.
	Publish(context.Context, outbox.Envelope) error
}

// Store is the lease-safe persistence contract used by Relay.
type Store interface {
	// Ping reports point-in-time store readiness.
	Ping(context.Context) error
	// Claim returns at most the requested batch; overflow is refused without
	// processing and acquired leases are left to expire.
	Claim(context.Context, postgres.ClaimRequest) ([]postgres.Claim, error)
	// ExtendLease renews only a still-valid ID/token ownership proof.
	ExtendLease(context.Context, postgres.LeaseRef, time.Duration) (time.Time, error)
	// MarkDelivered records publisher acceptance, not consumer processing.
	MarkDelivered(context.Context, postgres.LeaseRef) error
	// Retry durably schedules a failed publication; duplicates remain possible.
	Retry(context.Context, postgres.LeaseRef, time.Duration, error) error
	// DeadLetter makes the failed claim terminal under its ownership proof.
	DeadLetter(context.Context, postgres.LeaseRef, error) error
	// ReleaseLease relinquishes ownership; relay cancellation uses a separate
	// bounded cleanup context, without guaranteeing transition success.
	ReleaseLease(context.Context, postgres.LeaseRef) error
}

// Config bounds each polling cycle and injects borrowed policy seams.
type Config struct {
	// Owner Required opaque claim-owner label; PostgreSQL applies its own 255-byte ceiling.
	Owner string
	// BatchSize Per-RunOnce claim count, default 100 and maximum 1000.
	BatchSize int
	// Workers Per-RunOnce publication workers, default runtime.NumCPU and maximum 256; not an instance-wide guard.
	Workers int
	// LeaseDuration Claim/renewal duration, default 30 seconds and maximum 24 hours; not a publish timeout.
	LeaseDuration time.Duration
	// LeaseRenewalInterval Heartbeat interval, default half lease duration and required strictly shorter than it.
	LeaseRenewalInterval time.Duration
	// MaxAttempts Failure terminal threshold for claim count, default 10 and maximum 10000. Success can still deliver at the threshold.
	MaxAttempts int
	// PollInterval Wait after a partial batch, default one second; no absolute upper ceiling is added here.
	PollInterval time.Duration
	// TransitionTimeout Cancellation-release cleanup timeout, default five seconds; does not wrap all transitions.
	TransitionTimeout time.Duration
	// Clock Borrowed diagnostic clock, default time.Now; panics yield zero time.
	Clock func() time.Time
	// Backoff Borrowed concurrent callback; nil selects full jitter. Negative/panic delays become zero; over-minute values clamp and report errors.
	Backoff func(attempt int) time.Duration
	// ClassifyError Borrowed concurrent failure policy; nil is transient. Panic/unknown class selects transient retry and returns policy error.
	ClassifyError func(error) ErrorClass
	// Wait Borrowed polling callback, default context timer; it must return cooperatively. Panics are not contained.
	Wait func(context.Context, time.Duration) error
	// Serialization Claim scope forwarded to Store; zero disables scoped serialization.
	Serialization postgres.SerializationMode
	// Observer Borrowed synchronous callback; panic contained but latency/privacy/concurrency remain caller-owned.
	Observer outbox.Observer
	// Logger Borrowed slog logger/handler; IDs/topics are emitted and handler privacy/latency remain caller-owned.
	Logger *slog.Logger
	// Heartbeat Borrowed per-active-publication maintenance loop; it must honor cancellation and return. Panic is contained, arbitrary blocking is not.
	Heartbeat func(context.Context, time.Duration, func(context.Context) error) error
}

// Result summarizes one bounded polling cycle.
type Result struct {
	// Claimed Store-returned count, including overflow batches not processed.
	Claimed int
	// Published Nil publication results with acceptable heartbeat completion, not all possible broker acceptances.
	Published int
	// Delivered Successful MarkDelivered calls, not consumer processing.
	Delivered int
	// Retried Successfully scheduled retry transitions, including those returning a policy error.
	Retried int
	// DeadLettered Successful terminal-failure transitions.
	DeadLettered int
	// Released Successful cancellation-release transitions.
	Released int
}

// Relay coordinates claims, publisher calls, and state transitions. Bounds
// apply per invocation, not across concurrent RunOnce/Run calls. Store, publisher
// and callbacks must support any such concurrency; Relay closes none of them.
type Relay struct {
	store     Store
	publisher Publisher
	config    Config
}

// New validates and constructs a relay.
func New(store Store, publisher Publisher, config Config) (*Relay, error) {
	if store == nil {
		return nil, ErrStoreRequired
	}
	if publisher == nil {
		return nil, ErrPublisherRequired
	}
	if config.Owner == "" {
		return nil, ErrOwnerRequired
	}
	if config.BatchSize == 0 {
		config.BatchSize = defaultBatchSize
	}
	if config.Workers == 0 {
		config.Workers = runtime.NumCPU()
	}
	if config.LeaseDuration == 0 {
		config.LeaseDuration = defaultLeaseDuration
	}
	if config.LeaseRenewalInterval == 0 {
		config.LeaseRenewalInterval = config.LeaseDuration / 2
		if config.LeaseRenewalInterval == 0 {
			config.LeaseRenewalInterval = config.LeaseDuration
		}
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = defaultMaxAttempts
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaultPollInterval
	}
	if config.TransitionTimeout == 0 {
		config.TransitionTimeout = defaultTransitionTimeout
	}
	if invalidConfig(config) {
		return nil, ErrInvalidConfig
	}
	if config.Serialization > postgres.SerializeByTopic {
		return nil, ErrInvalidConfig
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	config.Clock = containClockPanic(config.Clock)
	if config.Backoff == nil {
		config.Backoff = exponentialBackoff
	}
	if config.ClassifyError == nil {
		config.ClassifyError = func(error) ErrorClass { return ErrorTransient }
	}
	if config.Wait == nil {
		config.Wait = waitContext
	}
	if config.Heartbeat == nil {
		config.Heartbeat = maintainLease
	}

	return &Relay{store: store, publisher: publisher, config: config}, nil
}

func invalidConfig(config Config) bool {
	return config.BatchSize < 0 || config.BatchSize > maximumBatchSize ||
		config.Workers < 0 || config.Workers > maximumWorkers ||
		config.LeaseDuration > maximumLeaseDuration ||
		config.LeaseRenewalInterval < 0 ||
		config.LeaseRenewalInterval >= config.LeaseDuration || config.MaxAttempts < 0 ||
		config.MaxAttempts > maximumAttempts ||
		config.PollInterval < 0 || config.TransitionTimeout < 0
}

func containClockPanic(clock func() time.Time) func() time.Time {
	return func() (value time.Time) {
		defer func() { _ = recover() }()

		return clock()
	}
}

// Run polls until cancellation. Full batches are followed immediately to
// drain backlog; partial batches wait through the injected polling function.
func (relay *Relay) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		result, err := relay.RunOnce(ctx)
		switch err {
		case nil:
		default:
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		switch relay.config.BatchSize - result.Claimed {
		case 0:
			continue
		}
		err = relay.config.Wait(ctx, relay.config.PollInterval)
		switch err {
		case nil:
		default:
			if ctx.Err() != nil {
				return nil
			}

			return fmt.Errorf("outbox/relay: wait for poll: %w", err)
		}
	}
}

// RunOnce claims at most one batch and drains it with bounded worker
// concurrency. Publisher failures that are durably scheduled are reflected in
// Result; persistence failures and contained policy errors are joined with
// partial counts after all workers finish. Peers are not canceled on one error.
// Only active publications have heartbeats; queued claims can expire before
// starting. Cancellation stops cooperatively and joins publisher/heartbeat
// completion, so a blocked callback prevents bounded shutdown. Broker acceptance
// can precede a renewal/mark failure and cause duplicate publication later.
func (relay *Relay) RunOnce(ctx context.Context) (Result, error) {
	startedAt := relay.config.Clock()
	claims, err := relay.store.Claim(ctx, postgres.ClaimRequest{
		Owner:         relay.config.Owner,
		Limit:         relay.config.BatchSize,
		LeaseDuration: relay.config.LeaseDuration,
		Serialization: relay.config.Serialization,
	})
	switch err {
	case nil:
	default:
		relay.observe(ctx, outbox.Event{Operation: outbox.OperationClaim, Outcome: outbox.OutcomeFailure,
			Duration: relay.durationSince(startedAt)})
		return Result{}, fmt.Errorf("outbox/relay: claim: %w", err)
	}
	relay.observe(ctx, outbox.Event{Operation: outbox.OperationClaim, Outcome: outbox.OutcomeSuccess,
		Count: len(claims), Duration: relay.durationSince(startedAt)})

	result := Result{Claimed: len(claims)}
	switch min(len(claims), relay.config.BatchSize) {
	case len(claims):
	default:
		return result, fmt.Errorf("%w: got %d, requested %d",
			ErrClaimBatchOverflow, len(claims), relay.config.BatchSize)
	}
	// This invocation owns and closes the queue before workers start. Queued
	// leases are not renewed here; only process starts a heartbeat.
	jobs := make(chan postgres.Claim, len(claims))
	for _, claim := range claims {
		jobs <- claim
	}
	close(jobs)

	workerCount := min(relay.config.Workers, len(claims))
	var wait sync.WaitGroup
	var lock sync.Mutex
	var transitionErrors []error
	for range workerCount {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for claim := range jobs {
				published, transition, transitionErr := relay.process(ctx, claim)
				lock.Lock()
				if published {
					result.Published++
				}
				switch transition {
				case transitionDelivered:
					result.Delivered++
				case transitionRetried:
					result.Retried++
				case transitionDeadLettered:
					result.DeadLettered++
				case transitionReleased:
					result.Released++
				}
				if transitionErr != nil {
					transitionErrors = append(transitionErrors, transitionErr)
				}
				lock.Unlock()
			}
		}()
	}
	wait.Wait()

	return result, errors.Join(transitionErrors...)
}

type transition uint8

const (
	transitionNone transition = iota
	transitionDelivered
	transitionRetried
	transitionDeadLettered
	transitionReleased
)

func (relay *Relay) process(ctx context.Context, claim postgres.Claim) (bool, transition, error) {
	lease := postgres.LeaseRef{ID: claim.Envelope.ID, Token: claim.LeaseToken}
	publishContext, cancelPublish := context.WithCancel(ctx)
	var publishFinished atomic.Bool
	type heartbeatResult struct {
		err                  error
		afterPublishFinished bool
	}
	heartbeatDone := make(chan heartbeatResult, 1)
	go func() {
		heartbeatErr := invokeHeartbeat(
			relay.config.Heartbeat,
			publishContext,
			relay.config.LeaseRenewalInterval,
			func(heartbeatContext context.Context) error {
				startedAt := relay.config.Clock()
				_, err := relay.store.ExtendLease(heartbeatContext, lease, relay.config.LeaseDuration)
				outcome := outbox.OutcomeSuccess
				switch err {
				case nil:
				default:
					outcome = outbox.OutcomeFailure
				}
				relay.observeEnvelope(heartbeatContext, claim.Envelope, outbox.OperationExtendLease, outcome, startedAt)

				return err
			},
		)
		afterPublishFinished := publishFinished.Load()
		if heartbeatShouldCancelPublish(heartbeatErr, afterPublishFinished) {
			cancelPublish()
		}
		heartbeatDone <- heartbeatResult{
			err:                  heartbeatErr,
			afterPublishFinished: afterPublishFinished,
		}
	}()
	publishStartedAt := relay.config.Clock()
	publishErr := relay.publish(publishContext, claim.Envelope)
	publishFinished.Store(true)
	cancelPublish()
	heartbeat := <-heartbeatDone
	expectedHeartbeatStop := heartbeatStopExpected(ctx.Err(), heartbeat.err, heartbeat.afterPublishFinished)
	if heartbeat.err != nil && !expectedHeartbeatStop {
		relay.observeEnvelope(ctx, claim.Envelope, outbox.OperationPublish, outbox.OutcomeFailure, publishStartedAt)

		return false, transitionNone, fmt.Errorf("outbox/relay: renew lease for %q: %w", claim.Envelope.ID, heartbeat.err)
	}
	if publishErr == nil {
		relay.observeEnvelope(ctx, claim.Envelope, outbox.OperationPublish, outbox.OutcomeSuccess, publishStartedAt)
		transitionStartedAt := relay.config.Clock()
		if err := relay.store.MarkDelivered(ctx, lease); err != nil {
			relay.observeEnvelope(ctx, claim.Envelope, outbox.OperationDeliver, outbox.OutcomeFailure, transitionStartedAt)
			return true, transitionNone, fmt.Errorf("outbox/relay: mark %q delivered: %w", claim.Envelope.ID, err)
		}
		relay.observeEnvelope(ctx, claim.Envelope, outbox.OperationDeliver, outbox.OutcomeSuccess, transitionStartedAt)

		return true, transitionDelivered, nil
	}
	relay.observeEnvelope(ctx, claim.Envelope, outbox.OperationPublish, outbox.OutcomeFailure, publishStartedAt)
	if canceledPublish(ctx.Err(), publishErr) {
		cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), relay.config.TransitionTimeout)
		defer cancel()
		transitionStartedAt := relay.config.Clock()
		if err := relay.store.ReleaseLease(cleanupContext, lease); err != nil {
			relay.observeEnvelope(cleanupContext, claim.Envelope, outbox.OperationRelease, outbox.OutcomeFailure, transitionStartedAt)
			return false, transitionNone, fmt.Errorf("outbox/relay: release %q: %w", claim.Envelope.ID, err)
		}
		relay.observeEnvelope(cleanupContext, claim.Envelope, outbox.OperationRelease, outbox.OutcomeSuccess, transitionStartedAt)

		return false, transitionReleased, nil
	}

	if claim.Envelope.Attempts >= relay.config.MaxAttempts {
		transitionStartedAt := relay.config.Clock()
		if err := relay.store.DeadLetter(ctx, lease, publishErr); err != nil {
			relay.observeEnvelope(ctx, claim.Envelope, outbox.OperationDeadLetter, outbox.OutcomeFailure, transitionStartedAt)
			return false, transitionNone, fmt.Errorf("outbox/relay: dead letter %q: %w", claim.Envelope.ID, err)
		}
		relay.observeEnvelope(ctx, claim.Envelope, outbox.OperationDeadLetter, outbox.OutcomeSuccess, transitionStartedAt)

		return false, transitionDeadLettered, nil
	}
	errorClass, policyErr := classifyError(relay.config.ClassifyError, publishErr)
	if errorClass == ErrorPermanent {
		transitionStartedAt := relay.config.Clock()
		if err := relay.store.DeadLetter(ctx, lease, publishErr); err != nil {
			relay.observeEnvelope(ctx, claim.Envelope, outbox.OperationDeadLetter, outbox.OutcomeFailure, transitionStartedAt)
			return false, transitionNone, fmt.Errorf("outbox/relay: dead letter %q: %w", claim.Envelope.ID, err)
		}
		relay.observeEnvelope(ctx, claim.Envelope, outbox.OperationDeadLetter, outbox.OutcomeSuccess, transitionStartedAt)

		return false, transitionDeadLettered, nil
	}

	retryDelay, backoffErr := boundedBackoff(relay.config.Backoff, claim.Envelope.Attempts)
	policyErr = errors.Join(policyErr, backoffErr)
	transitionStartedAt := relay.config.Clock()
	if err := relay.store.Retry(ctx, lease, retryDelay, publishErr); err != nil {
		relay.observeEnvelope(ctx, claim.Envelope, outbox.OperationRetry, outbox.OutcomeFailure, transitionStartedAt)
		return false, transitionNone, fmt.Errorf("outbox/relay: retry %q: %w", claim.Envelope.ID, err)
	}
	relay.observeEnvelope(ctx, claim.Envelope, outbox.OperationRetry, outbox.OutcomeSuccess, transitionStartedAt)
	if policyErr != nil {
		return false, transitionRetried, fmt.Errorf("outbox/relay: apply failure policy for %q: %w", claim.Envelope.ID, policyErr)
	}

	return false, transitionRetried, nil
}

func classifyError(classifier func(error) ErrorClass, cause error) (class ErrorClass, err error) {
	defer func() {
		if recover() != nil {
			class = ErrorTransient
			err = ErrClassifierPanic
		}
	}()

	class = classifier(cause)
	if class != ErrorTransient && class != ErrorPermanent {
		return ErrorTransient, ErrInvalidErrorClass
	}

	return class, nil
}

func boundedBackoff(backoff func(int) time.Duration, attempt int) (delay time.Duration, err error) {
	defer func() {
		if recover() != nil {
			delay = 0
			err = ErrBackoffPanic
		}
	}()

	delay = backoff(attempt)
	if delay < 0 {
		return 0, ErrInvalidBackoff
	}
	if delay > maximumBackoff {
		return maximumBackoff, ErrInvalidBackoff
	}

	return delay, nil
}

func maintainLease(ctx context.Context, interval time.Duration, extend func(context.Context) error) error {
	for {
		switch err := waitContext(ctx, interval); err {
		case nil:
		default:
			return nil
		}
		switch err := extend(ctx); err {
		case nil:
		default:
			return err
		}
	}
}

func invokeHeartbeat(
	heartbeat func(context.Context, time.Duration, func(context.Context) error) error,
	ctx context.Context,
	interval time.Duration,
	extend func(context.Context) error,
) (err error) {
	defer func() {
		switch recover() {
		case nil:
		default:
			err = ErrHeartbeatPanic
		}
	}()

	return heartbeat(ctx, interval, extend)
}

func (relay *Relay) publish(ctx context.Context, envelope outbox.Envelope) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrPublisherPanic
		}
	}()

	return relay.publisher.Publish(ctx, envelope)
}

// Readiness verifies database connectivity and, when supported, publisher
// connectivity. All failed checks are returned together.
func (relay *Relay) Readiness(ctx context.Context) error {
	var readinessErrors []error
	if err := relay.store.Ping(ctx); err != nil {
		readinessErrors = append(readinessErrors, fmt.Errorf("outbox/relay: database readiness: %w", err))
	}
	if publisher, ok := relay.publisher.(interface{ Health(context.Context) error }); ok {
		if err := publisher.Health(ctx); err != nil {
			readinessErrors = append(readinessErrors, fmt.Errorf("outbox/relay: publisher readiness: %w", err))
		}
	}

	return errors.Join(readinessErrors...)
}

func (relay *Relay) observeEnvelope(
	ctx context.Context,
	envelope outbox.Envelope,
	operation outbox.Operation,
	outcome outbox.Outcome,
	startedAt time.Time,
) {
	relay.observe(ctx, outbox.Event{
		Operation: operation,
		Outcome:   outcome,
		Count:     1,
		MessageID: envelope.ID,
		Topic:     envelope.Topic,
		Attempts:  envelope.Attempts,
		Duration:  relay.durationSince(startedAt),
	})
}

func (relay *Relay) observe(ctx context.Context, event outbox.Event) {
	if relay.config.Observer != nil {
		containDiagnosticPanic(func() {
			relay.config.Observer.Observe(ctx, event)
		})
	}
	if relay.config.Logger != nil {
		containDiagnosticPanic(func() {
			relay.config.Logger.LogAttrs(ctx, slog.LevelInfo, "outbox operation",
				slog.String("operation", string(event.Operation)),
				slog.String("outcome", string(event.Outcome)),
				slog.Int("count", event.Count),
				slog.String("message_id", event.MessageID),
				slog.String("topic", event.Topic),
				slog.Int("attempts", event.Attempts),
				slog.Duration("duration", event.Duration),
			)
		})
	}
}

func containDiagnosticPanic(callback func()) {
	defer func() { _ = recover() }()
	callback()
}

func (relay *Relay) durationSince(startedAt time.Time) time.Duration {
	duration := relay.config.Clock().Sub(startedAt)
	return max(duration, 0)
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func exponentialBackoff(attempt int) time.Duration {
	ceiling := backoffCeiling(attempt)

	// #nosec G404 -- this randomness schedules retry jitter, not security-sensitive values.
	return time.Duration(rand.Int64N(int64(ceiling)))
}

func heartbeatStopExpected(parentErr, heartbeatErr error, afterPublish bool) bool {
	if afterPublish && errors.Is(heartbeatErr, context.Canceled) {
		return true
	}

	return parentErr != nil && errors.Is(heartbeatErr, parentErr)
}

func heartbeatShouldCancelPublish(heartbeatErr error, afterPublish bool) bool {
	return heartbeatErr != nil && !afterPublish
}

func canceledPublish(parentErr, publishErr error) bool {
	if parentErr == nil {
		return false
	}

	return errors.Is(publishErr, context.Canceled) || errors.Is(publishErr, context.DeadlineExceeded)
}

func backoffCeiling(attempt int) time.Duration {
	attempt = max(attempt, 1)
	exponent := min(attempt-1, 10)

	return min(100*time.Millisecond*time.Duration(1<<exponent), maximumBackoff)
}
