package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/faustbrian/go-transactional-outbox/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultMaxClaimBatch      = 100
	defaultMaxAdminBatch      = 100
	defaultMaxLeaseDuration   = 5 * time.Minute
	maximumStoreBatch         = 1000
	maximumLeaseDuration      = 24 * time.Hour
	maximumRetryDelay         = time.Minute
	transactionCleanupTimeout = 5 * time.Second
	maxIdentifierBytes        = 255
	maxReplayReasonBytes      = 4096
	maxPayloadBytes           = 1 << 20
	maxEncodedMetadataBytes   = 128 << 10
)

var (
	// ErrPoolRequired requires a caller-owned pool.
	ErrPoolRequired = errors.New("outbox/postgres: pool is required")
	// ErrNotWritable reports a read-only or recovery session readiness check.
	ErrNotWritable = errors.New("outbox/postgres: database session is not a writable primary")
	// ErrValueOutsideBounds refuses persistence schema/identity admission.
	ErrValueOutsideBounds = errors.New("outbox/postgres: value is outside persistence bounds")
	// ErrClaimOwnerRequired requires a nonempty claim-owner label.
	ErrClaimOwnerRequired = errors.New("outbox/postgres: claim owner is required")
	// ErrInvalidClaimLimit refuses claim admission outside configured cardinality.
	ErrInvalidClaimLimit = errors.New("outbox/postgres: claim limit is outside configured bounds")
	// ErrInvalidLeaseDuration refuses lease duration outside configured positive bounds.
	ErrInvalidLeaseDuration = errors.New("outbox/postgres: lease duration is outside configured bounds")
	// ErrLeaseLost reports unmatched, expired or replaced lease capability.
	ErrLeaseLost = errors.New("outbox/postgres: lease is no longer owned")
	// ErrInvalidRetryDelay refuses retry delay outside zero..one minute.
	ErrInvalidRetryDelay = errors.New("outbox/postgres: retry delay is outside bounds")
	// ErrInvalidAdminLimit refuses administrative cardinality outside configured bounds.
	ErrInvalidAdminLimit = errors.New("outbox/postgres: administrative batch is outside configured bounds")
	// ErrReplayIDsRequired requires a nonempty replay selection and IDs.
	ErrReplayIDsRequired = errors.New("outbox/postgres: replay IDs are required")
	// ErrReplayRequestedBy requires a replay audit requester label.
	ErrReplayRequestedBy = errors.New("outbox/postgres: replay requester is required")
	// ErrReplayReasonRequired requires a replay audit reason.
	ErrReplayReasonRequired = errors.New("outbox/postgres: replay reason is required")
	// ErrReplayDuplicateID refuses duplicate IDs within a replay request.
	ErrReplayDuplicateID = errors.New("outbox/postgres: replay IDs must be unique")
	// ErrReplayUnauthorized denies absent, failing or panicking replay authorization without its details.
	ErrReplayUnauthorized = errors.New("outbox/postgres: replay is not authorized")
	// ErrReplayConflict refuses missing or nonterminal rows in an all-or-none replay selection.
	ErrReplayConflict = errors.New("outbox/postgres: replay selection contains missing or non-terminal records")
	// ErrPruneCutoffRequired requires a nonzero exclusive terminal-time cutoff.
	ErrPruneCutoffRequired = errors.New("outbox/postgres: prune cutoff is required")
	// ErrArchiverRequired requires an archive callback for archive-before-delete.
	ErrArchiverRequired = errors.New("outbox/postgres: archiver is required")
	// ErrArchiverPanic contains callback panic without exposing its value.
	ErrArchiverPanic = errors.New("outbox/postgres: archiver panicked")
	// ErrInvalidMessageState refuses an unknown administrative state filter.
	ErrInvalidMessageState = errors.New("outbox/postgres: message state is invalid")
	// ErrInvalidSerialization refuses an unknown claim serialization mode.
	ErrInvalidSerialization = errors.New("outbox/postgres: serialization mode is invalid")
)

// SerializationMode scopes claim serialization. Ordering-key mode treats an
// empty key as unordered; topic mode serializes every record in each topic.
type SerializationMode uint8

const (
	// SerializeNone permits unordered eligible claims.
	SerializeNone SerializationMode = iota
	// SerializeByOrderingKey admits the earliest nonterminal row per nonempty key; empty keys stay unordered.
	SerializeByOrderingKey
	// SerializeByTopic admits the earliest nonterminal row per topic.
	SerializeByTopic
)

// StoreConfig bounds relay claims and selects the outbox table.
type StoreConfig struct {
	// Schema Quoted schema name; empty selects public. Replay audit uses outbox_replay_audit in this schema.
	Schema string
	// Table Quoted message table name; empty selects outbox_messages.
	Table string
	// MaxClaimBatch Claim admission ceiling, default 100 and maximum 1000.
	MaxClaimBatch int
	// MaxAdminBatch Administrative batch ceiling, default 100 and maximum 1000.
	MaxAdminBatch int
	// MaxLeaseDuration Positive lease ceiling, default five minutes and maximum 24 hours. SQL durations truncate to microseconds.
	MaxLeaseDuration time.Duration
	// LeaseTokenGenerator Borrowed concurrent callback; nil uses random 128-bit hex. It must not reuse a generation token for a record.
	LeaseTokenGenerator func() (string, error)
	// ReplayAuthorizer Borrowed concurrent policy; nil denies replay. Approval occurs before transaction/row locking.
	ReplayAuthorizer ReplayAuthorizer
	// Observer Borrowed synchronous administrative observer; panics contained, blocking/privacy caller-owned.
	Observer outbox.Observer
	// Clock Borrowed diagnostic clock only, default time.Now; panics yield zero time, not DB scheduling time.
	Clock func() time.Time
}

// ClaimRequest describes one bounded lease acquisition.
type ClaimRequest struct {
	// Owner Required opaque owner label, at most 255 bytes; not authentication.
	Owner string
	// Limit Required positive count no greater than configured claim ceiling.
	Limit int
	// LeaseDuration Required positive duration within ceiling, converted to whole microseconds in SQL.
	LeaseDuration time.Duration
	// Serialization Optional scoped head-of-line claim policy; zero is unordered.
	Serialization SerializationMode
}

// Claim is an envelope together with the ownership proof required for every
// subsequent state transition.
type Claim struct {
	// Envelope Owned decoded mutable record, with attempts already incremented by claim.
	Envelope outbox.Envelope
	// Owner Current opaque lease-owner label.
	Owner string
	// LeaseToken Current fencing capability; one claim batch shares a token across distinct IDs.
	LeaseToken string
	// LeasedUntil PostgreSQL-clock expiry, not an application-clock deadline.
	LeasedUntil time.Time
}

// LeaseRef identifies a record and the opaque token for its current lease
// generation. An expired or replaced token cannot mutate the record.
// Expiry is checked under the row lock using the PostgreSQL clock.
type LeaseRef struct {
	// ID Record identity; validated for byte length before transition SQL.
	ID string
	// Token Current opaque generation capability; do not reuse or expose across trust boundaries.
	Token string
}

// ReplayRequest is an explicit, audited request to make terminal records
// publishable again. Replaying can produce duplicates and resets attempts.
type ReplayRequest struct {
	// IDs Unique required terminal IDs within admin bounds; replay return order is not input order.
	IDs []string
	// RequestedBy Required operator audit label, at most 255 bytes; not an authorization credential.
	RequestedBy string
	// Reason Required audit reason, at most 4096 bytes, retained as supplied.
	Reason string
	// AvailableAt Optional next eligibility time; zero uses PostgreSQL clock, explicit values use UTC.
	AvailableAt time.Time
}

// ReplayAuthorizer approves an explicit duplicate-producing replay request.
// Stores deny replay by default when no authorizer is configured.
type ReplayAuthorizer interface {
	// AuthorizeReplay runs before token generation/transaction; Store copies IDs
	// for this call. Error/panic denies replay without preserving its cause.
	// Authorization latency and concurrent safety remain caller-owned.
	AuthorizeReplay(context.Context, ReplayRequest) error
}

// ReplayAuthorizeFunc adapts a function to ReplayAuthorizer.
type ReplayAuthorizeFunc func(context.Context, ReplayRequest) error

// AuthorizeReplay directly forwards the request. Store copies IDs before
// invoking its configured authorizer; this function adapter itself does not.
func (authorize ReplayAuthorizeFunc) AuthorizeReplay(ctx context.Context, request ReplayRequest) error {
	return authorize(ctx, request)
}

// BacklogStats is the root payload-free backlog summary type.
type BacklogStats = outbox.BacklogStats

// MessageState is a durable outbox state accepted by administrative filters.
type MessageState string

const (
	// MessageStatePending identifies rows waiting for eligibility.
	MessageStatePending MessageState = "pending"
	// MessageStateLeased identifies stored leased state even after its deadline expires.
	MessageStateLeased MessageState = "leased"
	// MessageStateDelivered identifies publisher acceptance marked durable.
	MessageStateDelivered MessageState = "delivered"
	// MessageStateDead identifies terminal failed publication.
	MessageStateDead MessageState = "dead"
)

// InspectRequest selects a bounded administrative summary batch.
type InspectRequest struct {
	// State Optional state filter; empty includes all recognized states.
	State MessageState
	// Topic Optional exact topic filter; empty includes all topics.
	Topic string
	// Before Optional exclusive creation cutoff; zero omits it.
	Before time.Time
	// Limit Required positive result count within admin ceiling; query orders created_at then ID.
	Limit int
}

// MessageSummary intentionally excludes payload and metadata.
type MessageSummary struct {
	// ID Record identity, potentially sensitive.
	ID string
	// Topic Routing label, potentially sensitive.
	Topic string
	// OrderingKey Application ordering scope, not a payload-free privacy guarantee.
	OrderingKey string
	// IdempotencyKey Application deduplication key, potentially sensitive.
	IdempotencyKey string
	// Attempts Claim count, including acquisitions that never publish.
	Attempts int
	// AvailableAt Next eligibility time.
	AvailableAt time.Time
	// CreatedAt Creation ordering time.
	CreatedAt time.Time
	// UpdatedAt Last database transition time.
	UpdatedAt time.Time
	// State Stored lifecycle state; expiry alone does not change leased to pending.
	State MessageState
	// LeaseOwner Optional current owner label.
	LeaseOwner *string
	// LeasedUntil Optional PostgreSQL-clock lease expiry.
	LeasedUntil *time.Time
	// DeliveredAt Optional time the store marked publisher acceptance, not consumer processing.
	DeliveredAt *time.Time
	// DeadLetteredAt Optional terminal failure time.
	DeadLetteredAt *time.Time
	// LastError Optional stored diagnostic; this store writes fixed operation text, not arbitrary publisher causes.
	LastError *string
}

// DeliveredMessage contains mutable decoded message data supplied to an
// archive-before-delete hook.
type DeliveredMessage struct {
	// Envelope Mutable decoded archive snapshot; payload/map are not immutable by type.
	Envelope outbox.Envelope
	// DeliveredAt Stored delivery-transition time, not broker/end-consumer completion.
	DeliveredAt time.Time
}

// DeadMessage contains mutable decoded message data and terminal failure context for
// a dead-letter archive.
type DeadMessage struct {
	// Envelope Mutable decoded archive snapshot requiring restricted content handling.
	Envelope outbox.Envelope
	// DeadLetteredAt Stored terminal failure time.
	DeadLetteredAt time.Time
	// LastError Stored diagnostic from the selected row; do not assume arbitrary historical rows are redacted.
	LastError string
}

type terminalMessage struct {
	Envelope   outbox.Envelope
	TerminalAt time.Time
	LastError  *string
}

// Archiver persists delivered messages before the store deletes them.
// Implementations must tolerate duplicates because a successful archive can
// be followed by an ambiguous PostgreSQL commit.
type Archiver interface {
	// Archive receives mutable decoded snapshots while database row locks are
	// held. Do not reenter those rows; cooperate with context and deduplicate
	// by ID because external success can precede database failure/ambiguity.
	Archive(context.Context, []DeliveredMessage) error
}

// ArchiveFunc adapts a function to Archiver.
type ArchiveFunc func(context.Context, []DeliveredMessage) error

// Archive forwards delivered messages to the wrapped function.
func (archive ArchiveFunc) Archive(ctx context.Context, messages []DeliveredMessage) error {
	return archive(ctx, messages)
}

// DeadArchiver persists dead letters before the store deletes them.
type DeadArchiver interface {
	// ArchiveDead has the same locking/idempotency obligations as Archive;
	// historic LastError and message contents require restricted handling.
	ArchiveDead(context.Context, []DeadMessage) error
}

// DeadArchiveFunc adapts a function to DeadArchiver.
type DeadArchiveFunc func(context.Context, []DeadMessage) error

// ArchiveDead forwards dead letters to the wrapped function.
func (archive DeadArchiveFunc) ArchiveDead(ctx context.Context, messages []DeadMessage) error {
	return archive(ctx, messages)
}

// Store coordinates relay state through a borrowed PostgreSQL pool. It does
// not create/close the pool or install migrations. Callback concurrency and
// lifetime remain caller-owned; database response errors may leave committed
// transitions ambiguous, not necessarily rolled back.
type Store struct {
	pool                database
	table               string
	auditTable          string
	maxClaimBatch       int
	maxAdminBatch       int
	maxLeaseDuration    time.Duration
	leaseTokenGenerator func() (string, error)
	replayAuthorizer    ReplayAuthorizer
	observer            outbox.Observer
	clock               func() time.Time
}

type database interface {
	Ping(context.Context) error
	Begin(context.Context) (pgx.Tx, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Ping checks pool connectivity then a writable-primary query. These may use
// different pooled sessions; success is a point-in-time check, not schema
// validation, every-connection qualification or future availability.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("outbox/postgres: ping: %w", err)
	}
	var writable bool
	if err := s.pool.QueryRow(ctx, `
SELECT current_setting('transaction_read_only') = 'off'
   AND NOT pg_is_in_recovery()`).Scan(&writable); err != nil {
		return fmt.Errorf("outbox/postgres: check writable primary: %w", err)
	}
	if !writable {
		return ErrNotWritable
	}

	return nil
}

// Backlog returns current state counts and the oldest pending availability
// time. It is intended for low-frequency health and administrative checks.
func (s *Store) Backlog(ctx context.Context) (BacklogStats, error) {
	query := fmt.Sprintf(`
SELECT count(*) FILTER (WHERE state = 'pending'),
       count(*) FILTER (WHERE state = 'leased'),
       count(*) FILTER (WHERE state = 'dead'),
       min(available_at) FILTER (WHERE state = 'pending')
FROM %s`, s.table)
	var stats BacklogStats
	if err := s.pool.QueryRow(ctx, query).Scan(
		&stats.Pending,
		&stats.Leased,
		&stats.Dead,
		&stats.OldestPendingAt,
	); err != nil {
		return BacklogStats{}, fmt.Errorf("outbox/postgres: read backlog: %w", err)
	}

	return stats, nil
}

// NewStore creates a PostgreSQL relay store.
func NewStore(pool *pgxpool.Pool, config StoreConfig) (*Store, error) {
	if pool == nil {
		return nil, ErrPoolRequired
	}

	return newStore(pool, config)
}

func newStore(pool database, config StoreConfig) (*Store, error) {
	if config.Schema == "" {
		config.Schema = "public"
	}
	if config.Table == "" {
		config.Table = "outbox_messages"
	}
	switch {
	case config.MaxClaimBatch < 0:
		return nil, ErrInvalidClaimLimit
	case config.MaxClaimBatch == 0:
		config.MaxClaimBatch = defaultMaxClaimBatch
	case config.MaxClaimBatch > maximumStoreBatch:
		return nil, ErrInvalidClaimLimit
	}
	switch {
	case config.MaxAdminBatch < 0:
		return nil, ErrInvalidAdminLimit
	case config.MaxAdminBatch == 0:
		config.MaxAdminBatch = defaultMaxAdminBatch
	case config.MaxAdminBatch > maximumStoreBatch:
		return nil, ErrInvalidAdminLimit
	}
	switch {
	case config.MaxLeaseDuration < 0:
		return nil, ErrInvalidLeaseDuration
	case config.MaxLeaseDuration == 0:
		config.MaxLeaseDuration = defaultMaxLeaseDuration
	case config.MaxLeaseDuration > maximumLeaseDuration:
		return nil, ErrInvalidLeaseDuration
	}
	if config.LeaseTokenGenerator == nil {
		config.LeaseTokenGenerator = randomLeaseToken
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	config.Clock = containClockPanic(config.Clock)

	return &Store{
		pool:                pool,
		table:               sanitizeTable(config.Schema, config.Table),
		auditTable:          sanitizeTable(config.Schema, "outbox_replay_audit"),
		maxClaimBatch:       config.MaxClaimBatch,
		maxAdminBatch:       config.MaxAdminBatch,
		maxLeaseDuration:    config.MaxLeaseDuration,
		leaseTokenGenerator: config.LeaseTokenGenerator,
		replayAuthorizer:    config.ReplayAuthorizer,
		observer:            config.Observer,
		clock:               config.Clock,
	}, nil
}

func containClockPanic(clock func() time.Time) func() time.Time {
	return func() (value time.Time) {
		defer func() { _ = recover() }()

		return clock()
	}
}

// Inspect returns bounded payload-free summaries for operator tooling.
func (s *Store) Inspect(ctx context.Context, request InspectRequest) ([]MessageSummary, error) {
	if request.Limit <= 0 || request.Limit > s.maxAdminBatch {
		return nil, ErrInvalidAdminLimit
	}
	switch request.State {
	case "", MessageStatePending, MessageStateLeased, MessageStateDelivered, MessageStateDead:
	default:
		return nil, ErrInvalidMessageState
	}
	var before *time.Time
	if !request.Before.IsZero() {
		value := request.Before.UTC()
		before = &value
	}
	query := fmt.Sprintf(`
SELECT id, topic, ordering_key, idempotency_key, attempts, available_at,
       created_at, updated_at, state, lease_owner, leased_until, delivered_at,
       dead_lettered_at, last_error
FROM %s
WHERE ($1 = '' OR state = $1)
  AND ($2 = '' OR topic = $2)
  AND ($3::timestamptz IS NULL OR created_at < $3)
ORDER BY created_at, id
LIMIT $4`, s.table)
	rows, err := s.pool.Query(ctx, query, request.State, request.Topic, before, request.Limit)
	if err != nil {
		return nil, fmt.Errorf("outbox/postgres: inspect messages: %w", err)
	}
	defer rows.Close()

	summaries := make([]MessageSummary, 0, request.Limit)
	for rows.Next() {
		var summary MessageSummary
		if err := rows.Scan(
			&summary.ID, &summary.Topic, &summary.OrderingKey,
			&summary.IdempotencyKey, &summary.Attempts, &summary.AvailableAt,
			&summary.CreatedAt, &summary.UpdatedAt, &summary.State,
			&summary.LeaseOwner, &summary.LeasedUntil, &summary.DeliveredAt,
			&summary.DeadLetteredAt, &summary.LastError,
		); err != nil {
			return nil, fmt.Errorf("outbox/postgres: scan message summary: %w", err)
		}
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox/postgres: read message summaries: %w", err)
	}

	return summaries, nil
}

// Claim atomically leases available or expired messages. PostgreSQL row locks
// with SKIP LOCKED avoid selecting concurrently locked candidates. Expired
// claims may be reclaimed while an old publisher still runs. Attempts increment
// on acquisition (saturating at 10000), and one generated token covers the batch.
// Token generators must not reuse lease generations; transitions fence ID/token.
// Scoped serialization blocks later nonterminal rows, including future schedules;
// it is not broker/consumer ordering. Returned row order is unspecified.
// Durations are truncated to microseconds, including positive submicrosecond
// values. Response/scan failures can leave committed leases until expiry.
func (s *Store) Claim(ctx context.Context, request ClaimRequest) ([]Claim, error) {
	if request.Owner == "" {
		return nil, ErrClaimOwnerRequired
	}
	if len(request.Owner) > maxIdentifierBytes {
		return nil, fmt.Errorf("outbox/postgres: claim owner: %w", ErrValueOutsideBounds)
	}
	if request.Limit <= 0 || request.Limit > s.maxClaimBatch {
		return nil, ErrInvalidClaimLimit
	}
	if request.LeaseDuration <= 0 || request.LeaseDuration > s.maxLeaseDuration {
		return nil, ErrInvalidLeaseDuration
	}
	serializationPredicate, err := s.serializationPredicate(request.Serialization)
	if err != nil {
		return nil, err
	}

	leaseToken, err := s.leaseTokenGenerator()
	if err != nil {
		return nil, fmt.Errorf("outbox/postgres: generate lease token: %w", err)
	}
	if leaseToken == "" {
		return nil, errors.New("outbox/postgres: generated lease token is empty")
	}
	if len(leaseToken) > maxIdentifierBytes {
		return nil, fmt.Errorf("outbox/postgres: generated lease token: %w", ErrValueOutsideBounds)
	}

	query := fmt.Sprintf(`
WITH candidates AS (
    SELECT id
    FROM %s AS messages
    WHERE ((state = 'pending' AND available_at <= clock_timestamp())
       OR (state = 'leased' AND leased_until <= clock_timestamp()))
    %s
    ORDER BY available_at, created_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
UPDATE %s AS messages
SET state = 'leased',
    lease_owner = $2,
    lease_token = $3,
    leased_until = clock_timestamp() + ($4::bigint * interval '1 microsecond'),
    attempts = LEAST(attempts + 1, 10000),
    updated_at = clock_timestamp()
FROM candidates
WHERE messages.id = candidates.id
RETURNING messages.id, messages.topic, messages.payload,
          messages.payload_version, messages.metadata, messages.ordering_key,
          messages.idempotency_key, messages.attempts,
          messages.available_at, messages.created_at, messages.lease_owner,
          messages.lease_token, messages.leased_until`, s.table, serializationPredicate, s.table)

	rows, err := s.pool.Query(ctx, query, request.Limit, request.Owner, leaseToken, request.LeaseDuration.Microseconds())
	if err != nil {
		return nil, fmt.Errorf("outbox/postgres: claim messages: %w", err)
	}
	defer rows.Close()

	return scanClaims(rows, request.Limit)
}

func (s *Store) serializationPredicate(mode SerializationMode) (string, error) {
	var column string
	var unorderedGuard string
	switch mode {
	case SerializeNone:
		return "", nil
	case SerializeByOrderingKey:
		column = "ordering_key"
		unorderedGuard = "messages.ordering_key = '' OR "
	case SerializeByTopic:
		column = "topic"
	default:
		return "", ErrInvalidSerialization
	}

	return fmt.Sprintf(`
      AND (%sNOT EXISTS (
          SELECT 1
          FROM %s AS earlier
          WHERE earlier.%s = messages.%s
            AND earlier.state IN ('pending', 'leased')
            AND (earlier.created_at, earlier.id) < (messages.created_at, messages.id)
      ))`, unorderedGuard, s.table, column, column), nil
}

func scanClaims(rows pgx.Rows, capacity int) ([]Claim, error) {
	claims := make([]Claim, 0, capacity)
	for rows.Next() {
		var claim Claim
		var metadata []byte
		if err := rows.Scan(
			&claim.Envelope.ID,
			&claim.Envelope.Topic,
			&claim.Envelope.Payload,
			&claim.Envelope.PayloadVersion,
			&metadata,
			&claim.Envelope.OrderingKey,
			&claim.Envelope.IdempotencyKey,
			&claim.Envelope.Attempts,
			&claim.Envelope.AvailableAt,
			&claim.Envelope.CreatedAt,
			&claim.Owner,
			&claim.LeaseToken,
			&claim.LeasedUntil,
		); err != nil {
			return nil, fmt.Errorf("outbox/postgres: scan claimed message: %w", err)
		}
		if err := json.Unmarshal(metadata, &claim.Envelope.Metadata); err != nil {
			return nil, fmt.Errorf("outbox/postgres: decode metadata for %q: %w", claim.Envelope.ID, err)
		}
		claims = append(claims, claim)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox/postgres: read claimed messages: %w", err)
	}

	return claims, nil
}

// MarkDelivered records publisher success. A publisher success followed by a
// failure here must be retried and can therefore result in duplicate delivery.
func (s *Store) MarkDelivered(ctx context.Context, lease LeaseRef) error {
	if err := validateLeaseRef(lease); err != nil {
		return err
	}
	query := fmt.Sprintf(`
WITH locked AS MATERIALIZED (
    SELECT id, leased_until FROM %s
    WHERE id = $1 AND state = 'leased' AND lease_token = $2
    FOR UPDATE
)
UPDATE %s AS message
SET state = 'delivered',
    lease_owner = NULL,
    lease_token = NULL,
    leased_until = NULL,
    delivered_at = clock_timestamp(),
    last_error = NULL,
    updated_at = clock_timestamp()
FROM locked
WHERE message.id = locked.id AND locked.leased_until > clock_timestamp()`, s.table, s.table)

	return s.execLeaseUpdate(ctx, "mark delivered", query, lease.ID, lease.Token)
}

// ExtendLease sets a new deadline relative to PostgreSQL time after checking
// token and expiry under the row lock. It can shorten an existing lease; it
// does not add duration to the old deadline. Microsecond truncation applies.
func (s *Store) ExtendLease(ctx context.Context, lease LeaseRef, duration time.Duration) (time.Time, error) {
	if duration <= 0 || duration > s.maxLeaseDuration {
		return time.Time{}, ErrInvalidLeaseDuration
	}
	if err := validateLeaseRef(lease); err != nil {
		return time.Time{}, err
	}

	query := fmt.Sprintf(`
WITH locked AS MATERIALIZED (
    SELECT id, leased_until FROM %s
    WHERE id = $1 AND state = 'leased' AND lease_token = $2
    FOR UPDATE
)
UPDATE %s AS message
SET leased_until = clock_timestamp() + ($3::bigint * interval '1 microsecond'),
    updated_at = clock_timestamp()
FROM locked
WHERE message.id = locked.id AND locked.leased_until > clock_timestamp()
RETURNING message.leased_until`, s.table, s.table)
	var leasedUntil time.Time
	if err := s.pool.QueryRow(ctx, query, lease.ID, lease.Token, duration.Microseconds()).Scan(&leasedUntil); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return time.Time{}, ErrLeaseLost
		}

		return time.Time{}, fmt.Errorf("outbox/postgres: extend lease: %w", err)
	}

	return leasedUntil, nil
}

// Retry releases a lease and schedules the next eligible claim time.
func (s *Store) Retry(ctx context.Context, lease LeaseRef, delay time.Duration, cause error) error {
	if delay < 0 || delay > maximumRetryDelay {
		return ErrInvalidRetryDelay
	}
	if err := validateLeaseRef(lease); err != nil {
		return err
	}
	query := fmt.Sprintf(`
WITH locked AS MATERIALIZED (
    SELECT id, leased_until FROM %s
    WHERE id = $1 AND state = 'leased' AND lease_token = $2
    FOR UPDATE
)
UPDATE %s AS message
SET state = 'pending',
    lease_owner = NULL,
    lease_token = NULL,
    leased_until = NULL,
    available_at = clock_timestamp() + ($3::bigint * interval '1 microsecond'),
    last_error = $4,
    updated_at = clock_timestamp()
FROM locked
WHERE message.id = locked.id AND locked.leased_until > clock_timestamp()`, s.table, s.table)

	return s.execLeaseUpdate(ctx, "schedule retry", query,
		lease.ID, lease.Token, delay.Microseconds(), errorText(cause))
}

// DeadLetter moves a leased record to its terminal failure state.
func (s *Store) DeadLetter(ctx context.Context, lease LeaseRef, cause error) error {
	if err := validateLeaseRef(lease); err != nil {
		return err
	}
	query := fmt.Sprintf(`
WITH locked AS MATERIALIZED (
    SELECT id, leased_until FROM %s
    WHERE id = $1 AND state = 'leased' AND lease_token = $2
    FOR UPDATE
)
UPDATE %s AS message
SET state = 'dead',
    lease_owner = NULL,
    lease_token = NULL,
    leased_until = NULL,
    dead_lettered_at = clock_timestamp(),
    last_error = $3,
    updated_at = clock_timestamp()
FROM locked
WHERE message.id = locked.id AND locked.leased_until > clock_timestamp()`, s.table, s.table)

	return s.execLeaseUpdate(ctx, "dead letter", query, lease.ID, lease.Token, errorText(cause))
}

// ReleaseLease makes a currently owned claim immediately available. It is
// intended for graceful relay cancellation before publication completes.
func (s *Store) ReleaseLease(ctx context.Context, lease LeaseRef) error {
	if err := validateLeaseRef(lease); err != nil {
		return err
	}
	query := fmt.Sprintf(`
WITH locked AS MATERIALIZED (
    SELECT id, leased_until FROM %s
    WHERE id = $1 AND state = 'leased' AND lease_token = $2
    FOR UPDATE
)
UPDATE %s AS message
SET state = 'pending',
    lease_owner = NULL,
    lease_token = NULL,
    leased_until = NULL,
    available_at = clock_timestamp(),
    updated_at = clock_timestamp()
FROM locked
WHERE message.id = locked.id AND locked.leased_until > clock_timestamp()`, s.table, s.table)

	return s.execLeaseUpdate(ctx, "release lease", query, lease.ID, lease.Token)
}

// Replay atomically resets the selected terminal records and writes one audit
// row per record. If any ID is missing or non-terminal, no record is changed.
func (s *Store) Replay(ctx context.Context, request ReplayRequest) (ids []string, err error) {
	startedAt := s.clock()
	defer func() { s.observe(ctx, outbox.OperationReplay, len(ids), startedAt, err) }()
	if len(request.IDs) == 0 {
		return nil, ErrReplayIDsRequired
	}
	if len(request.IDs) > s.maxAdminBatch {
		return nil, ErrInvalidAdminLimit
	}
	if request.RequestedBy == "" {
		return nil, ErrReplayRequestedBy
	}
	if len(request.RequestedBy) > maxIdentifierBytes {
		return nil, fmt.Errorf("outbox/postgres: replay requester: %w", ErrValueOutsideBounds)
	}
	if request.Reason == "" {
		return nil, ErrReplayReasonRequired
	}
	if len(request.Reason) > maxReplayReasonBytes {
		return nil, fmt.Errorf("outbox/postgres: replay reason: %w", ErrValueOutsideBounds)
	}
	seen := make(map[string]struct{}, len(request.IDs))
	for _, id := range request.IDs {
		if id == "" {
			return nil, ErrReplayIDsRequired
		}
		if len(id) > maxIdentifierBytes {
			return nil, fmt.Errorf("outbox/postgres: replay message ID: %w", ErrValueOutsideBounds)
		}
		if _, exists := seen[id]; exists {
			return nil, ErrReplayDuplicateID
		}
		seen[id] = struct{}{}
	}
	if !request.AvailableAt.IsZero() {
		year := request.AvailableAt.Year()
		if year < 0 || year >= 10_000 {
			return nil, outbox.ErrTimestampOutOfRange
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.replayAuthorizer == nil {
		return nil, ErrReplayUnauthorized
	}
	authorizationRequest := request
	authorizationRequest.IDs = append([]string(nil), request.IDs...)
	if authorizeReplay(ctx, s.replayAuthorizer, authorizationRequest) != nil {
		return nil, ErrReplayUnauthorized
	}

	replayID, err := s.leaseTokenGenerator()
	if err != nil {
		return nil, fmt.Errorf("outbox/postgres: generate replay ID: %w", err)
	}
	if replayID == "" {
		return nil, errors.New("outbox/postgres: generated replay ID is empty")
	}
	if len(replayID) > maxIdentifierBytes {
		return nil, fmt.Errorf("outbox/postgres: generated replay ID: %w", ErrValueOutsideBounds)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("outbox/postgres: begin replay: %w", err)
	}
	defer rollbackTransaction(tx)

	var availableAt any
	if !request.AvailableAt.IsZero() {
		availableAt = request.AvailableAt.UTC()
	}
	query := fmt.Sprintf(`
WITH candidates AS (
    SELECT id, state
    FROM %s
    WHERE id = ANY($1::text[]) AND state IN ('delivered', 'dead')
    FOR UPDATE
)
UPDATE %s AS messages
SET state = 'pending',
    attempts = 0,
    available_at = COALESCE($2::timestamptz, clock_timestamp()),
    lease_owner = NULL,
    lease_token = NULL,
    leased_until = NULL,
    delivered_at = NULL,
    dead_lettered_at = NULL,
    last_error = NULL,
    updated_at = clock_timestamp()
FROM candidates
WHERE messages.id = candidates.id
RETURNING messages.id, candidates.state, messages.available_at`, s.table, s.table)
	rows, err := tx.Query(ctx, query, request.IDs, availableAt)
	if err != nil {
		return nil, fmt.Errorf("outbox/postgres: replay messages: %w", err)
	}

	type replayedRecord struct {
		id            string
		previousState string
		availableAt   time.Time
	}
	replayed := make([]replayedRecord, 0, len(request.IDs))
	for rows.Next() {
		var record replayedRecord
		if err := rows.Scan(&record.id, &record.previousState, &record.availableAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("outbox/postgres: scan replayed message: %w", err)
		}
		replayed = append(replayed, record)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("outbox/postgres: read replayed messages: %w", err)
	}
	rows.Close()
	if len(replayed) != len(request.IDs) {
		return nil, ErrReplayConflict
	}

	for _, record := range replayed {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s (replay_id, message_id, previous_state, requested_by, reason, available_at)
VALUES ($1, $2, $3, $4, $5, $6)`, s.auditTable),
			replayID, record.id, record.previousState, request.RequestedBy, request.Reason, record.availableAt); err != nil {
			return nil, fmt.Errorf("outbox/postgres: audit replay for %q: %w", record.id, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("outbox/postgres: commit replay: %w", err)
	}

	ids = make([]string, len(replayed))
	for index, record := range replayed {
		ids[index] = record.id
	}

	return ids, nil
}

func authorizeReplay(
	ctx context.Context,
	authorizer ReplayAuthorizer,
	request ReplayRequest,
) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrReplayUnauthorized
		}
	}()

	return authorizer.AuthorizeReplay(ctx, request)
}

// PruneDelivered deletes only delivered records older than cutoff, with a
// bounded SKIP LOCKED batch so concurrent maintenance workers stay disjoint.
func (s *Store) PruneDelivered(ctx context.Context, cutoff time.Time, limit int) (ids []string, err error) {
	startedAt := s.clock()
	defer func() { s.observe(ctx, outbox.OperationPrune, len(ids), startedAt, err) }()

	return s.pruneTerminal(ctx, cutoff, limit, MessageStateDelivered, "delivered_at")
}

// PruneDead deletes only dead letters older than cutoff in a bounded batch.
func (s *Store) PruneDead(ctx context.Context, cutoff time.Time, limit int) (ids []string, err error) {
	startedAt := s.clock()
	defer func() { s.observe(ctx, outbox.OperationPrune, len(ids), startedAt, err) }()

	return s.pruneTerminal(ctx, cutoff, limit, MessageStateDead, "dead_lettered_at")
}

func (s *Store) pruneTerminal(
	ctx context.Context,
	cutoff time.Time,
	limit int,
	state MessageState,
	timestampColumn string,
) ([]string, error) {
	if cutoff.IsZero() {
		return nil, ErrPruneCutoffRequired
	}
	if limit <= 0 || limit > s.maxAdminBatch {
		return nil, ErrInvalidAdminLimit
	}

	query := fmt.Sprintf(`
WITH candidates AS (
    SELECT id
    FROM %s
    WHERE state = $3 AND %s < $1
    ORDER BY %s, id
    FOR UPDATE SKIP LOCKED
    LIMIT $2
)
DELETE FROM %s AS messages
USING candidates
WHERE messages.id = candidates.id AND messages.state = $3
RETURNING messages.id`, s.table, timestampColumn, timestampColumn, s.table)
	rows, err := s.pool.Query(ctx, query, cutoff.UTC(), limit, state)
	if err != nil {
		return nil, fmt.Errorf("outbox/postgres: prune delivered messages: %w", err)
	}
	defer rows.Close()

	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("outbox/postgres: scan pruned message: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox/postgres: read pruned messages: %w", err)
	}

	return ids, nil
}

func (s *Store) observe(ctx context.Context, operation outbox.Operation, count int, startedAt time.Time, err error) {
	if s.observer == nil {
		return
	}
	outcome := outbox.OutcomeSuccess
	if err != nil {
		outcome = outbox.OutcomeFailure
	}
	duration := max(s.clock().Sub(startedAt), 0)
	containObserverPanic(func() {
		s.observer.Observe(ctx, outbox.Event{
			Operation: operation,
			Outcome:   outcome,
			Count:     count,
			Duration:  duration,
		})
	})
}

func containObserverPanic(callback func()) {
	defer func() { _ = recover() }()
	callback()
}

// ArchiveAndPruneDelivered locks a bounded delivered batch, invokes archiver,
// and deletes those records only after archival succeeds. The database
// transaction remains open during archival so concurrent maintenance workers
// cannot select the same rows. An ambiguous commit can archive a batch more
// than once, so archives must be idempotent by envelope ID.
func (s *Store) ArchiveAndPruneDelivered(
	ctx context.Context,
	cutoff time.Time,
	limit int,
	archiver Archiver,
) (ids []string, err error) {
	startedAt := s.clock()
	defer func() { s.observe(ctx, outbox.OperationArchive, len(ids), startedAt, err) }()
	if archiver == nil {
		return nil, ErrArchiverRequired
	}

	return s.archiveTerminal(ctx, cutoff, limit, MessageStateDelivered, "delivered_at",
		func(ctx context.Context, terminal []terminalMessage) error {
			messages := make([]DeliveredMessage, len(terminal))
			for index, message := range terminal {
				messages[index] = DeliveredMessage{Envelope: message.Envelope, DeliveredAt: message.TerminalAt}
			}

			return archiver.Archive(ctx, messages)
		})
}

// ArchiveAndPruneDead archives a bounded dead-letter batch before deletion,
// with the same row-lock, callback cooperation and duplicate windows as
// ArchiveAndPruneDelivered. Rollback is attempted with a separate bounded
// context, but rollback failure is not returned as proof of cleanup.
func (s *Store) ArchiveAndPruneDead(
	ctx context.Context,
	cutoff time.Time,
	limit int,
	archiver DeadArchiver,
) (ids []string, err error) {
	startedAt := s.clock()
	defer func() { s.observe(ctx, outbox.OperationArchive, len(ids), startedAt, err) }()
	if archiver == nil {
		return nil, ErrArchiverRequired
	}

	return s.archiveTerminal(ctx, cutoff, limit, MessageStateDead, "dead_lettered_at",
		func(ctx context.Context, terminal []terminalMessage) error {
			messages := make([]DeadMessage, len(terminal))
			for index, message := range terminal {
				lastError := ""
				if message.LastError != nil {
					lastError = *message.LastError
				}
				messages[index] = DeadMessage{
					Envelope: message.Envelope, DeadLetteredAt: message.TerminalAt, LastError: lastError,
				}
			}

			return archiver.ArchiveDead(ctx, messages)
		})
}

func (s *Store) archiveTerminal(
	ctx context.Context,
	cutoff time.Time,
	limit int,
	state MessageState,
	timestampColumn string,
	archive func(context.Context, []terminalMessage) error,
) ([]string, error) {
	if cutoff.IsZero() {
		return nil, ErrPruneCutoffRequired
	}
	if limit <= 0 || limit > s.maxAdminBatch {
		return nil, ErrInvalidAdminLimit
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("outbox/postgres: begin archive: %w", err)
	}
	defer rollbackTransaction(tx)

	query := fmt.Sprintf(`
SELECT id, topic, payload, payload_version, metadata, ordering_key,
       idempotency_key, attempts, available_at, created_at, %s, last_error
FROM %s
WHERE state = $3 AND %s < $1
ORDER BY %s, id
FOR UPDATE SKIP LOCKED
LIMIT $2`, timestampColumn, s.table, timestampColumn, timestampColumn)
	rows, err := tx.Query(ctx, query, cutoff.UTC(), limit, state)
	if err != nil {
		return nil, fmt.Errorf("outbox/postgres: select archive batch: %w", err)
	}

	messages := make([]terminalMessage, 0, limit)
	for rows.Next() {
		var message terminalMessage
		var metadata []byte
		if err := rows.Scan(
			&message.Envelope.ID,
			&message.Envelope.Topic,
			&message.Envelope.Payload,
			&message.Envelope.PayloadVersion,
			&metadata,
			&message.Envelope.OrderingKey,
			&message.Envelope.IdempotencyKey,
			&message.Envelope.Attempts,
			&message.Envelope.AvailableAt,
			&message.Envelope.CreatedAt,
			&message.TerminalAt,
			&message.LastError,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("outbox/postgres: scan archive batch: %w", err)
		}
		if err := json.Unmarshal(metadata, &message.Envelope.Metadata); err != nil {
			rows.Close()
			return nil, fmt.Errorf("outbox/postgres: decode archive metadata: %w", err)
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("outbox/postgres: read archive batch: %w", err)
	}
	rows.Close()

	if len(messages) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("outbox/postgres: commit empty archive: %w", err)
		}

		return []string{}, nil
	}
	if err := invokeArchiver(ctx, messages, archive); err != nil {
		return nil, fmt.Errorf("outbox/postgres: archive terminal messages: %w", err)
	}

	ids := make([]string, len(messages))
	for index, message := range messages {
		ids[index] = message.Envelope.ID
	}
	deleteQuery := fmt.Sprintf(`DELETE FROM %s WHERE id = ANY($1) AND state = $2`, s.table)
	if _, err := tx.Exec(ctx, deleteQuery, ids, state); err != nil {
		return nil, fmt.Errorf("outbox/postgres: delete archived messages: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("outbox/postgres: commit archive: %w", err)
	}

	return ids, nil
}

func invokeArchiver(
	ctx context.Context,
	messages []terminalMessage,
	archive func(context.Context, []terminalMessage) error,
) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrArchiverPanic
		}
	}()

	return archive(ctx, messages)
}

func rollbackTransaction(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), transactionCleanupTimeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func (s *Store) execLeaseUpdate(ctx context.Context, operation, query string, arguments ...any) error {
	tag, err := s.pool.Exec(ctx, query, arguments...)
	if err != nil {
		return fmt.Errorf("outbox/postgres: %s: %w", operation, err)
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}

	return nil
}

func validateLeaseRef(lease LeaseRef) error {
	if len(lease.ID) > maxIdentifierBytes {
		return fmt.Errorf("outbox/postgres: lease message ID: %w", ErrValueOutsideBounds)
	}
	if len(lease.Token) > maxIdentifierBytes {
		return fmt.Errorf("outbox/postgres: lease token: %w", ErrValueOutsideBounds)
	}

	return nil
}

func errorText(cause error) string {
	if cause == nil {
		return ""
	}

	return "operation failed"
}

func randomLeaseToken() (string, error) {
	return leaseTokenFromReader(rand.Reader)
}

func leaseTokenFromReader(reader io.Reader) (string, error) {
	var value [16]byte
	if _, err := io.ReadFull(reader, value[:]); err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}

	return hex.EncodeToString(value[:]), nil
}
