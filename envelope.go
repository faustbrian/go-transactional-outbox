// Package outbox provides transactional outbox records and relay primitives.
//
// Persisting an Envelope atomically with application state requires passing the
// same caller-owned database transaction to a transactional store. Publishing
// is at least once: consumers must tolerate duplicate envelopes.
package outbox

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

const defaultPayloadVersion uint16 = 1

var (
	// ErrIDRequired requires a nonempty ID.
	ErrIDRequired = errors.New("outbox: ID is required")
	// ErrIDTooLarge refuses ID bytes beyond construction limits.
	ErrIDTooLarge = errors.New("outbox: ID is too large")
	// ErrTopicRequired requires nonempty topic text.
	ErrTopicRequired = errors.New("outbox: topic is required")
	// ErrTopicTooLarge refuses topic bytes beyond limits.
	ErrTopicTooLarge = errors.New("outbox: topic is too large")
	// ErrPayloadTooLarge refuses opaque payload bytes beyond limits.
	ErrPayloadTooLarge = errors.New("outbox: payload is too large")
	// ErrMetadataTooLarge refuses summed key/value bytes beyond limits.
	ErrMetadataTooLarge = errors.New("outbox: metadata is too large")
	// ErrMetadataEntriesTooLarge refuses metadata cardinality beyond limits.
	ErrMetadataEntriesTooLarge = errors.New("outbox: metadata has too many entries")
	// ErrOrderingKeyTooLarge refuses ordering-key bytes beyond limits.
	ErrOrderingKeyTooLarge = errors.New("outbox: ordering key is too large")
	// ErrIdempotencyKeyTooLarge refuses idempotency-key bytes beyond limits.
	ErrIdempotencyKeyTooLarge = errors.New("outbox: idempotency key is too large")
	// ErrPayloadVersionRequired requires a nonzero persisted schema version.
	ErrPayloadVersionRequired = errors.New("outbox: payload version is required")
	// ErrAttemptsInvalid refuses nonzero attempts on a new insert.
	ErrAttemptsInvalid = errors.New("outbox: new envelope attempts must be zero")
	// ErrAvailableAtRequired requires nonzero availability time.
	ErrAvailableAtRequired = errors.New("outbox: availability time is required")
	// ErrCreatedAtRequired requires nonzero creation time.
	ErrCreatedAtRequired = errors.New("outbox: creation time is required")
	// ErrTimestampOutOfRange refuses timestamp years outside 0000..9999.
	ErrTimestampOutOfRange = errors.New("outbox: timestamp is outside JSON range")
	// ErrInvalidLimits requires every construction bound to be positive.
	ErrInvalidLimits = errors.New("outbox: limits must be positive")
)

// Limits bounds all caller-controlled variable-length envelope fields.
type Limits struct {
	// MaxIDBytes Maximum nonempty envelope ID bytes; default 255.
	MaxIDBytes int
	// MaxTopicBytes Maximum nonempty topic bytes; default 255, without name/encoding normalization.
	MaxTopicBytes int
	// MaxPayloadBytes Maximum opaque payload bytes; default 1 MiB, not total encoded/heap bytes.
	MaxPayloadBytes int
	// MaxMetadataEntries Maximum supplied metadata entries; default 64.
	MaxMetadataEntries int
	// MaxMetadataBytes Maximum summed key/value bytes; default 16 KiB, not JSON-escaped size.
	MaxMetadataBytes int
	// MaxOrderingKeyBytes Maximum optional ordering-key bytes; default 255.
	MaxOrderingKeyBytes int
	// MaxIdempotencyKeyBytes Maximum optional application idempotency-key bytes; default 255.
	MaxIdempotencyKeyBytes int
}

// Validate reports whether every envelope limit is positive.
func (limits Limits) Validate() error {
	if limits.MaxIDBytes <= 0 || limits.MaxTopicBytes <= 0 || limits.MaxPayloadBytes <= 0 ||
		limits.MaxMetadataEntries <= 0 || limits.MaxMetadataBytes <= 0 || limits.MaxOrderingKeyBytes <= 0 ||
		limits.MaxIdempotencyKeyBytes <= 0 {
		return ErrInvalidLimits
	}

	return nil
}

// DefaultLimits returns conservative defaults suitable for most relays.
func DefaultLimits() Limits {
	return Limits{
		MaxIDBytes:             255,
		MaxTopicBytes:          255,
		MaxPayloadBytes:        1 << 20,
		MaxMetadataEntries:     64,
		MaxMetadataBytes:       16 << 10,
		MaxOrderingKeyBytes:    255,
		MaxIdempotencyKeyBytes: 255,
	}
}

// Envelope is the stable record transferred from PostgreSQL to a publisher.
// Attempts is zero on insert and increases on claim acquisition, even if no
// publication follows. Payload and Metadata are mutable; the type is not immutable.
type Envelope struct {
	// ID Stable message identity; public construction does not itself guarantee uniqueness.
	ID string `json:"id"`
	// Topic Routing target, retained as supplied and validated by the selected publisher.
	Topic string `json:"topic"`
	// Payload Caller-owned mutable opaque bytes, not necessarily JSON; builder output is copied.
	Payload []byte `json:"payload"`
	// PayloadVersion Nonzero schema version for persistence; not validation against a schema registry.
	PayloadVersion uint16 `json:"payload_version"`
	// Metadata Caller-owned mutable string map; builder output is copied, not sanitized.
	Metadata map[string]string `json:"metadata,omitempty"`
	// OrderingKey Optional serialization/routing scope, not a global delivery-order guarantee.
	OrderingKey string `json:"ordering_key,omitempty"`
	// IdempotencyKey Optional stable application key; persistence uniqueness and consumer deduplication are separate.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	// Attempts Claim acquisitions, not confirmed publications; insert requires zero and SQL claim increment saturates at 10000.
	Attempts int `json:"attempts"`
	// AvailableAt Next claim eligibility time; not a publication completion deadline.
	AvailableAt time.Time `json:"available_at"`
	// CreatedAt Creation identity used in scoped ordering; builder uses UTC microsecond precision.
	CreatedAt time.Time `json:"created_at"`
}

// CanonicalJSON returns a deterministic JSON representation. The wire struct
// has a fixed field order, metadata keys are sorted by encoding/json, and
// timestamps are formatted explicitly so encoding cannot fail on a time year.
// This method does not validate fields or enforce limits. Payload uses JSON
// base64 encoding; nil and empty slices differ, and invalid UTF-8 string bytes
// are replaced by encoding/json. Do not mutate the borrowed map/slice during it.
func (e Envelope) CanonicalJSON() []byte {
	encoded, _ := json.Marshal(struct {
		ID             string            `json:"id"`
		Topic          string            `json:"topic"`
		Payload        []byte            `json:"payload"`
		PayloadVersion uint16            `json:"payload_version"`
		Metadata       map[string]string `json:"metadata,omitempty"`
		OrderingKey    string            `json:"ordering_key,omitempty"`
		IdempotencyKey string            `json:"idempotency_key,omitempty"`
		Attempts       int               `json:"attempts"`
		AvailableAt    string            `json:"available_at"`
		CreatedAt      string            `json:"created_at"`
	}{
		ID: e.ID, Topic: e.Topic, Payload: e.Payload,
		PayloadVersion: e.PayloadVersion, Metadata: e.Metadata,
		OrderingKey: e.OrderingKey, IdempotencyKey: e.IdempotencyKey,
		Attempts: e.Attempts, AvailableAt: e.AvailableAt.Format(time.RFC3339Nano),
		CreatedAt: e.CreatedAt.Format(time.RFC3339Nano),
	})

	return encoded
}

// ValidateForInsert checks a new envelope against limits and persistence
// invariants. Writers call this again because callers can construct the
// exported Envelope type without using EnvelopeBuilder.
func (e Envelope) ValidateForInsert(limits Limits) error {
	if err := limits.Validate(); err != nil {
		return err
	}
	if e.ID == "" {
		return ErrIDRequired
	}
	if len(e.ID) > limits.MaxIDBytes {
		return ErrIDTooLarge
	}
	if e.PayloadVersion == 0 {
		return ErrPayloadVersionRequired
	}
	if e.Attempts != 0 {
		return ErrAttemptsInvalid
	}
	if e.AvailableAt.IsZero() {
		return ErrAvailableAtRequired
	}
	if e.CreatedAt.IsZero() {
		return ErrCreatedAtRequired
	}
	if !jsonTimestampYear(e.AvailableAt) || !jsonTimestampYear(e.CreatedAt) {
		return ErrTimestampOutOfRange
	}

	return validateParams(NewEnvelopeParams{
		Topic:          e.Topic,
		Payload:        e.Payload,
		Metadata:       e.Metadata,
		OrderingKey:    e.OrderingKey,
		IdempotencyKey: e.IdempotencyKey,
	}, limits)
}

func jsonTimestampYear(value time.Time) bool {
	year := value.Year()

	return year >= 0 && year < 10_000
}

// NewEnvelopeParams contains caller-controlled data for a new envelope.
type NewEnvelopeParams struct {
	// Topic Required opaque routing topic; empty rejects, whitespace/UTF-8 are not normalized here.
	Topic string
	// Payload Borrowed opaque bytes through Build; zero-length snapshots become nil.
	Payload []byte
	// PayloadVersion Schema version; zero defaults to one during Build.
	PayloadVersion uint16
	// Metadata Borrowed map through Build; caller must not mutate concurrently or from callbacks.
	Metadata map[string]string
	// OrderingKey Optional application ordering/routing scope.
	OrderingKey string
	// IdempotencyKey Optional stable application key for downstream deduplication.
	IdempotencyKey string
	// AvailableAt Optional schedule; zero defaults to the injected creation clock.
	AvailableAt time.Time
}

// EnvelopeBuilder validates and constructs new envelopes.
type EnvelopeBuilder struct {
	clock       func() time.Time
	idGenerator func() (string, error)
	limits      Limits
}

// EnvelopeBuilderOption configures an EnvelopeBuilder.
type EnvelopeBuilderOption func(*EnvelopeBuilder) error

// WithClock injects the clock used for creation and default availability.
func WithClock(clock func() time.Time) EnvelopeBuilderOption {
	return func(builder *EnvelopeBuilder) error {
		if clock == nil {
			return errors.New("outbox: clock is nil")
		}
		builder.clock = clock

		return nil
	}
}

// WithIDGenerator injects the envelope ID generator.
func WithIDGenerator(generator func() (string, error)) EnvelopeBuilderOption {
	return func(builder *EnvelopeBuilder) error {
		if generator == nil {
			return errors.New("outbox: ID generator is nil")
		}
		builder.idGenerator = generator

		return nil
	}
}

// WithLimits replaces all envelope size limits.
func WithLimits(limits Limits) EnvelopeBuilderOption {
	return func(builder *EnvelopeBuilder) error {
		if err := limits.Validate(); err != nil {
			return err
		}
		builder.limits = limits

		return nil
	}
}

// NewEnvelopeBuilder creates a bounded envelope builder.
func NewEnvelopeBuilder(options ...EnvelopeBuilderOption) (*EnvelopeBuilder, error) {
	builder := &EnvelopeBuilder{
		clock:       time.Now,
		idGenerator: randomUUID,
		limits:      DefaultLimits(),
	}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("outbox: envelope builder option is nil")
		}
		if err := option(builder); err != nil {
			return nil, fmt.Errorf("outbox: configure envelope builder: %w", err)
		}
	}

	return builder, nil
}

// Build creates a validated envelope and takes ownership by copying mutable
// payload and metadata values. Callers must keep inputs unchanged throughout
// Build, including generator/clock callbacks. Zero-length payloads become nil;
// creation/schedule times use UTC microsecond precision. Callback side effects
// are not rolled back on failure, and the returned containers remain mutable.
func (b *EnvelopeBuilder) Build(params NewEnvelopeParams) (Envelope, error) {
	if err := validateParams(params, b.limits); err != nil {
		return Envelope{}, err
	}

	id, err := b.idGenerator()
	if err != nil {
		return Envelope{}, fmt.Errorf("outbox: generate envelope ID: %w", err)
	}
	now := databaseTime(b.clock())
	availableAt := params.AvailableAt
	if availableAt.IsZero() {
		availableAt = now
	} else {
		availableAt = databaseTime(availableAt)
	}
	payloadVersion := params.PayloadVersion
	if payloadVersion == 0 {
		payloadVersion = defaultPayloadVersion
	}

	envelope := Envelope{
		ID:             id,
		Topic:          params.Topic,
		Payload:        append([]byte(nil), params.Payload...),
		PayloadVersion: payloadVersion,
		Metadata:       cloneMetadata(params.Metadata),
		OrderingKey:    params.OrderingKey,
		IdempotencyKey: params.IdempotencyKey,
		Attempts:       0,
		AvailableAt:    availableAt,
		CreatedAt:      now,
	}
	if err := envelope.ValidateForInsert(b.limits); err != nil {
		return Envelope{}, err
	}

	return envelope, nil
}

func validateParams(params NewEnvelopeParams, limits Limits) error {
	if params.Topic == "" {
		return ErrTopicRequired
	}
	if len(params.Topic) > limits.MaxTopicBytes {
		return ErrTopicTooLarge
	}
	if len(params.Payload) > limits.MaxPayloadBytes {
		return ErrPayloadTooLarge
	}
	if len(params.Metadata) > limits.MaxMetadataEntries {
		return ErrMetadataEntriesTooLarge
	}
	if metadataSize(params.Metadata) > limits.MaxMetadataBytes {
		return ErrMetadataTooLarge
	}
	if len(params.OrderingKey) > limits.MaxOrderingKeyBytes {
		return ErrOrderingKeyTooLarge
	}
	if len(params.IdempotencyKey) > limits.MaxIdempotencyKeyBytes {
		return ErrIdempotencyKeyTooLarge
	}

	return nil
}

func metadataSize(metadata map[string]string) int {
	size := 0
	for key, value := range metadata {
		size += len(key) + len(value)
	}

	return size
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}

	cloned := make(map[string]string, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}

	return cloned
}

func databaseTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

func randomUUID() (string, error) {
	return uuidFromReader(rand.Reader)
}

func uuidFromReader(reader io.Reader) (string, error) {
	var value [16]byte
	if _, err := io.ReadFull(reader, value[:]); err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}

	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80

	var encoded [36]byte
	hex.Encode(encoded[0:8], value[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], value[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], value[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], value[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], value[10:16])

	return string(encoded[:]), nil
}
