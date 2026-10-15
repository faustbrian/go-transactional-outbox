package postgres

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/faustbrian/go-transactional-outbox/v2"
)

// ErrWriterRequired rejects a nil or uninitialized writer during preparation.
var ErrWriterRequired = errors.New("outbox/postgres: initialized writer required")

// PreparedInsert owns a validated, immutable PostgreSQL insert statement.
// Copies may be read concurrently. It grants no execution or transaction
// authority; a composition adapter must execute through its transaction owner.
// The zero value has no statement. It contains payloads and must not be logged.
type PreparedInsert struct {
	query     string
	arguments []any
}

// PrepareInsert validates the same bounded envelope/schema contract as
// InsertBatch and snapshots its SQL arguments without I/O or callbacks. It is
// the library-owned SQL preparation boundary for restricted transaction
// composition; callers must not concurrently mutate envelopes. It never
// begins, commits, rolls back or infers a durable outcome.
func (w *Writer) PrepareInsert(envelopes []outbox.Envelope) (PreparedInsert, error) {
	if w == nil || w.table == "" || w.maxBatchSize == 0 || w.limits.Validate() != nil {
		return PreparedInsert{}, ErrWriterRequired
	}
	if len(envelopes) == 0 {
		return PreparedInsert{}, ErrEmptyBatch
	}
	if len(envelopes) > w.maxBatchSize {
		return PreparedInsert{}, ErrBatchTooLarge
	}
	for index, envelope := range envelopes {
		if err := envelope.ValidateForInsert(w.limits); err != nil {
			return PreparedInsert{}, fmt.Errorf("outbox/postgres: validate envelope %d: %w", index, err)
		}
		if err := validateEnvelopeForSchema(envelope); err != nil {
			return PreparedInsert{}, fmt.Errorf("outbox/postgres: validate envelope %d for schema: %w", index, err)
		}
	}
	// Restricted transaction owners bind scalar strings rather than exposing
	// pgx codecs, so metadata needs an explicit JSONB parameter type.
	query, args := w.buildInsertQuery(envelopes, true)
	return PreparedInsert{query: query, arguments: cloneInsertArguments(args)}, nil
}

// Statement returns the fixed SQL text and an owned scalar argument snapshot.
// Payload versions use int32 rather than uint16 for restricted SQL executors.
// Changing returned payload bytes cannot change this plan. Nil arguments and
// empty SQL identify zero. Consumers must retain bounded context and the sole
// transaction owner's execution surface; successful execution is not commit.
func (p PreparedInsert) Statement() (string, []any) {
	args := cloneInsertArguments(p.arguments)
	for i, value := range args {
		if version, ok := value.(uint16); ok {
			args[i] = int32(version)
		}
	}
	return p.query, args
}

func cloneInsertArguments(args []any) []any {
	copy := slices.Clone(args)
	for i, value := range copy {
		if payload, ok := value.([]byte); ok {
			copy[i] = bytes.Clone(payload)
		}
	}
	return copy
}
