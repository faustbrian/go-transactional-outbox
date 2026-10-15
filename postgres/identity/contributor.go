// Package identityoutbox supplies the optional outbox-owned lifecycle phase
// for the identity PostgreSQL Coordinator. It has no transaction, pool or
// delivery authority. Apply the generic outbox and adapter migrations first.
package identityoutbox

import (
	"context"
	"errors"

	identity "github.com/faustbrian/go-identity"
	identitypostgres "github.com/faustbrian/go-identity/postgres"
	outboxpostgres "github.com/faustbrian/go-transactional-outbox/v2/postgres"
)

// Contributor persists one complete staged kernel lifecycle notification using
// the canonical public.outbox_messages schema. Instances are immutable and may
// serve independent Work values concurrently; each Work remains serial-use.
// The zero value is invalid. No Work, pool, transaction or callback is retained.
type Contributor struct{ writer *outboxpostgres.Writer }

// NewContributor validates the default outbox schema/limits without I/O. The
// generic outbox Migrations must precede this module's Migrations. Custom
// schema/table profiles are deliberately unsupported by this binding profile.
func NewContributor() (*Contributor, error) {
	w, err := outboxpostgres.NewWriter(outboxpostgres.WriterConfig{})
	if err != nil {
		return nil, invalid()
	}
	return &Contributor{writer: w}, nil
}

// Descriptor returns the pure, compile-time identity of the open lifecycle
// phase. Composition registration grants no transaction or commit authority.
func (*Contributor) Descriptor() identitypostgres.ContributorDescriptor {
	return identitypostgres.ContributorDescriptor{Owner: "outbox/postgres", ContractID: "identity.lifecycle", Version: 1}
}

// PersistLifecycle writes the complete staged event and its retained immutable
// command/index/digest binding through this same Work. Exact replay is idempotent
// even after payload pruning; changed bindings or bytes fail closed. Only the
// sole Coordinator may invoke this phase after domain/audit staging and must
// abort/reconcile on any error. Success is neither commit nor consumer delivery
// or cascade acknowledgement. Work is not retained. There is no independent
// transaction, sink, network call, savepoint or application callback.
// Nil receiver/context, cancellation and invalid/incomplete Work fail without
// persistence. Errors are classified and redacted without database diagnostics.
func (c *Contributor) PersistLifecycle(ctx context.Context, w identitypostgres.Work) error {
	if c == nil || c.writer == nil || ctx == nil {
		return invalid()
	}
	if err := ctx.Err(); err != nil {
		return failure(ctx, err)
	}
	records, err := w.StagedLifecycle()
	if err != nil {
		return err
	}
	if len(records) != 1 {
		return invalid()
	}
	return c.persist(ctx, w.Exec, workQuery(w), records[0])
}

func workQuery(w identitypostgres.Work) queryBool {
	return func(ctx context.Context, sql string, args ...any) (bool, error) {
		rows, err := w.Query(ctx, sql, args...)
		if err != nil {
			return false, err
		}
		defer rows.Close()
		var result bool
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return false, err
			}
			return false, identity.UnavailableError{}
		}
		if err := rows.Scan(&result); err != nil {
			return false, err
		}
		if rows.Next() {
			return false, identity.UnavailableError{}
		}
		return result, rows.Err()
	}
}

func invalid() error { return identity.InvalidInputError{Field: "lifecycle_outbox"} }
func failure(ctx context.Context, err error) error {
	if errors.Is(err, context.Canceled) || ctx.Err() == context.Canceled {
		return identity.CancelledError{}
	}
	if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
		return identity.DeadlineExceededError{}
	}
	return identity.UnavailableError{}
}

var _ identitypostgres.LifecycleContributor = (*Contributor)(nil)
