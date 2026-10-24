package identityoutbox

import (
	"context"

	kernel "github.com/faustbrian/go-identity/postgres"
)

var _ kernel.PasskeyRegistrationLifecycleContributor = (*Contributor)(nil)
var _ kernel.PasskeySignupSessionLifecycleContributor = (*Contributor)(nil)

// PersistPasskeyRegistration persists the canonical credential creation and
// separately staged user.created.v1 event through the original signup Work.
// Only the coordinator invokes it after actual kernel creation and successful
// primary audit/lifecycle staging. Both events retain their existing immutable
// command/index/digest bindings; no schema or second journal is introduced.
// Any error must abort the Work, rolling back both events with account creation
// and all other required effects. Success is neither commit nor authentication,
// session issuance, verification or downstream acknowledgement. Nil, cancelled
// or unstaged Work denies. Independent calls are safe; each borrowed Work stays
// serial-use and must not be retained or closed by this contributor.
func (c *Contributor) PersistPasskeyRegistration(ctx context.Context, w kernel.Work) error {
	if c == nil || c.writer == nil || ctx == nil {
		return invalid()
	}
	if err := ctx.Err(); err != nil {
		return failure(ctx, err)
	}
	primary, err := w.StagedLifecycle()
	if err != nil {
		return err
	}
	if len(primary) != 1 {
		return invalid()
	}
	creation, err := w.StagedPasskeyRegistrationLifecycle()
	if err != nil {
		return err
	}
	if err := c.persist(ctx, w.Exec, workQuery(w), primary[0]); err != nil {
		return err
	}
	return c.persist(ctx, w.Exec, workQuery(w), creation)
}

// PersistPasskeySignupSession writes the explicitly staged original session
// lifecycle after the canonical credential and account events in the same Work.
// Missing session staging denies before persistence; it never silently selects
// the legacy two-event phase. All three use the existing immutable bindings and
// must roll back with the protected signup on any failure. This phase issues no
// session, bearer or verification authority and does not acknowledge delivery.
// Nil, cancelled, expired or unstaged Work denies. Independent invocations are
// safe; each borrowed Work stays serial and must not be retained or closed.
func (c *Contributor) PersistPasskeySignupSession(ctx context.Context, w kernel.Work) error {
	if c == nil || c.writer == nil || ctx == nil {
		return invalid()
	}
	if err := ctx.Err(); err != nil {
		return failure(ctx, err)
	}
	session, present, err := w.StagedPasskeyRegistrationSessionLifecycle()
	if err != nil {
		return err
	}
	if !present {
		return invalid()
	}
	if err := c.PersistPasskeyRegistration(ctx, w); err != nil {
		return err
	}
	return c.persist(ctx, w.Exec, workQuery(w), session)
}
