package identityoutbox

import (
	"context"

	kernel "github.com/faustbrian/go-identity/postgres"
)

var _ kernel.PasskeyRegistrationLifecycleContributor = (*Contributor)(nil)

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
