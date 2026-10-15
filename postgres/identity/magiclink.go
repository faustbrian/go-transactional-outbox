package identityoutbox

import (
	"context"

	kernel "github.com/faustbrian/go-identity/postgres"
)

var _ kernel.MagicLinkRegistrationLifecycleContributor = (*Contributor)(nil)

// PersistMagicLinkRegistration writes the staged login and distinct account
// creation events through the original consuming Work. Only the coordinator
// invokes this after successful creation, protected session effects and audit
// staging. The existing immutable outbox binding preserves each command/index
// across retries and pruning; no schema or second journal is introduced.
// Nil, cancelled or unstaged Work denies. Any error must abort the Work, rolling
// back both events with creation and consumption. Success is not commitment or
// downstream delivery. Independent calls are safe; Work stays serial, borrowed
// and must not be retained or closed by this contributor.
func (c *Contributor) PersistMagicLinkRegistration(ctx context.Context, w kernel.Work) error {
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
	creation, err := w.StagedMagicLinkRegistrationLifecycle()
	if err != nil {
		return err
	}
	if err := c.persist(ctx, w.Exec, workQuery(w), primary[0]); err != nil {
		return err
	}
	return c.persist(ctx, w.Exec, workQuery(w), creation)
}
