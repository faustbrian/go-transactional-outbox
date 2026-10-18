package identityoutbox

import (
	"context"

	kernel "github.com/faustbrian/go-identity/postgres"
)

var _ kernel.OrganizationInvitationLifecycleContributor = (*Contributor)(nil)
var _ kernel.OAuthConsentLifecycleContributor = (*Contributor)(nil)

// PersistOrganizationInvitation writes invitation acceptance and its distinct
// actual membership event through the sole consuming Work. Only the coordinator
// calls it after protected native mutation and successful audit/lifecycle
// staging. Any error must abort that Work, rolling both events back with proof
// consumption and membership. No schema, journal, transaction or consumer ack is
// added; immutable command/index bindings retain existing replay semantics.
func (c *Contributor) PersistOrganizationInvitation(ctx context.Context, w kernel.Work) error {
	return c.persistClosedPair(ctx, w, w.StagedOrganizationInvitationMembership)
}

// PersistOAuthConsent writes the grant transition and its independent actual
// consent creation/reconfirmation through the same Work. Both retain original
// safe IDs/versions and distinct deterministic event indices. Nil/cancelled or
// unstaged Work denies; failures must abort the entire provisional mutation.
// Independent calls are safe, but each Work stays borrowed/serial and cannot be
// retained, closed or used for external IO. Success is not commit or delivery.
func (c *Contributor) PersistOAuthConsent(ctx context.Context, w kernel.Work) error {
	return c.persistClosedPair(ctx, w, w.StagedOAuthConsent)
}

// secondary is exclusively one of the kernel's closed, IO-free projections
// above; no caller callback or arbitrary payload crosses this private helper.
func (c *Contributor) persistClosedPair(ctx context.Context, w kernel.Work, secondary func() (kernel.StagedLifecycle, error)) error {
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
	other, err := secondary()
	if err != nil {
		return err
	}
	if err := c.persist(ctx, w.Exec, workQuery(w), primary[0]); err != nil {
		return err
	}
	return c.persist(ctx, w.Exec, workQuery(w), other)
}
