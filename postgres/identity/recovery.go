package identityoutbox

import (
	"context"
	"encoding/base64"

	identity "github.com/faustbrian/go-identity"
	identitypostgres "github.com/faustbrian/go-identity/postgres"
)

// RecoverParticipant reads this participant's retained command/index bindings
// through the sole Coordinator's frozen, invocation-bound RecoveryWork. No
// bindings proves no effect; one binding at the expected index proves a retained
// participant effect, not a command receipt or current payload integrity. Extra
// or wrong-index bindings remain unknown. Pruning, delivery and rescheduling do
// not release evidence. The Coordinator treats any positive/unknown evidence
// beside a pending journal as unresolved, never reconstructed success. No SQL
// writes, private transactions, network, callbacks or handles are retained.
// Invalid, cancelled or failed work returns RecoveryUnknown with a redacted
// classified error. Independent invocations may run concurrently.
func (c *Contributor) RecoverParticipant(ctx context.Context, w identitypostgres.RecoveryWork) (identitypostgres.RecoveryStatus, error) {
	if c == nil || c.writer == nil || ctx == nil {
		return identitypostgres.RecoveryUnknown, invalid()
	}
	if err := ctx.Err(); err != nil {
		return identitypostgres.RecoveryUnknown, failure(ctx, err)
	}
	binding, err := w.Scope()
	if err != nil {
		return identitypostgres.RecoveryUnknown, err
	}
	plan, err := w.LifecyclePlan()
	if err != nil {
		return identitypostgres.RecoveryUnknown, err
	}
	return c.recover(ctx, func(ctx context.Context, sql string, args ...any) (int16, error) {
		rows, err := w.Query(ctx, sql, args...)
		if err != nil {
			return 0, err
		}
		defer rows.Close()
		var status int16
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return 0, err
			}
			return 0, identity.UnavailableError{}
		}
		if err := rows.Scan(&status); err != nil {
			return 0, err
		}
		if rows.Next() {
			return 0, identity.UnavailableError{}
		}
		return status, rows.Err()
	}, binding, plan)
}

// The unique tenant/command/index index supplies a bounded prefix lookup. Any
// second binding conflicts with the single-event frozen lifecycle profile.
// No prunable payload or mutable delivery state is used as historical evidence.
const recoverySQL = `WITH evidence AS (
 SELECT event_index FROM public.identity_outbox_bindings
 WHERE tenant_id=$1::text AND command_id=$2::bytea
 ORDER BY event_index LIMIT 2
) SELECT (CASE WHEN count(*)=0 THEN 0
 WHEN count(*)=1 AND min(event_index)=$3::integer THEN 1 ELSE 2 END)::smallint
 FROM evidence`

type queryStatus func(context.Context, string, ...any) (int16, error)

func (c *Contributor) recover(ctx context.Context, query queryStatus, binding identity.CommandBinding, plan identitypostgres.LifecyclePlan) (identitypostgres.RecoveryStatus, error) {
	if err := ctx.Err(); err != nil {
		return identitypostgres.RecoveryUnknown, failure(ctx, err)
	}
	id, err := base64.RawURLEncoding.DecodeString(string(binding.CommandID))
	if err != nil || len(id) != 16 {
		return identitypostgres.RecoveryUnknown, invalid()
	}
	status, err := query(ctx, recoverySQL, string(binding.TenantID), id, int32(plan.EventIndex))
	if err != nil {
		return identitypostgres.RecoveryUnknown, failure(ctx, err)
	}
	switch status {
	case 0:
		return identitypostgres.RecoveryNotCommitted, nil
	case 1:
		return identitypostgres.RecoveryCommitted, nil
	case 2:
		return identitypostgres.RecoveryUnknown, nil
	default:
		return identitypostgres.RecoveryUnknown, identity.UnavailableError{}
	}
}

var _ identitypostgres.RecoveryContributor = (*Contributor)(nil)
