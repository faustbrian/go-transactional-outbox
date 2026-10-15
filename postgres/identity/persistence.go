package identityoutbox

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"time"

	identity "github.com/faustbrian/go-identity"
	identitypostgres "github.com/faustbrian/go-identity/postgres"
	outbox "github.com/faustbrian/go-transactional-outbox/v2"
)

func lifecycleEnvelope(r identitypostgres.StagedLifecycle) (outbox.Envelope, error) {
	payload, err := json.Marshal(struct {
		Profile       string             `json:"profile"`
		ID            string             `json:"id"`
		CommandID     identity.CommandID `json:"command_id"`
		TenantID      identity.TenantID  `json:"tenant_id"`
		UserID        identity.UserID    `json:"user_id"`
		OperationID   string             `json:"operation_id"`
		EventType     string             `json:"event_type"`
		EventIndex    uint16             `json:"event_index"`
		SubjectType   string             `json:"subject_type"`
		SubjectID     string             `json:"subject_id"`
		BeforeVersion string             `json:"before_version"`
		AfterVersion  string             `json:"after_version"`
		OccurredAt    string             `json:"occurred_at"`
	}{"identity-lifecycle-v1", r.ID, r.CommandID, r.TenantID, r.UserID, r.OperationID, r.EventType, r.EventIndex, r.SubjectType, r.SubjectID, strconv.FormatUint(r.BeforeVersion, 10), strconv.FormatUint(r.AfterVersion, 10), r.OccurredAt.Format(time.RFC3339Nano)})
	if err != nil {
		return outbox.Envelope{}, invalid()
	}
	// Hash a typed tuple: no separator ambiguity, cross-tenant serialization,
	// caller data in diagnostics or oversize ordering keys for long canonical IDs.
	ordering, _ := json.Marshal([4]string{"identity-ordering-v1", string(r.TenantID), r.SubjectType, r.SubjectID})
	key := sha256.Sum256(ordering)
	return outbox.Envelope{ID: r.ID, Topic: r.EventType, Payload: payload, PayloadVersion: 1,
		Metadata:    map[string]string{"identity.tenant_id": string(r.TenantID), "identity.command_id": string(r.CommandID)},
		OrderingKey: hex.EncodeToString(key[:]), IdempotencyKey: r.ID, AvailableAt: r.OccurredAt, CreatedAt: r.OccurredAt}, nil
}

const claimSQL = `WITH inserted AS (
 INSERT INTO public.identity_outbox_bindings(effect_id,tenant_id,command_id,event_index,canonical_sha256)
 VALUES($1::text,$2::text,$3::bytea,$4::integer,$5::bytea)
 ON CONFLICT DO NOTHING RETURNING effect_id
) SELECT EXISTS(SELECT 1 FROM inserted)`

const bindingSQL = `SELECT EXISTS(SELECT 1 FROM public.identity_outbox_bindings
 WHERE effect_id=$1::text AND tenant_id=$2::text AND command_id=$3::bytea
 AND event_index=$4::integer AND canonical_sha256=$5::bytea)`

// Delivery-owned attempts, availability, leases and terminal state are mutable.
// Compare only the immutable envelope profile; absence after authorized payload
// pruning is valid but cannot authorize insertion under the retained binding.
const envelopeSQL = `SELECT NOT EXISTS(SELECT 1 FROM public.outbox_messages WHERE id=$1::text)
 OR EXISTS(SELECT 1 FROM public.outbox_messages WHERE id=$1::text
 AND topic=$2::text AND payload=$3::bytea AND payload_version=$4::integer
 AND metadata=$5::jsonb AND ordering_key=$6::text AND idempotency_key=$7::text
 AND created_at=$8::timestamptz)`

// Private SQL seams let tests exercise the real owned statements with real
// PostgreSQL without manufacturing public Work authority or a pgx.Tx proxy.
// Production supplies only Work.Exec and the bounded WorkRows bool projection.
type execute func(context.Context, string, ...any) (int64, error)
type queryBool func(context.Context, string, ...any) (bool, error)

func (c *Contributor) persist(ctx context.Context, exec execute, query queryBool, r identitypostgres.StagedLifecycle) error {
	if err := ctx.Err(); err != nil {
		return failure(ctx, err)
	}
	envelope, err := lifecycleEnvelope(r)
	if err != nil {
		return err
	}
	return c.persistEnvelope(ctx, exec, query, r, envelope)
}

func (c *Contributor) persistEnvelope(ctx context.Context, exec execute, query queryBool, r identitypostgres.StagedLifecycle, envelope outbox.Envelope) error {
	if err := ctx.Err(); err != nil {
		return failure(ctx, err)
	}
	id, err := base64.RawURLEncoding.DecodeString(string(r.CommandID))
	if err != nil || len(id) != 16 {
		return invalid()
	}
	prepared, err := c.writer.PrepareInsert([]outbox.Envelope{envelope})
	if err != nil {
		return invalid()
	}
	digest := sha256.Sum256(envelope.CanonicalJSON())
	bound := []any{r.ID, string(r.TenantID), id, int32(r.EventIndex), digest[:]}
	inserted, err := query(ctx, claimSQL, bound...)
	if err != nil {
		return failure(ctx, err)
	}
	sql, args := prepared.Statement()
	if inserted {
		count, err := exec(ctx, sql, args...)
		if err != nil {
			return failure(ctx, err)
		}
		if count != 1 {
			return identity.UnavailableError{}
		}
		return nil
	}
	match, err := query(ctx, bindingSQL, bound...)
	if err != nil {
		return failure(ctx, err)
	}
	if !match {
		return identity.ConflictError{CommandID: r.CommandID}
	}
	match, err = query(ctx, envelopeSQL, args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[9])
	if err != nil {
		return failure(ctx, err)
	}
	if !match {
		return identity.ConflictError{CommandID: r.CommandID}
	}
	return nil
}
