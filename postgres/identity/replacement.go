package identityoutbox

import (
	"context"
	"encoding/json"
	"strconv"

	identity "github.com/faustbrian/go-identity"
	kernel "github.com/faustbrian/go-identity/postgres"
	outbox "github.com/faustbrian/go-transactional-outbox/v2"
)

var _ kernel.CredentialReplacementLifecycleContributor = (*Contributor)(nil)
var _ kernel.CredentialResetLifecycleContributor = (*Contributor)(nil)

// PersistCredentialReplacement persists the staged password-change event and
// its independent credential, user and credential-authority successors through
// the original Work. Only the kernel coordinator invokes this after successful
// mutation and audit staging. It never reads current authority, renews evidence,
// closes Work or acknowledges downstream invalidation. Original bindings and
// pruning-safe replay use the existing outbox owner; no new schema is required.
// Nil, cancelled or unstaged Work denies. Calls are safe across independent Work
// values; callers must not use one Work concurrently or retain it after return.
func (c *Contributor) PersistCredentialReplacement(ctx context.Context, w kernel.Work) error {
	return c.persistCredentialEffect(ctx, w, false)
}

// PersistCredentialReset persists the separately staged password-reset event
// and exact aggregate/credential-authority successors in the same Work. It does
// not authorize recovery or acknowledge session invalidation; those effects must
// already be staged by their native owners. No transaction, retry, fresh lease
// or additional journal is introduced. Nil/canceled/unowned Work denies, and
// callers must propagate errors to the coordinator. Independent calls are safe;
// the borrowed Work remains serial and must not be closed or retained.
func (c *Contributor) PersistCredentialReset(ctx context.Context, w kernel.Work) error {
	return c.persistCredentialEffect(ctx, w, true)
}

func (c *Contributor) persistCredentialEffect(ctx context.Context, w kernel.Work, reset bool) error {
	if c == nil || c.writer == nil || ctx == nil {
		return invalid()
	}
	if err := ctx.Err(); err != nil {
		return failure(ctx, err)
	}
	receipt, err := w.StagedCredentialReplacement()
	if err != nil {
		return err
	}
	records, err := w.StagedLifecycle()
	if err != nil {
		return err
	}
	if len(records) != 1 {
		return invalid()
	}
	envelope, err := replacementEnvelope(records[0], receipt)
	if reset {
		envelope, err = resetEnvelope(records[0], receipt)
	}
	if err != nil {
		return err
	}
	return c.persistEnvelope(ctx, w.Exec, workQuery(w), records[0], envelope)
}

func replacementEnvelope(r kernel.StagedLifecycle, s kernel.CredentialReplacementReceipt) (outbox.Envelope, error) {
	return credentialEffectEnvelope(r, s, "identity.password.change", "password.changed.v1")
}

func resetEnvelope(r kernel.StagedLifecycle, s kernel.CredentialReplacementReceipt) (outbox.Envelope, error) {
	switch r.OperationID {
	case "identity.password.reset-complete", "identity.phone.password-reset-complete":
		return credentialEffectEnvelope(r, s, r.OperationID, "password.reset.v1")
	default:
		return outbox.Envelope{}, invalid()
	}
}

func credentialEffectEnvelope(r kernel.StagedLifecycle, s kernel.CredentialReplacementReceipt, operation, event string) (outbox.Envelope, error) {
	c := s.Credential
	if r.OperationID != operation || r.EventType != event || r.SubjectType != "credential" || string(c.ID) != r.SubjectID || c.TenantID != r.TenantID || c.UserID != r.UserID || c.Kind != identity.CredentialPassword || c.Owner != "identity/password" || s.BeforeCredentialVersion == 0 || s.BeforeCredentialVersion == ^uint64(0) || r.BeforeVersion != s.BeforeCredentialVersion || r.AfterVersion != c.Version || c.Version != s.BeforeCredentialVersion+1 || s.BeforeUserVersion == 0 || s.BeforeUserVersion == ^uint64(0) || s.AfterUserVersion != s.BeforeUserVersion+1 || s.BeforeAuthorityVersion == 0 || s.BeforeAuthorityVersion == ^uint64(0) || s.AfterAuthorityVersion != s.BeforeAuthorityVersion+1 {
		return outbox.Envelope{}, invalid()
	}
	envelope, err := lifecycleEnvelope(r)
	if err != nil {
		return outbox.Envelope{}, err
	}
	// Keep the generic and existing change profiles byte-compatible. The credential
	// payload binds all independently owned successors to its retained digest.
	envelope.Payload, err = json.Marshal(struct {
		Profile         string          `json:"profile"`
		Lifecycle       json.RawMessage `json:"lifecycle"`
		BeforeUser      string          `json:"before_user_version"`
		AfterUser       string          `json:"after_user_version"`
		BeforeAuthority string          `json:"before_authority_version"`
		AfterAuthority  string          `json:"after_authority_version"`
	}{"identity-credential-replacement-v1", envelope.Payload, strconv.FormatUint(s.BeforeUserVersion, 10), strconv.FormatUint(s.AfterUserVersion, 10), strconv.FormatUint(s.BeforeAuthorityVersion, 10), strconv.FormatUint(s.AfterAuthorityVersion, 10)})
	if err != nil {
		return outbox.Envelope{}, invalid()
	}
	return envelope, nil
}
