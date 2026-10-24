package identityoutbox

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	identity "github.com/faustbrian/go-identity"
	kernel "github.com/faustbrian/go-identity/postgres"
)

func replacementFixture() (kernel.StagedLifecycle, kernel.CredentialReplacementReceipt) {
	r := lifecycleFixture()
	r.OperationID, r.EventType, r.SubjectType, r.SubjectID = "identity.password.change", "password.changed.v1", "credential", "password-a"
	return r, kernel.CredentialReplacementReceipt{Credential: identity.CredentialRef{ID: "password-a", TenantID: r.TenantID, UserID: r.UserID, Kind: identity.CredentialPassword, Owner: "identity/password", Version: r.AfterVersion}, BeforeUserVersion: r.BeforeVersion, AfterUserVersion: r.AfterVersion, BeforeAuthorityVersion: 7, AfterAuthorityVersion: 8, BeforeCredentialVersion: r.BeforeVersion}
}

func TestReplacementEnvelopePreservesIndependentSuccessors(t *testing.T) {
	r, receipt := replacementFixture()
	e, err := replacementEnvelope(r, receipt)
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Profile         string          `json:"profile"`
		Lifecycle       json.RawMessage `json:"lifecycle"`
		BeforeUser      string          `json:"before_user_version"`
		AfterUser       string          `json:"after_user_version"`
		BeforeAuthority string          `json:"before_authority_version"`
		AfterAuthority  string          `json:"after_authority_version"`
	}
	if json.Unmarshal(e.Payload, &payload) != nil || payload.Profile != "identity-credential-replacement-v1" || payload.BeforeUser != "18446744073709551614" || payload.AfterUser != "18446744073709551615" || payload.BeforeAuthority != "7" || payload.AfterAuthority != "8" {
		t.Fatal("independent successors lost")
	}
	original, _ := lifecycleEnvelope(r)
	if string(payload.Lifecycle) != string(original.Payload) || e.ID != original.ID || e.OrderingKey != original.OrderingKey || e.Topic != original.Topic {
		t.Fatal("original lifecycle binding changed")
	}
	for _, mutate := range []func(*kernel.CredentialReplacementReceipt){
		func(c *kernel.CredentialReplacementReceipt) { c.Credential.UserID = "other" },
		func(c *kernel.CredentialReplacementReceipt) { c.Credential.TenantID = "other" },
		func(c *kernel.CredentialReplacementReceipt) { c.Credential.ID = "other" },
		func(c *kernel.CredentialReplacementReceipt) { c.Credential.Owner = "passkey" },
		func(c *kernel.CredentialReplacementReceipt) { c.Credential.Kind = identity.CredentialAPIKey },
		func(c *kernel.CredentialReplacementReceipt) { c.BeforeCredentialVersion-- },
		func(c *kernel.CredentialReplacementReceipt) { c.Credential.Version-- },
		func(c *kernel.CredentialReplacementReceipt) { c.BeforeUserVersion = 0 },
		func(c *kernel.CredentialReplacementReceipt) { c.AfterUserVersion-- },
		func(c *kernel.CredentialReplacementReceipt) { c.BeforeAuthorityVersion = ^uint64(0) },
		func(c *kernel.CredentialReplacementReceipt) { c.AfterAuthorityVersion-- },
	} {
		bad := receipt
		mutate(&bad)
		if _, err := replacementEnvelope(r, bad); err == nil {
			t.Fatal("foreign or exhausted transition accepted")
		}
	}
}

func TestReplacementContributorRequiresNativeWork(t *testing.T) {
	c, _ := NewContributor()
	var _ kernel.CredentialReplacementLifecycleContributor = c
	for _, owner := range []*Contributor{nil, {}, c} {
		for _, ctx := range []context.Context{nil, context.Background()} {
			if err := owner.PersistCredentialReplacement(ctx, kernel.Work{}); err == nil {
				t.Fatal("missing authority accepted")
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.PersistCredentialReplacement(ctx, kernel.Work{}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost")
	}
}

func TestResetEnvelopePreservesSuccessorsAndDistinctPurpose(t *testing.T) {
	r, receipt := replacementFixture()
	r.OperationID, r.EventType = "identity.password.reset-complete", "password.reset.v1"
	envelope, err := resetEnvelope(r, receipt)
	if err != nil {
		t.Fatal("reset lifecycle is disconnected", err)
	}
	var payload map[string]json.RawMessage
	if json.Unmarshal(envelope.Payload, &payload) != nil || string(payload["before_authority_version"]) != `"7"` || string(payload["after_authority_version"]) != `"8"` || string(payload["profile"]) != `"identity-credential-replacement-v1"` {
		t.Fatal("independent reset successors lost")
	}
	original, _ := lifecycleEnvelope(r)
	if string(payload["lifecycle"]) != string(original.Payload) || envelope.ID != original.ID || envelope.OrderingKey != original.OrderingKey || envelope.Topic != original.Topic {
		t.Fatal("original reset lifecycle replaced")
	}
	bad := receipt
	bad.Credential.UserID = "foreign"
	if _, err := resetEnvelope(r, bad); err == nil {
		t.Fatal("foreign reset subject admitted")
	}
	if _, err := replacementEnvelope(r, receipt); err == nil {
		t.Fatal("change owner accepted reset")
	}
	r.EventType = "password.changed.v1"
	if _, err := resetEnvelope(r, receipt); err == nil {
		t.Fatal("mixed lifecycle admitted")
	}
	r, _ = replacementFixture()
	if _, err := resetEnvelope(r, receipt); err == nil {
		t.Fatal("reset owner accepted change")
	}
}

func TestResetContributorRequiresNativeWork(t *testing.T) {
	c, _ := NewContributor()
	var _ kernel.CredentialResetLifecycleContributor = c
	for _, owner := range []*Contributor{nil, {}, c} {
		if err := owner.PersistCredentialReset(context.Background(), kernel.Work{}); err == nil {
			t.Fatal("missing native reset authority accepted")
		}
	}
}
