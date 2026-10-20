package identityoutbox

import (
	"errors"
	"strings"
	"testing"

	identity "github.com/faustbrian/go-identity"
	kernel "github.com/faustbrian/go-identity/postgres"
)

func TestPhoneResetEnvelopePreservesCanonicalCredentialEffect(t *testing.T) {
	r, receipt := replacementFixture()
	r.OperationID, r.EventType = "identity.phone.password-reset-complete", "password.reset.v1"
	envelope, err := resetEnvelope(r, receipt)
	if err != nil {
		t.Fatal("canonical phone reset lifecycle is disconnected", err)
	}
	want := `{"profile":"identity-credential-replacement-v1","lifecycle":{"profile":"identity-lifecycle-v1","id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","command_id":"AQEBAQEBAQEBAQEBAQEBAQ","tenant_id":"tenant-a","user_id":"user-a","operation_id":"identity.phone.password-reset-complete","event_type":"password.reset.v1","event_index":1,"subject_type":"credential","subject_id":"password-a","before_version":"18446744073709551614","after_version":"18446744073709551615","occurred_at":"2026-10-02T12:00:00.000123Z"},"before_user_version":"18446744073709551614","after_user_version":"18446744073709551615","before_authority_version":"7","after_authority_version":"8"}`
	if string(envelope.Payload) != want {
		t.Fatal("canonical phone credential-effect bytes changed")
	}
	if envelope.ID != r.ID || envelope.IdempotencyKey != r.ID || envelope.Topic != "password.reset.v1" || envelope.PayloadVersion != 1 || envelope.CreatedAt != r.OccurredAt || envelope.AvailableAt != r.OccurredAt {
		t.Fatal("phone reset envelope lost original lifecycle binding")
	}
	if len(envelope.Metadata) != 2 || envelope.Metadata["identity.tenant_id"] != "tenant-a" || envelope.Metadata["identity.command_id"] != "AQEBAQEBAQEBAQEBAQEBAQ" {
		t.Fatal("phone reset metadata changed")
	}
	original, err := lifecycleEnvelope(r)
	if err != nil || envelope.OrderingKey != original.OrderingKey {
		t.Fatal("phone reset ordering differs from original lifecycle", err)
	}
}

func TestCredentialEffectLegacyPayloadBytes(t *testing.T) {
	for _, tc := range []struct {
		name, operation, event string
		reset                  bool
	}{
		{"email reset", "identity.password.reset-complete", "password.reset.v1", true},
		{"password change", "identity.password.change", "password.changed.v1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, receipt := replacementFixture()
			r.OperationID, r.EventType = tc.operation, tc.event
			format := replacementEnvelope
			if tc.reset {
				format = resetEnvelope
			}
			envelope, err := format(r, receipt)
			if err != nil {
				t.Fatal(err)
			}
			want := `{"profile":"identity-credential-replacement-v1","lifecycle":{"profile":"identity-lifecycle-v1","id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","command_id":"AQEBAQEBAQEBAQEBAQEBAQ","tenant_id":"tenant-a","user_id":"user-a","operation_id":"OPERATION","event_type":"EVENT","event_index":1,"subject_type":"credential","subject_id":"password-a","before_version":"18446744073709551614","after_version":"18446744073709551615","occurred_at":"2026-10-02T12:00:00.000123Z"},"before_user_version":"18446744073709551614","after_user_version":"18446744073709551615","before_authority_version":"7","after_authority_version":"8"}`
			want = strings.ReplaceAll(strings.ReplaceAll(want, "OPERATION", tc.operation), "EVENT", tc.event)
			if string(envelope.Payload) != want {
				t.Fatal("legacy credential-effect bytes changed under retained binding")
			}
		})
	}
}

func TestPhoneResetEnvelopeRejectsForeignBindings(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*kernel.StagedLifecycle, *kernel.CredentialReplacementReceipt)
	}{
		{"missing operation", func(r *kernel.StagedLifecycle, _ *kernel.CredentialReplacementReceipt) { r.OperationID = "" }},
		{"reset initiation", func(r *kernel.StagedLifecycle, _ *kernel.CredentialReplacementReceipt) {
			r.OperationID = "identity.phone.password-reset-request"
		}},
		{"unknown reset", func(r *kernel.StagedLifecycle, _ *kernel.CredentialReplacementReceipt) {
			r.OperationID = "identity.other.password-reset-complete"
		}},
		{"change operation", func(r *kernel.StagedLifecycle, _ *kernel.CredentialReplacementReceipt) {
			r.OperationID = "identity.password.change"
		}},
		{"change event", func(r *kernel.StagedLifecycle, _ *kernel.CredentialReplacementReceipt) {
			r.EventType = "password.changed.v1"
		}},
		{"unknown event", func(r *kernel.StagedLifecycle, _ *kernel.CredentialReplacementReceipt) {
			r.EventType = "phone.password.reset.v1"
		}},
		{"foreign subject type", func(r *kernel.StagedLifecycle, _ *kernel.CredentialReplacementReceipt) { r.SubjectType = "user" }},
		{"foreign credential", func(r *kernel.StagedLifecycle, _ *kernel.CredentialReplacementReceipt) { r.SubjectID = "other" }},
		{"foreign tenant", func(r *kernel.StagedLifecycle, _ *kernel.CredentialReplacementReceipt) { r.TenantID = "other" }},
		{"foreign user", func(r *kernel.StagedLifecycle, _ *kernel.CredentialReplacementReceipt) { r.UserID = "other" }},
		{"foreign kind", func(_ *kernel.StagedLifecycle, s *kernel.CredentialReplacementReceipt) {
			s.Credential.Kind = identity.CredentialAPIKey
		}},
		{"foreign owner", func(_ *kernel.StagedLifecycle, s *kernel.CredentialReplacementReceipt) {
			s.Credential.Owner = "identity/phone"
		}},
		{"zero credential predecessor", func(_ *kernel.StagedLifecycle, s *kernel.CredentialReplacementReceipt) { s.BeforeCredentialVersion = 0 }},
		{"exhausted credential predecessor", func(_ *kernel.StagedLifecycle, s *kernel.CredentialReplacementReceipt) {
			s.BeforeCredentialVersion = ^uint64(0)
		}},
		{"foreign credential predecessor", func(r *kernel.StagedLifecycle, _ *kernel.CredentialReplacementReceipt) { r.BeforeVersion-- }},
		{"foreign credential successor", func(r *kernel.StagedLifecycle, _ *kernel.CredentialReplacementReceipt) { r.AfterVersion-- }},
		{"nonadjacent credential successor", func(r *kernel.StagedLifecycle, s *kernel.CredentialReplacementReceipt) {
			r.AfterVersion--
			s.Credential.Version--
		}},
		{"zero user predecessor", func(_ *kernel.StagedLifecycle, s *kernel.CredentialReplacementReceipt) { s.BeforeUserVersion = 0 }},
		{"exhausted user predecessor", func(_ *kernel.StagedLifecycle, s *kernel.CredentialReplacementReceipt) {
			s.BeforeUserVersion = ^uint64(0)
		}},
		{"nonadjacent user successor", func(_ *kernel.StagedLifecycle, s *kernel.CredentialReplacementReceipt) { s.AfterUserVersion-- }},
		{"zero authority predecessor", func(_ *kernel.StagedLifecycle, s *kernel.CredentialReplacementReceipt) { s.BeforeAuthorityVersion = 0 }},
		{"exhausted authority predecessor", func(_ *kernel.StagedLifecycle, s *kernel.CredentialReplacementReceipt) {
			s.BeforeAuthorityVersion = ^uint64(0)
		}},
		{"nonadjacent authority successor", func(_ *kernel.StagedLifecycle, s *kernel.CredentialReplacementReceipt) { s.AfterAuthorityVersion-- }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, receipt := replacementFixture()
			r.OperationID, r.EventType = "identity.phone.password-reset-complete", "password.reset.v1"
			tc.mutate(&r, &receipt)
			_, err := resetEnvelope(r, receipt)
			var invalid identity.InvalidInputError
			if !errors.As(err, &invalid) || invalid.Field != "lifecycle_outbox" {
				t.Fatal("foreign phone reset binding did not fail closed", err)
			}
		})
	}
}

func TestReplacementEnvelopeRejectsPhoneReset(t *testing.T) {
	r, receipt := replacementFixture()
	r.OperationID, r.EventType = "identity.phone.password-reset-complete", "password.reset.v1"
	if _, err := replacementEnvelope(r, receipt); err == nil {
		t.Fatal("password change admitted a phone reset")
	}
}
