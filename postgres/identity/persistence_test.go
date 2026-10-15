package identityoutbox

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	identity "github.com/faustbrian/go-identity"
	identitypostgres "github.com/faustbrian/go-identity/postgres"
)

func lifecycleFixture() identitypostgres.StagedLifecycle {
	return identitypostgres.StagedLifecycle{
		ID: strings.Repeat("a", 64), CommandID: identity.CommandID("AQEBAQEBAQEBAQEBAQEBAQ"),
		TenantID: "tenant-a", UserID: "user-a", OperationID: "identity.user.update",
		EventType: "identity.updated.v1", EventIndex: 1, SubjectType: "user", SubjectID: "user-a",
		BeforeVersion: ^uint64(0) - 1, AfterVersion: ^uint64(0),
		OccurredAt: time.Date(2026, 10, 2, 12, 0, 0, 123000, time.UTC),
	}
}

func TestLifecycleEnvelopeHasLosslessSafeProfile(t *testing.T) {
	r := lifecycleFixture()
	e, err := lifecycleEnvelope(r)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(e.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload) != 13 || payload["before_version"] != "18446744073709551614" || payload["after_version"] != "18446744073709551615" || payload["tenant_id"] != "tenant-a" || payload["occurred_at"] != "2026-10-02T12:00:00.000123Z" {
		t.Fatal("safe profile changed", payload)
	}
	if e.ID != r.ID || e.Topic != r.EventType || e.IdempotencyKey != r.ID || e.PayloadVersion != 1 || e.CreatedAt != r.OccurredAt || e.AvailableAt != r.OccurredAt {
		t.Fatal("envelope lost binding")
	}
	r.TenantID = "tenant-b"
	other, err := lifecycleEnvelope(r)
	if err != nil || other.OrderingKey == e.OrderingKey {
		t.Fatal("ordering crossed tenant", err)
	}
}

func TestLifecycleEnvelopeRetainsCascadeCorrelation(t *testing.T) {
	r := lifecycleFixture()
	r.CascadeID = identity.CascadeID("AQEBAQEBAQEBAQEBAQEBAQ")
	r.CascadeGeneration = ^uint64(0)
	e, err := lifecycleEnvelope(r)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(e.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload) != 15 || payload["cascade_id"] != string(r.CascadeID) || payload["cascade_generation"] != "18446744073709551615" {
		t.Fatal("cascade correlation was dropped or rounded")
	}
}

func TestLifecycleEnvelopeWithoutCascadeKeepsOriginalBytes(t *testing.T) {
	e, err := lifecycleEnvelope(lifecycleFixture())
	if err != nil {
		t.Fatal(err)
	}
	want := `{"profile":"identity-lifecycle-v1","id":"` + strings.Repeat("a", 64) + `","command_id":"AQEBAQEBAQEBAQEBAQEBAQ","tenant_id":"tenant-a","user_id":"user-a","operation_id":"identity.user.update","event_type":"identity.updated.v1","event_index":1,"subject_type":"user","subject_id":"user-a","before_version":"18446744073709551614","after_version":"18446744073709551615","occurred_at":"2026-10-02T12:00:00.000123Z"}`
	if string(e.Payload) != want {
		t.Fatal("non-cascade bytes changed under an immutable event binding")
	}
}
