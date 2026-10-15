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
