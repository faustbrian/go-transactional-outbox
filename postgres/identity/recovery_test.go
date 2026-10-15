package identityoutbox

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	identity "github.com/faustbrian/go-identity"
	identitypostgres "github.com/faustbrian/go-identity/postgres"
)

func TestRecoveryClassifiesOnlyRetainedEvidence(t *testing.T) {
	c, err := NewContributor()
	if err != nil {
		t.Fatal(err)
	}
	binding := identity.CommandBinding{TenantID: "tenant-a", CommandID: "AQEBAQEBAQEBAQEBAQEBAQ"}
	plan := identitypostgres.LifecyclePlan{EventType: "identity.created.v1", EventIndex: 65535, BeforeVersion: 0, AfterVersion: 1}
	for _, test := range []struct {
		value int16
		want  identitypostgres.RecoveryStatus
		bad   bool
	}{
		{0, identitypostgres.RecoveryNotCommitted, false}, {1, identitypostgres.RecoveryCommitted, false},
		{2, identitypostgres.RecoveryUnknown, false}, {-1, identitypostgres.RecoveryUnknown, true}, {3, identitypostgres.RecoveryUnknown, true},
	} {
		status, err := c.recover(context.Background(), func(_ context.Context, sql string, args ...any) (int16, error) {
			if sql != recoverySQL || len(args) != 3 || args[0] != "tenant-a" || len(args[1].([]byte)) != 16 || args[2] != int32(65535) {
				t.Fatal("frozen scope changed")
			}
			return test.value, nil
		}, binding, plan)
		if status != test.want || (err != nil) != test.bad {
			t.Fatal("incorrect evidence classification", status, err)
		}
	}
	cause := errors.New("private database detail")
	status, err := c.recover(context.Background(), func(context.Context, string, ...any) (int16, error) { return 0, cause }, binding, plan)
	var unavailable identity.UnavailableError
	if status != identitypostgres.RecoveryUnknown || !errors.As(err, &unavailable) || strings.Contains(err.Error(), cause.Error()) {
		t.Fatal("failure became conclusive or disclosed diagnostics")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	status, err = c.recover(ctx, func(context.Context, string, ...any) (int16, error) { t.Fatal("cancelled query"); return 0, nil }, binding, plan)
	if status != identitypostgres.RecoveryUnknown || !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled recovery conclusive")
	}
	expired, stop := context.WithDeadline(context.Background(), time.Time{})
	defer stop()
	status, err = c.RecoverParticipant(expired, identitypostgres.RecoveryWork{})
	if status != identitypostgres.RecoveryUnknown || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("deadline classification lost")
	}
	for _, command := range []identity.CommandID{"invalid!", "AA"} {
		bad := binding
		bad.CommandID = command
		status, err := c.recover(context.Background(), func(context.Context, string, ...any) (int16, error) { t.Fatal("invalid scope queried"); return 0, nil }, bad, plan)
		if status != identitypostgres.RecoveryUnknown || err == nil {
			t.Fatal("invalid command conclusive")
		}
	}
}
