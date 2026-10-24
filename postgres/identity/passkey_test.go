package identityoutbox

import (
	"context"
	"errors"
	"testing"
	"time"

	kernel "github.com/faustbrian/go-identity/postgres"
)

func TestPasskeyRegistrationContributorRequiresNativeWork(t *testing.T) {
	c, err := NewContributor()
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []*Contributor{nil, {}, c} {
		phase, ok := any(owner).(kernel.PasskeyRegistrationLifecycleContributor)
		if !ok {
			t.Fatal("native passkey account-creation lifecycle is disconnected")
		}
		for _, ctx := range []context.Context{nil, context.Background()} {
			if err := phase.PersistPasskeyRegistration(ctx, kernel.Work{}); err == nil {
				t.Fatal("unstaged creation accepted")
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	phase := any(c).(kernel.PasskeyRegistrationLifecycleContributor)
	if err := phase.PersistPasskeyRegistration(ctx, kernel.Work{}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost")
	}
}

func TestPasskeySignupSessionContributorRequiresNativeWork(t *testing.T) {
	c, err := NewContributor()
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []*Contributor{nil, {}, c} {
		phase, ok := any(owner).(kernel.PasskeySignupSessionLifecycleContributor)
		if !ok {
			t.Fatal("native passkey signup session lifecycle is disconnected")
		}
		for _, ctx := range []context.Context{nil, context.Background()} {
			if err := phase.PersistPasskeySignupSession(ctx, kernel.Work{}); err == nil {
				t.Fatal("unstaged signup session accepted")
			}
		}
	}
	for _, deadline := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		want := error(context.Canceled)
		if deadline {
			ctx, cancel = context.WithDeadline(context.Background(), time.Time{})
			cancel()
			want = context.DeadlineExceeded
		}
		phase := any(c).(kernel.PasskeySignupSessionLifecycleContributor)
		if err := phase.PersistPasskeySignupSession(ctx, kernel.Work{}); !errors.Is(err, want) {
			t.Fatal("context classification lost")
		}
	}
}
