package identityoutbox

import (
	"context"
	"errors"
	"testing"

	kernel "github.com/faustbrian/go-identity/postgres"
)

func TestMagicLinkRegistrationContributorRequiresNativeWork(t *testing.T) {
	c, err := NewContributor()
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []*Contributor{nil, {}, c} {
		phase, ok := any(owner).(interface {
			PersistMagicLinkRegistration(context.Context, kernel.Work) error
		})
		if !ok {
			t.Fatal("native magic-link account-creation lifecycle is disconnected")
		}
		for _, ctx := range []context.Context{nil, context.Background()} {
			if err := phase.PersistMagicLinkRegistration(ctx, kernel.Work{}); err == nil {
				t.Fatal("unstaged creation accepted")
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	phase := any(c).(interface {
		PersistMagicLinkRegistration(context.Context, kernel.Work) error
	})
	if err := phase.PersistMagicLinkRegistration(ctx, kernel.Work{}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost")
	}
}
