package identityoutbox

import (
	"context"
	"errors"
	"testing"

	kernel "github.com/faustbrian/go-identity/postgres"
)

func TestPairedNativeLifecyclesRejectUnstagedWork(t *testing.T) {
	c, err := NewContributor()
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"invitation", "consent"} {
		t.Run(kind, func(t *testing.T) {
			for _, owner := range []*Contributor{nil, {}, c} {
				var persist func(context.Context, kernel.Work) error
				if kind == "invitation" {
					phase, ok := any(owner).(interface {
						PersistOrganizationInvitation(context.Context, kernel.Work) error
					})
					if !ok {
						t.Fatal("atomic invitation/membership outbox is disconnected")
					}
					persist = phase.PersistOrganizationInvitation
				} else {
					phase, ok := any(owner).(interface {
						PersistOAuthConsent(context.Context, kernel.Work) error
					})
					if !ok {
						t.Fatal("atomic grant/consent outbox is disconnected")
					}
					persist = phase.PersistOAuthConsent
				}
				for _, ctx := range []context.Context{nil, context.Background()} {
					if err := persist(ctx, kernel.Work{}); err == nil {
						t.Fatal("unstaged secondary event accepted")
					}
				}
				if owner == c {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					if err := persist(ctx, kernel.Work{}); !errors.Is(err, context.Canceled) {
						t.Fatal("original cancellation lost", err)
					}
				}
			}
		})
	}
}
