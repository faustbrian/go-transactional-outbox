package identityoutbox

import (
	"context"
	"errors"
	"testing"

	identity "github.com/faustbrian/go-identity"
	identitypostgres "github.com/faustbrian/go-identity/postgres"
)

func TestContributorRejectsMissingAuthority(t *testing.T) {
	c, err := NewContributor()
	if err != nil {
		t.Fatal(err)
	}
	var _ identitypostgres.RecoveryContributor = c
	// Deliberately absent input exercises rejection, not a live caller context.
	var missingContext context.Context
	for _, run := range []func() error{
		func() error { return c.PersistLifecycle(context.Background(), identitypostgres.Work{}) },
		func() error { return c.PersistLifecycle(missingContext, identitypostgres.Work{}) },
		func() error {
			return (*Contributor)(nil).PersistLifecycle(context.Background(), identitypostgres.Work{})
		},
		func() error { return (&Contributor{}).PersistLifecycle(context.Background(), identitypostgres.Work{}) },
	} {
		var invalid identity.InvalidInputError
		if err := run(); !errors.As(err, &invalid) {
			t.Fatalf("missing authority: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.PersistLifecycle(ctx, identitypostgres.Work{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	registry, err := identitypostgres.NewContributorRegistry([]identitypostgres.Contributor{c})
	if err != nil || registry.Descriptors()[0] != (identitypostgres.ContributorDescriptor{Owner: "outbox/postgres", ContractID: "identity.lifecycle", Version: 1}) {
		t.Fatal("composition contract rejected", err)
	}
}

func TestRecoveryRejectsMissingAuthority(t *testing.T) {
	c, err := NewContributor()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		c   *Contributor
		ctx context.Context
	}{
		{c, context.Background()}, {c, nil}, {nil, context.Background()}, {&Contributor{}, context.Background()},
	} {
		status, err := test.c.RecoverParticipant(test.ctx, identitypostgres.RecoveryWork{})
		var invalid identity.InvalidInputError
		if status != identitypostgres.RecoveryUnknown || !errors.As(err, &invalid) {
			t.Fatal("invalid authority became conclusive", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	status, err := c.RecoverParticipant(ctx, identitypostgres.RecoveryWork{})
	if status != identitypostgres.RecoveryUnknown || !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
}
