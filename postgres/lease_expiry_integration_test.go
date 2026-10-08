//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	outboxpostgres "github.com/faustbrian/go-transactional-outbox/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testLeaseExpiryFencing(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, "CREATE TABLE outbox_lease_expiry (LIKE outbox_messages INCLUDING ALL)"); err != nil {
		t.Fatalf("create lease-expiry table: %v", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := pool.Exec(cleanup, "DROP TABLE outbox_lease_expiry"); err != nil {
			t.Errorf("drop lease-expiry table: %v", err)
		}
	})
	writer, err := outboxpostgres.NewWriter(outboxpostgres.WriterConfig{Table: "outbox_lease_expiry"})
	if err != nil {
		t.Fatal(err)
	}
	store, err := outboxpostgres.NewStore(pool, outboxpostgres.StoreConfig{Table: "outbox_lease_expiry"})
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []struct {
		name  string
		state string
		apply func(outboxpostgres.LeaseRef) (time.Time, error)
	}{
		{"delivered", "delivered", func(lease outboxpostgres.LeaseRef) (time.Time, error) {
			return time.Time{}, store.MarkDelivered(ctx, lease)
		}},
		{"extend", "leased", func(lease outboxpostgres.LeaseRef) (time.Time, error) {
			return store.ExtendLease(ctx, lease, 2*time.Minute)
		}},
		{"retry", "pending", func(lease outboxpostgres.LeaseRef) (time.Time, error) {
			return time.Time{}, store.Retry(ctx, lease, time.Minute, errors.New("retry fixture"))
		}},
		{"dead", "dead", func(lease outboxpostgres.LeaseRef) (time.Time, error) {
			return time.Time{}, store.DeadLetter(ctx, lease, errors.New("dead-letter fixture"))
		}},
		{"release", "pending", func(lease outboxpostgres.LeaseRef) (time.Time, error) {
			return time.Time{}, store.ReleaseLease(ctx, lease)
		}},
	} {
		for _, expired := range []bool{true, false} {
			status := "active"
			if expired {
				status = "expired"
			}
			t.Run(operation.name+"/"+status, func(t *testing.T) {
				id := "lease-" + operation.name + "-" + status
				t.Cleanup(func() {
					cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if _, err := pool.Exec(cleanup, "DELETE FROM outbox_lease_expiry WHERE id = $1", id); err != nil {
						t.Errorf("delete lease fixture: %v", err)
					}
				})
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				if err := writer.Insert(ctx, tx, hardeningEnvelope(id)); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				claims, err := store.Claim(ctx, outboxpostgres.ClaimRequest{
					Owner: "lease-fixture", Limit: 1, LeaseDuration: time.Minute,
				})
				if err != nil || len(claims) != 1 || claims[0].Envelope.ID != id {
					t.Fatalf("claim isolated lease fixture: count=%d error=%v", len(claims), err)
				}
				claim := claims[0]
				if expired {
					// Expire without reclaiming: the original token remains current.
					if _, err := pool.Exec(ctx, `UPDATE outbox_lease_expiry
SET leased_until = clock_timestamp() - interval '1 second' WHERE id = $1`, id); err != nil {
						t.Fatal(err)
					}
				}
				readRow := func() string {
					var row string
					if err := pool.QueryRow(ctx, `SELECT to_jsonb(message)::text
FROM outbox_lease_expiry AS message WHERE id = $1`, id).Scan(&row); err != nil {
						t.Fatal(err)
					}
					return row
				}
				before := readRow()
				deadline, err := operation.apply(outboxpostgres.LeaseRef{ID: id, Token: claim.LeaseToken})
				if expired {
					if !errors.Is(err, outboxpostgres.ErrLeaseLost) || !deadline.IsZero() {
						t.Errorf("expired lease error/deadline = %v/%s, want lost/zero", err, deadline)
					}
					if readRow() != before {
						t.Error("expired lease changed the persisted record before replacement")
					}
					return
				}
				if err != nil {
					t.Fatalf("active lease transition: %v", err)
				}
				var state, token string
				if err := pool.QueryRow(ctx, `SELECT state, COALESCE(lease_token, '')
FROM outbox_lease_expiry WHERE id = $1`, id).Scan(&state, &token); err != nil {
					t.Fatal(err)
				}
				if state != operation.state {
					t.Fatalf("active transition state = %q, want %q", state, operation.state)
				}
				if operation.name == "extend" {
					if token != claim.LeaseToken || !deadline.After(claim.LeasedUntil) {
						t.Error("active extension did not preserve its token and advance its deadline")
					}
				} else if token != "" {
					t.Error("terminal or released transition retained its lease token")
				}
			})
		}
	}
}
