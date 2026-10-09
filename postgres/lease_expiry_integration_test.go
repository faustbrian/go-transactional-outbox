//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	outboxpostgres "github.com/faustbrian/go-transactional-outbox/v2/postgres"
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
		apply func(context.Context, outboxpostgres.LeaseRef) (time.Time, error)
	}{
		{"delivered", "delivered", func(ctx context.Context, lease outboxpostgres.LeaseRef) (time.Time, error) {
			return time.Time{}, store.MarkDelivered(ctx, lease)
		}},
		{"extend", "leased", func(ctx context.Context, lease outboxpostgres.LeaseRef) (time.Time, error) {
			return store.ExtendLease(ctx, lease, 2*time.Minute)
		}},
		{"retry", "pending", func(ctx context.Context, lease outboxpostgres.LeaseRef) (time.Time, error) {
			return time.Time{}, store.Retry(ctx, lease, time.Minute, errors.New("retry fixture"))
		}},
		{"dead", "dead", func(ctx context.Context, lease outboxpostgres.LeaseRef) (time.Time, error) {
			return time.Time{}, store.DeadLetter(ctx, lease, errors.New("dead-letter fixture"))
		}},
		{"release", "pending", func(ctx context.Context, lease outboxpostgres.LeaseRef) (time.Time, error) {
			return time.Time{}, store.ReleaseLease(ctx, lease)
		}},
	} {
		for _, status := range []string{"expired", "active", "waiting"} {
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
				envelope := hardeningEnvelope(id)
				// Immediate eligibility follows the database clock, not the host clock.
				if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&envelope.AvailableAt); err != nil {
					t.Fatal(err)
				}
				if err := writer.Insert(ctx, tx, envelope); err != nil {
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
				if status == "expired" {
					// Expire without reclaiming: the original token remains current.
					if _, err := pool.Exec(ctx, `UPDATE outbox_lease_expiry
SET leased_until = clock_timestamp() - interval '1 second' WHERE id = $1`, id); err != nil {
						t.Fatal(err)
					}
				}
				if status == "waiting" {
					if _, err := pool.Exec(ctx, `UPDATE outbox_lease_expiry
SET leased_until = clock_timestamp() + interval '1 second' WHERE id = $1`, id); err != nil {
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
				lease := outboxpostgres.LeaseRef{ID: id, Token: claim.LeaseToken}
				var deadline time.Time
				if status == "waiting" {
					deadline, err = transitionAfterLeaseLockWait(t, ctx, pool, id, func(ctx context.Context) (time.Time, error) {
						return operation.apply(ctx, lease)
					})
				} else {
					deadline, err = operation.apply(ctx, lease)
				}
				if status != "active" {
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

func transitionAfterLeaseLockWait(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string,
	apply func(context.Context) (time.Time, error),
) (time.Time, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	locker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = locker.Rollback(cleanup)
	}()
	var originalDeadline time.Time
	if err := locker.QueryRow(ctx, `SELECT leased_until FROM outbox_lease_expiry
WHERE id = $1 FOR UPDATE`, id).Scan(&originalDeadline); err != nil {
		t.Fatal(err)
	}
	type transitionResult struct {
		deadline time.Time
		err      error
	}
	result := make(chan transitionResult, 1)
	joined := false
	t.Cleanup(func() {
		cancel()
		if !joined {
			select {
			case <-result:
			case <-time.After(5 * time.Second):
				t.Error("lease transition did not finish after cancellation")
			}
		}
	})
	go func() {
		deadline, err := apply(ctx)
		result <- transitionResult{deadline, err}
	}()
	waitForQueryLock(t, ctx, pool, "outbox_lease_expiry")
	var active bool
	if err := pool.QueryRow(ctx, "SELECT clock_timestamp() < $1", originalDeadline).Scan(&active); err != nil || !active {
		t.Fatalf("lease was not active when the transition began waiting: active=%v error=%v", active, err)
	}
	// Observe actual database-clock expiry without changing the locked row.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for active {
		select {
		case <-ctx.Done():
			t.Fatal("database clock did not reach the lease deadline")
		case <-ticker.C:
			if err := pool.QueryRow(ctx, "SELECT clock_timestamp() < $1", originalDeadline).Scan(&active); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := locker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case outcome := <-result:
		joined = true
		return outcome.deadline, outcome.err
	case <-ctx.Done():
		t.Fatal("lease transition did not finish after releasing its row lock")
		return time.Time{}, ctx.Err()
	}
}
