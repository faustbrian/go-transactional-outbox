//go:build integration

package identityoutbox

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"time"

	identity "github.com/faustbrian/go-identity"
	identitypostgres "github.com/faustbrian/go-identity/postgres"
	outboxpostgres "github.com/faustbrian/go-transactional-outbox/v2/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestLifecyclePersistenceIntegration(t *testing.T) {
	config := IdentityTestPostgresConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal("disposable cluster unavailable")
	}
	defer admin.Close(context.Background())
	unique, err := identity.NewSubjectID()
	if err != nil {
		t.Fatal(err)
	}
	name := "outbox_test_" + string(unique)
	adminConfig := config.Copy()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		cleanupAdmin, err := pgx.ConnectConfig(cleanup, adminConfig)
		if err != nil {
			t.Error("cleanup connection unavailable")
			return
		}
		defer cleanupAdmin.Close(cleanup)
		if _, err := cleanupAdmin.Exec(cleanup, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
			t.Error("owned database cleanup failed")
		}
	})
	config.Database = name
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	apply := func(source fs.FS, name string) {
		b, err := fs.ReadFile(source, name)
		if err != nil {
			t.Fatal(err)
		}
		parts := strings.Split(string(b), "-- +migrations Down")
		up := strings.TrimPrefix(parts[0], "-- +migrations Up\n")
		if _, err := conn.Exec(ctx, up); err != nil {
			t.Fatal("migration failed", err)
		}
	}
	apply(outboxpostgres.Migrations(), "000001_create_outbox.sql")
	// A populated generic outbox remains untouched by the additive adapter migration.
	if _, err := conn.Exec(ctx, `INSERT INTO public.outbox_messages(id,topic,payload,payload_version,available_at,created_at) VALUES('existing','other',$1,1,clock_timestamp(),clock_timestamp())`, []byte("old")); err != nil {
		t.Fatal(err)
	}
	apply(Migrations(), "000001_identity_lifecycle.sql")
	if _, err := conn.Exec(ctx, `CREATE TABLE public.test_domain(id text PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	c, err := NewContributor()
	if err != nil {
		t.Fatal(err)
	}
	r := lifecycleFixture()
	persist := func(tx pgx.Tx, r identitypostgres.StagedLifecycle) error {
		return c.persist(ctx, func(ctx context.Context, sql string, args ...any) (int64, error) {
			tag, err := tx.Exec(ctx, sql, args...)
			return tag.RowsAffected(), err
		},
			func(ctx context.Context, sql string, args ...any) (bool, error) {
				var b bool
				err := tx.QueryRow(ctx, sql, args...).Scan(&b)
				return b, err
			}, r)
	}
	recover := func(tx pgx.Tx, scope identity.CommandBinding, plan identitypostgres.LifecyclePlan) (identitypostgres.RecoveryStatus, error) {
		return c.recover(ctx, func(ctx context.Context, sql string, args ...any) (int16, error) {
			var status int16
			err := tx.QueryRow(ctx, sql, args...).Scan(&status)
			return status, err
		}, scope, plan)
	}
	scope := identity.CommandBinding{TenantID: r.TenantID, CommandID: r.CommandID}
	plan := identitypostgres.LifecyclePlan{EventType: r.EventType, EventIndex: r.EventIndex, BeforeVersion: r.BeforeVersion, AfterVersion: r.AfterVersion}
	counts := func(domain, bindings, messages int) {
		var d, b, m int
		if err := conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM public.test_domain),(SELECT count(*) FROM public.identity_outbox_bindings),(SELECT count(*) FROM public.outbox_messages WHERE topic='identity.updated.v1')`).Scan(&d, &b, &m); err != nil || d != domain || b != bindings || m != messages {
			t.Fatalf("counts %d/%d/%d: %v", d, b, m, err)
		}
	}
	t.Run("atomic rollback and commit", func(t *testing.T) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err = tx.Exec(ctx, `INSERT INTO public.test_domain VALUES('user-a')`); err != nil {
			t.Fatal(err)
		}
		if err = persist(tx, r); err != nil {
			t.Fatal(err)
		}
		if err = tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		counts(0, 0, 0)
		tx, err = conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err = tx.Exec(ctx, `INSERT INTO public.test_domain VALUES('user-a')`); err != nil {
			t.Fatal(err)
		}
		if err = persist(tx, r); err != nil {
			t.Fatal(err)
		}
		if err = persist(tx, r); err != nil {
			t.Fatal("exact replay rejected", err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		counts(1, 1, 1)
	})
	t.Run("recovery is read only and scope exact", func(t *testing.T) {
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		status, err := recover(tx, scope, plan)
		if err != nil || status != identitypostgres.RecoveryCommitted {
			t.Fatal("retained effect missing", err)
		}
		absent := scope
		absent.CommandID = "BAQEBAQEBAQEBAQEBAQEBA"
		status, err = recover(tx, absent, plan)
		if err != nil || status != identitypostgres.RecoveryNotCommitted {
			t.Fatal("absence not proven", err)
		}
		absent = scope
		absent.TenantID = "tenant-b"
		status, err = recover(tx, absent, plan)
		if err != nil || status != identitypostgres.RecoveryNotCommitted {
			t.Fatal("tenant isolation failed", err)
		}
		wrong := plan
		wrong.EventIndex++
		status, err = recover(tx, scope, wrong)
		if err != nil || status != identitypostgres.RecoveryUnknown {
			t.Fatal("wrong-index binding became conclusive", err)
		}
		counts(1, 1, 1)
	})
	t.Run("extra binding remains unknown", func(t *testing.T) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		extra := r
		extra.ID = strings.Repeat("d", 64)
		extra.EventIndex++
		if err = persist(tx, extra); err != nil {
			t.Fatal(err)
		}
		status, err := recover(tx, scope, plan)
		if err != nil || status != identitypostgres.RecoveryUnknown {
			t.Fatal("extra effect ignored", err)
		}
	})
	t.Run("binding SELECT privilege alone permits recovery", func(t *testing.T) {
		// Role creation is transactional and rolled back with this test's
		// authority probe; no cluster-wide role survives the invocation.
		for _, forbidden := range []string{
			`SELECT id FROM public.outbox_messages`,
			`INSERT INTO public.identity_outbox_bindings(effect_id,tenant_id,command_id,event_index,canonical_sha256) SELECT effect_id,tenant_id,command_id,event_index,canonical_sha256 FROM public.identity_outbox_bindings`,
		} {
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if _, err = tx.Exec(ctx, `CREATE ROLE outbox_recovery_reader NOLOGIN; GRANT USAGE ON SCHEMA public TO outbox_recovery_reader; GRANT SELECT ON public.identity_outbox_bindings TO outbox_recovery_reader; SET LOCAL ROLE outbox_recovery_reader`); err != nil {
				t.Fatal(err)
			}
			status, err := recover(tx, scope, plan)
			if err != nil || status != identitypostgres.RecoveryCommitted {
				t.Fatal("binding-only reader cannot recover", err)
			}
			_, err = tx.Exec(ctx, forbidden)
			var failure *pgconn.PgError
			if !errors.As(err, &failure) || failure.Code != "42501" {
				t.Fatal("reader has excess privilege")
			}
			if err = tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("every immutable conflict rejected", func(t *testing.T) {
		for _, alter := range []func(*identitypostgres.StagedLifecycle){
			func(r *identitypostgres.StagedLifecycle) { r.TenantID = "tenant-b" },
			func(r *identitypostgres.StagedLifecycle) { r.CommandID = "AgICAgICAgICAgICAgICAg" },
			func(r *identitypostgres.StagedLifecycle) { r.EventIndex++ },
			func(r *identitypostgres.StagedLifecycle) { r.SubjectID = "user-b" },
			func(r *identitypostgres.StagedLifecycle) { r.OccurredAt = r.OccurredAt.Add(time.Microsecond) },
			func(r *identitypostgres.StagedLifecycle) { r.ID = strings.Repeat("b", 64) },
		} {
			changed := r
			alter(&changed)
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var conflict identity.ConflictError
			if err = persist(tx, changed); !errors.As(err, &conflict) {
				t.Errorf("different bytes/scope accepted: %v", err)
			}
			if err = tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
		}
		counts(1, 1, 1)
	})
	t.Run("retained binding immutable", func(t *testing.T) {
		for _, sql := range []string{`UPDATE public.identity_outbox_bindings SET tenant_id='tenant-b'`, `DELETE FROM public.identity_outbox_bindings`, `TRUNCATE public.identity_outbox_bindings`} {
			if _, err := conn.Exec(ctx, sql); err == nil {
				t.Fatal("retained evidence altered")
			}
		}
	})
	t.Run("relay reschedule does not alter original binding", func(t *testing.T) {
		if _, err := conn.Exec(ctx, `UPDATE public.outbox_messages SET attempts=1,available_at=clock_timestamp() WHERE id=$1`, r.ID); err != nil {
			t.Fatal(err)
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		status, err := recover(tx, scope, plan)
		if err != nil || status != identitypostgres.RecoveryCommitted {
			t.Fatal("relay state erased evidence", err)
		}
		if err = persist(tx, r); err != nil {
			t.Fatal("relay mutable state rejected", err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("changed retained payload denied", func(t *testing.T) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err = tx.Exec(ctx, `UPDATE public.outbox_messages SET payload=$1 WHERE id=$2`, []byte("other"), r.ID); err != nil {
			t.Fatal(err)
		}
		var conflict identity.ConflictError
		if err = persist(tx, r); !errors.As(err, &conflict) {
			t.Fatal("corrupted retained payload accepted", err)
		}
	})
	t.Run("pruning cannot resurrect delivery", func(t *testing.T) {
		if _, err := conn.Exec(ctx, `DELETE FROM public.outbox_messages WHERE id=$1`, r.ID); err != nil {
			t.Fatal(err)
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		status, err := recover(tx, scope, plan)
		if err != nil || status != identitypostgres.RecoveryCommitted {
			t.Fatal("pruning erased participant effect", err)
		}
		if err = persist(tx, r); err != nil {
			t.Fatal("pruned replay rejected", err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		counts(1, 1, 0)
	})
	t.Run("concurrent exact replay singular", func(t *testing.T) {
		other, err := pgx.ConnectConfig(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		defer other.Close(ctx)
		fresh := r
		fresh.ID = strings.Repeat("c", 64)
		fresh.CommandID = "AwMDAwMDAwMDAwMDAwMDAw"
		first, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer first.Rollback(ctx)
		if err = persist(first, fresh); err != nil {
			t.Fatal(err)
		}
		second, err := other.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer second.Rollback(ctx)
		started := make(chan struct{})
		done := make(chan error, 1)
		go func() { close(started); done <- persist(second, fresh) }()
		<-started
		joined := false
		defer func() {
			_ = first.Rollback(ctx)
			if !joined {
				<-done
			}
		}()
		wait, stop := context.WithTimeout(ctx, 5*time.Second)
		defer stop()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			var blocked bool
			if err := admin.QueryRow(wait, `SELECT cardinality(pg_blocking_pids($1))>0`, int32(other.PgConn().PID())).Scan(&blocked); err != nil {
				t.Fatal("concurrency barrier failed", err)
			}
			if blocked {
				break
			}
			select {
			case err := <-done:
				joined = true
				t.Fatal("second writer did not wait for conflicting transaction", err)
			case <-ticker.C:
			case <-wait.Done():
				t.Fatal("second writer did not reach unique-key wait")
			}
		}
		if err = first.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		err = <-done
		joined = true
		if err != nil {
			t.Fatal("concurrent replay failed", err)
		}
		if err = second.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		counts(1, 2, 1)
	})
}
