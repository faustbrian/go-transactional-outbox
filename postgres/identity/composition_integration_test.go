//go:build integration

package identityoutbox_test

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/faustbrian/go-audit"
	auditpostgres "github.com/faustbrian/go-audit/postgres"
	"github.com/faustbrian/go-authorization/v3"
	"github.com/faustbrian/go-clock"
	identity "github.com/faustbrian/go-identity"
	identitypostgres "github.com/faustbrian/go-identity/postgres"
	migrations "github.com/faustbrian/go-migrations/v3"
	migrationpostgres "github.com/faustbrian/go-migrations/v3/postgres"
	database "github.com/faustbrian/go-postgres"
	identityoutbox "github.com/faustbrian/go-transactional-outbox/postgres/identity"
	outboxpostgres "github.com/faustbrian/go-transactional-outbox/v2/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// This bounded test policy is deployment configuration, not a domain store,
// participant, workflow or replacement authorization engine.
type compositionPolicy struct{}

func (compositionPolicy) Decide(ctx context.Context, req authorization.Request) (authorization.Decision, error) {
	if err := ctx.Err(); err != nil {
		return authorization.Decision{}, err
	}
	outcome := authorization.Deny
	if req.Subject.ID == "actor-a" && req.Tenant == "tenant-a" {
		outcome = authorization.Allow
	}
	if req.Tenant == "tenant-a" && req.Subject.Kind == authorization.SubjectUser && req.Subject.ID != "" && string(req.Subject.ID) == string(req.Resource.ID) && (req.Action == "identity.profile.get" || req.Action == "identity.profile.update") {
		outcome = authorization.Allow
	}
	return authorization.Decision{Outcome: outcome, Revision: 1}, nil
}

func TestPublicIdentityComposition(t *testing.T) {
	config := identityoutbox.IdentityTestPostgresConfig(t)
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	admin, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal("disposable administrative connection unavailable")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if admin.Close(cleanup) != nil {
			t.Error("admin cleanup failed")
		}
	}()
	unique, err := identity.NewSubjectID()
	if err != nil {
		t.Fatal(err)
	}
	name := "identity_composition_" + string(unique)
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+quoted); err != nil {
		t.Fatal(err)
	}
	ownedAuditRoles := false
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP DATABASE "+quoted); err != nil {
			t.Error("database cleanup failed")
		}
		if ownedAuditRoles {
			if _, err := admin.Exec(cleanup, "DROP ROLE audit_writer, audit_reader, audit_retention"); err != nil {
				t.Error("owned role cleanup failed")
			}
		}
	}()
	config.Database = name
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if conn.Close(cleanup) != nil {
			t.Error("fixture connection cleanup failed")
		}
	}()
	applyFiles := func(tx pgx.Tx, source fs.FS) {
		t.Helper()
		names, err := fs.ReadDir(source, ".")
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range names {
			contents, err := fs.ReadFile(source, file.Name())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Exec(ctx, strings.Split(string(contents), "-- +migrations Down")[0]); err != nil {
				t.Fatal("owned migration failed", file.Name(), err)
			}
		}
	}
	install, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer install.Rollback(ctx)
	if _, err = install.Exec(ctx, auditpostgres.FreshInstallPreflightSQL()); err != nil {
		t.Fatal(err)
	}
	applyFiles(install, auditpostgres.Migrations())
	applyFiles(install, outboxpostgres.Migrations())
	applyFiles(install, identityoutbox.Migrations())
	if err = install.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// Preflight rejects existing roles; only a successful atomic installation
	// proves these cluster-global names belong to this disposable fixture.
	ownedAuditRoles = true
	db := stdlib.OpenDB(*config)
	defer func() {
		if db.Close() != nil {
			t.Error("migration connection cleanup failed")
		}
	}()
	installIdentity(t, ctx, db)
	pool, err := database.Connect(ctx, database.Config{DSN: config.ConnString(), MaxConns: 4, Configure: func(native *database.PoolConfig) error { native.ConnConfig.Database = name; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if pool.Close(cleanup) != nil {
			t.Error("pool cleanup failed")
		}
	}()
	schema := identity.AttributeSchema{ID: "fields", Version: 1, Fields: []identity.FieldPolicy{{Name: "display_name", Kind: identity.AttributeString, Cardinality: identity.CardinalityScalar, Normalization: identity.NormalizeNone, InputPermission: identity.FieldWrite, OutputPermission: identity.FieldRead, WriteAuthorization: identity.FieldWriteAdministrator, Sensitivity: identity.SensitivityPersonal, MigrationVersion: 1}}}
	schema.Fields = append(schema.Fields, identity.FieldPolicy{Name: "preference", Kind: identity.AttributeString, Cardinality: identity.CardinalityScalar, Normalization: identity.NormalizeNone, InputPermission: identity.FieldWrite, OutputPermission: identity.FieldRead, WriteAuthorization: identity.FieldWriteSelf, Sensitivity: identity.SensitivityPersonal, MigrationVersion: 1})
	repo, err := identitypostgres.NewRepository(pool, identitypostgres.Config{MaximumDuration: 2 * time.Second, AttributeSchemas: []identity.AttributeSchema{schema}, ProfileSchema: identitypostgres.ProfileSchemaRef{ID: schema.ID, Version: schema.Version}})
	if err != nil {
		t.Fatal(err)
	}
	auditing, err := auditpostgres.NewIdentityContributor(auditpostgres.Config{})
	if err != nil {
		t.Fatal(err)
	}
	lifecycle, err := identityoutbox.NewContributor()
	if err != nil {
		t.Fatal(err)
	}
	registry, err := identitypostgres.NewContributorRegistry([]identitypostgres.Contributor{repo, auditing, lifecycle})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := authorization.NewDecisionService(compositionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := identitypostgres.NewCoordinator(pool, registry, identitypostgres.CoordinatorConfig{MaximumDuration: 2 * time.Second, LeaseDuration: time.Second, CurrentFingerprintKeyVersion: 1, RetainedFingerprintKeys: map[uint32][32]byte{1: {1}}, QueryPolicy: &identitypostgres.CommandQueryPolicy{Clock: clock.System{}, PolicyVersion: 1, Authorization: policy}})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := identitypostgres.NewCreationUnitOfWork(coordinator, repo, identitypostgres.CreationUnitOfWorkConfig{RetentionPolicyVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	service, err := identity.NewUserCreationService(owner, policy,
		func(_ context.Context, in identity.RedactionPolicyInput) (identity.RedactionPolicyDecision, error) {
			return identity.RedactionPolicyDecision{VisibleFields: in.Fields, PolicyID: "composition", PolicyVersion: 1, Reason: "visible"}, nil
		},
		identity.UserCreationConfig{Query: identity.UserQueryConfig{Clock: clock.System{}, MaximumDuration: 2 * time.Second, PolicyID: "composition", PolicyVersion: 1, RedactionReasons: []identity.PolicyReason{"visible"}, ValidateCanonicalUserID: func(context.Context, identity.UserID) error { return nil }}, Schema: schema, Normalize: func(_ context.Context, _ identity.NormalizationPolicy, scalar identity.AttributeScalar) (identity.AttributeScalar, error) {
			return scalar, nil
		}, ValidateValue: func(context.Context, identity.FieldPolicy, identity.AttributeValue) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	request := func(t *testing.T) identity.UserCreateRequest {
		t.Helper()
		command, err := identity.NewCommandID()
		if err != nil {
			t.Fatal(err)
		}
		id, err := identity.NewSubjectID()
		if err != nil {
			t.Fatal(err)
		}
		caller := authorization.DecisionContext{PolicyRevision: 1, Request: authorization.Request{Subject: authorization.Subject{Kind: authorization.SubjectUser, ID: "actor-a"}, Tenant: "tenant-a", Action: "identity.user.create", Resource: authorization.Resource{Type: "identity.user", ID: authorization.ResourceID(id)}}}
		return identity.UserCreateRequest{CommandID: command, TenantID: "tenant-a", UserID: identity.UserID(id), SchemaVersion: 1, ActorScope: caller, CorrelationID: "composition"}
	}
	counts := func(t *testing.T, req identity.UserCreateRequest, users, audits, events int) {
		t.Helper()
		id, err := base64.RawURLEncoding.DecodeString(string(req.CommandID))
		if err != nil {
			t.Fatal(err)
		}
		var u, a, e int
		if err = conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM identity.users WHERE tenant_id=$1 AND user_id=$2),(SELECT count(*) FROM audit.identity_command_bindings WHERE tenant_id=$1 AND command_id=$3),(SELECT count(*) FROM public.identity_outbox_bindings WHERE tenant_id=$1 AND command_id=$3)`, string(req.TenantID), string(req.UserID), id).Scan(&u, &a, &e); err != nil || u != users || a != audits || e != events {
			t.Fatal("unexpected composed durable effects", u, a, e, err)
		}
	}
	assertAudit := func(t *testing.T, req identity.UserCreateRequest, outcome audit.Outcome, reason, after string) {
		t.Helper()
		id, err := base64.RawURLEncoding.DecodeString(string(req.CommandID))
		if err != nil {
			t.Fatal(err)
		}
		var matches bool
		err = conn.QueryRow(ctx, `SELECT EXISTS(
 SELECT 1 FROM audit.identity_command_bindings b
 JOIN audit.records r ON r.record_id=b.event_id
 WHERE b.tenant_id=$1 AND b.command_id=$2 AND b.event_index=0
 AND r.tenant_id=$1 AND r.actor_id='actor-a' AND r.subject_id=$3
 AND r.action='identity.user.create' AND r.outcome=$4
 AND convert_from(r.canonical_record,'UTF8')::jsonb->>'reason_code'=$5
 AND convert_from(r.canonical_record,'UTF8')::jsonb->'attributes'->>'aggregate.after_version'=$6
 AND b.canonical_sha256=r.canonical_sha256
)`, string(req.TenantID), id, string(req.UserID), int16(outcome), reason, after).Scan(&matches)
		if err != nil || !matches {
			t.Fatal("composed audit payload mismatch", err)
		}
	}
	assertLifecycle := func(t *testing.T, req identity.UserCreateRequest) {
		t.Helper()
		id, err := base64.RawURLEncoding.DecodeString(string(req.CommandID))
		if err != nil {
			t.Fatal(err)
		}
		var matches bool
		err = conn.QueryRow(ctx, `SELECT EXISTS(
 SELECT 1 FROM public.identity_outbox_bindings b
 JOIN public.outbox_messages m ON m.id=b.effect_id
 WHERE b.tenant_id=$1 AND b.command_id=$2 AND b.event_index=0
 AND m.topic='identity.created.v1' AND m.payload_version=1
 AND m.metadata=jsonb_build_object('identity.tenant_id',$1::text,'identity.command_id',$3::text)
 AND convert_from(m.payload,'UTF8')::jsonb->>'profile'='identity-lifecycle-v1'
 AND convert_from(m.payload,'UTF8')::jsonb->>'command_id'=$3
 AND convert_from(m.payload,'UTF8')::jsonb->>'tenant_id'=$1
 AND convert_from(m.payload,'UTF8')::jsonb->>'user_id'=$4
 AND convert_from(m.payload,'UTF8')::jsonb->>'before_version'='0'
 AND convert_from(m.payload,'UTF8')::jsonb->>'after_version'='1'
)`, string(req.TenantID), id, string(req.CommandID), string(req.UserID)).Scan(&matches)
		if err != nil || !matches {
			t.Fatal("composed lifecycle payload mismatch", err)
		}
	}
	profileOwner, err := identitypostgres.NewProfileUnitOfWork(coordinator, repo, identitypostgres.CreationUnitOfWorkConfig{RetentionPolicyVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	var hideProfileName bool
	profile, err := identity.NewProfileService(repo, profileOwner, policy, func(_ context.Context, in identity.RedactionPolicyInput) (identity.RedactionPolicyDecision, error) {
		out := identity.RedactionPolicyDecision{PolicyID: "composition", PolicyVersion: 1, Reason: "visible"}
		for _, field := range in.Fields {
			if hideProfileName && field.Value == "name" {
				out.RedactedFields = append(out.RedactedFields, field)
			} else {
				out.VisibleFields = append(out.VisibleFields, field)
			}
		}
		return out, nil
	}, identity.ProfileConfig{AllowedImageOrigins: []string{"https://images.example.test"}, Fields: identity.UserCreationConfig{Query: identity.UserQueryConfig{Clock: clock.System{}, MaximumDuration: 2 * time.Second, PolicyID: "composition", PolicyVersion: 1, RedactionReasons: []identity.PolicyReason{"visible"}, ValidateCanonicalUserID: func(context.Context, identity.UserID) error { return nil }}, Schema: schema, Normalize: func(_ context.Context, _ identity.NormalizationPolicy, scalar identity.AttributeScalar) (identity.AttributeScalar, error) {
		return scalar, nil
	}, ValidateValue: func(context.Context, identity.FieldPolicy, identity.AttributeValue) error { return nil }}})
	if err != nil {
		t.Fatal(err)
	}
	profileRequest := func(t *testing.T) identity.ProfileUpdateRequest {
		t.Helper()
		created := request(t)
		created.Fields = []identity.AttributeUpdate{{Name: "display_name", Value: identity.AttributeValue{Kind: identity.AttributeString, Scalar: identity.AttributeScalar{Kind: identity.AttributeString, String: "retained administrator field"}}}}
		if result, err := service.UserCreate(ctx, created); err != nil || result.CommitOutcome != identity.CommitOutcomeCommitted {
			t.Fatal("profile fixture creation failed", err)
		}
		command, err := identity.NewCommandID()
		if err != nil {
			t.Fatal(err)
		}
		caller := authorization.DecisionContext{PolicyRevision: 1, Request: authorization.Request{Subject: authorization.Subject{Kind: authorization.SubjectUser, ID: authorization.SubjectID(created.UserID)}, Tenant: "tenant-a", Action: "identity.profile.update", Resource: authorization.Resource{Type: "identity.user", ID: authorization.ResourceID(created.UserID)}}}
		return identity.ProfileUpdateRequest{TenantID: created.TenantID, CommandID: command, Context: caller, SchemaVersion: 1, ExpectedVersion: 1, Name: "profile first", Image: "https://images.example.test/first.png", AdditionalFields: []identity.AttributeUpdate{{Name: "preference", Value: identity.AttributeValue{Kind: identity.AttributeString, Scalar: identity.AttributeScalar{Kind: identity.AttributeString, String: "compact"}}}}, CorrelationID: "composition"}
	}
	profileCounts := func(t *testing.T, req identity.ProfileUpdateRequest, audits, events int) {
		t.Helper()
		counts(t, identity.UserCreateRequest{CommandID: req.CommandID, TenantID: req.TenantID, UserID: identity.UserID(req.Context.Request.Subject.ID)}, 1, audits, events)
	}
	assertProfileAudit := func(t *testing.T, req identity.ProfileUpdateRequest, outcome audit.Outcome, reason, before, after string) {
		t.Helper()
		id, err := base64.RawURLEncoding.DecodeString(string(req.CommandID))
		if err != nil {
			t.Fatal(err)
		}
		var matches bool
		err = conn.QueryRow(ctx, `SELECT EXISTS(
 SELECT 1 FROM audit.identity_command_bindings b
 JOIN audit.records r ON r.record_id=b.event_id
 WHERE b.tenant_id=$1 AND b.command_id=$2 AND b.event_index=0
 AND r.tenant_id=$1 AND r.actor_id=$3 AND r.subject_id=$3
 AND r.action='identity.user.update' AND r.outcome=$4
 AND convert_from(r.canonical_record,'UTF8')::jsonb->>'reason_code'=$5
 AND convert_from(r.canonical_record,'UTF8')::jsonb->'attributes'->>'aggregate.before_version'=$6
 AND convert_from(r.canonical_record,'UTF8')::jsonb->'attributes'->>'aggregate.after_version'=$7
 AND b.canonical_sha256=r.canonical_sha256
)`, string(req.TenantID), id, string(req.Context.Request.Subject.ID), int16(outcome), reason, before, after).Scan(&matches)
		if err != nil || !matches {
			t.Fatal("composed profile audit payload mismatch", err)
		}
	}
	assertProfileLifecycle := func(t *testing.T, req identity.ProfileUpdateRequest, before, after string) {
		t.Helper()
		id, err := base64.RawURLEncoding.DecodeString(string(req.CommandID))
		if err != nil {
			t.Fatal(err)
		}
		var matches bool
		err = conn.QueryRow(ctx, `SELECT EXISTS(
 SELECT 1 FROM public.identity_outbox_bindings b
 JOIN public.outbox_messages m ON m.id=b.effect_id
 WHERE b.tenant_id=$1 AND b.command_id=$2 AND b.event_index=0
 AND m.topic='identity.updated.v1' AND m.payload_version=1
 AND m.metadata=jsonb_build_object('identity.tenant_id',$1::text,'identity.command_id',$3::text)
 AND convert_from(m.payload,'UTF8')::jsonb->>'profile'='identity-lifecycle-v1'
 AND convert_from(m.payload,'UTF8')::jsonb->>'command_id'=$3
 AND convert_from(m.payload,'UTF8')::jsonb->>'tenant_id'=$1
 AND convert_from(m.payload,'UTF8')::jsonb->>'user_id'=$4
 AND convert_from(m.payload,'UTF8')::jsonb->>'operation_id'='identity.profile.update'
 AND convert_from(m.payload,'UTF8')::jsonb->>'before_version'=$5
 AND convert_from(m.payload,'UTF8')::jsonb->>'after_version'=$6
)`, string(req.TenantID), id, string(req.CommandID), string(req.Context.Request.Subject.ID), before, after).Scan(&matches)
		if err != nil || !matches {
			t.Fatal("composed profile lifecycle payload mismatch", err)
		}
	}
	t.Run("profile mutation replay and recovery preserve original safe effects", func(t *testing.T) {
		req := profileRequest(t)
		first, err := profile.ProfileUpdate(ctx, req)
		if err != nil || first.CommitOutcome != identity.CommitOutcomeCommitted || first.User.Version != 2 || first.User.Name != req.Name || first.User.Image != req.Image || len(first.User.AdditionalFields) != 2 || first.User.AdditionalFields[0].Value.Scalar.String != "retained administrator field" || first.User.AdditionalFields[1].Value.Scalar.String != "compact" {
			t.Fatal("public profile mutation failed", err)
		}
		profileCounts(t, req, 1, 1)
		assertProfileAudit(t, req, audit.OutcomeSucceeded, "completed", "1", "2")
		assertProfileLifecycle(t, req, "1", "2")
		next := req
		next.CommandID, err = identity.NewCommandID()
		if err != nil {
			t.Fatal(err)
		}
		next.ExpectedVersion, next.Name, next.Image = 2, "profile second", ""
		next.AdditionalFields = nil
		second, err := profile.ProfileUpdate(ctx, next)
		if err != nil || second.User.Version != 3 || second.User.Name != next.Name || second.User.Image != req.Image || len(second.User.AdditionalFields) != 2 || second.User.AdditionalFields[1].Value.Scalar.String != "compact" {
			t.Fatal("partial profile update changed omitted image", err)
		}
		profileCounts(t, next, 1, 1)
		assertProfileAudit(t, next, audit.OutcomeSucceeded, "completed", "2", "3")
		assertProfileLifecycle(t, next, "2", "3")
		replay, err := profile.ProfileUpdate(ctx, req)
		if err != nil || replay.User.Version != 2 || replay.User.Name != req.Name || replay.User.Image != req.Image {
			t.Fatal("exact replay replaced original profile receipt", err)
		}
		hideProfileName = true
		t.Cleanup(func() { hideProfileName = false })
		recovery, err := profile.RecoverProfileUpdate(ctx, identity.ProfileRecoveryRequest{TenantID: req.TenantID, CommandID: req.CommandID, Context: req.Context})
		if err != nil || recovery.CommitOutcome != identity.CommitOutcomeCommitted || recovery.User.Version != 2 || recovery.User.Name != "" || recovery.User.Image != req.Image {
			t.Fatal("original recovery ignored current redaction", err)
		}
		hideProfileName = false
		changed := req
		changed.Name = "conflicting intent"
		result, err := profile.ProfileUpdate(ctx, changed)
		var conflict identity.ConflictError
		if result.CommitOutcome != identity.CommitOutcomeConflict || !errors.As(err, &conflict) {
			t.Fatal("changed profile intent reused command", err)
		}
		profileCounts(t, req, 1, 1)
		current, err := repo.GetProfile(ctx, identity.ProfileQuery{TenantID: req.TenantID, UserID: identity.UserID(req.Context.Request.Subject.ID)})
		if err != nil || current.User.Version != 3 || current.Profile.Name != next.Name {
			t.Fatal("historic observation mutated current profile", err)
		}
	})
	t.Run("profile self service cannot write administrator fields", func(t *testing.T) {
		req := profileRequest(t)
		req.AdditionalFields = []identity.AttributeUpdate{{Name: "display_name", Value: identity.AttributeValue{Kind: identity.AttributeString, Scalar: identity.AttributeScalar{Kind: identity.AttributeString, String: "unauthorized replacement"}}}}
		result, err := profile.ProfileUpdate(ctx, req)
		var denied identity.ForbiddenError
		if !errors.As(err, &denied) || result.User.ID != "" {
			t.Fatal("self service accepted administrator field", err)
		}
		profileCounts(t, req, 0, 0)
		current, err := repo.GetProfile(ctx, identity.ProfileQuery{TenantID: req.TenantID, UserID: identity.UserID(req.Context.Request.Subject.ID)})
		if err != nil || current.User.Version != 1 || len(current.Profile.AdditionalFields) != 1 || current.Profile.AdditionalFields[0].Value.Scalar.String != "retained administrator field" {
			t.Fatal("denied self field changed current profile", err)
		}
	})
	t.Run("real outbox failure rolls back profile and successful audit", func(t *testing.T) {
		req := profileRequest(t)
		// A database-owned fixture failure reaches the real outbox SQL after
		// domain mutation and audit staging. No participant is replaced.
		if _, err := conn.Exec(ctx, `CREATE FUNCTION public.reject_profile_effect() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.topic='identity.updated.v1' THEN RAISE EXCEPTION 'fixture effect rejected'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_profile_effect BEFORE INSERT ON public.outbox_messages FOR EACH ROW EXECUTE FUNCTION public.reject_profile_effect()`); err != nil {
			t.Fatal("rollback fixture installation failed", err)
		}
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			if _, err := conn.Exec(cleanup, `DROP TRIGGER reject_profile_effect ON public.outbox_messages; DROP FUNCTION public.reject_profile_effect()`); err != nil {
				t.Error("rollback fixture cleanup failed")
			}
		}()
		result, err := profile.ProfileUpdate(ctx, req)
		var notCommitted identity.NotCommittedError
		if result.CommitOutcome != identity.CommitOutcomeNotCommitted || !errors.As(err, &notCommitted) || result.User.ID != "" {
			t.Fatal("outbox failure did not classify rolled-back profile", result.CommitOutcome, err)
		}
		profileCounts(t, req, 1, 0)
		assertProfileAudit(t, req, audit.OutcomeFailed, "provider_unavailable", "1", "1")
		current, err := repo.GetProfile(ctx, identity.ProfileQuery{TenantID: req.TenantID, UserID: identity.UserID(req.Context.Request.Subject.ID)})
		if err != nil || current.User.Version != 1 || current.Profile.Name != "" || current.Profile.Image != "" || len(current.Profile.AdditionalFields) != 1 || current.Profile.AdditionalFields[0].Value.Scalar.String != "retained administrator field" {
			t.Fatal("outbox failure left a domain prefix", err)
		}
		recovered, err := profile.RecoverProfileUpdate(ctx, identity.ProfileRecoveryRequest{TenantID: req.TenantID, CommandID: req.CommandID, Context: req.Context})
		if recovered.CommitOutcome != identity.CommitOutcomeNotCommitted || !errors.As(err, &notCommitted) {
			t.Fatal("failed profile recovery changed original outcome", err)
		}
		profileCounts(t, req, 1, 0)
	})
	t.Run("expired profile recovers through real frozen participants", func(t *testing.T) {
		req := profileRequest(t)
		binding := identity.CommandBinding{CommandID: req.CommandID, TenantID: req.TenantID, UserID: identity.UserID(req.Context.Request.Subject.ID), ActorID: identity.ActorID(req.Context.Request.Subject.ID), ActorKind: authorization.SubjectUser, OperationID: "identity.profile.update", PolicyID: "composition", PolicyVersion: 1, SchemaID: schema.ID, SchemaVersion: 1, CorrelationID: req.CorrelationID}
		command, err := identity.NewProfileUpdateCommand(identity.ProfileUpdateCommand{Binding: binding, ExpectedVersion: 1, Name: req.Name, Image: req.Image, AdditionalFields: req.AdditionalFields})
		if err != nil {
			t.Fatal(err)
		}
		record := compositionAudit(t, ctx, binding)
		digest, err := identitypostgres.AuditBindingDigest(binding, record)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := coordinator.PrepareCommandPlan(binding, command, identitypostgres.CommandPlan{Contributors: registry.Descriptors(), Audits: []identitypostgres.AuditPlan{{EventType: "identity.user.update", SchemaVersion: 1, BindingDigest: digest, RetentionPolicyVersion: 1, RequiredRecordCount: 1, MaxRecordBytes: 16384, MaxAggregateBytes: 16384, Required: true}}, Lifecycle: &identitypostgres.LifecyclePlan{EventType: "identity.updated.v1", BeforeVersion: 1, AfterVersion: 2}})
		if err != nil {
			t.Fatal(err)
		}
		_, pending, err := coordinator.ReserveCommand(ctx, binding, command, plan)
		if err != nil || pending.Outcome != identity.CommitOutcomeInProgress {
			t.Fatal("public profile reserve failed", err)
		}
		profileCounts(t, req, 0, 0)
		waitForCommandLease(t, ctx, conn, req.CommandID)
		result, err := profile.RecoverProfileUpdate(ctx, identity.ProfileRecoveryRequest{TenantID: req.TenantID, CommandID: req.CommandID, Context: req.Context})
		var notCommitted identity.NotCommittedError
		if result.CommitOutcome != identity.CommitOutcomeNotCommitted || !errors.As(err, &notCommitted) {
			t.Fatal("real profile participant recovery did not abort", result.CommitOutcome, err)
		}
		profileCounts(t, req, 1, 0)
		assertProfileAudit(t, req, audit.OutcomeFailed, "provider_unavailable", "1", "1")
		current, err := repo.GetProfile(ctx, identity.ProfileQuery{TenantID: req.TenantID, UserID: binding.UserID})
		if err != nil || current.User.Version != 1 || current.Profile.Name != "" || current.Profile.Image != "" {
			t.Fatal("negative recovery changed domain state", err)
		}
	})
	testAdministrativeComposition(t, ctx, conn, repo, coordinator, registry.Descriptors(), policy, schema, service, request, counts)
	t.Run("creation exact replay and terminal recovery", func(t *testing.T) {
		req := request(t)
		first, err := service.UserCreate(ctx, req)
		if err != nil || first.CommitOutcome != identity.CommitOutcomeCommitted || first.User.ID != req.UserID || first.User.Version != 1 {
			t.Fatal("public creation failed", err)
		}
		counts(t, req, 1, 1, 1)
		assertAudit(t, req, audit.OutcomeSucceeded, "completed", "1")
		assertLifecycle(t, req)
		replay, err := service.UserCreate(ctx, req)
		if err != nil || replay.CommitOutcome != identity.CommitOutcomeCommitted || replay.User.ID != first.User.ID || replay.User.Version != first.User.Version {
			t.Fatal("public replay changed receipt", err)
		}
		recovered, err := service.RecoverUserCreate(ctx, identity.UserCreateRecoveryRequest{CommandID: req.CommandID, TenantID: req.TenantID, UserID: req.UserID, ActorScope: req.ActorScope})
		if err != nil || recovered.CommitOutcome != identity.CommitOutcomeCommitted || recovered.User.ID != req.UserID {
			t.Fatal("public terminal recovery failed", err)
		}
		changed := req
		changed.Fields = []identity.AttributeUpdate{{Name: "display_name", Value: identity.AttributeValue{Kind: identity.AttributeString, Scalar: identity.AttributeScalar{Kind: identity.AttributeString, String: "changed"}}}}
		conflict, err := service.UserCreate(ctx, changed)
		var conflictError identity.ConflictError
		if conflict.CommitOutcome != identity.CommitOutcomeConflict || !errors.As(err, &conflictError) {
			t.Fatal("changed intent reused command", err)
		}
		counts(t, req, 1, 1, 1)
	})
	t.Run("current denial leaves no effects", func(t *testing.T) {
		req := request(t)
		req.ActorScope.Request.Subject.ID = "denied-actor"
		got, err := service.UserCreate(ctx, req)
		var denied identity.ForbiddenError
		if !errors.As(err, &denied) || got.User.ID != "" {
			t.Fatal("denied creation disclosed or persisted", err)
		}
		counts(t, req, 0, 0, 0)
	})
	t.Run("abandoned reservation recovers through real participants", func(t *testing.T) {
		req := request(t)
		binding := identity.CommandBinding{CommandID: req.CommandID, TenantID: req.TenantID, UserID: req.UserID, ActorID: "actor-a", ActorKind: authorization.SubjectUser, OperationID: "identity.user.create", PolicyID: "composition", PolicyVersion: 1, SchemaID: "fields", SchemaVersion: 1, CorrelationID: req.CorrelationID}
		command, err := identity.NewUserCreationCommand(identity.CreateUserCommand{Binding: binding, Status: identity.StatusActive})
		if err != nil {
			t.Fatal(err)
		}
		record := compositionAudit(t, ctx, binding)
		digest, err := identitypostgres.AuditBindingDigest(binding, record)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := coordinator.PrepareCommandPlan(binding, command, identitypostgres.CommandPlan{Contributors: registry.Descriptors(), Audits: []identitypostgres.AuditPlan{{EventType: "identity.user.create", SchemaVersion: 1, BindingDigest: digest, RetentionPolicyVersion: 1, RequiredRecordCount: 1, MaxRecordBytes: 16384, MaxAggregateBytes: 16384, Required: true}}, Lifecycle: &identitypostgres.LifecyclePlan{EventType: "identity.created.v1", AfterVersion: 1}})
		if err != nil {
			t.Fatal(err)
		}
		_, reserved, err := coordinator.ReserveCommand(ctx, binding, command, plan)
		if err != nil || reserved.Outcome != identity.CommitOutcomeInProgress {
			t.Fatal("public reserve failed", err)
		}
		counts(t, req, 0, 0, 0)
		id, _ := base64.RawURLEncoding.DecodeString(string(req.CommandID))
		wait, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			var expired bool
			if err := conn.QueryRow(wait, `SELECT lease_until<=clock_timestamp() FROM identity.commands WHERE command_id=$1`, id).Scan(&expired); err != nil {
				t.Fatal("primary lease observation failed")
			}
			if expired {
				break
			}
			select {
			case <-ticker.C:
			case <-wait.Done():
				t.Fatal("primary lease did not expire")
			}
		}
		query := identity.RecoveryQuery{CommandID: binding.CommandID, TenantID: binding.TenantID, UserID: binding.UserID, ActorID: binding.ActorID, ActorKind: binding.ActorKind, OperationID: binding.OperationID}
		got, err := coordinator.RecoverCommand(ctx, query, req.ActorScope)
		var unavailable identity.UnavailableError
		if got.Outcome != identity.CommitOutcomeNotCommitted || !errors.As(err, &unavailable) {
			t.Fatal("real participant recovery did not abort", got.Outcome, err)
		}
		counts(t, req, 0, 1, 0)
		assertAudit(t, req, audit.OutcomeFailed, "provider_unavailable", "0")
		again, err := service.RecoverUserCreate(ctx, identity.UserCreateRecoveryRequest{CommandID: req.CommandID, TenantID: req.TenantID, UserID: req.UserID, ActorScope: req.ActorScope})
		var notCommitted identity.NotCommittedError
		if again.CommitOutcome != identity.CommitOutcomeNotCommitted || !errors.As(err, &notCommitted) || again.User.ID != "" {
			t.Fatal("aborted replay changed outcome", err)
		}
		counts(t, req, 0, 1, 0)
	})
}

func installIdentity(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	backend, err := migrationpostgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := migrations.NewRunner(identitypostgres.MigrationSource{}, backend)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Up(ctx)
	if err != nil || len(result.Records()) != 9 {
		t.Fatal("identity migration failed", err)
	}
}

func compositionAudit(t *testing.T, ctx context.Context, b identity.CommandBinding) audit.Record {
	t.Helper()
	builder, err := audit.NewBuilder(audit.BuilderConfig{IDGenerator: func() (string, error) { id, err := identity.NewSubjectID(); return string(id), err }})
	if err != nil {
		t.Fatal(err)
	}
	action, before, after, retention := b.OperationID, "0", "1", "standard"
	if b.OperationID == "identity.profile.update" || b.OperationID == "identity.user.update-admin" {
		action, before, after = "identity.user.update", "1", "2"
	}
	if b.OperationID == "identity.user.update-admin" {
		retention = "protected"
	}
	r, err := builder.Build(audit.RecordInput{Action: action, OccurredAt: time.Now().UTC().Truncate(time.Microsecond), Outcome: audit.OutcomeSucceeded, ReasonCode: "completed", Actor: audit.ActorInput{Kind: audit.ActorHuman, ID: string(b.ActorID)}, Subject: audit.SubjectInput{Type: "user", ID: string(b.UserID)}, Context: audit.ContextInput{TenantID: string(b.TenantID), CorrelationID: b.CorrelationID}, Changes: audit.ChangeSetInput{Redacted: true}, Policy: audit.PolicyMetadata{PolicyID: b.PolicyID, Version: "1"}, Attributes: map[string]string{"identity.profile": "identity.security.v1", "command.id": string(b.CommandID), "retention.class": retention, "aggregate.type": "user", "aggregate.id": string(b.UserID), "aggregate.before_version": before, "aggregate.after_version": after}})
	if err != nil {
		t.Fatal(err)
	}
	redactor, err := audit.NewRedactor(audit.RedactionRules{AllowedAttributes: []string{"identity.profile", "command.id", "retention.class", "aggregate.type", "aggregate.id", "aggregate.before_version", "aggregate.after_version"}})
	if err != nil {
		t.Fatal(err)
	}
	r, err = redactor.Redact(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func waitForCommandLease(t *testing.T, ctx context.Context, conn *pgx.Conn, command identity.CommandID) {
	t.Helper()
	id, err := base64.RawURLEncoding.DecodeString(string(command))
	if err != nil {
		t.Fatal(err)
	}
	wait, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var expired bool
		if err := conn.QueryRow(wait, `SELECT lease_until<=clock_timestamp() FROM identity.commands WHERE command_id=$1`, id).Scan(&expired); err != nil {
			t.Fatal("primary lease observation failed")
		}
		if expired {
			return
		}
		select {
		case <-ticker.C:
		case <-wait.Done():
			t.Fatal("primary lease did not expire")
		}
	}
}
