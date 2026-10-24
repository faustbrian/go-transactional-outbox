//go:build integration

package identityoutbox_test

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/faustbrian/go-audit"
	"github.com/faustbrian/go-authorization/v3"
	"github.com/faustbrian/go-clock"
	identity "github.com/faustbrian/go-identity"
	identitypostgres "github.com/faustbrian/go-identity/postgres"
	"github.com/jackc/pgx/v5"
)

func testAdministrativeComposition(t *testing.T, ctx context.Context, conn *pgx.Conn, repo *identitypostgres.Repository, coordinator *identitypostgres.Coordinator, descriptors []identitypostgres.ContributorDescriptor, policy authorization.Service, schema identity.AttributeSchema, creation *identity.UserCreationService, request func(*testing.T) identity.UserCreateRequest, counts func(*testing.T, identity.UserCreateRequest, int, int, int)) {
	owner, err := identitypostgres.NewUserUpdateUnitOfWork(coordinator, repo, identitypostgres.CreationUnitOfWorkConfig{RetentionPolicyVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	var withhold bool
	service, err := identity.NewUserUpdateAdminService(repo, owner, policy, func(_ context.Context, in identity.RedactionPolicyInput) (identity.RedactionPolicyDecision, error) {
		if withhold {
			return identity.RedactionPolicyDecision{}, nil
		}
		return identity.RedactionPolicyDecision{VisibleFields: in.Fields, PolicyID: "composition", PolicyVersion: 1, Reason: "visible"}, nil
	}, identity.UserCreationConfig{Query: identity.UserQueryConfig{Clock: clock.System{}, MaximumDuration: 2 * time.Second, PolicyID: "composition", PolicyVersion: 1, RedactionReasons: []identity.PolicyReason{"visible"}, ValidateCanonicalUserID: func(context.Context, identity.UserID) error { return nil }}, Schema: schema, Normalize: func(_ context.Context, _ identity.NormalizationPolicy, s identity.AttributeScalar) (identity.AttributeScalar, error) {
		return s, nil
	}, ValidateValue: func(context.Context, identity.FieldPolicy, identity.AttributeValue) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	newRequest := func(t *testing.T) identity.UserUpdateAdminRequest {
		t.Helper()
		created := request(t)
		if result, err := creation.UserCreate(ctx, created); err != nil || result.CommitOutcome != identity.CommitOutcomeCommitted {
			t.Fatal("administrative fixture creation failed", err)
		}
		command, err := identity.NewCommandID()
		if err != nil {
			t.Fatal(err)
		}
		created.ActorScope.Request.Action = "identity.user.update-admin"
		return identity.UserUpdateAdminRequest{CommandID: command, TenantID: created.TenantID, UserID: created.UserID, ExpectedVersion: 1, SchemaVersion: 1, ActorScope: created.ActorScope, CorrelationID: "composition", Fields: []identity.AttributeUpdate{{Name: "display_name", Value: identity.AttributeValue{Kind: identity.AttributeString, Scalar: identity.AttributeScalar{Kind: identity.AttributeString, String: "administrative first"}}}}}
	}
	recoverRequest := func(req identity.UserUpdateAdminRequest) identity.UserUpdateAdminRecoveryRequest {
		return identity.UserUpdateAdminRecoveryRequest{CommandID: req.CommandID, TenantID: req.TenantID, UserID: req.UserID, ActorScope: req.ActorScope}
	}
	assertEffects := func(t *testing.T, req identity.UserUpdateAdminRequest, outcome audit.Outcome, after string, events int) {
		t.Helper()
		counts(t, identity.UserCreateRequest{CommandID: req.CommandID, TenantID: req.TenantID, UserID: req.UserID}, 1, 1, events)
		id, err := base64.RawURLEncoding.DecodeString(string(req.CommandID))
		if err != nil {
			t.Fatal(err)
		}
		var valid bool
		err = conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM audit.identity_command_bindings b JOIN audit.records r ON r.record_id=b.event_id
 WHERE b.tenant_id=$1 AND b.command_id=$2 AND b.event_index=0 AND r.actor_id='actor-a' AND r.subject_id=$3
 AND r.action='identity.user.update' AND r.outcome=$4 AND b.canonical_sha256=r.canonical_sha256
 AND convert_from(r.canonical_record,'UTF8')::jsonb->'attributes'->>'retention.class'='protected'
 AND convert_from(r.canonical_record,'UTF8')::jsonb->'attributes'->>'aggregate.before_version'='1'
 AND convert_from(r.canonical_record,'UTF8')::jsonb->'attributes'->>'aggregate.after_version'=$5)`, string(req.TenantID), id, string(req.UserID), int16(outcome), after).Scan(&valid)
		if err != nil || !valid {
			t.Fatal("administrative protected audit mismatch", err)
		}
		if events == 0 {
			return
		}
		err = conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM public.identity_outbox_bindings b JOIN public.outbox_messages m ON m.id=b.effect_id
 WHERE b.tenant_id=$1 AND b.command_id=$2 AND b.event_index=0 AND m.topic='identity.updated.v1' AND m.payload_version=1
 AND m.metadata=jsonb_build_object('identity.tenant_id',$1::text,'identity.command_id',$3::text)
 AND convert_from(m.payload,'UTF8')::jsonb->>'command_id'=$3 AND convert_from(m.payload,'UTF8')::jsonb->>'user_id'=$4
 AND convert_from(m.payload,'UTF8')::jsonb->>'operation_id'='identity.user.update-admin'
 AND convert_from(m.payload,'UTF8')::jsonb->>'before_version'='1' AND convert_from(m.payload,'UTF8')::jsonb->>'after_version'=$5)`, string(req.TenantID), id, string(req.CommandID), string(req.UserID), after).Scan(&valid)
		if err != nil || !valid {
			t.Fatal("administrative lifecycle mismatch", err)
		}
	}
	t.Run("administrative production effects and original receipt", func(t *testing.T) {
		req := newRequest(t)
		first, err := service.UserUpdateAdmin(ctx, req)
		if err != nil || first.CommitOutcome != identity.CommitOutcomeCommitted || first.Version != 2 {
			t.Fatal("administrative production composition failed", err)
		}
		assertEffects(t, req, audit.OutcomeSucceeded, "2", 1)
		next := req
		next.CommandID, err = identity.NewCommandID()
		if err != nil {
			t.Fatal(err)
		}
		next.ExpectedVersion = 2
		next.Fields = []identity.AttributeUpdate{{Name: "display_name", Value: identity.AttributeValue{Kind: identity.AttributeString, Scalar: identity.AttributeScalar{Kind: identity.AttributeString, String: "administrative second"}}}}
		if result, err := service.UserUpdateAdmin(ctx, next); err != nil || result.Version != 3 {
			t.Fatal("later administrative update failed", err)
		}
		replay, err := service.UserUpdateAdmin(ctx, req)
		if err != nil || replay != first {
			t.Fatal("administrative replay changed original receipt", err)
		}
		recovered, err := service.RecoverUserUpdateAdmin(ctx, recoverRequest(req))
		if err != nil || recovered != first {
			t.Fatal("administrative recovery changed original receipt", err)
		}
		withhold = true
		withheld, err := service.RecoverUserUpdateAdmin(ctx, recoverRequest(req))
		withhold = false
		var forbidden identity.ForbiddenError
		if !errors.As(err, &forbidden) || withheld.CommitOutcome != identity.CommitOutcomeCommitted || withheld.User.ID != "" {
			t.Fatal("administrative recovery bypassed current redaction", err)
		}
		current, err := repo.GetProfile(ctx, identity.ProfileQuery{TenantID: req.TenantID, UserID: req.UserID})
		if err != nil || current.User.Version != 3 || len(current.Profile.AdditionalFields) != 1 || current.Profile.AdditionalFields[0].Value.Scalar.String != "administrative second" {
			t.Fatal("historic observation altered current attributes", err)
		}
		assertEffects(t, req, audit.OutcomeSucceeded, "2", 1)
	})
	t.Run("expired administrative reservation needs all real participants", func(t *testing.T) {
		req := newRequest(t)
		binding := identity.CommandBinding{CommandID: req.CommandID, TenantID: req.TenantID, UserID: req.UserID, ActorID: "actor-a", ActorKind: authorization.SubjectUser, OperationID: "identity.user.update-admin", PolicyID: "composition", PolicyVersion: 1, SchemaID: schema.ID, SchemaVersion: 1, CorrelationID: req.CorrelationID}
		command, err := identity.NewUserUpdateCommand(identity.UpdateUserCommand{Binding: binding, ExpectedVersion: 1, Attributes: req.Fields})
		if err != nil {
			t.Fatal(err)
		}
		digest, err := identitypostgres.AuditBindingDigest(binding, compositionAudit(t, ctx, binding))
		if err != nil {
			t.Fatal(err)
		}
		plan, err := coordinator.PrepareCommandPlan(binding, command, identitypostgres.CommandPlan{Contributors: descriptors, Audits: []identitypostgres.AuditPlan{{EventType: "identity.user.update", SchemaVersion: 1, BindingDigest: digest, RetentionPolicyVersion: 1, RequiredRecordCount: 1, MaxRecordBytes: 16384, MaxAggregateBytes: 16384, Required: true}}, Lifecycle: &identitypostgres.LifecyclePlan{EventType: "identity.updated.v1", BeforeVersion: 1, AfterVersion: 2}})
		if err != nil {
			t.Fatal(err)
		}
		_, pending, err := coordinator.ReserveCommand(ctx, binding, command, plan)
		if err != nil || pending.Outcome != identity.CommitOutcomeInProgress {
			t.Fatal("administrative reservation failed", err)
		}
		waitForCommandLease(t, ctx, conn, req.CommandID)
		result, err := service.RecoverUserUpdateAdmin(ctx, recoverRequest(req))
		var notCommitted identity.NotCommittedError
		if result.CommitOutcome != identity.CommitOutcomeNotCommitted || !errors.As(err, &notCommitted) {
			t.Fatal("real administrative participant recovery did not abort", result.CommitOutcome, err)
		}
		assertEffects(t, req, audit.OutcomeFailed, "1", 0)
		current, err := repo.GetProfile(ctx, identity.ProfileQuery{TenantID: req.TenantID, UserID: req.UserID})
		if err != nil || current.User.Version != 1 || len(current.Profile.AdditionalFields) != 0 {
			t.Fatal("administrative negative recovery mutated fields", err)
		}
	})
	t.Run("administrative outbox failure rolls back domain and success audit", func(t *testing.T) {
		req := newRequest(t)
		if _, err := conn.Exec(ctx, `CREATE FUNCTION public.reject_admin_effect() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.topic='identity.updated.v1' THEN RAISE EXCEPTION 'fixture effect rejected'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_admin_effect BEFORE INSERT ON public.outbox_messages FOR EACH ROW EXECUTE FUNCTION public.reject_admin_effect()`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, err := conn.Exec(cleanup, `DROP TRIGGER reject_admin_effect ON public.outbox_messages; DROP FUNCTION public.reject_admin_effect()`); err != nil {
				t.Error("administrative trigger cleanup failed")
			}
		}()
		result, err := service.UserUpdateAdmin(ctx, req)
		var notCommitted identity.NotCommittedError
		if result.CommitOutcome != identity.CommitOutcomeNotCommitted || !errors.As(err, &notCommitted) {
			t.Fatal("administrative rollback misclassified", result.CommitOutcome, err)
		}
		assertEffects(t, req, audit.OutcomeFailed, "1", 0)
		current, err := repo.GetProfile(ctx, identity.ProfileQuery{TenantID: req.TenantID, UserID: req.UserID})
		if err != nil || current.User.Version != 1 || len(current.Profile.AdditionalFields) != 0 {
			t.Fatal("administrative failure retained domain prefix", err)
		}
		recovered, err := service.RecoverUserUpdateAdmin(ctx, recoverRequest(req))
		if recovered.CommitOutcome != identity.CommitOutcomeNotCommitted || !errors.As(err, &notCommitted) {
			t.Fatal("failed administrative recovery changed outcome", err)
		}
		assertEffects(t, req, audit.OutcomeFailed, "1", 0)
	})
}
