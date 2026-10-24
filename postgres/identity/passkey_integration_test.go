//go:build integration

package identityoutbox_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/faustbrian/go-audit"
	auditpostgres "github.com/faustbrian/go-audit/postgres"
	"github.com/faustbrian/go-authorization/v3"
	"github.com/faustbrian/go-clock"
	identity "github.com/faustbrian/go-identity"
	kernel "github.com/faustbrian/go-identity/postgres"
	database "github.com/faustbrian/go-postgres"
	identityoutbox "github.com/faustbrian/go-transactional-outbox/postgres/identity"
	outbox "github.com/faustbrian/go-transactional-outbox/v2"
	outboxpostgres "github.com/faustbrian/go-transactional-outbox/v2/postgres"
	"github.com/jackc/pgx/v5"
)

// This native-owner seam exercises the real kernel creation and outbox phases.
// It deliberately proves no WebAuthn verification, MFA or session authority.
type passkeySignupPhase struct {
	domain           *kernel.PasskeyRegistrationDomain
	command          identity.Command
	intent           kernel.PasskeyRegistrationIntent
	failFinal        bool
	sessionMode      string
	sessionID        string
	sessionPersisted bool
}

func (*passkeySignupPhase) Descriptor() kernel.ContributorDescriptor {
	return kernel.ContributorDescriptor{Owner: "passkey/postgres", ContractID: "identity.passkey", Version: 1}
}
func (*passkeySignupPhase) FeatureOperations() []identity.FeatureOperation {
	return []identity.FeatureOperation{{Owner: "passkey", OperationID: "identity.passkey.register-verify", SchemaID: "identity.passkey.register-verify.signup-v1", SchemaVersion: 1, RecordRequirement: identity.FeatureRecordRequired, SubjectRequirement: identity.FeatureSubjectAnonymousFlow}}
}
func (p *passkeySignupPhase) ValidateFeatureCommand(c identity.Command, _ kernel.CommandPlan) error {
	if c != p.command {
		return identity.InvalidInputError{Field: "test_signup"}
	}
	return nil
}
func (p *passkeySignupPhase) PasskeyRegistration(c identity.Command) (kernel.PasskeyRegistrationIntent, error) {
	if c != p.command {
		return kernel.PasskeyRegistrationIntent{}, identity.InvalidInputError{Field: "test_signup"}
	}
	return p.intent, nil
}
func (p *passkeySignupPhase) ApplyFeature(ctx context.Context, w kernel.Work, c identity.Command) (identity.FeatureReceipt, error) {
	if _, err := p.domain.Apply(ctx, w, p, c); err != nil {
		return identity.FeatureReceipt{}, err
	}
	if p.sessionMode != "" && p.sessionMode != "session-unstaged" {
		id := p.sessionID
		if p.sessionMode == "session-substituted" {
			id = "substituted-session"
		}
		if err := w.StagePasskeyRegistrationSession(ctx, p, c, id); err != nil {
			return identity.FeatureReceipt{}, err
		}
		if p.sessionMode == "session-duplicate" {
			_ = w.StagePasskeyRegistrationSession(ctx, p, c, id)
		}
	}
	return identity.FeatureReceipt{Operation: p.FeatureOperations()[0], AfterVersion: 1}, nil
}

// Select the session-specific phase without its required staging, proving the
// outbox denies rather than silently writing the legacy two-event profile.
type unstagedPasskeySessionOutbox struct{ *identityoutbox.Contributor }

func (c unstagedPasskeySessionOutbox) PersistPasskeyRegistration(ctx context.Context, w kernel.Work) error {
	phase, ok := any(c.Contributor).(kernel.PasskeySignupSessionLifecycleContributor)
	if !ok {
		return identity.InvalidInputError{Field: "test_session_outbox"}
	}
	return phase.PersistPasskeySignupSession(ctx, w)
}
func (p *passkeySignupPhase) CheckFeatureCommit(ctx context.Context, w kernel.Work, c identity.Command) error {
	if p.failFinal {
		if p.sessionMode != "" {
			id, _ := base64.RawURLEncoding.DecodeString(string(p.intent.Creation.Binding.CommandID))
			rows, err := w.Query(ctx, `SELECT count(*) FROM public.identity_outbox_bindings WHERE tenant_id=$1 AND command_id=$2`, string(p.intent.Creation.Binding.TenantID), id)
			if err != nil {
				return err
			}
			defer rows.Close()
			var count int64
			if !rows.Next() || rows.Scan(&count) != nil || count != 3 {
				return identity.UnavailableError{}
			}
			p.sessionPersisted = true
		}
		return identity.ForbiddenError{}
	}
	_, err := w.PasskeyRegistrationUser(ctx, p, c)
	return err
}
func (*passkeySignupPhase) RecoverParticipant(context.Context, kernel.RecoveryWork) (kernel.RecoveryStatus, error) {
	return kernel.RecoveryUnknown, nil
}

func passkeyOutboxDatabase(t *testing.T) (context.Context, *database.Pool) {
	t.Helper()
	config := identityoutbox.IdentityTestPostgresConfig(t)
	ctx, stop := context.WithTimeout(t.Context(), 40*time.Second)
	t.Cleanup(stop)
	admin, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal("disposable administrative connection unavailable")
	}
	t.Cleanup(func() {
		bounded, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if admin.Close(bounded) != nil {
			t.Error("admin cleanup")
		}
	})
	id, err := identity.NewSubjectID()
	if err != nil {
		t.Fatal(err)
	}
	name := "identity_passkey_outbox_" + string(id)
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+quoted); err != nil {
		t.Fatal(err)
	}
	ownedRoles := false
	t.Cleanup(func() {
		bounded, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(bounded, "DROP DATABASE "+quoted); err != nil {
			t.Error("database cleanup")
		}
		if ownedRoles {
			if _, err := admin.Exec(bounded, "DROP ROLE audit_writer, audit_reader, audit_retention"); err != nil {
				t.Error("owned audit role cleanup")
			}
		}
	})
	pool, err := database.Connect(ctx, database.Config{DSN: config.ConnString(), MaxConns: 3, Configure: func(c *database.PoolConfig) error { c.ConnConfig.Database = name; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		bounded, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if pool.Close(bounded) != nil {
			t.Error("pool cleanup")
		}
	})
	install, err := pool.Raw().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer install.Rollback(ctx)
	if _, err := install.Exec(ctx, auditpostgres.FreshInstallPreflightSQL()); err != nil {
		t.Fatal(err)
	}
	for _, source := range []fs.FS{auditpostgres.Migrations(), outboxpostgres.Migrations(), identityoutbox.Migrations()} {
		files, err := fs.ReadDir(source, ".")
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			content, err := fs.ReadFile(source, file.Name())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := install.Exec(ctx, strings.Split(string(content), "-- +migrations Down")[0]); err != nil {
				t.Fatal("owned migration", file.Name(), err)
			}
		}
	}
	if err := install.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	ownedRoles = true
	base := kernel.CredentialMigrationSource{Base: kernel.StatusMigrationSource{Base: kernel.MigrationSource{}, Version: 10}, Version: 11}
	history, err := (kernel.PasskeyRegistrationMigrationSource{Base: kernel.PasswordSignupAcceptanceMigrationSource{Base: kernel.PasswordRegistrationMigrationSource{Base: base, Version: 12}, Version: 13}, Version: 14}).Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range history {
		if _, err := pool.Raw().Exec(ctx, step.UpSQL()); err != nil {
			t.Fatal("kernel migration", err)
		}
	}
	return ctx, pool
}

func passkeyAudit(t *testing.T, ctx context.Context, b identity.CommandBinding) audit.Record {
	t.Helper()
	builder, err := audit.NewBuilder(audit.BuilderConfig{IDGenerator: func() (string, error) { id, err := identity.NewSubjectID(); return string(id), err }})
	if err != nil {
		t.Fatal(err)
	}
	record, err := builder.Build(audit.RecordInput{Action: "identity.passkey.create_credential", OccurredAt: time.Now().UTC().Truncate(time.Microsecond), Outcome: audit.OutcomeSucceeded, ReasonCode: "completed", Actor: audit.ActorInput{Kind: audit.ActorService, ID: string(b.ActorID)}, Subject: audit.SubjectInput{Type: "credential", ID: b.RecordID}, Context: audit.ContextInput{TenantID: string(b.TenantID), CorrelationID: b.CorrelationID}, Changes: audit.ChangeSetInput{Redacted: true}, Policy: audit.PolicyMetadata{PolicyID: b.PolicyID, Version: "1"}, Attributes: map[string]string{"identity.profile": "identity.security.v1", "command.id": string(b.CommandID), "retention.class": "protected", "aggregate.type": "credential", "aggregate.id": b.RecordID, "aggregate.before_version": "0", "aggregate.after_version": "1"}})
	if err != nil {
		t.Fatal(err)
	}
	redactor, err := audit.NewRedactor(audit.RedactionRules{AllowedAttributes: []string{"identity.profile", "command.id", "retention.class", "aggregate.type", "aggregate.id", "aggregate.before_version", "aggregate.after_version"}})
	if err != nil {
		t.Fatal(err)
	}
	record, err = redactor.Redact(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestPasskeyRegistrationOutboxComposition(t *testing.T) {
	ctx, pool := passkeyOutboxDatabase(t)
	repo, err := kernel.NewRepository(pool, kernel.Config{MaximumDuration: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	domain, err := kernel.NewPasskeyRegistrationDomain(repo)
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
	policy, err := authorization.NewDecisionService(compositionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"commit", "primary-failure", "creation-failure", "final-failure", "session-commit", "session-primary-failure", "session-creation-failure", "session-event-failure", "session-final-failure", "session-substituted", "session-duplicate", "session-missing-field", "session-unstaged"} {
		t.Run(mode, func(t *testing.T) {
			commandID, err := identity.NewCommandID()
			if err != nil {
				t.Fatal(err)
			}
			userID := identity.UserID("passkey-user-" + mode)
			credentialID := identity.CredentialID("passkey-ref-" + mode)
			creation := identity.CommandBinding{CommandID: commandID, TenantID: "tenant-a", UserID: userID, ActorID: "actor-a", ActorKind: authorization.SubjectServiceAccount, OperationID: "identity.user.create", PolicyID: "policy", PolicyVersion: 1, SchemaID: "fields", SchemaVersion: 1, CorrelationID: "original-signup"}
			p := &passkeySignupPhase{domain: domain, failFinal: strings.HasSuffix(mode, "final-failure"), intent: kernel.PasskeyRegistrationIntent{Creation: identity.CreateUserCommand{Binding: creation, Status: identity.StatusActive}, Credential: identity.CredentialRef{ID: credentialID, TenantID: creation.TenantID, UserID: userID, Kind: identity.CredentialPasskey, Owner: "passkey", Version: 1}}}
			if strings.HasPrefix(mode, "session-") {
				p.sessionMode, p.sessionID = mode, "signup-session-"+mode
			}
			op := p.FeatureOperations()[0]
			binding := creation
			binding.UserID, binding.AnonymousFlowID, binding.RecordID = "", "original-flow-"+mode, string(credentialID)
			binding.OperationID, binding.SchemaID = op.OperationID, op.SchemaID
			fields, err := kernel.PasskeyRegistrationFields(p.intent)
			if err != nil {
				t.Fatal(err)
			}
			if p.sessionMode != "" && mode != "session-missing-field" {
				fields = append(fields, identity.FeatureField{Name: "session.result.session.id", Value: identity.FeatureValue{Kind: identity.FeatureText, Text: p.sessionID}})
			}
			p.command, err = identity.NewFeatureCommand(identity.FeatureCommandSpec{Binding: binding, Operation: op, Fields: fields})
			if err != nil {
				t.Fatal(err)
			}
			var selectedOutbox kernel.Contributor = lifecycle
			if mode == "session-unstaged" {
				selectedOutbox = unstagedPasskeySessionOutbox{lifecycle}
			}
			registry, err := kernel.NewContributorRegistry([]kernel.Contributor{repo, auditing, selectedOutbox, p})
			if err != nil {
				t.Fatal(err)
			}
			coordinator, err := kernel.NewCoordinator(pool, registry, kernel.CoordinatorConfig{MaximumDuration: 2 * time.Second, LeaseDuration: time.Second, CurrentFingerprintKeyVersion: 1, RetainedFingerprintKeys: map[uint32][32]byte{1: {1}}, QueryPolicy: &kernel.CommandQueryPolicy{Authorization: policy, Clock: clock.System{}, PolicyVersion: 1}})
			if err != nil {
				t.Fatal(err)
			}
			digest, err := kernel.PasskeyRegistrationDigest(p.intent)
			if err != nil {
				t.Fatal(err)
			}
			auditDigest, err := kernel.FeatureAuditBindingDigest(binding, op, "credential", passkeyAudit(t, ctx, binding))
			if err != nil {
				t.Fatal(err)
			}
			plan, err := coordinator.PrepareCommandPlan(binding, p.command, kernel.CommandPlan{Contributors: registry.Descriptors(), Feature: &kernel.FeatureRegistration{Contributor: p.Descriptor(), Operation: op}, SubjectType: "credential", Audits: []kernel.AuditPlan{{EventType: "identity.passkey.create_credential", SchemaVersion: 1, BindingDigest: auditDigest, RetentionPolicyVersion: 1, RequiredRecordCount: 1, MaxRecordBytes: 16384, MaxAggregateBytes: 16384, Required: true, RetentionClass: "protected"}}, Lifecycle: &kernel.LifecyclePlan{EventType: "identity.passkey.create_credential", AfterVersion: 1}, PasskeyRegistration: &kernel.PasskeyRegistrationPlan{UserID: userID, CredentialID: credentialID, CreationDigest: digest, CreationEventIndex: 1}})
			if err != nil {
				t.Fatal("prepare", err)
			}
			if strings.HasSuffix(mode, "primary-failure") || strings.HasSuffix(mode, "creation-failure") || mode == "session-event-failure" {
				event := "identity.passkey.create_credential"
				if strings.HasSuffix(mode, "creation-failure") {
					event = "user.created.v1"
				}
				if mode == "session-event-failure" {
					event = "session.created.v1"
				}
				if _, err := pool.Raw().Exec(ctx, `ALTER TABLE public.outbox_messages ADD CONSTRAINT test_passkey_event_rejection CHECK(topic<>`+"'"+event+"') NOT VALID"); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if _, err := pool.Raw().Exec(ctx, `ALTER TABLE public.outbox_messages DROP CONSTRAINT test_passkey_event_rejection`); err != nil {
						t.Error(err)
					}
				}()
			}
			reservation, _, err := coordinator.ReserveCommand(ctx, binding, p.command, plan)
			if err != nil {
				t.Fatal("reserve", err)
			}
			work, err := coordinator.Begin(ctx, reservation)
			if err != nil {
				t.Fatal("begin", err)
			}
			result, applyErr := coordinator.ApplyFeature(ctx, work, p.command)
			if applyErr != nil {
				result, err = coordinator.Abort(work, applyErr)
			} else {
				result, err = coordinator.Commit(work, result)
			}
			success := mode == "commit" || mode == "session-commit"
			if success && (err != nil || result.Outcome != identity.CommitOutcomeCommitted) {
				t.Fatal("paired events did not commit", err)
			}
			if !success && result.Outcome == identity.CommitOutcomeCommitted {
				t.Fatal("failed required effect committed")
			}
			if mode == "session-final-failure" && !p.sessionPersisted {
				t.Fatal("late rollback did not reach third event persistence")
			}
			rawID, _ := base64.RawURLEncoding.DecodeString(string(commandID))
			eventCount := 2
			if p.sessionMode != "" {
				eventCount = 3
			}
			counts := func(want int) {
				t.Helper()
				var users, credentials, bindings, audits, events, retained int
				if err := pool.Raw().QueryRow(ctx, `SELECT (SELECT count(*) FROM identity.users WHERE tenant_id=$1 AND user_id=$2),(SELECT count(*) FROM identity.credential_references WHERE tenant_id=$1 AND user_id=$2),(SELECT count(*) FROM identity.registration_bindings WHERE tenant_id=$1 AND command_id=$3),(SELECT count(*) FROM audit.identity_command_bindings b JOIN audit.records r ON r.record_id=b.event_id WHERE b.tenant_id=$1 AND b.command_id=$3 AND r.outcome=1),(SELECT count(*) FROM public.outbox_messages WHERE metadata->>'identity.command_id'=$4),(SELECT count(*) FROM public.identity_outbox_bindings WHERE tenant_id=$1 AND command_id=$3)`, string(binding.TenantID), string(userID), rawID, string(commandID)).Scan(&users, &credentials, &bindings, &audits, &events, &retained); err != nil {
					t.Fatal(err)
				}
				if users != want || credentials != want || bindings != want || audits != want || events != eventCount*want || retained != eventCount*want {
					t.Fatal("non-atomic creation/audit/paired outbox", users, credentials, bindings, audits, events, retained)
				}
			}
			if !success {
				counts(0)
				return
			}
			counts(1)
			assertPasskeyLifecycleBindings(t, ctx, pool, binding, eventCount, p.sessionID)
			var paired bool
			if err := pool.Raw().QueryRow(ctx, `SELECT count(*)=2 AND count(DISTINCT b.effect_id)=2 AND bool_and(m.id=b.effect_id AND m.idempotency_key=b.effect_id AND m.payload_version=1 AND m.metadata=jsonb_build_object('identity.tenant_id',$1::text,'identity.command_id',$3::text) AND convert_from(m.payload,'UTF8')::jsonb->>'profile'='identity-lifecycle-v1' AND convert_from(m.payload,'UTF8')::jsonb->>'operation_id'='identity.passkey.register-verify' AND convert_from(m.payload,'UTF8')::jsonb->>'before_version'='0' AND convert_from(m.payload,'UTF8')::jsonb->>'after_version'='1' AND CASE b.event_index WHEN 0 THEN m.topic='identity.passkey.create_credential' AND convert_from(m.payload,'UTF8')::jsonb->>'subject_id'=$4 AND convert_from(m.payload,'UTF8')::jsonb->>'subject_type'='credential' WHEN 1 THEN m.topic='user.created.v1' AND convert_from(m.payload,'UTF8')::jsonb->>'subject_id'=$5 AND convert_from(m.payload,'UTF8')::jsonb->>'user_id'=$5 AND convert_from(m.payload,'UTF8')::jsonb->>'subject_type'='user' ELSE false END) FROM public.identity_outbox_bindings b JOIN public.outbox_messages m ON m.id=b.effect_id WHERE b.tenant_id=$1 AND b.command_id=$2 AND b.event_index IN (0,1)`, string(binding.TenantID), rawID, string(commandID), string(credentialID), string(userID)).Scan(&paired); err != nil || !paired {
				t.Fatal("canonical paired event projection", err)
			}
			_, retry, err := coordinator.ReserveCommand(ctx, binding, p.command, plan)
			if err != nil || retry.Outcome != identity.CommitOutcomeCommitted || retry.AuditEventID != result.AuditEventID {
				t.Fatal("exact retry lost original receipt", err)
			}
			counts(1)
			caller := authorization.DecisionContext{PolicyRevision: 1, Request: authorization.Request{Subject: authorization.Subject{Kind: binding.ActorKind, ID: authorization.SubjectID(binding.ActorID)}, Tenant: authorization.TenantID(binding.TenantID), Action: authorization.Action(binding.OperationID), Resource: authorization.Resource{Type: "identity.anonymous-flow", ID: authorization.ResourceID(binding.AnonymousFlowID)}}}
			query := identity.RecoveryQuery{CommandID: binding.CommandID, TenantID: binding.TenantID, ActorID: binding.ActorID, ActorKind: binding.ActorKind, OperationID: binding.OperationID, AnonymousFlowID: binding.AnonymousFlowID, RecordID: binding.RecordID}
			recovered, err := coordinator.RecoverFeatureCommand(ctx, query, caller)
			if err != nil || recovered.Outcome != identity.CommitOutcomeCommitted || recovered.AuditEventID != result.AuditEventID || recovered.Feature == nil || recovered.Feature.Operation != op {
				t.Fatal("original metadata recovery lost committed result", err)
			}
			counts(1)
		})
	}
}

func assertPasskeyLifecycleBindings(t *testing.T, ctx context.Context, pool *database.Pool, b identity.CommandBinding, want int, sessionID string) {
	t.Helper()
	id, _ := base64.RawURLEncoding.DecodeString(string(b.CommandID))
	rows, err := pool.Raw().Query(ctx, `SELECT m.id,m.topic,m.payload,m.payload_version,m.metadata,m.ordering_key,m.idempotency_key,m.attempts,m.available_at,m.created_at,b.canonical_sha256,b.event_index FROM public.identity_outbox_bindings b JOIN public.outbox_messages m ON m.id=b.effect_id WHERE b.tenant_id=$1 AND b.command_id=$2 ORDER BY b.event_index`, string(b.TenantID), id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	var occurred time.Time
	for rows.Next() {
		var e outbox.Envelope
		var metadata, digest []byte
		var index uint16
		if err := rows.Scan(&e.ID, &e.Topic, &e.Payload, &e.PayloadVersion, &metadata, &e.OrderingKey, &e.IdempotencyKey, &e.Attempts, &e.AvailableAt, &e.CreatedAt, &digest, &index); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(metadata, &e.Metadata); err != nil {
			t.Fatal(err)
		}
		// pgx returns timestamptz in local time; staged lifecycle timestamps are UTC.
		e.AvailableAt, e.CreatedAt = e.AvailableAt.UTC(), e.CreatedAt.UTC()
		canonical := sha256.Sum256(e.CanonicalJSON())
		if !bytes.Equal(canonical[:], digest) || index != uint16(count) {
			t.Fatal("immutable original event digest/index changed")
		}
		if count == 0 {
			occurred = e.CreatedAt
		} else if !e.CreatedAt.Equal(occurred) {
			t.Fatal("signup events lost original occurrence time")
		}
		if index == 2 {
			var event struct {
				ID          string `json:"id"`
				CommandID   string `json:"command_id"`
				TenantID    string `json:"tenant_id"`
				UserID      string `json:"user_id"`
				EventIndex  uint16 `json:"event_index"`
				SubjectType string `json:"subject_type"`
				SubjectID   string `json:"subject_id"`
				Before      string `json:"before_version"`
				After       string `json:"after_version"`
			}
			if err := json.Unmarshal(e.Payload, &event); err != nil {
				t.Fatal(err)
			}
			if e.Topic != "session.created.v1" || event.ID != e.ID || event.CommandID != string(b.CommandID) || event.TenantID != string(b.TenantID) || event.UserID == "" || event.EventIndex != 2 || event.SubjectType != "session" || event.SubjectID != sessionID || event.Before != "0" || event.After != "1" {
				t.Fatal("original signup session projection changed")
			}
		}
		count++
	}
	if rows.Err() != nil || count != want {
		t.Fatal("incomplete immutable lifecycle binding evidence", count, rows.Err())
	}
}
