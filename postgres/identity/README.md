# Identity lifecycle outbox contributor

Optional Go 1.27 module, package `identityoutbox`. It persists safe staged
identity lifecycle events in `public.outbox_messages` through the **same**
`identity/postgres.Work` as the protected mutation and audit. Core outbox
consumers do not acquire an identity dependency.

Selected identifier-removal events retain their original `cascade_id` and
lossless decimal-string `cascade_generation` in the existing JSON payload.
These correlate a pending owner admission, not downstream acknowledgement.
Both fields are omitted for non-cascade events, preserving their existing
payload bytes and immutable replay bindings. This additive projection is not
the platform's complete lifecycle-envelope/cascade consumer qualification.

The independent module path is
`github.com/faustbrian/go-transactional-outbox/postgres/identity`. It depends on
the `/v2` outbox engine; the engine's major suffix is not part of this module's
directory prefix. Use matching current identity and audit adapter pins. Native
integration tests provision digest-pinned, disposable PostgreSQL when no
explicit task-owned loopback test DSN is supplied.

## Adoption and API

```go
contributor, err := identityoutbox.NewContributor()
if err != nil {
    return err
}
// Supply contributor to the sole identity Coordinator's compile-time registry.
```

The [executable example](example_test.go) demonstrates registration without
I/O or transaction authority. `Descriptor` is exactly owner `outbox/postgres`,
contract `identity.lifecycle`, version 1. `PersistLifecycle(ctx, work)` is a
Coordinator-only phase after complete successful audit/lifecycle staging.
There is no public constructor for Work; do not invent a transaction proxy or
invoke a contributor outside that owner.

`PersistCredentialReplacement(ctx, work)` additionally implements the kernel's
native password-change lifecycle contract. It checks the staged original
credential, tenant/user and exact successors, then persists one envelope using
the same Work and retained binding. Payload profile
`identity-credential-replacement-v1` contains the unchanged `identity-lifecycle-v1`
record as `lifecycle`, plus decimal-string `before_user_version`,
`after_user_version`, `before_authority_version` and `after_authority_version`.
These independently owned versions let consumers invalidate credential authority
without confusing it with the credential record version. No fresh authority
evidence, secrets or consumer acknowledgement is minted. Existing generic
lifecycle bytes and schema remain unchanged; no new migration is required.

`PersistCredentialReset(ctx, work)` implements the distinct kernel reset
contract for `identity.password.reset-complete` / `password.reset.v1`.
It retains the same credential-replacement envelope shape and independent
successors without accepting password-change events through the reset method
or reset events through the change method. Native proof/current recovery
admission, password material and atomic session revocation remain their owners'
obligations. Publication is not an acknowledgement of downstream invalidation.

`PersistMagicLinkRegistration(ctx, work)` persists the staged login event and
the kernel's distinct `user.created.v1` event for signup-by-link. Both retain
their original command/index identities in the existing outbox schema. Any
failure must abort the same Work, rolling back events, creation, session and
one-time use together. This phase does not establish email possession, create
an account, acknowledge delivery or manage its own transaction.

`PersistPasskeyRegistration(ctx, work)` implements the kernel's typed
passkey-only signup lifecycle phase. It persists the canonical
`identity.passkey.create_credential` primary event and the separately staged
`user.created.v1` event through the original Work, retaining their distinct
command/index/digest bindings. Any failure rolls back both with kernel account
creation and all required effects. This phase creates no session or verification
authority and performs no WebAuthn validation. The native signup owner must
compose those actual owners before committing. Existing lifecycle APIs, payload
profiles and migration bytes remain unchanged; no adapter migration is added.

The same instance implements `RecoveryContributor`. Only the Coordinator calls
`RecoverParticipant(ctx, recoveryWork)`, with the frozen lifecycle declaration
and an invocation-bound primary read capability. One bounded indexed lookup
reads at most two retained binding rows: absence proves no participant effect;
one binding at the expected index proves a retained participant effect; a
different index or extra binding remains unknown. This does not validate a
current delivery payload or reconstruct a command receipt. Positive/unknown
evidence beside a pending journal remains unresolved. Pruning, relay attempts
and scheduling cannot erase historical evidence. No extra migration, mutation,
grant, private transaction, callback or retry is needed; existing binding SELECT
privileges suffice. The recovery handle and rows must not be retained.

**Development limitation:** the complete identity domain/cascade pipelines are
still under construction. Public user creation, exact replay, current denial,
terminal observation and abandoned-reservation recovery are exercised with
the real identity Repository, Coordinator, CreationUnitOfWork, public
UserCreationService and audit/outbox contributors on PostgreSQL. The
composition also covers self-profile updates through ProfileService and
ProfileUnitOfWork: partial typed-field merge, original replay/recovery under
current redaction, canonical user-update audits, updated lifecycle payloads,
atomic rollback after an actual outbox SQL failure, and expired reservations
reconciled through all real frozen participants. Self-service cannot overwrite
administrator-only fields. Administrative typed-field updates also compose
through `UserUpdateAdminService` and `UserUpdateUnitOfWork`, retaining protected
audit classification, exact original receipts and current redaction. Real
outbox SQL rejection rolls back domain changes and successful audit while
preserving one failed protected audit. Expired administrative reservations
reconcile only through all frozen production participants' negative evidence.
These tests do not certify administrative lifecycle cascades or API-key actors.
Retained core creation/profile composition uses the nine base identity
migrations. Passkey-only signup additionally requires the kernel's credential,
registration-binding, signup-acceptance and passkey-registration histories;
apply their forward successors in the application's selected identity history.
Dependency pins resolve to authenticated public module revisions, not tagged
release qualification. Final
clean-consumer, complete composition, coverage/mutation and release evidence
remain pending. Existing released v1 outbox adapters keep their v1 dependencies.

## Migration and storage ownership

Install the root outbox [migrations](../migrations.go) first, then every
file returned by this module's `Migrations` under a **separate migration
ledger/namespace**. Both sources start at version 1; do not merge them into
one sequence. The additive adapter migration leaves populated generic outbox
messages untouched and creates `public.identity_outbox_bindings` containing
only effect ID, tenant, command bytes, event index and canonical SHA-256 digest.
It creates no second payload store. It does not backfill generic messages.

Each effect ID and tenant/command/index tuple is non-reassignable. Binding
records have no payload foreign key and remain after the generic outbox's
authorized pruning. Exact replay verifies the retained immutable binding and,
if the message still exists, its immutable profile; it never recreates a
pruned message. Different scope or canonical bytes return
`identity.ConflictError`. Database/integrity failures return redacted
`identity.UnavailableError`; cancellation/deadline errors retain their
standard context classification. No raw database diagnostics escape.

The owner must roll back/reconcile the entire transaction after **any**
contributor failure. Success is staging, not commit, downstream delivery or
cascade acknowledgement. Command-journal recovery remains the Coordinator's
responsibility. No contributor retry loop, savepoint, independent transaction,
hook or external network effect exists.

Down explicitly fails: deleting retained bindings would permit replay.
Deploy migrations with bounded lock/statement timeouts using the migration
owner, and separately grant the application `SELECT, INSERT` on bindings and
the outbox writer's required privileges. Relay/prune roles need no binding
write privileges. Applications/relays must not own tables or have DDL,
TRUNCATE, trigger-disable, replication-bypass or superuser privileges.
Immutable UPDATE/DELETE/TRUNCATE triggers protect ordinary DML, not hostile
owners or administrators. The trigger uses SECURITY INVOKER and a fixed
`pg_catalog` search path; it emits no row values.

## Safe wire profile and ordering

Payload version 1 uses fixed JSON profile `identity-lifecycle-v1` with effect
ID, command ID, tenant ID, user ID, operation ID, event type/index, subject
type/ID, before/after version and occurrence time. Versions are decimal
**strings**, preserving all uint64 values for consumers without exact integer
JSON support. Timestamps are UTC microseconds. No attributes, identifier
values, credentials, tokens or arbitrary extension payloads are accepted.
Metadata contains only tenant and command IDs. The envelope ID and idempotency
key retain Work's deterministic effect ID; ordering uses a SHA-256 typed
tenant/subject tuple. Generic outbox attempts, scheduling, leases and terminal
delivery state may change without changing the original replay binding.

Instances are immutable and safe for independent Work invocations concurrently;
each Work itself is serial-use and must not be retained. Execution is bounded
by the earlier caller/Work deadline. Each event prepares one bounded envelope
and uses at most four database statements; paired signup phases process exactly
two events in order, with no unbounded loops.
It delegates insert validation, SQL and schema ownership to root
`Writer.PrepareInsert`. Default schema/table only; custom schemas are not
silently approximated. Payloads are safe projections but IDs may still be
personal metadata: do not log envelopes, SQL arguments or plans.

## Verification and FAQ

Run `go test ./...` for the public constructor/profile tests. The integration
lane is `go test -tags integration -race ./...`, with
`IDENTITY_OUTBOX_TEST_DSN` pointing explicitly to a **disposable**, loopback
`identity_test` database. Tests create/drop private database namespaces and
fail, rather than skip, when infrastructure is absent. They prove commit and
rollback, conflicting/parallel replay, immutable bindings, relay rescheduling,
retained-payload corruption denial and pruning-safe replay on PostgreSQL 18.
Recovery tests also prove absent/exact/wrong-index/extra evidence, tenant
isolation and read-only execution independently of delivery payload state.
The [public composition test](composition_integration_test.go) checks actual
success/failure audit payloads and lifecycle metadata, not only binding counts.
The [passkey composition test](passkey_integration_test.go) uses the real
coordinator, kernel creation domain, audit and outbox contributors to prove
paired canonical events, primary/secondary SQL failure atomicity, late rejection,
original retry and metadata recovery. Its synthetic native participant is an
explicit seam, not proof of real WebAuthn signup, MFA or session issuance.

- **Exactly once?** One atomic persisted identity notification does not make
  broker delivery exactly once. Root relay remains at-least-once; consumers
  must deduplicate effect IDs under their documented retention policy.
- **Privacy deletion?** Binding retention is required for non-reassignment;
  lifecycle/privacy policy and legal holds must explicitly govern its IDs.
  This module does not acknowledge deletion or implement a cascade engine.
- **Can callers write arbitrary events?** No. Only complete Work staging is
  accepted; public generic outbox Writer APIs remain intentionally separate.

See [release notes](CHANGELOG.md), [root guarantees](../../docs/guarantees.md)
and [root security](../../SECURITY.md).
