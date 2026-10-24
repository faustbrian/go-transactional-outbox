# Changelog

## Unreleased

- Persist canonical passkey-credential and distinct account-creation lifecycle
  events in the original signup transaction using retained command/index/digest
  bindings. Adopt the public kernel passkey-registration contract and current
  outbox-v2 writer; existing APIs, payloads and migration bytes remain unchanged.
  The native owner still supplies actual verification, MFA and session effects.

- Preserve pending identifier-removal cascade ID and generation in the
  transactional lifecycle payload. Non-cascade payload bytes and replay
  bindings remain unchanged; publication does not acknowledge consumer cleanup.

- Restore this independent adapter against current identity and outbox-v2
  engines. Use its unversioned repository-relative module path, not a `/v2`
  segment before `postgres/identity`; module tags are independent. Existing
  lifecycle payloads, retained bindings and migration bytes are unchanged.

- Persist both login and passwordless account-creation lifecycle events through
  the original consuming transaction. Reuse immutable command/index bindings;
  an event failure rolls back with account creation, session and one-time use.

- Persist native password-reset lifecycle events with exact independent user,
  credential and credential-authority successors through the original Work.
  Keep reset and change operation/event bindings distinct while preserving
  existing replacement payloads, outbox bindings and schema. This does not
  authorize recovery or acknowledge another owner's session cleanup.

- Persist native password-change lifecycle events with separate user and
  credential-authority version successors in the original transaction. The
  distinct credential-replacement payload retains exact decimal versions and
  existing immutable outbox bindings; no schema or generic payload change is
  required. Adopt the current identity replacement contract before composition.

- Adopt the primary identifier/verification storage migration. Existing
  creation, profile and administrative composition retains atomic audit/outbox
  behavior and original receipts with the expanded identity schema. Generic
  outbox contracts and migrations remain unchanged.

- Adopt the administrative typed-field identity adapter. Public administrative
  updates compose with production audit/outbox participants, retaining protected
  audit classification, original receipts and current redaction. Actual outbox
  write failures roll back domain and successful-audit prefixes; expired
  unexecuted reservations need all frozen participants' negative evidence.
  Generic outbox contracts and migrations remain unchanged.

- Adopt the durable profile identity adapter and canonical audit-alias recovery
  correction. Public profile updates compose with real PostgreSQL audit/outbox
  participants, preserving original receipts, current redaction, partial typed
  field updates and atomic rollback. Expired unexecuted commands reconcile only
  from the frozen participants' negative evidence. Generic outbox APIs and
  storage profiles remain unchanged.

- Exercise public user creation, exact replay, denial and abandoned-command
  recovery with the real identity, audit and outbox PostgreSQL adapters.
  The optional integration module pins the audit PostgreSQL contributor;
  generic outbox consumers gain no identity or audit dependency.

- Add invocation-bound read-only RecoveryContributor evidence over retained
  tenant/command/index bindings. Pruning and relay scheduling do not erase
  participant effects; extra or wrong-index bindings remain unknown. No new
  migration or write privilege is needed. Positive evidence is not a receipt.

- Add an optional outbox-owned identity lifecycle contributor using the sole
  Coordinator's generation-bound Work and existing outbox payload schema.
- Persist immutable command/index/envelope-digest bindings so exact replay
  cannot recreate pruned notifications; reject conflicting bindings and bytes.
- Supply additive, forward-only binding migrations in a separate ledger
  namespace. Existing generic outbox messages and released v1 adapters remain
  unchanged. Requires Go 1.27 and the pinned unpublished development contracts.
