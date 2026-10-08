# Security Policy

## Supported versions

Each independently versioned module receives security fixes in the latest
patch release of its latest published major. Until a new major is published,
the existing published major remains current; preparing v2 on `main` does not
end v1 support. Older majors require migration to the supported major.

Deprecated adapter names follow the [adapter support interval](docs/adapter-migration.md).
Path deprecation does not change the supported-major policy. Root and adapter
major versions are independent; publishing root v2 does not end adapter v1
support while v1 is still an adapter's latest published major.

Reports are acknowledged, triaged, and coordinated under the shared
[Golib vulnerability-management policy](https://github.com/faustbrian/go-library-tools/blob/ebd8d754223cf8cfc60fd8f3f63709881093d51f/docs/ecosystem/security/vulnerability-management.md).
Advisories identify exact affected and fixed module versions and safe upgrade
guidance. Fixes are scoped to affected modules, not unrelated package releases.

## Reporting

Report vulnerabilities through the
[private reporting form](https://github.com/faustbrian/go-transactional-outbox/security/advisories/new).
Do not open a public issue containing credentials, payloads, exploit details,
or tenant data.

## Published findings

The following corrected issues have conditional medium severity. They do not
establish authentication bypass or measured resource exhaustion.

- Root `github.com/faustbrian/go-transactional-outbox` v1.0.0 accepts durable
  lease transitions with an expired token when that token has not been
  replaced. A paused worker can therefore change a record after its lease
  expires. Root `github.com/faustbrian/go-transactional-outbox/v2` v2.0.0
  checks the fresh PostgreSQL deadline after acquiring row ownership. Upgrade
  root and relay consumers together. A relay-host clock check alone does
  not close a database lock-wait window.
- `adapters/queue` v1.0.0, `adapters/rabbitstream` v1.0.0 and v1.0.1, and
  `adapters/gorabbitstream` v1.0.0 through v1.0.2 copy input-sized payload or
  metadata before admission. Passing oversized envelopes directly to these
  publishers can cause allocations before rejection. Their corresponding
  `/v2` modules at v2.0.0 admit input before copying. Until migration, callers
  must enforce finite payload and mapped metadata budgets before publication.
  Kafka, deprecated Go Kafka, and telemetry adapters are not included in this
  finding; supplier transport advisories are separate.

These fixes are published as new module identities, not patched v1 versions.
The v2 modules require Go 1.27. Follow the
[adapter migration guide](docs/adapter-migration.md) when changing imports and
keep independently versioned root and adapters coherent.

The [versioned threat model](docs/security-threat-model-v1.md) records trust
boundaries, sensitive data, controls, and remaining application obligations.
It is not a substitute for release or transport qualification.

## Operational responsibilities

- Restrict database roles to the required schema and statements.
- Treat payloads and metadata as sensitive; do not include them in logs,
  metrics, traces, or support tickets.
- Authorize replay and retention outside the library and audit every operator.
- Use TLS and authenticated connections to PostgreSQL and publishers.
- Keep consumers idempotent and bound their own retries.
- Require injected publishers, health checks, heartbeats, replay authorizers,
  and archive hooks to honor context cancellation and enforce finite I/O
  deadlines.

Lease tokens prevent stale transitions but do not authorize callers. Replay
request fields provide audit context but do not replace application access
control.

Go cannot safely terminate a callback that ignores its context. A stuck
publisher can delay shutdown, and a stuck archive hook can retain transaction
locks. Treat callback liveness as application security and availability policy.
