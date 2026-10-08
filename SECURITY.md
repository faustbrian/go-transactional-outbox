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

Report vulnerabilities privately through GitHub's security advisory workflow.
Do not open a public issue containing credentials, payloads, exploit details,
or tenant data.

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
