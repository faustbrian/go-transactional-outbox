# Security threat model, version 1

## Scope and assets

This model covers the root outbox, PostgreSQL writer and store, relay, and
the six optional adapter modules. It describes the corrected public v2
contracts; it does not retroactively certify v1. See the
[published-version guidance](../SECURITY.md#published-findings) and the
[shared security model](https://github.com/faustbrian/go-library-tools/tree/main/docs/ecosystem/security).

Assets are durable application changes, envelopes, ordering and idempotency
keys, lease ownership, replay audit records, database and broker credentials,
and availability. Payloads, metadata, message IDs, topics, and failure causes
may contain sensitive application data. A lease token is a concurrency
capability, not an application authorization decision.

Inputs crossing trust boundaries include application envelopes, operator
replay and retention requests, stored records, publisher results, injected
callbacks, and dependency behavior. Database, broker, queue, and telemetry
clients are supplied and configured by the application. Construction does
not implicitly discover destinations or grant network or filesystem access.

## Boundary controls

| Boundary | Control and limitation |
|---|---|
| Envelope to writer or publisher | Validate byte and entry limits before accepted writes and transport copies. Direct serialization helpers are not an input-admission sandbox. |
| Application mutation to persistence | The writer uses the caller's exact transaction. Atomicity requires the application mutation and insert to share that transaction and a successful commit. Roll back on writer errors. |
| SQL and schema selection | Values use parameters; configured identifiers are validated and quoted. Restrict database privileges independently. |
| Claim to durable transition | Random lease generations distinguish owners. Corrected transitions lock the row before checking the fresh PostgreSQL deadline. An expired or replaced token cannot authorize a transition. |
| Relay to external effect | Bound batches, workers, attempts, leases, backoff, and cleanup. Publication and database acknowledgement are not one transaction: duplicate effects remain possible. |
| Operator to replay or deletion | Replay requires an explicit authorizer and records audit context. Applications authorize retention separately; archive callbacks can hold row locks. |
| Envelope to queue or stream adapter | Corrected adapters admit payload and mapped metadata before copying. Deprecated module identities and successor modules remain separately versioned. |
| Observation and errors | Stored failure text is a fixed category; the telemetry adapter excludes payloads, identities, destinations, and error text. Root observer events still include bounded IDs and topics; database errors and inspected causes are not universally redacted. |
| Build and release to consumers | Pinned dependencies and automation, scanner gates, signed release artifacts, checksums, and public-consumer verification provide separate supply-chain controls. Scanner success does not prove application confinement. |

The [inventory](inventory.md) records concrete field, batch, worker, lease,
retry, and schema ceilings. The [guarantees](guarantees.md) define ownership,
crash windows, ordering, archival, and duplicate delivery. The root does not
promise exactly-once external effects. Consumers need durable idempotency;
archive destinations need idempotency by envelope ID.

Lease generation uses the standard cryptographic random source. Custom
generators and clocks are trusted application collaborators, not independent
security authorities. PostgreSQL time, rather than relay host time, decides
durable lease eligibility. TLS, credentials, endpoint authorization, proxy and
DNS policy, and client lifecycle remain application-owned.

## Residual risks and ownership

These boundaries are operating requirements, not acceptance of an uncorrected
high-severity defect or certification of an unverified transport.

| Risk and owner | Rationale and mitigation | Review condition |
|---|---|---|
| Non-cooperative collaborators: application owner | Go cannot safely terminate arbitrary callbacks. Require cancellation and finite backend deadlines. The synchronous queue producer has no context parameter; configure its own request and network timeouts. Blocking archive hooks retain locks. | Changing providers, deadlines, callback implementations, or shutdown requirements. |
| Duplicate effects: application and consumer owners | A successful publish can precede a failed acknowledgement; archive success can precede an ambiguous commit. Use durable consumer and archive idempotency and bounded retries. | Changing side effects, storage transactions, retry policy, or replay use. |
| Sensitive operational identities and causes: application owner | Root events carry IDs and topics, and raw database causes can expose details. Use non-sensitive identifiers, restricted sinks, and application redaction; do not export raw causes to untrusted clients. | Changing identifiers, observers, error reporting, or telemetry destinations. |
| External clients and dependencies: integration owner | Admission in this adapter does not bound all upstream transport decoding, logging, or blocking calls. Review supplier advisories separately and configure authenticated, bounded clients. | Supplier updates, new transports, or changed upstream security guarantees. |
| Maintainer or release compromise: maintainers and consumers | Repository access can subvert source and automation. Restrict credentials, review changes, verify immutable artifacts and public module resolution, and retain vulnerability scanning. | Credential incidents, workflow changes, signing changes, or a new release. |

Report suspected vulnerabilities through the [private reporting process](../SECURITY.md#reporting).
Published findings must name the actual affected module paths and releases;
a new major import path is not a patched version of an old module identity.
