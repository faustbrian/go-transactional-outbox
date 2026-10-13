# Source documentation audit

[Documentation map](index.md) · [Repository README](../README.md)
[Next: API Reference](api.md)

## Pre-edit scope and plan

Base `af8e3c19e1760d2cdb49ce45aea279e0d319b5ab`, detached main-source worktree;
comments/docs-only Tier A. Existing unrelated and identity-platform worktrees
stay untouched. Seven modules (root plus six adapters), nine catalog production
packages and three explicit migration-fixture directories; all 82 tracked Go
files are included. Reproduce through `git ls-files '*.go'`, module/package
catalogs, native directory-aware `source-docs inventory/check --json` and
`.golib.yaml`. Initial 238 exported-comment omissions identify useful gaps,
not API correctness or prose quality. All seven API baselines use apidiff;
no full-prose snapshot generation applies.

## Complete tracked Go inventory

- `adapters/gokafka`: `benchmark_test.go`, `concurrency_test.go`, `doc.go`, `example_test.go`, `golden_test.go`, `integration_test.go`, `publisher.go`, `publisher_fuzz_test.go`, `publisher_test.go`, `relay_integration_test.go`.
- `adapters/gorabbitstream`: `benchmark_test.go`, `doc.go`, `example_test.go`, `facade_test.go`, `integration_test.go`, `publisher.go`, `publisher_fuzz_test.go`, `publisher_test.go`.
- `adapters/kafka`: `benchmark_test.go`, `concurrency_test.go`, `example_test.go`, `golden_test.go`, `integration_test.go`, `migration_test.go`, `publisher.go`, `publisher_fuzz_test.go`, `publisher_test.go`, `relay_integration_test.go`, `successor_contract_test.go`.
- `adapters/otel`: `benchmark_test.go`, `convention_inventory_test.go`, `example_test.go`, `fuzz_test.go`, `lifecycle_test.go`, `privacy_test.go`, `relay_integration_test.go`, `telemetry.go`, `telemetry_test.go`.
- `adapters/queue`: `allocation_admission_test.go`, `durable_integration_test.go`, `example_test.go`, `publisher.go`, `publisher_fuzz_test.go`, `publisher_test.go`.
- `adapters/rabbitstream`: `allocation_admission_test.go`, `benchmark_test.go`, `example_test.go`, `integration_test.go`, `migration_test.go`, `publisher.go`, `publisher_fuzz_test.go`, `publisher_test.go`, `successor_contract_test.go`, `version_admission_termination_test.go`.
- `.`: `envelope.go`, `envelope_fuzz_test.go`, `envelope_internal_test.go`, `envelope_test.go`, `examples_test.go`, `observability.go`, `observability_test.go`.
- `postgres`: `atomicity_integration_test.go`, `benchmark_integration_test.go`, `hardening_integration_test.go`, `lease_expiry_integration_test.go`, `migration_integration_test.go`, `migrations.go`, `migrations_test.go`, `mutation_boundaries_internal_test.go`, `store.go`, `store_test.go`, `writer.go`, `writer_fuzz_test.go`, `writer_test.go`.
- `postgres/testdata/gomigrations/filesystem`: `filesystem.go`.
- `postgres/testdata/gomigrations/runner`: `gomigrations_test.go`.
- `postgres/testdata/gomigrations/source`: `source_test.go`.
- `relay`: `main_test.go`, `relay.go`, `relay_fuzz_test.go`, `relay_internal_test.go`, `relay_test.go`.

## Contracts, variants and important private rationale

- Root mutable Envelope versus builder-owned snapshots, opaque payload bytes,
  positive whole limits, metadata byte/entry admission, default version and UTC
  microsecond truncation; injected generator/clock/option callbacks and failure
  effects. Canonical JSON is deterministic, not validation, memory/time admission
  or a lossless arbitrary-invalid-UTF8-string guarantee.
- PostgreSQL Writer borrows a caller transaction: one statement, ten parameters
  per row and 6553-row ceiling, full prevalidation, schema ceilings, quoted names
  and no transaction finalization. Store borrows a pool, uses DB-clock row-lock
  expiry checks and (ID,token) fencing; one batch shares a token and generators
  must not reuse generations. Claim increments attempts, including claims that
  never publish, and decoder/commit failures can leave an ambiguous durable state.
- Default claim selection versus per-key/topic head-of-line serialization;
  empty keys unordered, future earlier rows block, terminal rows stop blocking.
  Leases/time budgets are not per-publication deadlines or remote delivery proof.
- Replay authorization copies IDs but does not create a cross-system transaction;
  replay/audit commit is atomic in PostgreSQL with ambiguity windows. Archive
  callbacks run while row locks/transaction are held; duplicate-idempotent
  archival, no same-row reentrancy and bounded callbacks remain caller obligations.
  Error persistence deliberately uses fixed operation text, not arbitrary causes;
  wrapped operational errors and summary identifiers remain potentially sensitive.
- Relay owns per-RunOnce workers/jobs and one heartbeat per active publication;
  producer closes jobs, workers join and heartbeat completion is awaited. No
  Close/lifecycle owner or instance-wide worker guard; callbacks/providers must
  cooperate with context and support concurrency. Renewal failure cancels publish
  and refuses transitions; nil acceptance can still precede renewal/mark failure.
  Partial Result counts/joined errors, cancellation cleanup context, policy panic
  containment and full-jitter retry ceilings have separate observable semantics.
- Kafka canonical and retained implementations are parallel sources with distinct
  named/sentinel identities, not aliases. Adapter owns mapped byte snapshots and
  ordered metadata, not client lifetime or producer acceptance semantics. Fixed
  partition-key precedence, exact reserved headers, panic ambiguity and raw-cause
  redaction boundary remain visible to consumers.
- Rabbit Streams successor requires confirmed result; legacy facade delegates
  to a pinned published successor, translating legacy sentinel trees and retaining
  types/categories. Both current main source and actual pinned legacy behavior
  must be distinguished. Queue maps a stable versioned task, separates backend
  acceptance from disposition, borrows inputs until encode and cannot interrupt
  synchronous Queue after its initial context check.
- Telemetry borrows standard providers, uses fixed metric/span labels, retains
  caller cancellation/values rather than arbitrary provider context modifications,
  contains provider panics and rethrows publisher panics. It forwards only
  traceparent, not tracestate/baggage; copied preexisting metadata remains intact.
  Injection is not byte-bounded; constructor/instrument errors can wrap causes.
  SDK flush/export/shutdown and custom provider safety remain caller-owned.
- Migration fixture FS only adapts trusted embedded SQL; it checks byte/entry
  limits after fs reads and polls cancellation before/after, not during borrowed
  filesystem calls. External migrations composition uses a disposable workspace
  and optional live PostgreSQL; those fixture packages are parser-audited, not
  catalog production packages. SQL/embedded inputs remain byte-exact.

## Missing documentation versus questions

Repair useful member/error/interface gaps and consolidate two duplicate legacy
package comments into one canonical doc.go while preserving deprecation markers.
No generated Go/platform variants exist. Embedded SQL directive and integration
build tags, fuzz/bench/example bodies, saved fixtures and dependency graphs stay
unchanged. Include tests by constructor/persistence lease/relay cancellation/
provider mapping/privacy/concurrency/recovery/migration families; no live-service
qualification is inferred from source or in-process examples.

Questions/limits: callback cooperation is required for bounded shutdown; queued
claims are not individually heartbeated until publication starts; RunOnce bounds
are per invocation, not aggregate concurrent calls. Store transaction outcomes
can be ambiguous after external acceptance/archival or response decoding. No
stronger message-processing/delivery promise or total heap/time bound is invented.
Observe/Inject metadata remains caller-controlled even where labels are bounded;
root events/logs contain IDs/topics. Traceparent-only propagation contradicts one
current telemetry guide claim and requires narrow maintained-prose clarification.
Unknown rationale for ignoring tracestate and for non-renewed queued claims is
recorded, not replaced with an invented standards interpretation or runtime fix.

## Selected proof

Pinned Go 1.27.2 only after explicit serialized slot: all 82 comment-free AST/
signature/literal/directive preservation; raw significant-token comparison with
only grammar-optional implicit semicolons immediately before closing braces
normalized when multiline field GoDoc requires them; all nine native catalog
GoDocs and existing examples in each owning module. Full root documentation
script includes package enumeration; cheap `scripts/check-docs.sh --local`
preserves required files and refusal of false delivery promises before Go.
Nine catalog entrypoints render; three trusted testdata fixtures get native
parser/comment proof, not an external checkout or live migration campaign.

Shared source/offline specification checks, affected local links/markers,
frozen baseline/dependency/SQL/example authority equality, pinned staged added
content secrets and complete ordinary Tier A truth/usefulness review precede
explicit-path Intent/ACK safe-wrapper commit. No runtime/dependency/fixture repair,
DB/broker/race/fuzz/mutation/benchmark/consumer/release campaign. Every affected
module receives material Unreleased clarification. All three owned caches and
helpers are cleaned success/failure; parent owns CI/publication/enforcement.

## Coverage detail

All tracked test/helper sources were reviewed by their source families above:
11 fuzz targets (construction, writer identifiers, relay options/failures, Kafka
mapping twice, Rabbit ownership/classification twice, queue encoding and two
telemetry lifecycle/isolation targets), 12 benchmark entrypoints and seven
executable examples. These remain unchanged, including Output blocks and
service-tagged cases. Twelve integration-tagged files remain excluded from
ordinary package rendering; no platform build split or Go generator is present.
The sole embedded-SQL directive and retry-jitter security annotation are intact.
No unfinished source markers were found. The catalog has no specification
decisions; protocol/provider compatibility remains bounded by existing pinned
dependencies and frozen baseline/fixture records, not invented conformance.

The operations guide now explicitly scopes capacity to one loop per replica and
notes queued-lease expiry. Historical reviews/pins and release entries are
untouched; all seven existing Unreleased sections retain their prior entries.

## Completed bounded evidence

- Directory-aware `source-docs check --json`: 238 initial omissions repaired;
  final zero findings. Source truth/usefulness was reviewed separately from
  that structural result, including every tracked test/helper source family.
- Go 1.27.2 `gofmt -l` over all 82 tracked files: clean. Disposable native
  scanner/parser comparison against the base: exact raw significant tokens,
  literals, full comment-free AST/signatures and directives identical across
  all 82 files; zero raw token differences and no normalization needed.
  All 450 final comment groups parsed and rendered to text.
- Native `scripts/check-docs.sh` passed, including root package enumeration.
  `go doc -all` rendered all nine catalog entrypoints from their owning modules.
  Final Writer/Relay wording refresh rechecked both affected files' exact
  preservation, rendered 70 groups to HTML and rerendered PostgreSQL/Relay
  GoDoc; seven unchanged package renders retain their original result.
- `go test . -run '^Example' -count=1 -timeout 2m` passed in the root and each
  of six adapter modules. No example bodies/Output blocks changed; no broader
  unit, service-tagged, race/leak, fuzz, benchmark or conformance claim follows.
- Legacy Rabbit's actual go.mod/go.sum-authenticated published successor
  v2.0.0 was inspected in the owned module cache: its admission helper precedes
  mapped payload copying. No assumption that current successor main replaces
  that pinned implementation is made.
- Native `specification check`: zero applicable modules/decisions. Local docs
  policy and `git diff --check` passed. Affected Markdown has no local link
  targets to resolve; existing external versioned links/pins remain unchanged.
  All API apidiff baselines, dependency catalogs/graphs, SQL migrations, frozen
  authorities, test bodies and fixture inputs are byte-exact.
- Pinned Gitleaks v8.30.1 staged-added/new-file boundary: zero findings. Ordinary
  complete Tier A final-diff review found no remaining misleading new claims
  or unintended executable changes. ACK/profile and safe-wrapper commit hygiene
  are the delivery step, not runtime or hosted-CI qualification.

All exact task-owned Go caches, helpers, renders, dependency copy and secret
snapshots were removed after their bounded sessions, including failure cleanup.
No live database/broker, migration workspace or public consumer was exercised.
The unanswered rationale and existing limitations above remain explicit; this
pass neither repairs them nor represents stronger protocol/delivery guarantees.

## Objective Enforcement

The checksum-pinned public Tools v2.4.0 verifier now supplies
`make source-docs-check` as a prerequisite of the existing local and CI Make
routes. Ordinary shared CI selects strict source-documentation checking and
requires its result. Native prerequisites, operation budgets, security and
release controls are retained. Hosted exact-head evidence is recorded by the
coordination ledger; configuration alone does not establish green delivery.
