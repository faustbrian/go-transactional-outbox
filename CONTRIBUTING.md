# Contributing

## Before Editing

1. Read [`AGENTS.md`](AGENTS.md) and the affected module's current docs.
2. Run `make inventory` and the narrow baseline gate for the module.
3. Identify owned dependencies and reverse dependants in `modules.json`.
4. Preserve unrelated work and generated/corpus provenance.

## Changes

Keep commits focused and conventional. Update every affected changelog with
the behavior and migration impact. Public API changes require compatibility
evidence and documentation. Specification behavior requires a decision record,
fixture coverage, and interoperability evidence.

New direct dependencies and dependency updates must follow the
[dependency governance policy](AGENTS.md#dependencies-and-supply-chain). Package-local
update bots are forbidden; the root policy owns every module and action update.

Specification-backed changes must follow the
[specification governance contract](AGENTS.md#design), update
the affected stable decision entries, and complete the Specification Decisions
section of the pull request template. An unresolved interpretation or stale
source pin is release-blocking; peer behavior cannot silently select policy.

Required release mutation gates must finish with zero surviving viable mutants.

Do not add package-local workflows, permanent replacements, machine-specific
paths, bypass flags, broad mutation exclusions, or aggregate quality metrics
that hide a failing package.

## Verification

Use Go 1.27.2 for verification; public module minima remain Go 1.27.0.
An isolated checkout needs no sibling repositories or coordination goal files.
Begin with deterministic root tests and one active target when its envelope
boundary changes, using a disposable cache for each bounded session:

```bash
(
  set -eu
  GOCACHE="$(mktemp -d)"
  export GOCACHE
  trap 'rm -rf "${GOCACHE}"' EXIT
  GOWORK=off go test ./...
  GOWORK=off go test . -run '^$' -fuzz '^FuzzEnvelopeBuilder$' -fuzztime=2s -timeout=2m
)
```

The deterministic command executes fuzz seeds/corpus, not active fuzzing.
It excludes intentional nested modules and the `integration` tag; it is not
database or broker qualification. Install the public v2.4.0 binary archive for
your platform from
[go-library-tools releases](https://github.com/faustbrian/go-library-tools/releases/tag/v2.4.0),
checking `checksums.txt` against `.golib.yaml` and the archive against that
checksum set. Put the extracted `golib` binary on `PATH`, or select it through
`GOLIB`; do not build Tools from source. CI verifies this same public release
while checking out its pinned reusable workflow source.

```bash
make inventory
make check
make ci
make specification-check
golib mutation --module . --package ./relay
```

Run adjacent commands in one disposable-cache session as shown above.
`make check` and `make ci` select the bounded contract and the owned cheap
documentation policy. `scripts/check-docs.sh --local` preserves required-file
presence and false exactly-once claim refusal; only Go package/doc enumeration
is deferred to the unchanged no-argument full mode. The credential-free,
two-minute Documentation policy CI job and shared CI must both succeed for
Required. Shared safety checks retain unsafe/cgo/go:linkname refusal.
The relay race runs once with a two-minute timeout and proves lease-renewal
failure cancels publication without committing a delivery/retry/dead/release
transition, while cancellation releases the owned claim. These fake-store
assertions do not qualify live SQL locking or broker acknowledgements.
Base-free checks run seeds/corpus; changed-source CI selects causal active
fuzz smoke. Optional mutation selects one production package, one worker and
a twenty-minute limit. All-module checks preserve PostgreSQL, RabbitMQ Stream,
Redis and Valkey services and integration tags, including owned Kafka fixtures;
they are not Docker- or service-free. Focused modules use, for example,
`golib check --local --module adapters/otel` from this repository root.

Select full qualification and release rehearsal explicitly:

```bash
make check-full
./scripts/check-docs.sh
make specification-check-online
make recovery POSTGRES_VERSION=18
make migration-integration POSTGRES_VERSION=18
golib api check
golib release dry-run --module .
```

Full checks retain all 11 declared fuzz targets at 100x each with their original
two- or five-minute caps, seven 100ms/20m benchmark operations, mutation imports,
API baselines and all original module gates. Recovery uses owned ephemeral
fixtures; migration interoperability is an explicit source-composition check
using `GO_MIGRATIONS_DIR` (default `../go-migrations`), not an ordinary standalone
prerequisite. Independently released adapters select their own module path for
rehearsal. The scheduled carrier uses the shared bounded route, not an automatic
full or PostgreSQL-major/broker matrix. Offline specification checking is
ordinary; online authority/errata checks are explicit. Report unavailable or
failing commands; bounded checks alone do not establish release qualification.

## Adding A Module

Follow [repository structure policy](AGENTS.md#repository-structure). New modules
require an explicit purpose, ownership boundary, dependency review, package
catalog entry, full quality gates, documentation, changelog, license, security
policy, compatibility plan, and release dry-run.

## Source Documentation

Install the checksum-pinned public verifier declared in `.golib.yaml`.
`make source-docs-check` validates objective Go comment, marker, link and
generated-producer structure; `make check`, `make check-full` and `make ci`
require it before their existing checks. Shared CI runs the same strict
contract and includes its result in Required. This is structural enforcement,
not a substitute for source-truth review or behavioral verification.
