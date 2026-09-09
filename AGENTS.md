# Engineering Policy

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and
"OPTIONAL" in this document are to be interpreted as described in BCP 14
[RFC2119] [RFC8174] when, and only when, they appear in all capitals, as
shown here.

## Scope And Authority

- This file is the canonical policy for the complete repository.
- Package policies MAY add stricter domain rules but MUST NOT weaken this file.
- `CLAUDE.md` and tool-specific files MUST point here rather than duplicate it.
- Historical implementation plans belong in repository history or issue
  tracking, not in the released source tree. Current checks MUST pass.

## Repository Structure

- The public root module MUST live at the repository root.
- Intentional optional or test modules MAY live in explicit nested directories.
- Commands MUST live under `cmd/`; private shared code MUST live under
  `internal/`; root automation MUST live under `scripts/`.
- Public module paths MUST match their repository-relative directories beneath
  the module path declared by the root `go.mod`.
- Every module MUST be declared in `modules.json`, and every package MUST be
  declared in `packages.json`.
- Independently releasable modules MUST retain independent `go.mod` files and
  directory-prefixed semantic-version tags.
- Cross-module dependencies MUST remain acyclic and MUST use public contracts.
- Permanent `replace` directives, sibling repositories, and absolute developer
  paths are forbidden in releasable modules.

## Design

- Prefer standard-library interfaces and explicit composition over hidden
  registration, global state, reflection-driven wiring, or service locators.
- Public APIs MUST make ownership, cancellation, retries, timeouts, resource
  limits, error semantics, and concurrency behavior observable.
- Interfaces SHOULD be defined by consumers and MUST remain narrowly scoped.
- Optional integrations SHOULD be adapters or nested modules, not mandatory
  dependencies of a core package.
- Breaking protocol or specification ambiguities MUST be documented as explicit
  decisions and covered by tests.

## Safety And Concurrency

- Shared mutable state MUST have one documented synchronization owner.
- Goroutines MUST have explicit lifetime, cancellation, shutdown, and leak
  tests. Fire-and-forget goroutines are forbidden.
- Channels MUST have documented ownership and closure rules.
- Locks MUST NOT be held across caller callbacks, network IO, blocking channel
  operations, or unbounded work.
- Every external operation MUST accept or derive a bounded `context.Context`.
- Response bodies, files, rows, transactions, timers, tickers, connections,
  and temporary resources MUST be closed on every path.
- Integer conversions, sizes, offsets, recursion, decompression, and allocation
  from untrusted input MUST be bounded before allocation or conversion.
- Secrets and credentials MUST NOT appear in errors, logs, traces, snapshots,
  fixtures, mutation reports, or generated artifacts.

## Proportional Assurance

- Changes MUST be classified by their actual risk before verification.
- **Tier A** covers documentation, metadata, registration, and generated
  documentation without runtime behavior. Validate the affected structure,
  links, examples, or generation and inspect the final diff.
- **Tier B** covers internal behavior without a public contract change. Run a
  focused behavior test, affected package or module tests, applicable format
  and static checks, and one complete review. A bounded repository gate SHOULD
  run; unrelated expensive checks MAY remain scheduled.
- **Tier C** covers public APIs, lifecycle, security, persistence, and
  concurrency. Require an observable regression or characterization test,
  focused behavior, API compatibility where applicable, directly affected
  package and integration tests, direct owned reverse consumers, and one
  independent complete-diff review.
- **Tier D** covers public releases and ecosystem milestones. Bind immutable
  release or milestone inputs once and run only the relevant compatibility,
  composition, consumer, and aggregate checks.
- Tests MUST assert outcomes, invariants, errors, cleanup, and state transitions;
  line execution without behavioral assertions is not acceptable evidence.
- Race, fuzz, mutation, leak, performance, conformance, external-service,
  clean-consumer, release-rehearsal, and aggregate fleet checks MUST run only
  when they exercise a material risk or the applicable Tier D boundary. They
  MUST NOT block an unrelated change merely because the check exists.
- Additional independent reviews beyond the default one MUST each cover a
  distinct named high-risk domain. Fixed review counts and cryptographically
  bound review records are prohibited as routine requirements.

## Commands And Evidence

- `make inventory` validates repository and package manifests.
- `make check` provides the full available repository gate; it is not required
  for a change whose proportional assurance is satisfied by narrower checks.
- `make ci` provides the complete CI contract for selected modules.
- Evidence for unchanged immutable inputs SHOULD be reused. Mutable progress
  ledgers, plans, review notes, prose changes, and evidence already bound to an
  immutable commit and CI run MUST NOT require new hashes or recursive
  provenance.
- After a change, rerun only the gates, modules, packages, and direct reverse
  consumers whose behavior-affecting inputs changed.
- Missing or ambiguous evidence MUST be rerun only at the affected boundary;
  it MUST NOT trigger an unrelated repository-wide restart.
- Task-owned execution output, caches, temporary directories, containers,
  images, and volumes MUST be removed after their evidence is captured,
  including after failure or interruption.

## CI And Workflows

- `.github/workflows/ci.yml` is the only owned GitHub Actions workflow.
- Package-local workflows MUST NOT be added.
- Actions and external tools MUST be pinned to immutable versions.
- Every selected module MUST have an attributable result and evidence artifact.
- The stable required job MUST fail for failed, cancelled, skipped, or missing
  module results.
- Required checks MUST NOT use `continue-on-error`, `|| true`, permissive
  thresholds, or warning substitutions.

## Dependencies And Supply Chain

- Dependencies MUST be necessary, maintained, license-compatible, and pinned to
  reviewed current versions.
- Standard-library functionality MUST NOT be wrapped merely to create an owned
  abstraction; wrappers require a stable policy or portability boundary.
- Generated code and vendored corpora MUST record source, version, checksum,
  license, generation command, and update procedure.
- Vulnerability, secret, and license checks are release gates when applicable
  to the released dependency surface.
- Public releases MUST publish the repository's required checksums, SBOM, and
  provenance. Clean-consumer checks are required when a public package or
  dependency identity changes.

## Documentation

- Public identifiers MUST have useful Go documentation describing semantics,
  invariants, ownership, errors, concurrency, and caveats where relevant.
- Comments MUST explain why a constraint or non-obvious implementation exists;
  they MUST NOT narrate obvious syntax.
- Every public module MUST provide a quick start, API reference, examples,
  guidance on when to use it, explicit limitations, security notes, FAQ, and
  release notes.
- The root README MUST remain a concise entry point. Detailed guides,
  operations, audits, and maintainer material belong under `docs/` and MUST be
  linked through `docs/README.md`.
- Documentation and examples MUST compile and be checked in CI.

## Changelogs

- Every user-visible change MUST update the affected module `CHANGELOG.md` in
  the same commit.
- Entries MUST describe behavior and migration impact, not internal activity.
- Changes to multiple modules MUST update every affected changelog.
- Unreleased entries MUST NOT be silently rewritten or removed.
- Generated, dependency, security, compatibility, and deprecation changes are
  user-visible and require entries.

## Completion

- Run the proportional affected gates during development and the applicable
  release gates before declaring completion.
- Re-run affected gates after the final source, test, dependency, documentation,
  workflow, or generated-file change.
- Report exact commands and results. A skipped, blocked, stale, or warning-only
  gate is not a pass.

[RFC2119]: https://www.rfc-editor.org/rfc/rfc2119
[RFC8174]: https://www.rfc-editor.org/rfc/rfc8174
