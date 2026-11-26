# Changelog

All notable changes to this module are documented here.

## Unreleased

## [3.0.1] - 2026-11-26

### Security

- Require klauspost/compress v1.20.2 to address GO-2026-6865.

- Require `golang.org/x/crypto` v0.59.0 (from v0.55.0; GO-2026-5932,
  GO-2026-6354 and GO-2026-6355), `golang.org/x/net` v0.62.0 (from v0.58.0;
  GO-2026-6603, GO-2026-6610, GO-2026-6611, GO-2026-6612, GO-2026-6617,
  GO-2026-6860 and GO-2026-6866) and OpenTelemetry v1.47.0 (from v1.45.0;
  GO-2026-6974). The vulnerable `golang.org/x/net` and OpenTelemetry versions
  were reachable only from tests. Production packages import
  `golang.org/x/crypto`, but govulncheck found no imported vulnerable package.

## [3.0.0] - 2026-10-22

### Changed

- Move to `github.com/faustbrian/go-transactional-outbox/adapters/kafka/v3`
  and build on `github.com/faustbrian/go-kafka/v2`. `Client` and `Limits` now
  use go-kafka v2 types; message mapping, headers, error classification and
  the `outbox/gokafka:` diagnostic prefix are unchanged.

## [2.0.1] - 2026-10-09

### Changed

- Use Go 1.27.2 for development and CI with compiler-compatible source
  tooling, retaining the public Go 1.27.0 minimum. Rebuild applications
  with a patched toolchain to receive standard-library security fixes.
- Refresh dependency selections without changing the adapter public API,
  publication semantics, module identity, or Go 1.27.0 minimum.

## Unreleased

### Changed

- Clarify borrowed producer lifetime, mapped-copy ownership, partition-key
  precedence, admission timing and acceptance/error privacy boundaries.
- Prepare the adapter as `/v2` for v2 outbox envelopes and relay types. Upgrade
  this adapter and the root together; Go 1.27 is required.

### Added

- Add the target-oriented Kafka adapter module with the complete synchronous
  publisher, bounded mapping, error classification, health, concurrency,
  integration, fuzz, benchmark, and operational documentation contracts from
  the stable legacy adapter.
- Preserve existing low-cardinality `outbox/gokafka` diagnostics so changing
  the import path does not change logs or alert classification.
- Document migration from `adapters/gokafka`, including selector-preserving
  import aliases and the intentionally distinct identities of independently
  implemented module paths.
