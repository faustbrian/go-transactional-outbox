# Changelog

All notable changes to this module are documented here.

## [2.1.1] - 2026-11-26

### Security

- Require `github.com/klauspost/compress` v1.20.2 (from v1.19.1; GO-2026-6865),
  `golang.org/x/crypto` v0.59.0 (from v0.55.0; GO-2026-5932, GO-2026-6354 and
  GO-2026-6355) and `golang.org/x/net` v0.62.0 (from v0.57.0; GO-2026-6603,
  GO-2026-6610, GO-2026-6611, GO-2026-6612, GO-2026-6617, GO-2026-6860 and
  GO-2026-6866). The vulnerable versions were reachable only from tests.

## [2.0.1] - 2026-10-09

### Changed

- Use Go 1.27.2 for development and CI with compiler-compatible source
  tooling, retaining the public Go 1.27.0 minimum. Rebuild applications
  with a patched toolchain to receive standard-library security fixes.
- Refresh dependency selections without changing the adapter public API,
  publication semantics, module identity, or Go 1.27.0 minimum.

## Unreleased

### Fixed

- Admit payload and mapped metadata budgets before copying envelope data into
  a stream message, preserving owned accepted messages and confirmations.

### Changed

- Clarify mapped input ownership, routing and confirmed-publication
  boundaries without extending client lifecycle or delivery guarantees.
- Prepare the adapter as `/v2` for v2 outbox envelopes and relay types. Upgrade
  this adapter and the root together; Go 1.27 is required.
- Verify broker interoperability through the target-oriented
  `go-rabbitmq-streams/adapters/rabbitmq` transport adapter while preserving
  the outbox publisher contract.

## 1.0.0 - 2026-09-05

### Added

- Add the target-oriented RabbitMQ Streams adapter module with the complete
  bounded confirmed-publication, error-classification, ownership, integration,
  fuzz, benchmark, and operational documentation contracts from the stable
  legacy adapter.
- Preserve existing low-cardinality `outbox/gorabbitstream` diagnostics so
  changing the import path does not change logs or alert classification.
- Document migration from `adapters/gorabbitstream`, including
  selector-preserving import aliases and the intentionally distinct identities
  of independently implemented module paths.
