# Changelog

All notable changes to this module are documented here.

## [2.0.1] - 2026-10-09

### Changed

- Refresh dependency selections without changing the adapter public API,
  publication semantics, module identity, or Go 1.27.0 minimum.

## Unreleased

### Fixed

- Admit payload and mapped metadata budgets before copying envelope data into
  a stream message, preserving owned accepted messages and confirmations.

### Changed

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
