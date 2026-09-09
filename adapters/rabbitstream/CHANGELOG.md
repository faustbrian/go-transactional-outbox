# Changelog

All notable changes to this module are documented here.

## Unreleased

### Changed

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
