# Changelog

All notable changes to this module are documented here.

## Unreleased

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
