# Changelog

All notable changes to this module are documented here.

## Unreleased

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
