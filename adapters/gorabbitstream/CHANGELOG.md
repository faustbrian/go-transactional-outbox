# Changelog

All notable changes to this module are documented here.

## Unreleased

### Deprecated

- Deprecate this legacy module path in favor of the target-oriented
  `github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream`
  successor. Migration changes the import path; an explicit `gorabbitstream`
  import alias can preserve selectors. The modules intentionally retain
  distinct exported sentinel and concrete-type identities during the
  compatibility interval.

### Changed

- Adopt checksum-verified `go-library-tools` v1.4.0 W14 enforcement and resolve
  the RabbitMQ Streams and outbox dependencies against their immutable public
  releases.

### Documentation

- Link directly to the immutable v1.4.0 persistence-and-durability family
  guidance.
- Publish schema-v2 cohesion metadata and versioned Golib ecosystem
  navigation for the RabbitMQ Streams adapter.
- Add a module documentation index for direct navigation.
## 1.0.0 - 2026-08-25

### Changed

- Publish the module from its standalone `github.com/faustbrian/go-transactional-outbox/adapters/gorabbitstream` identity while preserving its documented API and behavior.

### Fixed

- Link the module README to the repository documentation portal.

### Added

- Add a bounded synchronous adapter from persisted outbox envelopes to
  confirmed RabbitMQ Stream or Super Stream messages.
- Preserve stable event, schema, correlation, trace, content-type, and routing
  identities while separating application identity from publishing IDs.
- Expose relay error classification that keeps ambiguous confirmation windows
  retryable and rejects definite invalid input permanently.

### Compatibility

- The stable v1 API is independently versioned.
