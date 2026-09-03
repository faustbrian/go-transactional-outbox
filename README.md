# outbox

[![CI](https://github.com/faustbrian/go-transactional-outbox/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/faustbrian/go-transactional-outbox/actions/workflows/ci.yml)
[![CodeQL](https://img.shields.io/badge/CodeQL-required-blue)](https://github.com/faustbrian/go-transactional-outbox/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-100%25_required-blue)](CONTRIBUTING.md#verification)
[![Mutation](https://img.shields.io/badge/mutation-100%25_required-blue)](CONTRIBUTING.md#verification)
[![Documentation](https://img.shields.io/badge/docs-checked_in_CI-blue)](docs/)
[![Go Reference](https://pkg.go.dev/badge/github.com/faustbrian/go-transactional-outbox.svg)](https://pkg.go.dev/github.com/faustbrian/go-transactional-outbox)
[![Release](https://img.shields.io/github/v/release/faustbrian/go-transactional-outbox?sort=semver)](https://github.com/faustbrian/go-transactional-outbox/releases)
[![Go](https://img.shields.io/badge/go-1.26.6-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`outbox` is a PostgreSQL-first Go implementation of the transactional
outbox pattern. It writes application state and publishable envelopes in the
same caller-owned `pgx` transaction, then relays committed envelopes to a
small publisher contract with at-least-once delivery.

The stable v1 API follows the compatibility surfaces described in
[the compatibility policy](docs/compatibility.md). Delivery remains at
least once; upgrading the library does not remove the consumer's idempotency
duty.

## Guarantees

- Atomic application and outbox persistence only when both writes use the
  same successful `pgx.Tx`.
- At-least-once relay delivery. Publisher acceptance followed by a failed or
  ambiguous delivered update can publish the same envelope again.
- Concurrent claims use PostgreSQL row locks and `SKIP LOCKED`.
- Every mutation of a leased record requires its current opaque lease token.
- Batch, worker, lease, retry, administrative, payload, and polling limits are
  explicit and bounded.
- Optional ordering-key or topic serialization is enforced at the PostgreSQL
  claim seam across relay processes. There is no global ordering guarantee.

Consumers **must be idempotent**. This project does not provide distributed
transactions or exactly-once delivery.

## Packages

- `github.com/faustbrian/go-transactional-outbox`: envelope construction and validation.
- `github.com/faustbrian/go-transactional-outbox/postgres`: migrations, transactional writer,
  claims, leases, retries, dead letters, replay, and retention.
- `github.com/faustbrian/go-transactional-outbox/relay`: bounded embedded relay.
- `github.com/faustbrian/go-transactional-outbox/adapters/queue`: separately versioned
  `queue` publisher adapter; importing core does not add `queue`.
- `github.com/faustbrian/go-transactional-outbox/adapters/otel`: separately versioned
  metrics and trace-linkage integration compatible with `telemetry`.

## Quick start

```go
builder, err := outbox.NewEnvelopeBuilder()
if err != nil {
    return err
}

envelope, err := builder.Build(outbox.NewEnvelopeParams{
    Topic:          "orders.created",
    Payload:        payload,
    OrderingKey:    customerID,
    IdempotencyKey: commandID,
})
if err != nil {
    return err
}

tx, err := pool.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback(ctx)

if _, err := tx.Exec(ctx, insertOrderSQL, orderID); err != nil {
    return err
}
writer, err := postgres.NewWriter(postgres.WriterConfig{})
if err != nil {
    return err
}
if err := writer.Insert(ctx, tx, envelope); err != nil {
    return err
}

return tx.Commit(ctx)
```

The writer never opens or commits a transaction. Passing a pool or standalone
connection is impossible because the API requires `pgx.Tx`.

See the [documentation index](docs/README.md),
[full quickstart](docs/quickstart.md), [delivery guarantees](docs/guarantees.md),
and [architecture and crash matrix](docs/architecture.md).

## Development gates

```sh
make check
make recovery POSTGRES_VERSION=18
make migration-integration POSTGRES_VERSION=18
```

Integration tests use ephemeral Testcontainers PostgreSQL instances. They do
not use an existing application or production database. The migration gate
uses `GO_MIGRATIONS_DIR` when the sibling checkout is not at
`../migrations`.

## License

MIT. See [LICENSE](LICENSE).

Contribution, conduct, support, and vulnerability-reporting policies are in
[CONTRIBUTING.md](CONTRIBUTING.md),
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md), [SUPPORT.md](SUPPORT.md), and
[SECURITY.md](SECURITY.md).

## Documentation

Use the [documentation index](docs/README.md) for package-owned guides,
operational contracts, examples, and maintainer references.

Shared construction, ownership, lifecycle, and composition expectations are in
the versioned [Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.3.0/docs/ecosystem/README.md).
