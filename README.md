# outbox

[![CI](https://github.com/faustbrian/go-transactional-outbox/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/faustbrian/go-transactional-outbox/actions/workflows/ci.yml)
[![Documentation](https://img.shields.io/badge/docs-adoption_and_API-blue)](docs/index.md)
[![Go Reference](https://pkg.go.dev/badge/github.com/faustbrian/go-transactional-outbox/v2.svg)](https://pkg.go.dev/github.com/faustbrian/go-transactional-outbox/v2)
[![Release](https://img.shields.io/github/v/release/faustbrian/go-transactional-outbox?sort=semver)](https://github.com/faustbrian/go-transactional-outbox/releases)
[![Go](https://img.shields.io/badge/go-1.27.0-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`outbox` is a PostgreSQL-first Go implementation of the transactional
outbox pattern. It writes application state and publishable envelopes in the
same caller-owned `pgx` transaction, then relays committed envelopes to a
small publisher contract with at-least-once delivery.

The root v2 API requires Go 1.27 and follows the compatibility surfaces described in
[the compatibility policy](docs/compatibility.md). Delivery remains at
least once; upgrading the library does not remove the consumer's idempotency
duty.

## Adoption entry points

The root module is `github.com/faustbrian/go-transactional-outbox/v2` (stable).
It requires Go 1.27.0. Pin the published `v2.0.1` root tag
with `go get github.com/faustbrian/go-transactional-outbox/v2@v2.0.1`.
Install only the modules your application imports; nested module status and
migration boundaries remain documented separately.

Start with the [documentation map](docs/index.md) for package selection,
examples, operational constraints, and contributor references.

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

- `github.com/faustbrian/go-transactional-outbox/v2`: envelope construction and validation.
- `github.com/faustbrian/go-transactional-outbox/v2/postgres`: migrations, transactional writer,
  claims, leases, retries, dead letters, replay, and retention.
- `github.com/faustbrian/go-transactional-outbox/v2/relay`: bounded embedded relay.
- `github.com/faustbrian/go-transactional-outbox/adapters/kafka/v2`: preferred,
  separately versioned Kafka publisher adapter.
- `github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream/v2`: preferred,
  separately versioned RabbitMQ Streams publisher adapter.
- `github.com/faustbrian/go-transactional-outbox/adapters/queue/v3`: separately versioned
  `queue` publisher adapter; importing core does not add `queue`.
- `github.com/faustbrian/go-transactional-outbox/adapters/otel/v2`: separately versioned
  metrics and trace-linkage integration compatible with `telemetry`.

The adapter v2 releases accept root-v2 contracts. The former
`adapters/gokafka/v2` and `adapters/gorabbitstream/v2` compatibility modules
are no longer maintained; use `adapters/kafka/v2` and `adapters/rabbitstream/v2`.
Existing v1 releases remain separate
and cannot accept root-v2 envelopes. See the
[adapter migration guide](docs/adapter-migration.md).

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

Use Go 1.27.2 and the verified public Tools v2.1.1 binary; module minima remain
Go 1.27.0. See [contributing](CONTRIBUTING.md#verification) for installation,
standalone tests, seed versus active fuzzing and optional focused mutation.

```sh
make check
make check-full
make recovery POSTGRES_VERSION=18
make migration-integration POSTGRES_VERSION=18
```

`make check` selects bounded checks and the cheap documentation policy;
`make check-full` selects the original full campaign. Selected relay assertions
use fake stores, not live SQL/broker proof. All-module integration retains
database/broker services and tags. PostgreSQL tests use ephemeral Testcontainers
instances. They do
not use an existing application or production database. The migration gate
uses `GO_MIGRATIONS_DIR` when the sibling checkout is not at
`../go-migrations`; this is explicit source-composition qualification, not an
ordinary standalone prerequisite. Scheduled CI is bounded, not a full matrix.

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
the versioned [Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.8.4/docs/ecosystem/README.md)
and its [Persistence and durability family](https://github.com/faustbrian/go-library-tools/blob/v1.8.4/docs/ecosystem/design-language.md#package-families-and-selection).
