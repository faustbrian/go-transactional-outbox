# Outbox RabbitMQ Streams adapter

`outboxrabbitstream` maps one persisted `outbox.Envelope` to one confirmed
`rabbitstream.Message`. It owns no producer, topology, relay, retry loop,
database transaction, or outbox state transition.

This release-candidate module requires Go 1.26.6 and is not yet published.

## Install

After `adapters/rabbitstream/v1.0.0` is released, install the exact version:

```sh
go get github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream@v1.0.0
```

## Quick start

```go
producer, err := rabbitmq.OpenProducer(ctx, connection, rabbitstream.ProducerConfig{
    Stream: "billing.events",
})
if err != nil {
    return err
}

publisher, err := outboxrabbitstream.New(producer, outboxrabbitstream.Config{
    Stream: "billing.events",
})
if err != nil {
    return err
}

worker, err := relay.New(store, publisher, relay.Config{
    Owner: "billing-outbox-1",
    ClassifyError: outboxrabbitstream.ClassifyError,
})
```

The caller creates and closes the producer. Production topology remains
operator-owned.

## Construction, ownership, and concurrency

`New` validates the client, target, and message limits without opening a
connection. A zero `Config.Limits` selects `rabbitstream.DefaultLimits`; other
configuration remains explicit. `Publisher` is immutable after construction
and safe for concurrent use only when the supplied `Client` is safe. The caller
owns that client, operation contexts, connection lifecycle, and shutdown. The
adapter starts no goroutines and retains no request context.

## Wire mapping

| Envelope field | Stream message field |
| --- | --- |
| `Topic` | configured Stream or Super Stream; it must match exactly |
| `Payload` | opaque copied payload bytes |
| `ID` | AMQP message ID |
| `OrderingKey`, then `IdempotencyKey`, then `ID` | routing key |
| `PayloadVersion` | `schema-version` application property |
| `IdempotencyKey` | optional `idempotency-key` application property |
| `CreatedAt` | creation timestamp |
| `Metadata["es.content_type"]` | content type, default `application/json` |
| `Metadata["correlation-id"]` | correlation ID |
| `traceparent` and `tracestate` metadata | W3C message annotations |
| remaining metadata | sorted application properties |

The adapter copies every mutable value before client admission and enforces the
configured root message limits. `schema-version`, `idempotency-key`, and
`content-type` metadata are reserved because the adapter owns those fields.

## Confirmation, retries, and duplicates

A nil `Publish` error means the first-party client returned
`DeliveryConfirmed`. Memory admission alone is never success. Rejection,
ambiguity, timeout, connection loss, and malformed client results remain
errors.

RabbitMQ confirmation and the database `MarkDelivered` transition are separate
effects. A process failure after confirmation but before the database commit
causes a safe at-least-once retry and may publish a duplicate. Consumers must
durably deduplicate the stable event ID with the business side effect.

The adapter does not derive a RabbitMQ publishing ID from the string event ID.
Broker deduplication requires an application-owned stable producer name and a
durable monotonic publishing-ID sequence; it is optional and never replaces
application idempotency.

`ClassifyError` treats local validation, oversized messages, invalid producer
configuration, and broker rejection as permanent. Cancellation, authorization,
connection loss, timeouts, unconfirmed or ambiguous outcomes, fatal producer
state, and contained client panics remain transient because replacing the
runtime or credentials can make the same durable envelope publishable. Retrying
an ambiguous outcome can duplicate an accepted event.

## Ordering and Super Streams

One adapter targets exactly one Stream or Super Stream. The stable routing key
preserves per-aggregate routing. A Super Stream provides order only within its
selected backing stream; topology changes can change routing and there is no
global partition order. Concurrent callers must preserve their own submission
order for one aggregate.

## Performance and operational limits

`BenchmarkPublisherMappingAndConfirmation` uses an in-memory client and
measures envelope validation, deterministic mapping, defensive copying, and
one synchronous client call. It excludes broker acknowledgement latency,
network I/O, producer buffering, and topology operations. Payload and metadata
size therefore affect mapping allocations, while real throughput is bounded by
the caller-owned producer and broker. Synchronous publication provides relay
backpressure instead of creating an adapter-owned queue.

## When to use this adapter

Use this adapter for retained event distribution from a transactional outbox.
Use the existing queue adapter for executable jobs, delayed work, or competing
workers. Do not hide both behind one generic messaging API.

The adapter intentionally has no topology administration, replay, consumer,
health, telemetry-provider, transaction, domain-envelope, JSON-schema, or
cross-system exactly-once API. Use the root inspection/replay contracts and
nested RabbitMQ/OpenTelemetry adapters for those responsibilities.

## Security

Returned adapter diagnostics are fixed and omit payload, metadata, routing
keys, endpoints, credentials, and panic values. Wrapped causes remain available
through `errors.Is` and `errors.As`; do not unwrap unknown client errors into
untrusted logs. Treat all event metadata as untrusted input.

## FAQ

### Does confirmation mark the outbox row delivered?

No. The relay performs that separate database transition.

### Does this provide exactly-once delivery?

No. It provides confirmed at-least-once publication with a stable identity for
application deduplication.

### Can one publisher route arbitrary topics?

No. One publisher accepts one exact configured Stream or Super Stream target.

## Documentation and support

- [Documentation index](docs/README.md)
- [Go API reference](https://pkg.go.dev/github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream)
- [Compiled example](example_test.go)
- [Troubleshooting](../../docs/troubleshooting.md)
- [Parent package documentation](../../docs/README.md)
- [Support policy](../../SUPPORT.md)
- [Security reporting](../../SECURITY.md)

See [CHANGELOG.md](CHANGELOG.md) and [LICENSE](LICENSE).

See the [root package documentation](../../README.md) for delivery semantics,
adapter boundaries, and related packages.

Shared adapter, ownership, and lifecycle expectations are in the versioned
[Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/README.md)
and its [Persistence and durability family](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/design-language.md#package-families-and-selection).

This module is the target-oriented successor to `adapters/gorabbitstream`.
Existing callers can change only the import path and either use the default
`outboxrabbitstream` qualifier or preserve the old qualifier with an explicit
import alias. The modules are independent copies: exported sentinels and
concrete/reflection identities do not compare equal across the two paths. See
the [migration guide](../../docs/adapter-migration.md).
