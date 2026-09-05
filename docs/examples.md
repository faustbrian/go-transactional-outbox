# Examples And Integrations

## pgx and sqlc

Begin one `pgx.Tx`, bind sqlc queries with `WithTx(tx)`, and pass that exact
transaction to `Writer.Insert`. Commit only after both writes succeed. The
[quickstart](quickstart.md) contains the complete pattern. Queries still bound
to the pool break atomicity.

## queue

```go
queuePublisher, err := outboxqueue.New(queue)
worker, err := relay.New(store, queuePublisher, relay.Config{Owner: hostname})
```

The adapter sends canonical envelope JSON. Nil means broker acceptance, not
consumer completion; a later delivered-state failure can publish it again.
The compiled adapter example wires this publisher into a relay and runs in the
adapter test suite.

## Kafka and RabbitMQ Streams

Use [`adapters/kafka`](../adapters/kafka/README.md) for synchronous confirmed
Kafka records and [`adapters/rabbitstream`](../adapters/rabbitstream/README.md)
for synchronous confirmed Stream or Super Stream messages. Both adapters
preserve the envelope's stable delivery identity, add no retry loop, and leave
producer lifecycle and topology with the caller. Their compiled examples are
in the independently versioned adapter modules.

## idempotency consumers

Use envelope ID or a stable application key as delivery identity and
fingerprint canonical envelope bytes. Treat `OutcomeReplayed` as
acknowledgement, `OutcomeInProgress` as retryable, conflicts as incidents, and
storage failure as fail-closed. Commit business effects and deduplication in
one transaction or use a fencing invariant. If producing another outbox
message, pass the same `pgx.Tx` to the business write, `outbox`, and
`idempotency` `CompleteTx`. None of these measures changes relay delivery
from at least once. See the [complete integration guide](idempotency.md).

The root executable example deliberately delivers one envelope twice and
asserts that the consumer applies its effect once by stable envelope ID.
