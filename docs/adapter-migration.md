# Adapter Path Migration

The Kafka and RabbitMQ Streams adapters now use target-oriented module paths.
The published legacy modules remain available during a compatibility interval;
new adoption should use the successors.

| Target | Preferred module | Default package | Deprecated module | Legacy package |
|---|---|---|---|---|
| Kafka | `github.com/faustbrian/go-transactional-outbox/adapters/kafka` | `outboxkafka` | `github.com/faustbrian/go-transactional-outbox/adapters/gokafka` | `gokafka` |
| RabbitMQ Streams | `github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream` | `outboxrabbitstream` | `github.com/faustbrian/go-transactional-outbox/adapters/gorabbitstream` | `gorabbitstream` |

## Source migration

Change only the module import path. To avoid changing existing selectors,
alias the preferred import:

```go
import gokafka "github.com/faustbrian/go-transactional-outbox/adapters/kafka"
import gorabbitstream "github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream"
```

Constructors, configuration, mapping, classification, cancellation,
ownership, concurrency, and at-least-once delivery behavior are preserved.
The successor modules also preserve the established `outbox/gokafka` and
`outbox/gorabbitstream` diagnostic strings so dashboards and alert routing do
not change merely because an import path changes.

The RabbitMQ Streams legacy module is a compatibility facade over the
target-oriented successor. It retains distinct public types and sentinel
errors so applications may import both paths during migration, while runtime
mapping and publication delegate to the successor. The Kafka modules remain
independent implementations with path-specific sentinel and concrete-type
identities. Migrate each application boundary as one coherent dependency
change.

## Compatibility and release order

The preferred adapter releases are public. The RabbitMQ Streams compatibility
facade depends on the public successor release and the canonical
`go-rabbitmq-streams/adapters/rabbitmq` transport adapter. Release it only after
those dependencies resolve through the public proxy and checksum database.

Legacy modules remain supported for the longer of 180 days and two stable
minor releases. Removal requires an explicitly authorized v2 and evidence that
owned consumers and a clean public-consumer search no longer depend on the
legacy paths.
