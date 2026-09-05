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

The module paths are independent implementations, not Go aliases. Exported
sentinel errors and concrete or reflection-visible types have path-specific
identity. Do not mix values or compare sentinels across old and new paths;
migrate each application boundary as one coherent dependency change.

## Compatibility and release order

Publish `adapters/kafka/v1.0.0` and `adapters/rabbitstream/v1.0.0` first. Owned
consumers then change imports and run their complete applicable gates. Publish
the legacy deprecation patches as `adapters/gokafka/v1.0.1` and
`adapters/gorabbitstream/v1.0.1` only after both successor releases resolve
through the public proxy and checksum database.

Legacy modules remain supported for the longer of 180 days and two stable
minor releases. Removal requires an explicitly authorized v2 and evidence that
owned consumers and a clean public-consumer search no longer depend on the
legacy paths.
