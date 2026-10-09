# Adapter Path Migration

The Kafka and RabbitMQ Streams adapters now use target-oriented module paths.
The published legacy modules remain available during a compatibility interval;
new adoption should use the successors.

| Target | Preferred module | Default package | Deprecated module | Legacy package |
|---|---|---|---|---|
| Kafka | `github.com/faustbrian/go-transactional-outbox/adapters/kafka/v2` | `outboxkafka` | `github.com/faustbrian/go-transactional-outbox/adapters/gokafka/v2` | `gokafka` |
| RabbitMQ Streams | `github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream/v2` | `outboxrabbitstream` | `github.com/faustbrian/go-transactional-outbox/adapters/gorabbitstream/v2` | `gorabbitstream` |

## Source migration

### Root v2 migration

Root v2 requires Go 1.27.0; published root v1.0.0 supported Go 1.26.6. Change
root imports to `github.com/faustbrian/go-transactional-outbox/v2`, with
`postgres` and `relay` following that suffix. Package names, envelope encoding,
database migrations, publisher semantics, and error categories are unchanged
by this module identity change; no database migration is required solely to
change imports.

All six adapter v2.0.0 releases are public and consume root v2, including
the deprecated GoRabbitStream facade. Change each application's root, relay
and adapter imports
together. Existing adapter v1 releases still accept only root-v1 types and
cannot satisfy v2 relay interfaces. A development workspace is not
public-consumer proof. Deprecated names are retained, not removed by the
root major upgrade.

For integrations in CloudEvents, Event Sourcing, Idempotency, Service,
Webhook, or library-tools, select a release or maintained composition that
explicitly consumes root v2. An older v1 integration does not certify root v2
compatibility merely because this repository has published a new major.

### Target-oriented adapter migration within v1

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
minor releases. Removal requires an explicitly authorized future major and evidence that
owned consumers and a clean public-consumer search no longer depend on the
legacy paths.
