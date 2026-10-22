# Adapter Path Migration

[Documentation map](index.md) · [Repository README](../README.md)
[Next: Quickstart](quickstart.md)

The Kafka and RabbitMQ Streams adapters use target-oriented module paths. The
former legacy modules are no longer maintained and their sources are removed
from this repository; migrate to the successors.

| Target | Module | Default package | Removed module | Legacy package |
|---|---|---|---|---|
| Kafka | `github.com/faustbrian/go-transactional-outbox/adapters/kafka/v3` | `outboxkafka` | `github.com/faustbrian/go-transactional-outbox/adapters/gokafka/v2` | `gokafka` |
| RabbitMQ Streams | `github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream/v2` | `outboxrabbitstream` | `github.com/faustbrian/go-transactional-outbox/adapters/gorabbitstream/v2` | `gorabbitstream` |

## Source migration

### Root v2 migration

Root v2 requires Go 1.27.0; published root v1.0.0 supported Go 1.26.6. Change
root imports to `github.com/faustbrian/go-transactional-outbox/v2`, with
`postgres` and `relay` following that suffix. Package names, envelope encoding,
database migrations, publisher semantics, and error categories are unchanged
by this module identity change; no database migration is required solely to
change imports.

The adapter v2 releases consume root v2. Change each application's root, relay
and adapter imports together. Existing adapter v1 releases still accept only
root-v1 types and cannot satisfy v2 relay interfaces. The queue adapter moved to
`adapters/queue/v3` on `go-queue/v2`; its `Queue` interface takes go-queue v2
`core` and `job` types. The Kafka adapter moved to `adapters/kafka/v3`
on `go-kafka/v2`; its `Client` and `Limits` take go-kafka v2 types.

For integrations in CloudEvents, Event Sourcing, Idempotency, Service,
Webhook, or library-tools, select a release or maintained composition that
explicitly consumes root v2. An older v1 integration does not certify root v2
compatibility merely because this repository has published a new major.

### Moving from the removed modules

Change the module import path. The Kafka successor is `adapters/kafka/v3`,
which takes `go-kafka/v2` types, so move go-kafka to `/v2` in the same change
(see the go-kafka migration guide). To avoid changing existing selectors,
alias the successor import:

```go
import gokafka "github.com/faustbrian/go-transactional-outbox/adapters/kafka/v3"
import gorabbitstream "github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream/v2"
```

Constructors, configuration, mapping, classification, cancellation,
ownership, concurrency, and at-least-once delivery behavior are preserved.
The successors keep the established `outbox/gokafka` and
`outbox/gorabbitstream` diagnostic strings so dashboards and alert routing do
not change merely because an import path changes.

## Support for the removed modules

The removed `adapters/gokafka/v2` and `adapters/gorabbitstream/v2` modules
remain available at their last published releases. They receive security fixes
only through the interval their deprecation notices announced: the longer of
180 days after the successors were published and two later stable minor
releases. Migrate to the successors; no new features ship for the removed
paths.
