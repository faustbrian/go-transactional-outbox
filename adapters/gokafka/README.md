# Outbox Kafka adapter

> **Deprecated:** use
> `github.com/faustbrian/go-transactional-outbox/adapters/kafka`. The
> target-oriented path removes the redundant `go` prefix. This compatibility
> module remains available for the longer of 180 days and two stable minor
> releases, and may be removed only in an authorized v2 after owned consumers
> and clean public-consumer checks have migrated.

`gokafka` is the released compatibility adapter from `outbox.Envelope` to the
first-party `kafka.Producer`. It maps one persisted envelope to one Kafka
record and returns only after the producer reports the broker delivery result.
It owns no worker, retry loop, transaction, topic, or producer lifecycle.

## Install

```sh
go get github.com/faustbrian/go-transactional-outbox/adapters/gokafka@v1
```

## Quick start

```go
producer, err := kafka.NewProducer(kafka.ProducerConfig{
	Brokers:       brokers,
	ClientID:      "billing-outbox",
	AllowedTopics: []string{"billing.events.v1"},
	Limits:        gokafka.DefaultLimits().Kafka,
	Security:      kafka.DevelopmentPlaintextSecurity(), // development only
})
if err != nil {
	return err
}
defer producer.Close()

publisher, err := gokafka.New(producer)
if err != nil {
	return err
}
relayConfig.ClassifyError = gokafka.ClassifyError
relay, err := outboxrelay.New(store, publisher, relayConfig)
```

The compiling examples in this module contain complete imports and setup.

## Guarantees and limitations

The [complete guide](docs/reference.md) defines ownership, failure semantics,
bounds, concurrency, security, and unsupported behavior. Do not infer
additional guarantees beyond the documented module boundary.

## Documentation

- [Documentation index](docs/README.md)
- [Complete technical guide](docs/reference.md)
- [Go API reference](https://pkg.go.dev/github.com/faustbrian/go-transactional-outbox/adapters/gokafka)
- [Compiled example](example_test.go)
- [Troubleshooting](../../docs/troubleshooting.md)
- [Parent package documentation](../../docs/README.md)
- [Support policy](../../SUPPORT.md)
- [Security reporting](../../SECURITY.md)

## Compatibility and support

This module requires Go 1.27.0 and follows Semantic Versioning. Report
vulnerabilities through the [parent security policy](../../SECURITY.md).

Migration changes the import path only. Callers may use the successor's
default `outboxkafka` qualifier or alias it as `gokafka`. The legacy and
successor modules contain independent implementations: their exported
sentinels and concrete/reflection identities are distinct and must not be
compared across paths. See the [migration guide](../../docs/adapter-migration.md).

## License

MIT. See [LICENSE](LICENSE).

Shared adapter, ownership, and lifecycle expectations are in the versioned
[Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/README.md)
and its [Persistence and durability family](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/design-language.md#package-families-and-selection).
