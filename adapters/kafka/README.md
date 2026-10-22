# Outbox Kafka adapter

[Documentation map](../../docs/index.md) · [Repository README](../../README.md)
[Next: Documentation map](../../docs/index.md)

`outboxkafka` is the canonical synchronous adapter from `outbox.Envelope` to the
first-party `kafka.Producer`. It maps one persisted envelope to one Kafka
record and returns only after the producer reports the broker delivery result.
It owns no worker, retry loop, transaction, topic, or producer lifecycle.

## Install

Install the v3 module, built on `go-kafka/v2`:

```sh
go get github.com/faustbrian/go-transactional-outbox/adapters/kafka/v3@v3.0.0
```

## Quick start

```go
producer, err := kafka.NewProducer(kafka.ProducerConfig{
	Brokers:       brokers,
	ClientID:      "billing-outbox",
	AllowedTopics: []string{"billing.events.v1"},
	Limits:        outboxkafka.DefaultLimits().Kafka,
	Security:      kafka.DevelopmentPlaintextSecurity(), // development only
})
if err != nil {
	return err
}
defer producer.Close()

publisher, err := outboxkafka.New(producer)
if err != nil {
	return err
}
relayConfig.ClassifyError = outboxkafka.ClassifyError
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
- [Go API reference](https://pkg.go.dev/github.com/faustbrian/go-transactional-outbox/adapters/kafka/v3)
- [Compiled example](example_test.go)
- [Troubleshooting](../../docs/troubleshooting.md)
- [Parent package documentation](../../docs/README.md)
- [Support policy](../../SUPPORT.md)
- [Security reporting](../../SECURITY.md)

## Compatibility and support

This module requires Go 1.27.0 and follows Semantic Versioning. It is the
target-oriented successor to the removed `adapters/gokafka`. Callers change the
import path, move go-kafka to `/v2` (the v3 `Client` and `Limits` take go-kafka
v2 types), and either use the default `outboxkafka` qualifier
or preserve the old qualifier with an explicit import alias. The modules are
independent copies: exported sentinels and concrete/reflection identities do
not compare equal across the two paths. See the
[migration guide](../../docs/adapter-migration.md).

Report vulnerabilities through the [parent security policy](../../SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).

Shared adapter, ownership, and lifecycle expectations are in the versioned
[Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.8.4/docs/ecosystem/README.md)
and its [Persistence and durability family](https://github.com/faustbrian/go-library-tools/blob/v1.8.4/docs/ecosystem/design-language.md#package-families-and-selection).
