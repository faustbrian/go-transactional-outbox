# Outbox Kafka adapter

`outboxkafka` is the canonical synchronous adapter from `outbox.Envelope` to the
first-party `kafka.Producer`. It maps one persisted envelope to one Kafka
record and returns only after the producer reports the broker delivery result.
It owns no worker, retry loop, transaction, topic, or producer lifecycle.

## Install

This successor is implemented but not yet published. After
`adapters/kafka/v1.0.0` is released, install the exact version:

```sh
go get github.com/faustbrian/go-transactional-outbox/adapters/kafka@v1.0.0
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
- [Go API reference](https://pkg.go.dev/github.com/faustbrian/go-transactional-outbox/adapters/kafka)
- [Compiled example](example_test.go)
- [Troubleshooting](../../docs/troubleshooting.md)
- [Parent package documentation](../../docs/README.md)
- [Support policy](../../SUPPORT.md)
- [Security reporting](../../SECURITY.md)

## Compatibility and support

This release-candidate module requires Go 1.26.6 and will follow Semantic
Versioning after publication. It is
the target-oriented successor to `adapters/gokafka`. Existing callers can
change only the import path and either use the default `outboxkafka` qualifier
or preserve the old qualifier with an explicit import alias. The modules are
independent copies: exported sentinels and concrete/reflection identities do
not compare equal across the two paths. See the
[migration guide](../../docs/adapter-migration.md).

Report vulnerabilities through the [parent security policy](../../SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).

Shared adapter, ownership, and lifecycle expectations are in the versioned
[Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/README.md)
and its [Persistence and durability family](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/design-language.md#package-families-and-selection).
