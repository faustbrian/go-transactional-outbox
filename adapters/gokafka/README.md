# Outbox Kafka adapter

`gokafka` is the canonical synchronous adapter from `outbox.Envelope` to the
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
- [Parent package documentation](../../docs/README.md)

## Compatibility and support

This module follows Semantic Versioning. Report vulnerabilities through the
[parent security policy](../../SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).

Shared adapter, ownership, and lifecycle expectations are in the versioned
[Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/README.md).
