# Outbox OpenTelemetry adapter

`outboxotel` adds optional OpenTelemetry spans and metrics to
[`github.com/faustbrian/go-transactional-outbox`](../..). The core outbox module stays
independent of OpenTelemetry, and exporter lifecycle remains caller-owned.

## Install

```sh
go get github.com/faustbrian/go-transactional-outbox/adapters/otel@v1
```

## Quick start

```go
instrumentation, err := outboxotel.New(telemetryRuntime)
if err != nil {
    return err
}

publisher, err := instrumentation.WrapPublisher(kafkaPublisher)
if err != nil {
    return err
}

relay, err := outboxrelay.New(store, publisher, outboxrelay.Config{
    Observer: instrumentation,
})
```

The compiling examples in this module contain complete imports and setup.

## Guarantees and limitations

The [complete guide](docs/reference.md) defines ownership, failure semantics,
bounds, concurrency, security, and unsupported behavior. Do not infer
additional guarantees beyond the documented module boundary.

## Documentation

- [Documentation index](docs/README.md)
- [Complete technical guide](docs/reference.md)
- [Go API reference](https://pkg.go.dev/github.com/faustbrian/go-transactional-outbox/adapters/otel)
- [Parent package documentation](../../docs/README.md)

## Compatibility and support

This module follows Semantic Versioning. Report vulnerabilities through the
[parent security policy](../../SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).

Shared adapter, ownership, and lifecycle expectations are in the versioned
[Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/README.md)
and its [Persistence and durability family](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/design-language.md#package-families-and-selection).
