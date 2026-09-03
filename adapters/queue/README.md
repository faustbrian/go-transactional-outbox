# outbox queue adapter

`outboxqueue` is the canonical synchronous adapter from `outbox.Envelope` to the
first-party `queue` contract. It maps one persisted envelope to one owned JSON
task and returns only after the queue reports its enqueue result. It owns no
worker, retry loop, dead-letter policy, scheduler, transaction, or queue
lifecycle.

## Install

```sh
go get github.com/faustbrian/go-transactional-outbox/adapters/queue@v1
```

## Quick start

```go
publisher, err := outboxqueue.New(queue)
if err != nil {
    return err
}

worker, err := relay.New(store, publisher, relay.Config{
    Owner:         "outbox-relay-1",
    ClassifyError: outboxqueue.ClassifyError,
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
- [Go API reference](https://pkg.go.dev/github.com/faustbrian/go-transactional-outbox/adapters/queue)
- [Parent package documentation](../../docs/README.md)

## Compatibility and support

This module follows Semantic Versioning. Report vulnerabilities through the
[parent security policy](../../SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).

Shared adapter, ownership, and lifecycle expectations are in the versioned
[Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/README.md)
and its [Persistence and durability family](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/design-language.md#package-families-and-selection).
