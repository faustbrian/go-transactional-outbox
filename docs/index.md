# outbox documentation map

[Repository README](../README.md) · [Current Golib portal](https://github.com/faustbrian/go-library-tools/blob/main/docs/ecosystem/README.md) · [Published ecosystem snapshot](https://github.com/faustbrian/go-library-tools/blob/v1.8.4/docs/ecosystem/README.md)

This map links package-owned adoption and operational guidance. Start with
the repository README for purpose, installation, and ownership boundaries.
Published compatibility sets are frozen multi-package baselines; they do not
certify every current main revision or a deployed application.

## Module boundaries

Import only the modules needed by your application. A deprecated facade,
preview adapter, or internal harness is not interchangeable with a stable
root API; follow its own README and migration guidance before composing it.

| Module | Status | Owner detail |
| --- | --- | --- |
| `github.com/faustbrian/go-transactional-outbox/v2` | stable | [README](../README.md) |
| `github.com/faustbrian/go-transactional-outbox/adapters/otel/v2` | stable | [README](../adapters/otel/README.md) |
| `github.com/faustbrian/go-transactional-outbox/adapters/queue/v3` | stable | [README](../adapters/queue/README.md) |
| `github.com/faustbrian/go-transactional-outbox/adapters/kafka/v2` | preview | [README](../adapters/kafka/README.md) |
| `github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream/v2` | stable | [README](../adapters/rabbitstream/README.md) |

## Choose and adopt

- [Documentation](README.md).
- [Adapter Path Migration](adapter-migration.md).
- [API Reference](api.md).
- [Architecture And Recovery](architecture.md).
- [Compatibility Policy](compatibility.md).
- [Examples And Integrations](examples.md).
- [Delivery Guarantees](guarantees.md).
- [Consumer Idempotency With idempotency](idempotency.md).
- [Schema Migrations And Upgrades](migrations.md).
- [PostgreSQL Schema And Operations](postgresql.md).
- [Quickstart](quickstart.md).

## Operate and evaluate boundaries

- [Kubernetes Operations And Capacity](operations.md).
- [Recovery Runbooks](runbooks.md).
- [Security threat model, version 1](security-threat-model-v1.md).
- [Telemetry integration](telemetry.md).
- [Troubleshooting And FAQ](troubleshooting.md).

## Owned adapters, examples, and harnesses

- [Outbox Kafka adapter](../adapters/kafka/README.md).
- [Documentation](../adapters/kafka/docs/README.md).
- [Outbox OpenTelemetry adapter](../adapters/otel/README.md).
- [Documentation](../adapters/otel/docs/README.md).
- [outbox queue adapter](../adapters/queue/README.md).
- [Documentation](../adapters/queue/docs/README.md).
- [Outbox RabbitMQ Streams adapter](../adapters/rabbitstream/README.md).
- [Documentation](../adapters/rabbitstream/docs/README.md).

## Maintainer, specification, and evidence references

- [Hardening Inventory](inventory.md).
- [Source documentation audit](source-documentation-audit.md).

## Related next steps

[Contributor setup and checks](../CONTRIBUTING.md), [Compatibility policy](../COMPATIBILITY.md), [Release history](../CHANGELOG.md), [Private vulnerability reporting](../SECURITY.md), [Support](../SUPPORT.md).
