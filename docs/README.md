# Documentation

- [Quickstart](quickstart.md), [API](api.md), [examples](examples.md), and
  [consumer idempotency](idempotency.md)
- [Architecture](architecture.md) and [guarantees](guarantees.md)
- [PostgreSQL](postgresql.md), [migrations](migrations.md), and
  [compatibility](compatibility.md)
- [Telemetry](telemetry.md), [operations](operations.md), and
  [recovery runbooks](runbooks.md)
- [Troubleshooting](troubleshooting.md) and [security inventory](inventory.md)

## Optional modules

- [Kafka adapter](../adapters/gokafka/docs/README.md)
- [RabbitMQ Streams adapter](../adapters/gorabbitstream/docs/README.md)
- [OpenTelemetry adapter](../adapters/otel/docs/README.md)
- [Queue adapter](../adapters/queue/docs/README.md)

Read the guarantees and crash matrix before adopting the package. Operational
guides do not turn at-least-once delivery into exactly-once delivery.
