package outboxrabbitstream_test

import (
	"context"

	streamcontract "github.com/faustbrian/go-rabbitmq-streams"
	"github.com/faustbrian/go-transactional-outbox"
	gorabbitstream "github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream"
)

// Compile-time migration evidence: callers can preserve the former qualifier
// while moving to the target-oriented module path.
var (
	_ gorabbitstream.Client = migrationRabbitStreamClient{}
	_                       = gorabbitstream.New
	_                       = gorabbitstream.ClassifyError
	_                       = gorabbitstream.ErrInvalidEnvelope
	_                       = migrationRabbitStreamPublisher
	_                       = migrationRabbitStreamPublish
)

type migrationRabbitStreamClient struct{}

func (migrationRabbitStreamClient) Publish(
	context.Context,
	streamcontract.Message,
) (streamcontract.DeliveryResult, error) {
	return streamcontract.DeliveryResult{State: streamcontract.DeliveryConfirmed}, nil
}

func migrationRabbitStreamPublisher(
	client gorabbitstream.Client,
) (*gorabbitstream.Publisher, error) {
	return gorabbitstream.New(client, gorabbitstream.Config{Stream: "events"})
}

func migrationRabbitStreamPublish(
	publisher *gorabbitstream.Publisher,
	envelope outbox.Envelope,
) error {
	return publisher.Publish(context.Background(), envelope)
}
