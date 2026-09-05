package outboxkafka_test

import (
	"context"

	kafkacontract "github.com/faustbrian/go-kafka"
	"github.com/faustbrian/go-transactional-outbox"
	gokafka "github.com/faustbrian/go-transactional-outbox/adapters/kafka"
)

// Compile-time migration evidence: callers can preserve the former qualifier
// while moving to the target-oriented module path.
var (
	_ gokafka.Client = migrationKafkaClient{}
	_                = gokafka.New
	_                = gokafka.DefaultLimits
	_                = gokafka.WithLimits
	_                = gokafka.ClassifyError
	_                = gokafka.ErrInvalidEnvelope
	_                = migrationKafkaPublisher
	_                = migrationKafkaPublish
)

type migrationKafkaClient struct{}

func (migrationKafkaClient) Publish(context.Context, kafkacontract.Message) error { return nil }
func (migrationKafkaClient) Health(context.Context) error                         { return nil }

func migrationKafkaPublisher(client gokafka.Client) (*gokafka.Publisher, error) {
	return gokafka.New(client, gokafka.WithLimits(gokafka.DefaultLimits()))
}

func migrationKafkaPublish(publisher *gokafka.Publisher, envelope outbox.Envelope) error {
	return publisher.Publish(context.Background(), envelope)
}
