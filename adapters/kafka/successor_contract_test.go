package outboxkafka

import (
	"context"
	"errors"
	"testing"

	kafkacontract "github.com/faustbrian/go-kafka"
	"github.com/faustbrian/go-transactional-outbox"
	"github.com/faustbrian/go-transactional-outbox/relay"
)

func TestSuccessorPreservesKafkaPublisherContract(t *testing.T) {
	t.Parallel()

	client := &successorKafkaClient{}
	publisher, err := New(client, WithLimits(DefaultLimits()))
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("event")
	envelope := outbox.Envelope{
		ID: "event-1", Topic: "events", Payload: payload, PayloadVersion: 1,
		Metadata: map[string]string{"application": "billing"},
	}
	if err := publisher.Publish(t.Context(), envelope); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	envelope.Metadata["application"] = "changed"
	if string(client.message.Value) != "event" || string(client.message.Headers[3].Value) != "billing" {
		t.Fatalf("publisher retained caller-owned values: %#v", client.message)
	}
	if err := publisher.Health(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := ClassifyError(ErrInvalidEnvelope); got != relay.ErrorPermanent {
		t.Fatalf("ClassifyError() = %v, want %v", got, relay.ErrorPermanent)
	}
}

func TestSuccessorPreservesKafkaSafeFailureContract(t *testing.T) {
	t.Parallel()

	cause := errors.New("credential-secret")
	publisher, err := New(&successorKafkaClient{publishErr: cause})
	if err != nil {
		t.Fatal(err)
	}
	publishErr := publisher.Publish(t.Context(), outbox.Envelope{
		ID: "event-1", Topic: "events", PayloadVersion: 1,
	})
	if !errors.Is(publishErr, cause) || publishErr.Error() != "outbox/gokafka: publish failed" {
		t.Fatalf("Publish() error = %v", publishErr)
	}
}

type successorKafkaClient struct {
	message    kafkacontract.Message
	publishErr error
}

func (client *successorKafkaClient) Publish(_ context.Context, message kafkacontract.Message) error {
	client.message = message

	return client.publishErr
}

func (*successorKafkaClient) Health(context.Context) error { return nil }
