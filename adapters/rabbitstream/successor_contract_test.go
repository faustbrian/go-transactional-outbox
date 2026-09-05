package outboxrabbitstream

import (
	"context"
	"errors"
	"testing"

	"github.com/faustbrian/go-rabbitmq-streams"
	"github.com/faustbrian/go-transactional-outbox"
	"github.com/faustbrian/go-transactional-outbox/relay"
)

func TestSuccessorPreservesRabbitStreamPublisherContract(t *testing.T) {
	t.Parallel()

	client := &successorRabbitStreamClient{}
	publisher, err := New(client, Config{Stream: "events"})
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
	if string(client.message.Payload) != "event" || string(client.message.Properties[1].Value) != "billing" {
		t.Fatalf("publisher retained caller-owned values: %#v", client.message)
	}
	if got := ClassifyError(ErrInvalidEnvelope); got != relay.ErrorPermanent {
		t.Fatalf("ClassifyError() = %v, want %v", got, relay.ErrorPermanent)
	}
}

func TestSuccessorPreservesRabbitStreamConfirmationContract(t *testing.T) {
	t.Parallel()

	publisher, err := New(&successorRabbitStreamClient{
		result: rabbitstream.DeliveryResult{State: rabbitstream.DeliveryAmbiguous},
	}, Config{Stream: "events"})
	if err != nil {
		t.Fatal(err)
	}
	publishErr := publisher.Publish(t.Context(), outbox.Envelope{
		ID: "event-1", Topic: "events", PayloadVersion: 1,
	})
	if !errors.Is(publishErr, rabbitstream.ErrPublishAmbiguous) ||
		ClassifyError(publishErr) != relay.ErrorTransient {
		t.Fatalf("Publish() error/class = %v/%v", publishErr, ClassifyError(publishErr))
	}
}

type successorRabbitStreamClient struct {
	message rabbitstream.Message
	result  rabbitstream.DeliveryResult
}

func (client *successorRabbitStreamClient) Publish(
	_ context.Context,
	message rabbitstream.Message,
) (rabbitstream.DeliveryResult, error) {
	client.message = message
	if client.result.State == 0 {
		return rabbitstream.DeliveryResult{State: rabbitstream.DeliveryConfirmed}, nil
	}

	return client.result, nil
}
