package outboxrabbitstream_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams"
	"github.com/faustbrian/go-transactional-outbox"
	"github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream"
	"github.com/faustbrian/go-transactional-outbox/relay"
)

func FuzzPublisherEnvelopeOwnershipAndFailureClassification(f *testing.F) {
	f.Add("event-1", "events", []byte("payload"), uint16(1), "order", "idem", "key", "value", uint8(0))
	f.Add("", "", []byte{}, uint16(0), "", "", "schema-version", "forged", uint8(2))
	f.Fuzz(func(
		t *testing.T,
		id, topic string,
		payload []byte,
		version uint16,
		orderingKey, idempotencyKey, metadataKey, metadataValue string,
		mode uint8,
	) {
		client := &fuzzClient{mode: mode % 3}
		publisher, err := outboxrabbitstream.New(client, outboxrabbitstream.Config{Stream: "events"})
		if err != nil {
			t.Fatal(err)
		}
		envelope := outbox.Envelope{
			ID: id, Topic: topic, Payload: payload, PayloadVersion: version,
			OrderingKey: orderingKey, IdempotencyKey: idempotencyKey,
			Metadata:  map[string]string{metadataKey: metadataValue},
			CreatedAt: time.Unix(1_700_000_000, 0).UTC(),
		}
		publishErr := publisher.Publish(context.Background(), envelope)
		if client.calls == 0 {
			if !errors.Is(publishErr, outboxrabbitstream.ErrInvalidEnvelope) {
				t.Fatalf("pre-client error = %v", publishErr)
			}
			return
		}
		if client.mode == 0 {
			if publishErr != nil {
				t.Fatalf("confirmed error = %v", publishErr)
			}
			if len(payload) > 0 {
				payload[0] ^= 0xff
				if len(client.message.Payload) > 0 && client.message.Payload[0] == payload[0] {
					t.Fatal("client message retained caller payload")
				}
			}
			return
		}
		if outboxrabbitstream.ClassifyError(publishErr) != relay.ErrorTransient {
			t.Fatalf("failure classified as permanent: %v", publishErr)
		}
	})
}

type fuzzClient struct {
	mode    uint8
	calls   int
	message rabbitstream.Message
}

func (client *fuzzClient) Publish(
	_ context.Context,
	message rabbitstream.Message,
) (rabbitstream.DeliveryResult, error) {
	client.calls++
	client.message = message
	switch client.mode {
	case 1:
		return rabbitstream.DeliveryResult{State: rabbitstream.DeliveryAmbiguous},
			&rabbitstream.OperationError{
				Operation: rabbitstream.OperationPublish,
				Category:  rabbitstream.CategoryPublishAmbiguous,
			}
	case 2:
		panic("hostile client panic")
	default:
		return rabbitstream.DeliveryResult{State: rabbitstream.DeliveryConfirmed}, nil
	}
}
