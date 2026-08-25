package gorabbitstream_test

import (
	"context"
	"testing"

	"github.com/faustbrian/go-rabbitmq-streams"
	"github.com/faustbrian/go-transactional-outbox"
	"github.com/faustbrian/go-transactional-outbox/adapters/gorabbitstream"
)

func BenchmarkPublisherMappingAndConfirmation(b *testing.B) {
	publisher, err := gorabbitstream.New(
		&recordingClient{result: rabbitstream.DeliveryResult{State: rabbitstream.DeliveryConfirmed}},
		gorabbitstream.Config{SuperStream: "tracking.events"},
	)
	if err != nil {
		b.Fatal(err)
	}
	envelope := outbox.Envelope{
		ID: "event-1", Topic: "tracking.events", OrderingKey: "tracked-item-1",
		PayloadVersion: 1, Payload: make([]byte, 1024),
		Metadata: map[string]string{
			"traceparent": "00-00000000000000000000000000000001-0000000000000001-01",
			"schema":      "tracking-event",
		},
	}
	b.SetBytes(1024)
	b.ReportAllocs()
	for b.Loop() {
		if err := publisher.Publish(context.Background(), envelope); err != nil {
			b.Fatal(err)
		}
	}
}
