package outboxrabbitstream_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/faustbrian/go-rabbitmq-streams"
	"github.com/faustbrian/go-transactional-outbox"
	"github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream"
)

func TestPublisherRejectsBorrowedInputsBeforeMessageCopies(t *testing.T) {
	for _, test := range []struct {
		name     string
		limits   func(*rabbitstream.Limits)
		envelope outbox.Envelope
	}{
		{
			name: "payload", limits: func(limits *rabbitstream.Limits) { limits.MaxPayloadBytes = 8 },
			envelope: outbox.Envelope{ID: "event", Topic: "events", PayloadVersion: 1, Payload: make([]byte, 32)},
		},
		{
			name: "metadata count", limits: func(limits *rabbitstream.Limits) { limits.MaxMetadataEntries = 1 },
			envelope: outbox.Envelope{ID: "event", Topic: "events", PayloadVersion: 1,
				Metadata: map[string]string{"a": "1", "b": "2", "c": "3"}},
		},
		{
			name: "metadata bytes", limits: func(limits *rabbitstream.Limits) { limits.MaxMetadataBytes = 64 },
			envelope: outbox.Envelope{ID: "event", Topic: "events", PayloadVersion: 1,
				Metadata: map[string]string{"a": strings.Repeat("v", 64)}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			limits := rabbitstream.DefaultLimits()
			test.limits(&limits)
			client := &recordingClient{}
			publisher, err := outboxrabbitstream.New(client, outboxrabbitstream.Config{Stream: "events", Limits: limits})
			if err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			var publishErr error
			// Small ordinary fixtures: one warmup and one measured publication.
			// Allow error-chain construction, not payload or metadata snapshots.
			allocations := testing.AllocsPerRun(1, func() {
				publishErr = publisher.Publish(ctx, test.envelope)
			})
			if !errors.Is(publishErr, outboxrabbitstream.ErrInvalidEnvelope) || client.calls != 0 {
				t.Fatalf("rejected publication error/calls = %v/%d", publishErr, client.calls)
			}
			if allocations > 4 {
				t.Fatalf("rejected publication copied message fields: allocations = %g, want at most four error-chain allocations", allocations)
			}
		})
	}
}

func TestPublisherAdmitsExactMappedBudgets(t *testing.T) {
	envelope := outbox.Envelope{
		ID: "event", Topic: "events", PayloadVersion: 65535,
		Payload: []byte("payload"), IdempotencyKey: "command",
		Metadata: map[string]string{
			"correlation-id": "correlation", "traceparent": "trace",
			"es.content_type": "application/json", "kind": "created",
		},
	}
	client := &recordingClient{result: rabbitstream.DeliveryResult{State: rabbitstream.DeliveryConfirmed}}
	publisher, err := outboxrabbitstream.New(client, outboxrabbitstream.Config{Stream: "events"})
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.Publish(t.Context(), envelope); err != nil {
		t.Fatal(err)
	}
	wire := client.message
	limits := rabbitstream.DefaultLimits()
	limits.MaxPayloadBytes = len(wire.Payload)
	limits.MaxMetadataEntries = len(wire.Headers) + len(wire.Properties)
	limits.MaxMetadataBytes = len(wire.ContentType) + len(wire.MessageID) + len(wire.CorrelationID)
	for _, group := range [][]rabbitstream.MetadataEntry{wire.Headers, wire.Properties} {
		for _, entry := range group {
			limits.MaxMetadataBytes += len(entry.Key) + len(entry.Value)
		}
	}
	for _, dimension := range []string{"exact", "payload", "metadata count", "metadata bytes"} {
		t.Run(dimension, func(t *testing.T) {
			bounded := limits
			switch dimension {
			case "payload":
				bounded.MaxPayloadBytes--
			case "metadata count":
				bounded.MaxMetadataEntries--
			case "metadata bytes":
				bounded.MaxMetadataBytes--
			}
			client := &recordingClient{result: rabbitstream.DeliveryResult{State: rabbitstream.DeliveryConfirmed}}
			publisher, err := outboxrabbitstream.New(client, outboxrabbitstream.Config{Stream: "events", Limits: bounded})
			if err != nil {
				t.Fatal(err)
			}
			err = publisher.Publish(t.Context(), envelope)
			if dimension == "exact" {
				if err != nil || client.calls != 1 {
					t.Fatalf("exact mapped budget error/calls = %v/%d", err, client.calls)
				}
			} else if !errors.Is(err, outboxrabbitstream.ErrInvalidEnvelope) || client.calls != 0 {
				t.Fatalf("exceeded %s budget error/calls = %v/%d", dimension, err, client.calls)
			}
		})
	}
}

func TestPublisherRejectsGeneratedAndApplicationPropertyBounds(t *testing.T) {
	for _, test := range []struct {
		name        string
		limits      func(*rabbitstream.Limits)
		idempotency string
		metadata    map[string]string
	}{
		{
			name: "generated schema key", limits: func(limits *rabbitstream.Limits) { limits.MaxMetadataKeyBytes = len("schema-version") - 1 },
		},
		{
			name: "generated entry count", limits: func(limits *rabbitstream.Limits) { limits.MaxMetadataEntries = 1 },
			idempotency: "command",
		},
		{
			name: "idempotency value", limits: func(limits *rabbitstream.Limits) { limits.MaxMetadataValueBytes = 16 },
			idempotency: strings.Repeat("c", 32),
		},
		{
			name: "application value", limits: func(limits *rabbitstream.Limits) { limits.MaxMetadataValueBytes = 16 },
			metadata: map[string]string{"a": strings.Repeat("v", 32)},
		},
		{
			name: "application key syntax", limits: func(*rabbitstream.Limits) {},
			metadata: map[string]string{"": "value"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			limits := rabbitstream.DefaultLimits()
			test.limits(&limits)
			client := &recordingClient{}
			publisher, err := outboxrabbitstream.New(client, outboxrabbitstream.Config{Stream: "events", Limits: limits})
			if err != nil {
				t.Fatal(err)
			}
			err = publisher.Publish(t.Context(), outbox.Envelope{
				ID: "event", Topic: "events", PayloadVersion: 1,
				OrderingKey: "route", IdempotencyKey: test.idempotency, Metadata: test.metadata,
			})
			if !errors.Is(err, outboxrabbitstream.ErrInvalidEnvelope) ||
				!errors.Is(err, rabbitstream.ErrValidation) || client.calls != 0 {
				t.Fatalf("property budget error/calls = %v/%d", err, client.calls)
			}
		})
	}
}
