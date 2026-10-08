package outboxrabbitstream_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/faustbrian/go-rabbitmq-streams"
	"github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream/v2"
	"github.com/faustbrian/go-transactional-outbox/v2"
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
			if dimension == "exact" {
				err = publisher.Publish(t.Context(), envelope)
				if err != nil || client.calls != 1 {
					t.Fatalf("exact mapped budget error/calls = %v/%d", err, client.calls)
				}
			} else {
				ctx := t.Context()
				allocations := testing.AllocsPerRun(1, func() { err = publisher.Publish(ctx, envelope) })
				if !errors.Is(err, outboxrabbitstream.ErrInvalidEnvelope) || client.calls != 0 {
					t.Fatalf("exceeded %s budget error/calls = %v/%d", dimension, err, client.calls)
				}
				if allocations > 4 {
					t.Fatalf("mapped budget rejection retained caller data: %g allocations", allocations)
				}
			}
		})
	}
}

func TestPublisherRejectsApplicationBoundsAlongsideCorrelationBeforeCopies(t *testing.T) {
	for _, test := range []struct {
		name  string
		key   string
		value string
	}{
		{name: "key", key: strings.Repeat("k", 15), value: "value"},
		{name: "value", key: "kind", value: strings.Repeat("v", 17)},
	} {
		t.Run(test.name, func(t *testing.T) {
			limits := rabbitstream.DefaultLimits()
			limits.MaxMetadataKeyBytes = 14
			limits.MaxMetadataValueBytes = 16
			client := &recordingClient{}
			publisher, err := outboxrabbitstream.New(client, outboxrabbitstream.Config{Stream: "events", Limits: limits})
			if err != nil {
				t.Fatal(err)
			}
			envelope := outbox.Envelope{
				ID: "event", Topic: "events", PayloadVersion: 1, Payload: []byte("payload"),
				Metadata: map[string]string{"correlation-id": "correlation", test.key: test.value},
			}
			ctx := t.Context()
			// Map order is unspecified. Finite samples exercise both metadata
			// roles without claiming deterministic traversal-order coverage.
			// Measure each call separately so early copies cannot be averaged away.
			for sample := 0; sample < 64; sample++ {
				allocations := testing.AllocsPerRun(1, func() { err = publisher.Publish(ctx, envelope) })
				if !errors.Is(err, outboxrabbitstream.ErrInvalidEnvelope) ||
					!errors.Is(err, rabbitstream.ErrValidation) || client.calls != 0 {
					t.Fatalf("application %s error/calls = %v/%d", test.name, err, client.calls)
				}
				if allocations > 4 {
					t.Fatalf("application %s copied before rejection: %g allocations", test.name, allocations)
				}
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
		version     uint16
	}{
		{
			name: "generated schema key", limits: func(limits *rabbitstream.Limits) { limits.MaxMetadataKeyBytes = len("schema-version") - 1 },
		},
		{
			name: "generated schema value", limits: func(limits *rabbitstream.Limits) { limits.MaxMetadataValueBytes = 4 },
			version:  65535,
			metadata: map[string]string{"es.content_type": "t"},
		},
		{
			name: "generated entry count", limits: func(limits *rabbitstream.Limits) { limits.MaxMetadataEntries = 1 },
			idempotency: "command",
		},
		{
			name: "application key", limits: func(limits *rabbitstream.Limits) { limits.MaxMetadataKeyBytes = 14 },
			metadata: map[string]string{strings.Repeat("k", 15): "value"},
		},
		{
			name: "idempotency key", limits: func(limits *rabbitstream.Limits) { limits.MaxMetadataKeyBytes = 14 },
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
			version := test.version
			if version == 0 {
				version = 1
			}
			id := "event"
			if test.name == "generated schema value" {
				id = "e" // Standard property limits must not mask the schema value.
			}
			envelope := outbox.Envelope{
				ID: id, Topic: "events", PayloadVersion: version, Payload: []byte("payload"),
				OrderingKey: "route", IdempotencyKey: test.idempotency, Metadata: test.metadata,
			}
			ctx := t.Context()
			allocations := testing.AllocsPerRun(1, func() { err = publisher.Publish(ctx, envelope) })
			if !errors.Is(err, outboxrabbitstream.ErrInvalidEnvelope) ||
				!errors.Is(err, rabbitstream.ErrValidation) || client.calls != 0 {
				t.Fatalf("property budget error/calls = %v/%d", err, client.calls)
			}
			// Key syntax is deliberately checked by the final authoritative
			// representation validator after bounded materialization.
			if test.name != "application key syntax" && allocations > 4 {
				t.Fatalf("property budget rejection retained caller data: %g allocations", allocations)
			}
		})
	}
}

func TestPublisherAdmitsExactGeneratedAndApplicationProperties(t *testing.T) {
	for _, idempotency := range []string{"", strings.Repeat("i", 16)} {
		for _, metadata := range []map[string]string{nil, {strings.Repeat("k", 14): strings.Repeat("v", 16)}} {
			limits := rabbitstream.DefaultLimits()
			limits.MaxMetadataKeyBytes = 14
			limits.MaxMetadataValueBytes = 16
			limits.MaxMetadataEntries = 1 + len(metadata)
			if idempotency != "" {
				limits.MaxMetadataKeyBytes = len("idempotency-key")
				limits.MaxMetadataEntries++
				if metadata != nil {
					metadata = map[string]string{strings.Repeat("k", limits.MaxMetadataKeyBytes): strings.Repeat("v", 16)}
				}
			}
			client := &recordingClient{result: rabbitstream.DeliveryResult{State: rabbitstream.DeliveryConfirmed}}
			publisher, err := outboxrabbitstream.New(client, outboxrabbitstream.Config{Stream: "events", Limits: limits})
			if err != nil {
				t.Fatal(err)
			}
			if err := publisher.Publish(t.Context(), outbox.Envelope{
				ID: "event", Topic: "events", PayloadVersion: 1,
				OrderingKey: "route", IdempotencyKey: idempotency, Metadata: metadata,
			}); err != nil || client.calls != 1 {
				t.Fatalf("exact generated/application property admission error/calls = %v/%d", err, client.calls)
			}
		}
	}
}

func TestPublisherAdmitsExactDecimalVersionBytes(t *testing.T) {
	for _, test := range []struct {
		version uint16
		text    string
	}{
		{1, "1"}, {9, "9"}, {10, "10"}, {99, "99"}, {100, "100"},
		{999, "999"}, {1000, "1000"}, {9999, "9999"}, {10000, "10000"}, {65535, "65535"},
	} {
		t.Run(test.text, func(t *testing.T) {
			for _, contentType := range []string{"", "t"} {
				limits := rabbitstream.DefaultLimits()
				limits.MaxMetadataEntries = 1
				limits.MaxMetadataKeyBytes = len("schema-version")
				limits.MaxMetadataValueBytes = len(test.text)
				// Standard content type and ID plus the emitted schema property.
				wireContentType := contentType
				if wireContentType == "" {
					wireContentType = "application/json"
					limits.MaxMetadataValueBytes = len(wireContentType)
				}
				metadata := map[string]string(nil)
				if contentType != "" {
					metadata = map[string]string{"es.content_type": contentType}
					limits.MaxMetadataEntries++
					limits.MaxMetadataKeyBytes = len("es.content_type")
				}
				limits.MaxMetadataBytes = len(wireContentType) + len("e") + len("schema-version") + len(test.text)
				if metadata != nil {
					limits.MaxMetadataBytes += len("es.content_type") + len(contentType)
				}
				client := &recordingClient{result: rabbitstream.DeliveryResult{State: rabbitstream.DeliveryConfirmed}}
				publisher, err := outboxrabbitstream.New(client, outboxrabbitstream.Config{Stream: "events", Limits: limits})
				if err != nil {
					t.Fatal(err)
				}
				envelope := outbox.Envelope{ID: "e", Topic: "events", PayloadVersion: test.version, Metadata: metadata, Payload: []byte("payload")}
				if err := publisher.Publish(t.Context(), envelope); err != nil || client.calls != 1 {
					t.Fatalf("exact version property admission error/calls = %v/%d", err, client.calls)
				}
				found := false
				for _, property := range client.message.Properties {
					if property.Key == "schema-version" {
						found = string(property.Value) == test.text
					}
				}
				if !found {
					t.Fatal("emitted schema version differs from the decimal contract")
				}
				limits.MaxMetadataBytes--
				rejectingClient := &recordingClient{}
				rejectingPublisher, err := outboxrabbitstream.New(rejectingClient, outboxrabbitstream.Config{Stream: "events", Limits: limits})
				if err != nil {
					t.Fatal(err)
				}
				ctx := t.Context()
				allocations := testing.AllocsPerRun(1, func() { err = rejectingPublisher.Publish(ctx, envelope) })
				if !errors.Is(err, outboxrabbitstream.ErrInvalidEnvelope) || rejectingClient.calls != 0 || allocations > 4 {
					t.Fatalf("decimal metadata overflow error/calls/allocations = %v/%d/%g", err, rejectingClient.calls, allocations)
				}
			}
		})
	}
}
