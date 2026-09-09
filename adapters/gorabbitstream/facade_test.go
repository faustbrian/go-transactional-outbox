package gorabbitstream_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/faustbrian/go-transactional-outbox"
	legacy "github.com/faustbrian/go-transactional-outbox/adapters/gorabbitstream"
	successor "github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream"
)

func TestLegacyPathRetainsDistinctPublicIdentityWhileDelegating(t *testing.T) {
	t.Parallel()

	for name, pair := range map[string][2]error{
		"client required":   {legacy.ErrClientRequired, successor.ErrClientRequired},
		"invalid config":    {legacy.ErrInvalidConfig, successor.ErrInvalidConfig},
		"invalid envelope":  {legacy.ErrInvalidEnvelope, successor.ErrInvalidEnvelope},
		"reserved metadata": {legacy.ErrReservedMetadata, successor.ErrReservedMetadata},
		"unconfirmed":       {legacy.ErrUnconfirmed, successor.ErrUnconfirmed},
		"context required":  {legacy.ErrContextRequired, successor.ErrContextRequired},
		"client panic":      {legacy.ErrClientPanic, successor.ErrClientPanic},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if errors.Is(pair[0], pair[1]) || errors.Is(pair[1], pair[0]) {
				t.Fatalf("legacy and successor errors share identity: %v / %v", pair[0], pair[1])
			}
		})
	}

	client := &recordingClient{}
	legacyPublisher, err := legacy.New(client, legacy.Config{Stream: "events"})
	if err != nil {
		t.Fatal(err)
	}
	successorPublisher, err := successor.New(client, successor.Config{Stream: "events"})
	if err != nil {
		t.Fatal(err)
	}
	if reflect.TypeOf(legacyPublisher) == reflect.TypeOf(successorPublisher) {
		t.Fatalf("legacy publisher type collapsed into successor type: %T", legacyPublisher)
	}
	if path := publisherPath(legacyPublisher); path != "legacy" {
		t.Fatalf("legacy publisher path = %q", path)
	}
	if path := publisherPath(successorPublisher); path != "successor" {
		t.Fatalf("successor publisher path = %q", path)
	}
}

func TestLegacyPathTranslatesSuccessorErrors(t *testing.T) {
	t.Parallel()

	_, constructionErr := legacy.New(nil, legacy.Config{Stream: "events"})
	assertLegacyError(t, constructionErr, legacy.ErrClientRequired, successor.ErrClientRequired)

	publisher, err := legacy.New(&recordingClient{}, legacy.Config{Stream: "events"})
	if err != nil {
		t.Fatal(err)
	}
	var nilContext context.Context
	assertLegacyError(t, publisher.Publish(nilContext, outbox.Envelope{}),
		legacy.ErrContextRequired, successor.ErrContextRequired)

	panicPublisher, err := legacy.New(&panickingClient{}, legacy.Config{Stream: "events"})
	if err != nil {
		t.Fatal(err)
	}
	panicErr := panicPublisher.Publish(t.Context(), outbox.Envelope{
		ID: "event-1", Topic: "events", PayloadVersion: 1,
	})
	assertLegacyError(t, panicErr, legacy.ErrClientPanic, successor.ErrClientPanic)
}

func TestLegacyPathPreservesTypedClientErrorsWhileTranslatingSentinels(t *testing.T) {
	t.Parallel()

	clientCause := &typedClientError{cause: successor.ErrInvalidEnvelope}
	publisher, err := legacy.New(&recordingClient{err: clientCause}, legacy.Config{Stream: "events"})
	if err != nil {
		t.Fatal(err)
	}
	publishErr := publisher.Publish(t.Context(), outbox.Envelope{
		ID: "event-1", Topic: "events", PayloadVersion: 1,
	})
	assertLegacyError(t, publishErr, legacy.ErrInvalidEnvelope, successor.ErrInvalidEnvelope)
	var preserved *typedClientError
	if !errors.As(publishErr, &preserved) || preserved != clientCause {
		t.Fatalf("typed client error was not preserved: %v", publishErr)
	}
}

func assertLegacyError(t *testing.T, err, legacyError, successorError error) {
	t.Helper()
	if !errors.Is(err, legacyError) {
		t.Fatalf("error = %v, want legacy identity %v", err, legacyError)
	}
	if errors.Is(err, successorError) {
		t.Fatalf("error retained successor identity %v", successorError)
	}
}

func publisherPath(value any) string {
	switch value.(type) {
	case *legacy.Publisher:
		return "legacy"
	case *successor.Publisher:
		return "successor"
	default:
		return "unknown"
	}
}

type typedClientError struct{ cause error }

func (*typedClientError) Error() string           { return "typed client error" }
func (clientErr *typedClientError) Unwrap() error { return clientErr.cause }
