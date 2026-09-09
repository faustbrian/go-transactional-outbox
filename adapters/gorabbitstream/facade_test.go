package gorabbitstream_test

import (
	"errors"
	"reflect"
	"testing"

	legacy "github.com/faustbrian/go-transactional-outbox/adapters/gorabbitstream"
	successor "github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream"
)

func TestLegacyPathDelegatesToTargetOrientedSuccessor(t *testing.T) {
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
			if !errors.Is(pair[0], pair[1]) || !errors.Is(pair[1], pair[0]) {
				t.Fatalf("legacy and successor errors are not identical: %v / %v", pair[0], pair[1])
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
	if reflect.TypeOf(legacyPublisher) != reflect.TypeOf(successorPublisher) {
		t.Fatalf("legacy publisher type = %T, successor type = %T", legacyPublisher, successorPublisher)
	}
}
