package outboxqueue_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/faustbrian/go-transactional-outbox"
	"github.com/faustbrian/go-transactional-outbox/adapters/queue"
)

func TestPublisherRejectsBorrowedInputsBeforeSnapshotAllocation(t *testing.T) {
	limits := outboxqueue.Limits{
		MaxTaskBytes: 512, MaxIdentityBytes: 16, MaxContentBytes: 8,
		MaxMetadataEntries: 1, MaxMetadataBytes: 8,
	}
	for name, envelope := range map[string]outbox.Envelope{
		"payload": {
			ID: "event", Topic: "created", PayloadVersion: 1, Payload: make([]byte, 32),
		},
		"metadata count": {
			ID: "event", Topic: "created", PayloadVersion: 1,
			Metadata: map[string]string{"a": "1", "b": "2"},
		},
		"metadata bytes": {
			ID: "event", Topic: "created", PayloadVersion: 1,
			Metadata: map[string]string{"a": strings.Repeat("v", 32)},
		},
	} {
		t.Run(name, func(t *testing.T) {
			queue := &recordingQueue{}
			publisher, err := outboxqueue.New(queue, outboxqueue.WithLimits(limits))
			if err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			var publishErr error
			// One warmup and one measured call with small ordinary fixtures.
			// The rejected outcome may allocate its error, but not a snapshot.
			allocations := testing.AllocsPerRun(1, func() {
				publishErr = publisher.Publish(ctx, envelope)
			})
			if !errors.Is(publishErr, outboxqueue.ErrInvalidEnvelope) || queue.calls != 0 {
				t.Fatalf("rejected publication error/calls = %v/%d", publishErr, queue.calls)
			}
			if allocations > 1 {
				t.Fatalf("rejected publication allocated snapshots: allocations = %g, want at most one outcome", allocations)
			}
		})
	}
}
