package outboxrabbitstream_test

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams"
	"github.com/faustbrian/go-transactional-outbox"
	"github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream"
)

func TestPublisherVersionAdmissionTerminates(t *testing.T) {
	if os.Getenv("OUTBOX_VERSION_ADMISSION_CHILD") == "1" {
		for _, version := range []uint16{1, 10, 100, 65535} {
			client := &recordingClient{result: rabbitstream.DeliveryResult{State: rabbitstream.DeliveryConfirmed}}
			publisher, err := outboxrabbitstream.New(client, outboxrabbitstream.Config{Stream: "events"})
			if err != nil {
				t.Fatal(err)
			}
			if err := publisher.Publish(t.Context(), outbox.Envelope{
				ID: "event", Topic: "events", PayloadVersion: version,
			}); err != nil || client.calls != 1 {
				t.Fatal("supported version did not reach confirmed publication")
			}
		}
		return
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	// #nosec G204 -- the executable is this trusted test binary, with fixed arguments and a bounded, waited child lifetime.
	command := exec.CommandContext(ctx, self, "-test.run=^TestPublisherVersionAdmissionTerminates$", "-test.count=1")
	command.Env = append(os.Environ(), "OUTBOX_VERSION_ADMISSION_CHILD=1")
	command.WaitDelay = 100 * time.Millisecond
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("finite version admission did not complete: %v; child output: %s", err, output)
	}
}
