package outboxrabbitstream_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams"
	"github.com/faustbrian/go-transactional-outbox"
	"github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream"
)

// TestMain checks progress before any ordinary test can enter the same loop.
// The child skips this preflight and runs only the finite version fixture.
func TestMain(m *testing.M) {
	if os.Getenv("OUTBOX_VERSION_ADMISSION_CHILD") != "1" {
		if err := versionAdmissionPreflight(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}

func versionAdmissionPreflight() error {
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve trusted test binary: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// #nosec G204 -- the executable is this trusted test binary, with fixed arguments and a bounded, waited child lifetime.
	command := exec.CommandContext(ctx, self, "-test.run=^TestPublisherVersionAdmissionTerminates$", "-test.count=1")
	command.Env = append(os.Environ(), "OUTBOX_VERSION_ADMISSION_CHILD=1")
	command.WaitDelay = 100 * time.Millisecond
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("finite version admission did not complete: %w; child output: %s", err, output)
	}
	return nil
}

func TestPublisherVersionAdmissionTerminates(t *testing.T) {
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
}
