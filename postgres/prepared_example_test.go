package postgres_test

import (
	"fmt"
	"time"

	outbox "github.com/faustbrian/go-transactional-outbox/v2"
	"github.com/faustbrian/go-transactional-outbox/v2/postgres"
)

func ExampleWriter_PrepareInsert() {
	writer, err := postgres.NewWriter(postgres.WriterConfig{})
	if err != nil {
		panic(err)
	}
	now := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	plan, err := writer.PrepareInsert([]outbox.Envelope{{
		ID: "event", Topic: "identity.created.v1", Payload: []byte(`{"version":1}`),
		PayloadVersion: 1, CreatedAt: now, AvailableAt: now,
	}})
	if err != nil {
		panic(err)
	}
	_, args := plan.Statement()
	// A transaction owner must execute the statement with its protected write.
	// Obtaining a plan does not execute SQL or establish a committed outcome.
	fmt.Println(len(args), args[3])
	// Output: 10 1
}
