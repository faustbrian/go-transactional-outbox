// Package gorabbitstream preserves the released RabbitMQ Streams outbox
// adapter path while delegating to the target-oriented successor.
//
// Deprecated: use github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream.
package gorabbitstream

import (
	successor "github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream"
	"github.com/faustbrian/go-transactional-outbox/relay"
)

var (
	// ErrClientRequired preserves the released construction error identity.
	ErrClientRequired = successor.ErrClientRequired
	// ErrInvalidConfig preserves the released configuration error identity.
	ErrInvalidConfig = successor.ErrInvalidConfig
	// ErrInvalidEnvelope preserves the released envelope error identity.
	ErrInvalidEnvelope = successor.ErrInvalidEnvelope
	// ErrReservedMetadata preserves the released metadata error identity.
	ErrReservedMetadata = successor.ErrReservedMetadata
	// ErrUnconfirmed preserves the released confirmation error identity.
	ErrUnconfirmed = successor.ErrUnconfirmed
	// ErrContextRequired preserves the released context error identity.
	ErrContextRequired = successor.ErrContextRequired
	// ErrClientPanic preserves the released panic error identity.
	ErrClientPanic = successor.ErrClientPanic
)

// Client preserves the released confirmed-publisher boundary.
//
// Deprecated: use successor.Client.
type Client = successor.Client

// Config preserves the released adapter configuration.
//
// Deprecated: use successor.Config.
type Config = successor.Config

// Publisher preserves the released publisher identity and behavior.
//
// Deprecated: use successor.Publisher.
type Publisher = successor.Publisher

// New delegates construction to the target-oriented successor.
//
// Deprecated: use successor.New.
func New(client Client, config Config) (*Publisher, error) {
	return successor.New(client, config)
}

// ClassifyError delegates relay error classification to the successor.
//
// Deprecated: use successor.ClassifyError.
func ClassifyError(err error) relay.ErrorClass {
	return successor.ClassifyError(err)
}
