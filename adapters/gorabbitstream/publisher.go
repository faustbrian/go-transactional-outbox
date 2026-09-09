// Package gorabbitstream preserves the released RabbitMQ Streams outbox
// adapter path while delegating to the target-oriented successor.
//
// Deprecated: use github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream.
package gorabbitstream

import (
	"context"
	"errors"
	"reflect"

	"github.com/faustbrian/go-rabbitmq-streams"
	"github.com/faustbrian/go-transactional-outbox"
	successor "github.com/faustbrian/go-transactional-outbox/adapters/rabbitstream"
	"github.com/faustbrian/go-transactional-outbox/relay"
)

var (
	// ErrClientRequired preserves the released construction error identity.
	ErrClientRequired = errors.New("outbox/gorabbitstream: publishing client is required")
	// ErrInvalidConfig preserves the released configuration error identity.
	ErrInvalidConfig = errors.New("outbox/gorabbitstream: configuration is invalid")
	// ErrInvalidEnvelope preserves the released envelope error identity.
	ErrInvalidEnvelope = errors.New("outbox/gorabbitstream: envelope is invalid")
	// ErrReservedMetadata preserves the released metadata error identity.
	ErrReservedMetadata = errors.New("outbox/gorabbitstream: metadata uses a reserved field")
	// ErrUnconfirmed preserves the released confirmation error identity.
	ErrUnconfirmed = errors.New("outbox/gorabbitstream: publication was not confirmed")
	// ErrContextRequired preserves the released context error identity.
	ErrContextRequired = errors.New("outbox/gorabbitstream: context is required")
	// ErrClientPanic preserves the released panic error identity.
	ErrClientPanic = errors.New("outbox/gorabbitstream: publishing client panicked")
)

// Client is the released confirmed-publisher boundary.
//
// Deprecated: use successor.Client.
type Client interface {
	Publish(context.Context, rabbitstream.Message) (rabbitstream.DeliveryResult, error)
}

// Config is the released adapter configuration.
//
// Deprecated: use successor.Config.
type Config struct {
	Stream      string
	SuperStream string
	Limits      rabbitstream.Limits
}

// Publisher preserves the released type while delegating behavior.
//
// Deprecated: use successor.Publisher.
type Publisher struct {
	delegate *successor.Publisher
}

// New delegates construction while preserving legacy types and errors.
//
// Deprecated: use successor.New.
func New(client Client, config Config) (*Publisher, error) {
	delegate, err := successor.New(client, successor.Config{
		Stream: config.Stream, SuperStream: config.SuperStream, Limits: config.Limits,
	})
	if err != nil {
		return nil, translateError(err)
	}

	return &Publisher{delegate: delegate}, nil
}

// Publish delegates publication while preserving legacy error identities.
func (publisher *Publisher) Publish(ctx context.Context, envelope outbox.Envelope) error {
	return translateError(publisher.delegate.Publish(ctx, envelope))
}

// ClassifyError preserves the released relay classification contract.
//
// Deprecated: use successor.ClassifyError.
func ClassifyError(err error) relay.ErrorClass {
	if errors.Is(err, ErrInvalidEnvelope) || errors.Is(err, rabbitstream.ErrInvalidConfiguration) ||
		errors.Is(err, rabbitstream.ErrValidation) || errors.Is(err, rabbitstream.ErrMessageTooLarge) ||
		errors.Is(err, rabbitstream.ErrBrokerRejected) {
		return relay.ErrorPermanent
	}

	return relay.ErrorTransient
}

func translateError(err error) error {
	if err == nil {
		return nil
	}

	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		translated := make([]error, len(causes))
		for index, cause := range causes {
			translated[index] = translateError(cause)
		}

		return errors.Join(translated...)
	}

	if operationError, ok := err.(*rabbitstream.OperationError); ok {
		translatedCause := translateError(operationError.Cause)
		if sameError(translatedCause, operationError.Cause) {
			return err
		}
		copy := *operationError
		copy.Cause = translatedCause

		return &copy
	}

	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		cause := wrapped.Unwrap()
		translatedCause := translateError(cause)
		if sameError(translatedCause, cause) {
			return err
		}

		return translatedError{message: err.Error(), cause: translatedCause}
	}

	for _, mapping := range []struct {
		successor error
		legacy    error
	}{
		{successor.ErrClientRequired, ErrClientRequired},
		{successor.ErrInvalidConfig, ErrInvalidConfig},
		{successor.ErrInvalidEnvelope, ErrInvalidEnvelope},
		{successor.ErrReservedMetadata, ErrReservedMetadata},
		{successor.ErrUnconfirmed, ErrUnconfirmed},
		{successor.ErrContextRequired, ErrContextRequired},
		{successor.ErrClientPanic, ErrClientPanic},
	} {
		if errors.Is(err, mapping.successor) {
			return mapping.legacy
		}
	}

	return err
}

func sameError(left, right error) bool {
	leftType := reflect.TypeOf(left)
	return leftType != nil && leftType == reflect.TypeOf(right) && leftType.Comparable() && left == right
}

type translatedError struct {
	message string
	cause   error
}

func (err translatedError) Error() string { return err.message }
func (err translatedError) Unwrap() error { return err.cause }
