package client

import (
	"errors"
	"fmt"
)

// ErrMessageRejected triggered when message should not be delivered.
var ErrMessageRejected = errors.New("message rejected")

// NewMessageRejectedError creates new MessageRejected error
func NewMessageRejectedError(message string) error {
	return fmt.Errorf("%w: %s", ErrMessageRejected, message)
}

// WrapMessageRejectedError wraps some error with MessageRejected error
func WrapMessageRejectedError(err error, message string) error {
	return fmt.Errorf("%w: %s: %w", ErrMessageRejected, message, err)
}
