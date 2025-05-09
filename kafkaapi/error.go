package kafkaapi

import (
	"github.com/gozix/kafka/internal/client"
)

var (
	// ErrMessageRejected triggered when message should not be delivered.
	ErrMessageRejected = client.ErrMessageRejected
	// NewMessageRejectedError creates new MessageRejected error
	NewMessageRejectedError = client.NewMessageRejectedError
	// WrapMessageRejectedError wraps some error with MessageRejected error
	WrapMessageRejectedError = client.WrapMessageRejectedError
)
