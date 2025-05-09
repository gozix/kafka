package kafkaapi

import (
	"errors"

	"github.com/IBM/sarama"

	"github.com/gozix/kafka/internal/client"
)

var (
	// ErrTopicNotSet topic is required
	ErrTopicNotSet = errors.New("topic not set")
	// ErrGroupNotSet group is required
	ErrGroupNotSet = errors.New("group not set")
)

const DEFAULT = client.DEFAULT

type (
	// Config alias for sarama.Config for fewer imports
	Config = *sarama.Config
	// ConsumerMessage alias for sarama.ConsumerMessage for fewer imports
	ConsumerMessage = *sarama.ConsumerMessage
	// Subscription defines the settings used to initialize consumer instances.
	Subscription struct {
		// Connection name; if empty, the default connection will be used.
		Connection string

		// Kafka topic to subscribe to; must not be empty.
		Topic string

		// Consumer group ID; must not be empty.
		Group string

		/*
			Number of listener instances in this application instance.
			The total number of listeners across all application instances
			must not exceed the number of topic partitions.
			Default: 1
		*/
		Count int

		// Size of the message batch (used only by batch consumers).
		// Default: 1
		Size int
	}

	// ListenerSettings base settings for listener instance
	ListenerSettings interface {
		Subscription() Subscription
		SetConfiguration(Config)
	}

	// Listener handling a single message at a time.
	Listener interface {
		ListenerSettings
		Handle(message ConsumerMessage) (err error)
	}

	// BatchListener handling an array of messages at time
	BatchListener interface {
		ListenerSettings
		Handle(messages []ConsumerMessage) (err error)
	}
	ListenerFactory      func() ([]Listener, error)
	BatchListenerFactory func() ([]BatchListener, error)
)

func (s *Subscription) Normalize() error {
	if s.Connection == "" {
		s.Connection = client.DEFAULT
	}
	if s.Topic == "" {
		return ErrTopicNotSet
	}
	if s.Group == "" {
		return ErrGroupNotSet
	}
	if s.Count < 1 {
		s.Count = 1
	}
	if s.Size < 1 {
		s.Size = 1
	}
	return nil
}
