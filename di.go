package kafka

import (
	"github.com/gozix/di"

	"gitlab.mobbtech.com/gozix/kafka/internal/command"
	"gitlab.mobbtech.com/gozix/kafka/internal/modifier"
	"gitlab.mobbtech.com/gozix/kafka/kafkaapi"
)

// AsKafkaListener is syntax sugar for the di container.
func AsKafkaListener(name string) di.ProvideOption {
	return di.ProvideOptions(
		di.As(new(kafkaapi.Listener)),
		di.Tags{{
			Name: command.TagListener,
			Args: di.Args{{
				Key:   modifier.ArgNameKey,
				Value: name,
			}},
		}},
	)
}

// AsKafkaBatchListener is syntax sugar for the di container.
func AsKafkaBatchListener(name string) di.ProvideOption {
	return di.ProvideOptions(
		di.As(new(kafkaapi.BatchListener)),
		di.Tags{{
			Name: command.TagBatchListener,
			Args: di.Args{{
				Key:   modifier.ArgNameKey,
				Value: name,
			}},
		}},
	)
}

// AsKafkaListenerFactory is syntax sugar for the di container.
func AsKafkaListenerFactory(name string) di.ProvideOption {
	return di.ProvideOptions(
		di.Tags{{
			Name: command.TagListenerFactory,
			Args: di.Args{{
				Key:   modifier.ArgNameKey,
				Value: name,
			}},
		}},
	)
}

// AsKafkaBatchListenerFactory is syntax sugar for the di container.
func AsKafkaBatchListenerFactory(name string) di.ProvideOption {
	return di.ProvideOptions(
		di.Tags{{
			Name: command.TagBatchListenerFactory,
			Args: di.Args{{
				Key:   modifier.ArgNameKey,
				Value: name,
			}},
		}},
	)
}

// AsKafkaMiddleware is syntax sugar for the di container.
func AsKafkaMiddleware() di.ProvideOption {
	return di.ProvideOptions(
		di.As(new(kafkaapi.Middleware)),
		di.Tags{{
			Name: command.TagMiddleware,
		}},
	)
}
