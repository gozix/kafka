package kafka

import (
	"strconv"

	"github.com/gozix/di"

	"github.com/gozix/kafka/internal/command"
	"github.com/gozix/kafka/internal/modifier"
	"github.com/gozix/kafka/kafkaapi"
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
func AsKafkaMiddleware(priority int64) di.ProvideOption {
	return di.ProvideOptions(
		di.As(new(kafkaapi.Middleware)),
		di.Tags{{
			Name: command.TagMiddleware,
			Args: di.Args{{
				Key:   command.ArgMiddlewarePriority,
				Value: strconv.FormatInt(priority, 10),
			}},
		}},
	)
}

// AsKafkaListenerDynamic is syntax sugar for the di container.
func AsKafkaListenerDynamic(name string) di.ProvideOption {
	return di.ProvideOptions(
		di.As(new(kafkaapi.ListenerDynamic)),
		di.Tags{{
			Name: "kafka.listener_dynamic",
			Args: di.Args{{
				Key:   "name",
				Value: name,
			}},
		}},
	)
}

// AsKafkaBatchListenerDynamic is syntax sugar for the di container.
func AsKafkaBatchListenerDynamic(name string) di.ProvideOption {
	return di.ProvideOptions(
		di.As(new(kafkaapi.BatchListenerDynamic)),
		di.Tags{{
			Name: "kafka.batch_listener_dynamic",
			Args: di.Args{{
				Key:   "name",
				Value: name,
			}},
		}},
	)
}
