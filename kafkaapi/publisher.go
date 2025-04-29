package kafkaapi

import (
	"github.com/IBM/sarama"
	"github.com/spf13/viper"
	"gitlab.mobbtech.com/gozix/kafka/internal/client"
	"gitlab.mobbtech.com/gozix/kafka/logger"
)

type (
	// ProducerMessage alias for sarama.ProducerMessage for fewer imports
	ProducerMessage = sarama.ProducerMessage
	// Publisher is responsible for sending messages to a Kafka topic.
	Publisher interface {
		Publish(message *ProducerMessage) error
	}
)

// NewPublisherWithName provide easy way to register publisher
func NewPublisherWithName(name string, config *Config) func(cfg *viper.Viper, log logger.InternalLogger) (Publisher, error) {
	return func(cfg *viper.Viper, log logger.InternalLogger) (Publisher, error) {
		var f, errFactory = client.NewFactory(cfg, log)
		if errFactory != nil {
			return nil, errFactory
		}
		var opts = make([]client.Option, 0, 2)
		opts = append(opts, client.WithConnectName(name))
		if config != nil {
			opts = append(opts, client.WithConfig(*config))
		}
		var p, err = f.NewPublisher(opts...)
		if err != nil {
			return nil, err
		}
		return p, nil
	}
}
