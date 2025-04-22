package client

import (
	"time"

	"github.com/IBM/sarama"
)

type Config *sarama.Config

func ListenerDefaultConfig() Config {
	var config = sarama.NewConfig()

	config.Consumer.MaxProcessingTime = 5 * time.Minute
	config.Consumer.Group.Session.Timeout = 60 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 10 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	return config
}

func PublisherDefaultConfig() Config {
	var config = sarama.NewConfig()

	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true

	return config
}
