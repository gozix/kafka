package client

import (
	"errors"

	"github.com/IBM/sarama"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type Factory interface {
	NewListener(options ...Option) (Listener, error)
	NewPublisher(options ...Option) (Publisher, error)
}

type factory struct {
	log   *zap.Logger
	nodes map[string][]string
}

func NewFactory(cfg *viper.Viper, log *zap.Logger) (Factory, error) {
	var nodes = make(map[string][]string)
	for name := range cfg.GetStringMap("kafka") {
		nodes[name] = cfg.GetStringSlice("kafka." + name + ".nodes")
	}

	return &factory{
		log:   log,
		nodes: nodes,
	}, nil
}

func (f *factory) NewListener(options ...Option) (_ Listener, err error) {
	var opt = newClientOptions(options...)

	var config = opt.config
	if config == nil {
		config = ListenerDefaultConfig()
	}

	var nodes, ok = f.nodes[opt.connectName]
	if !ok {
		return nil, errors.New("undefined connection's name")
	}

	var client sarama.Client
	if client, err = sarama.NewClient(nodes, config); err != nil {
		return nil, err
	}

	return &listener{
		logger: f.log,
		client: client,
	}, nil
}

func (f *factory) NewPublisher(options ...Option) (_ Publisher, err error) {
	var opt = newClientOptions(options...)

	var config = opt.config
	if config == nil {
		config = PublisherDefaultConfig()
	}

	var nodes, ok = f.nodes[opt.connectName]
	if !ok {
		return nil, errors.New("undefined connection's name")
	}

	var producer sarama.SyncProducer
	if producer, err = sarama.NewSyncProducer(nodes, config); err != nil {
		return nil, err
	}

	return &publisher{
		producer: producer,
	}, nil
}
