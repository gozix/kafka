package client

import (
	"errors"

	"github.com/IBM/sarama"
	"github.com/spf13/viper"

	"github.com/gozix/kafka/logger"
)

type Factory interface {
	NewListener(options ...Option) (Listener, error)
	NewPublisher(options ...Option) (Publisher, error)
}

type factory struct {
	log     logger.InternalLogger
	nodes   map[string][]string
	configs map[string]*sarama.Config
}

func NewFactory(cfg *viper.Viper, log logger.InternalLogger) (Factory, error) {
	var nodes = make(map[string][]string)
	var configs = make(map[string]*sarama.Config)

	for name := range cfg.GetStringMap("kafka") {
		nodes[name] = cfg.GetStringSlice("kafka." + name + ".nodes")

		baseCfg, err := buildBaseConfigFromViper(cfg, name)
		if err != nil {
			return nil, err
		}
		configs[name] = baseCfg
	}

	return &factory{
		log:     log,
		nodes:   nodes,
		configs: configs,
	}, nil
}

func (f *factory) NewListener(options ...Option) (_ Listener, err error) {
	var opt = newClientOptions(options...)

	var nodes, ok = f.nodes[opt.connectName]
	if !ok {
		return nil, errors.New("undefined connection's name")
	}

	// default listener config
	config := ListenerDefaultConfig()
	// merge connection-level config from file
	if base := f.configs[opt.connectName]; base != nil {
		mergeSaramaConfig(config, base)
	}
	// merge explicit overrides (highest priority)
	if opt.config != nil {
		mergeSaramaConfig(config, opt.config)
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

	var nodes, ok = f.nodes[opt.connectName]
	if !ok {
		return nil, errors.New("undefined connection's name")
	}

	// default publisher config
	config := PublisherDefaultConfig()
	// merge connection-level config from file
	if base := f.configs[opt.connectName]; base != nil {
		mergeSaramaConfig(config, base)
	}
	// merge explicit overrides (highest priority)
	if opt.config != nil {
		mergeSaramaConfig(config, opt.config)
	}

	var producer sarama.SyncProducer
	if producer, err = sarama.NewSyncProducer(nodes, config); err != nil {
		return nil, err
	}

	return &publisher{
		producer: producer,
		monitor:  opt.monitor,
	}, nil
}
