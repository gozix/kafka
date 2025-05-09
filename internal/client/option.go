package client

import (
	"github.com/IBM/sarama"

	"github.com/gozix/kafka/monitor"
)

const (
	DEFAULT = "default"
)

type (
	// Option interface.
	Option interface {
		apply(*clientOptions)
	}

	// optionFunc wraps a func, so it satisfies the Option interface.
	optionFunc func(*clientOptions)

	clientOptions struct {
		config      *sarama.Config
		connectName string
		monitor     monitor.Monitor
	}
)

func newClientOptions(options ...Option) *clientOptions {
	var opt = &clientOptions{
		connectName: DEFAULT,
	}

	for _, o := range options {
		o.apply(opt)
	}

	return opt
}

// WithConnectName option.
func WithConnectName(name string) Option {
	return optionFunc(func(o *clientOptions) {
		o.connectName = name
	})
}

// WithConfig option.
func WithConfig(config *sarama.Config) Option {
	return optionFunc(func(o *clientOptions) {
		o.config = config
	})
}

// WithMonitor option.
func WithMonitor(monitor monitor.Monitor) Option {
	return optionFunc(func(o *clientOptions) {
		o.monitor = monitor
	})
}

// apply implements Option.
func (f optionFunc) apply(o *clientOptions) {
	f(o)
}
