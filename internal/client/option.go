package client

import "github.com/IBM/sarama"

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

// apply implements Option.
func (f optionFunc) apply(o *clientOptions) {
	f(o)
}
