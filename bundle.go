// Package kafka contains gozix bundle implementation.
package kafka

import (
	"github.com/gozix/di"
	gzGlue "github.com/gozix/glue/v3"
	"github.com/gozix/kafka/internal/command"
	"github.com/gozix/kafka/logger"
	"github.com/gozix/kafka/monitor"
	gzPrometheus "github.com/gozix/prometheus/v2"
	gzZap "github.com/gozix/zap/v3"
)

// Bundle implements the gzGlue.Bundle interface.
type (
	// Option interface.
	Option interface {
		apply(b *Bundle)
	}

	// optionFunc wraps a func, so it satisfies the Option interface.
	optionFunc func(b *Bundle)
	Bundle     struct {
		logger  di.Function
		monitor di.Function
	}
)

// BundleName is default definition name.
const BundleName = "kafka"

// Compile time check.
var (
	_ gzGlue.Bundle          = (*Bundle)(nil)
	_ gzGlue.BundleDependsOn = (*Bundle)(nil)
)

// NewBundle create bundle instance.
func NewBundle(options ...Option) *Bundle {
	var b = &Bundle{}

	for _, option := range options {
		option.apply(b)
	}
	return b
}

// Name implements the gzGlue.Bundle interface.
func (b *Bundle) Name() string {
	return BundleName
}

// Build implements the gzGlue.Bundle interface.
func (b *Bundle) Build(builder di.Builder) error {
	var tagSubcommand = "kafka.cmd.subcommand"
	if b.logger == nil {
		b.logger = logger.NewNopLogger
	}
	if b.monitor == nil {
		b.monitor = monitor.NewNop
	}
	return builder.Apply(
		// commands
		di.Provide(command.NewKafka, di.Constraint(0, di.WithTags(tagSubcommand)), gzGlue.AsCliCommand()),
		di.Provide(command.NewKafkaListener, di.Tags{{Name: tagSubcommand}}),

		// publisher
		// example for register default publisher
		// di.Provide(kafkaapi.NewPublisherWithName(client.DEFAULT, nil), di.As(new(DefaultPublisher))),

		// logger
		di.Provide(b.logger),
		// monitor
		di.Provide(b.monitor),
	)
}

func WithInternalLogger(logger di.Function) Option {
	return optionFunc(func(b *Bundle) {
		b.logger = logger
	})
}

func WithMonitor(monitor di.Function) Option {
	return optionFunc(func(b *Bundle) {
		b.monitor = monitor
	})
}

// apply implements Option.
func (f optionFunc) apply(bundle *Bundle) {
	f(bundle)
}

// DependsOn implements the gzGlue.DependsOn interface.
func (b *Bundle) DependsOn() []string {
	return []string{
		gzZap.BundleName,
		gzPrometheus.BundleName,
	}
}
