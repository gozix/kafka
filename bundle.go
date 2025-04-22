// Package kafka contains gozix bundle implementation.
package kafka

import (
	"github.com/gozix/di"
	gzGlue "github.com/gozix/glue/v3"
	gzPrometheus "github.com/gozix/prometheus/v2"
	gzZap "github.com/gozix/zap/v3"
	"gitlab.mobbtech.com/gozix/kafka/internal/command"
	"gitlab.mobbtech.com/gozix/kafka/internal/monitor"
)

// Bundle implements the gzGlue.Bundle interface.
type Bundle struct {
}

// BundleName is default definition name.
const BundleName = "kafka"

// Compile time check.
var (
	_ gzGlue.Bundle          = (*Bundle)(nil)
	_ gzGlue.BundleDependsOn = (*Bundle)(nil)
)

// NewBundle create bundle instance.
func NewBundle() *Bundle {
	return &Bundle{}
}

// Name implements the gzGlue.Bundle interface.
func (b *Bundle) Name() string {
	return BundleName
}

// Build implements the gzGlue.Bundle interface.
func (b *Bundle) Build(builder di.Builder) error {
	var tagSubcommand = "kafka.cmd.subcommand"
	return builder.Apply(
		// commands
		di.Provide(command.NewKafka, di.Constraint(0, di.WithTags(tagSubcommand)), gzGlue.AsCliCommand()),
		di.Provide(command.NewKafkaListener, di.Tags{{Name: tagSubcommand}}),

		// publisher
		// example for register default publisher
		// di.Provide(usage.NewPublisherWithName(client.DEFAULT, nil), di.Tags{{Name: client.DEFAULT}}),

		// monitor
		di.Provide(monitor.NewPrometheusMonitor),
	)
}

// DependsOn implements the gzGlue.DependsOn interface.
func (b *Bundle) DependsOn() []string {
	return []string{
		gzZap.BundleName,
		gzPrometheus.BundleName,
	}
}
