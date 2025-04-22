// Package command contains cli command definitions.
package command

import "github.com/spf13/cobra"

func NewKafka(subcommands []*cobra.Command) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "kafka [command]",
		Short: "Kafka Commands Group",
	}

	cmd.AddCommand(subcommands...)

	return cmd
}
