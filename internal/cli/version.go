package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	// Version is set at build time via -ldflags.
	Version = "dev"
	// Commit is set at build time via -ldflags.
	Commit = "unknown"
)

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print CLI version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintf(cmd.OutOrStdout(), "ocp version %s (commit %s)\n", Version, Commit)
		},
	}
}
