package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/liadyahav/ocp-cli/internal/pkg/sshutil"
)

func newNodeJournalCommand() *cobra.Command {
	var user string
	var identityFile string
	var maxRetries int
	var unit string
	var lines int
	var since string
	var follow bool
	var grep string

	cmd := &cobra.Command{
		Use:               "journal <node>",
		Short:             "View node systemd journal logs",
		Long:              `View systemd journal logs from a node via SSH. Supports filtering by unit, time range, and pattern.`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeNodeNames,
		Example: `  # View last 100 lines of kubelet logs
  ocp node journal worker-0

  # Follow crio logs
  ocp node journal worker-0 --unit=crio --follow

  # View NetworkManager logs from last 2 hours
  ocp node journal master-0 --unit=NetworkManager --since=2h

  # Search for OOM kills
  ocp node journal worker-1 --grep="oom-kill"`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := ensureContext(cmd.Context())
			nodeName := args[0]

			ip, err := resolveNodeIP(ctx, nodeName)
			if err != nil {
				return fmt.Errorf("failed to resolve node IP: %w", err)
			}

			resolvedKey, err := resolveIdentityFile(identityFile)
			if err != nil {
				return err
			}

			// Build journalctl command
			var parts []string
			parts = append(parts, "sudo", "journalctl")
			parts = append(parts, "-u", unit)
			if !follow {
				parts = append(parts, fmt.Sprintf("--lines=%d", lines))
			}
			if since != "" {
				parts = append(parts, fmt.Sprintf("--since='%s ago'", since))
			}
			if follow {
				parts = append(parts, "-f")
			}
			if grep != "" {
				parts = append(parts, fmt.Sprintf("--grep='%s'", grep))
			}
			parts = append(parts, "--no-pager")

			runner := sshutil.NewRunner(user, resolvedKey)
			runner.MaxRetries = maxRetries
			runner.Stdout = cmd.OutOrStdout()
			runner.Stderr = cmd.ErrOrStderr()

			return runner.Run(ctx, ip, strings.Join(parts, " "))
		},
	}

	cmd.Flags().StringVarP(&user, "user", "u", "core", "SSH username")
	cmd.Flags().StringVarP(&identityFile, "identity", "i", "", "SSH identity file")
	cmd.Flags().IntVar(&maxRetries, "max-retries", 3, "Max SSH retries")
	cmd.Flags().StringVar(&unit, "unit", "kubelet", "Systemd unit to query")
	cmd.Flags().IntVar(&lines, "lines", 100, "Number of lines to show")
	cmd.Flags().StringVar(&since, "since", "", "Show logs since duration (e.g. 1h, 30m)")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Follow log output")
	cmd.Flags().StringVar(&grep, "grep", "", "Filter by pattern")

	_ = cmd.RegisterFlagCompletionFunc("unit", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		units := []string{"kubelet", "crio", "containerd", "NetworkManager", "chronyd"}
		return filterCompletions(units, toComplete), cobra.ShellCompDirectiveNoFileComp
	})

	return cmd
}
