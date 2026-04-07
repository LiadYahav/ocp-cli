package cli

import (
	"context"
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newAPIVersionsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "api-versions",
		Short: "Print the supported API versions on the server",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			discoveryClient, err := kube.NewDiscoveryClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create discovery client: %w", err)
			}

			groups, err := discoveryClient.ServerGroups()
			if err != nil {
				return fmt.Errorf("failed to get server groups: %w", err)
			}

			var versions []string
			for _, group := range groups.Groups {
				for _, version := range group.Versions {
					versions = append(versions, version.GroupVersion)
				}
			}

			sort.Strings(versions)
			for _, v := range versions {
				fmt.Fprintln(cmd.OutOrStdout(), v)
			}
			return nil
		},
	}
}
