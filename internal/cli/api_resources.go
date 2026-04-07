package cli

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newAPIResourcesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "api-resources",
		Short: "Print the supported API resources on the server",
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

			_, apiResourceLists, err := discoveryClient.ServerGroupsAndResources()
			if err != nil {
				return fmt.Errorf("failed to discover resources: %w", err)
			}

			type resourceEntry struct {
				name       string
				shortNames string
				apiVersion string
				namespaced string
				kind       string
			}

			var entries []resourceEntry

			for _, list := range apiResourceLists {
				for _, r := range list.APIResources {
					if strings.Contains(r.Name, "/") {
						continue
					}

					ns := "true"
					if !r.Namespaced {
						ns = "false"
					}

					entries = append(entries, resourceEntry{
						name:       r.Name,
						shortNames: strings.Join(r.ShortNames, ","),
						apiVersion: list.GroupVersion,
						namespaced: ns,
						kind:       r.Kind,
					})
				}
			}

			sort.Slice(entries, func(i, j int) bool {
				return entries[i].name < entries[j].name
			})

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 1, 8, 3, ' ', 0)
			fmt.Fprintln(w, "NAME\tSHORTNAMES\tAPIVERSION\tNAMESPACED\tKIND")
			for _, e := range entries {
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", e.name, e.shortNames, e.apiVersion, e.namespaced, e.kind)
			}
			return w.Flush()
		},
	}

	return cmd
}
