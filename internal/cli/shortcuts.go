package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newEventsCommand() *cobra.Command {
	var namespace string
	var allNamespaces bool

	cmd := &cobra.Command{
		Use:   "events",
		Short: "List events sorted by creation timestamp",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			ns := resolveNamespace(ctx, namespace, allNamespaces)
			return getResource(ctx, "events", "", ns, allNamespaces, "", "", "", false, ".metadata.creationTimestamp", false, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "All namespaces")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	return cmd
}

func newContextCommand() *cobra.Command {
	return &cobra.Command{
		Use:               "context [name]",
		Short:             "Show current context or switch to a new one",
		ValidArgsFunction: completeContextNames,
		Args:              cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			if len(args) == 1 {
				// Switch context
				contextName := args[0]
				loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
				if configPath := kube.ConfigPathFromContext(ctx); configPath != "" {
					loadingRules.ExplicitPath = configPath
				}

				config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{}).RawConfig()
				if err != nil {
					return err
				}

				if _, exists := config.Contexts[contextName]; !exists {
					return fmt.Errorf("context %q not found", contextName)
				}

				config.CurrentContext = contextName
				configAccess := clientcmd.NewDefaultPathOptions()
				if configPath := kube.ConfigPathFromContext(ctx); configPath != "" {
					configAccess.LoadingRules.ExplicitPath = configPath
				}
				if err := clientcmd.ModifyConfig(configAccess, config, true); err != nil {
					return err
				}

				fmt.Fprintf(cmd.OutOrStdout(), "Switched to context %q.\n", contextName)
				return nil
			}

			// Show current context
			contextName, clusterName, err := kube.GetCurrentContext(ctx)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Current context: %s\n", contextName)
			fmt.Fprintf(cmd.OutOrStdout(), "Cluster: %s\n", clusterName)
			return nil
		},
	}
}

func newWhoamiCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "whoami",
		Short: "Show the current user, context, cluster, and namespace",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			contextName, clusterName, err := kube.GetCurrentContext(ctx)
			if err != nil {
				return err
			}

			ns, _ := kube.GetCurrentNamespace(ctx)
			if ns == "" {
				ns = "default"
			}

			// Get auth info from raw config
			loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
			if configPath := kube.ConfigPathFromContext(ctx); configPath != "" {
				loadingRules.ExplicitPath = configPath
			}
			config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{}).RawConfig()
			if err != nil {
				return err
			}

			user := "<unknown>"
			if ctxObj, ok := config.Contexts[contextName]; ok {
				user = ctxObj.AuthInfo
			}

			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "User:      %s\n", user)
			fmt.Fprintf(out, "Context:   %s\n", contextName)
			fmt.Fprintf(out, "Cluster:   %s\n", clusterName)
			fmt.Fprintf(out, "Namespace: %s\n", ns)
			return nil
		},
	}
}

func newResourcesCommand() *cobra.Command {
	return newAPIResourcesCommand()
}
