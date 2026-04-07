package cli

import (
	"context"
	"fmt"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newConfigCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config <subcommand>",
		Short: "Modify kubeconfig files",
	}

	cmd.AddCommand(
		newConfigViewCommand(),
		newConfigGetContextsCommand(),
		newConfigCurrentContextCommand(),
		newConfigUseContextCommand(),
	)

	return cmd
}

func newConfigViewCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "view",
		Short: "Display the current kubeconfig",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
			if configPath := kube.ConfigPathFromContext(cmd.Context()); configPath != "" {
				loadingRules.ExplicitPath = configPath
			}

			config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{}).RawConfig()
			if err != nil {
				return fmt.Errorf("failed to load kubeconfig: %w", err)
			}

			data, err := clientcmd.Write(config)
			if err != nil {
				return fmt.Errorf("failed to serialize kubeconfig: %w", err)
			}

			_, err = cmd.OutOrStdout().Write(data)
			return err
		},
	}
}

func newConfigGetContextsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "get-contexts",
		Short: "List all contexts in kubeconfig",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
			if configPath := kube.ConfigPathFromContext(cmd.Context()); configPath != "" {
				loadingRules.ExplicitPath = configPath
			}

			config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{}).RawConfig()
			if err != nil {
				return fmt.Errorf("failed to load kubeconfig: %w", err)
			}

			var names []string
			for name := range config.Contexts {
				names = append(names, name)
			}
			sort.Strings(names)

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 1, 8, 3, ' ', 0)
			fmt.Fprintln(w, "CURRENT\tNAME\tCLUSTER\tAUTHINFO\tNAMESPACE")
			for _, name := range names {
				ctx := config.Contexts[name]
				current := ""
				if name == config.CurrentContext {
					current = "*"
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", current, name, ctx.Cluster, ctx.AuthInfo, ctx.Namespace)
			}
			return w.Flush()
		},
	}
}

func newConfigCurrentContextCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "current-context",
		Short: "Display the current context",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			contextName, _, err := kube.GetCurrentContext(ctx)
			if err != nil {
				return err
			}

			fmt.Fprintln(cmd.OutOrStdout(), contextName)
			return nil
		},
	}
}

func newConfigUseContextCommand() *cobra.Command {
	return &cobra.Command{
		Use:               "use-context <context-name>",
		Short:             "Set the current context in kubeconfig",
		ValidArgsFunction: completeContextNames,
		Args:              cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			contextName := args[0]

			loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
			if configPath := kube.ConfigPathFromContext(cmd.Context()); configPath != "" {
				loadingRules.ExplicitPath = configPath
			}

			config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{}).RawConfig()
			if err != nil {
				return fmt.Errorf("failed to load kubeconfig: %w", err)
			}

			if _, exists := config.Contexts[contextName]; !exists {
				return fmt.Errorf("context %q not found in kubeconfig", contextName)
			}

			config.CurrentContext = contextName

			configAccess := clientcmd.NewDefaultPathOptions()
			if configPath := kube.ConfigPathFromContext(cmd.Context()); configPath != "" {
				configAccess.LoadingRules.ExplicitPath = configPath
			}

			if err := clientcmd.ModifyConfig(configAccess, config, true); err != nil {
				return fmt.Errorf("failed to update kubeconfig: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Switched to context %q.\n", contextName)
			return nil
		},
	}
}
