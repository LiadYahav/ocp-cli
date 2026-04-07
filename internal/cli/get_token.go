package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newGetTokenCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "get-token",
		Short: "Print the current authentication token",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
			if configPath := kube.ConfigPathFromContext(ctx); configPath != "" {
				loadingRules.ExplicitPath = configPath
			}

			config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{}).RawConfig()
			if err != nil {
				return fmt.Errorf("failed to load kubeconfig: %w", err)
			}

			currentContext := config.CurrentContext
			if currentContext == "" {
				return fmt.Errorf("no current context set")
			}

			ctxObj, exists := config.Contexts[currentContext]
			if !exists {
				return fmt.Errorf("context %q not found", currentContext)
			}

			authInfo, exists := config.AuthInfos[ctxObj.AuthInfo]
			if !exists {
				return fmt.Errorf("user %q not found in kubeconfig", ctxObj.AuthInfo)
			}

			if authInfo.Token == "" {
				return fmt.Errorf("no token found for current user %q", ctxObj.AuthInfo)
			}

			fmt.Fprintln(cmd.OutOrStdout(), authInfo.Token)
			return nil
		},
	}
}
