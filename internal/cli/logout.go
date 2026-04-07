package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newLogoutCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "logout",
		Short: "Log out by clearing credentials from kubeconfig",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			configAccess := clientcmd.NewDefaultPathOptions()
			if configPath := kube.ConfigPathFromContext(ctx); configPath != "" {
				configAccess.LoadingRules.ExplicitPath = configPath
			}

			config, err := configAccess.GetStartingConfig()
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

			// Clear auth info for current user
			if authInfo, ok := config.AuthInfos[ctxObj.AuthInfo]; ok {
				authInfo.Token = ""
				authInfo.Username = ""
				authInfo.Password = ""
				authInfo.ClientCertificate = ""
				authInfo.ClientCertificateData = nil
				authInfo.ClientKey = ""
				authInfo.ClientKeyData = nil
			}

			if err := clientcmd.ModifyConfig(configAccess, *config, true); err != nil {
				return fmt.Errorf("failed to save kubeconfig: %w", err)
			}

			// Clear stored project namespace
			if filePath, err := getProjectNamespaceFile(); err == nil {
				os.Remove(filePath)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Logged out from %q\n", currentContext)
			return nil
		},
	}
}
