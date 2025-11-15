package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newProjectCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "project [project-name]",
		Short: "Set or display the current project namespace",
		Long: `Set or display the current project namespace.

The project namespace is used as the default namespace for all namespace-scoped commands.
You can override it by using the --namespace flag on individual commands.

Examples:
  # Set the current project
  ocp project my-namespace
  
  # Display the current project
  ocp project`,
		Args: cobra.MaximumNArgs(1),
		Example: `  # Set the current project
  ocp project default
  
  # Display the current project
  ocp project`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			// If no args, display current project
			if len(args) == 0 {
				// Check file first (source of truth)
				projectNS := getProjectNamespace()
				if projectNS == "" {
					// Try to get from kubeconfig context
					projectNS = getCurrentNamespace(ctx)
					if projectNS == "" {
						fmt.Fprintf(cmd.OutOrStdout(), "No project is currently set\n")
						return nil
					}
					fmt.Fprintf(cmd.OutOrStdout(), "Using project %q (from kubeconfig)\n", projectNS)
				} else {
					fmt.Fprintf(cmd.OutOrStdout(), "Using project %q\n", projectNS)
				}
				return nil
			}

			// Set project namespace
			projectName := args[0]

			// Verify namespace exists
			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			_, err = clientset.CoreV1().Namespaces().Get(ctx, projectName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("namespace %q does not exist: %w", projectName, err)
			}

			// Store project namespace in file for persistence
			if err := setProjectNamespace(projectName); err != nil {
				return fmt.Errorf("failed to set project namespace: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Now using project %q\n", projectName)
			return nil
		},
	}

	return cmd
}

// getProjectNamespaceFile returns the path to the project namespace file
func getProjectNamespaceFile() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".ocp", "project"), nil
}

// getProjectNamespace retrieves the stored project namespace from file
func getProjectNamespace() string {
	filePath, err := getProjectNamespaceFile()
	if err != nil {
		return ""
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return ""
	}

	namespace := string(data)
	return strings.TrimSpace(namespace)
}

// setProjectNamespace stores the project namespace to file
func setProjectNamespace(namespace string) error {
	filePath, err := getProjectNamespaceFile()
	if err != nil {
		return err
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %q: %w", dir, err)
	}

	// Write namespace to file
	if err := os.WriteFile(filePath, []byte(namespace), 0644); err != nil {
		return fmt.Errorf("failed to write project namespace file: %w", err)
	}

	return nil
}

