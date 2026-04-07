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

func newExtractCommand() *cobra.Command {
	var namespace string
	var destDir string
	var confirm bool
	var keys []string

	cmd := &cobra.Command{
		Use:   "extract <secret/name|configmap/name>",
		Short: "Extract secret or configmap contents to files",
		Long: `Extract the contents of a secret or configmap to individual files.

Each key in the resource becomes a file in the destination directory.

Examples:
  # Extract a secret to the current directory
  ocp extract secret/my-secret

  # Extract to a specific directory
  ocp extract configmap/my-config --to=./config

  # Extract specific keys only
  ocp extract secret/my-secret --keys=tls.crt,tls.key

  # Overwrite existing files
  ocp extract secret/my-secret --confirm`,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return completeExtractTargets(cmd, toComplete, namespace)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			ns := resolveNamespace(ctx, namespace, false)
			out := cmd.OutOrStdout()

			// Parse resource type/name
			parts := strings.SplitN(args[0], "/", 2)
			if len(parts) != 2 {
				return fmt.Errorf("expected TYPE/NAME format (e.g. secret/my-secret)")
			}

			resourceType := strings.ToLower(parts[0])
			resourceName := parts[1]

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			var data map[string][]byte

			switch resourceType {
			case "secret", "secrets":
				secret, err := clientset.CoreV1().Secrets(ns).Get(ctx, resourceName, metav1.GetOptions{})
				if err != nil {
					return fmt.Errorf("failed to get secret %q: %w", resourceName, err)
				}
				data = secret.Data

			case "configmap", "configmaps", "cm":
				cm, err := clientset.CoreV1().ConfigMaps(ns).Get(ctx, resourceName, metav1.GetOptions{})
				if err != nil {
					return fmt.Errorf("failed to get configmap %q: %w", resourceName, err)
				}
				data = make(map[string][]byte, len(cm.Data)+len(cm.BinaryData))
				for k, v := range cm.Data {
					data[k] = []byte(v)
				}
				for k, v := range cm.BinaryData {
					data[k] = v
				}

			default:
				return fmt.Errorf("unsupported resource type %q (use secret or configmap)", resourceType)
			}

			// Filter by keys if specified
			keySet := make(map[string]bool)
			for _, k := range keys {
				keySet[k] = true
			}

			// Create destination directory
			if err := os.MkdirAll(destDir, 0755); err != nil {
				return fmt.Errorf("failed to create directory %q: %w", destDir, err)
			}

			extracted := 0
			for key, value := range data {
				if len(keySet) > 0 && !keySet[key] {
					continue
				}

				filePath := filepath.Join(destDir, key)

				// Check if file exists
				if _, err := os.Stat(filePath); err == nil && !confirm {
					fmt.Fprintf(out, "Skipping %s (already exists, use --confirm to overwrite)\n", filePath)
					continue
				}

				if err := os.WriteFile(filePath, value, 0644); err != nil {
					return fmt.Errorf("failed to write %q: %w", filePath, err)
				}

				fmt.Fprintf(out, "%s\n", filePath)
				extracted++
			}

			if extracted == 0 {
				fmt.Fprintln(out, "No files extracted.")
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	cmd.Flags().StringVar(&destDir, "to", ".", "Destination directory")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "Overwrite existing files")
	cmd.Flags().StringSliceVar(&keys, "keys", nil, "Extract only these keys")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

// completeExtractTargets returns secret/name and configmap/name completions
func completeExtractTargets(cmd *cobra.Command, toComplete string, namespace string) ([]string, cobra.ShellCompDirective) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	ns := resolveNamespace(ctx, namespace, false)
	completions := make([]string, 0)

	// If user hasn't typed a prefix or typed "s", offer secrets
	if toComplete == "" || strings.HasPrefix("secret/", toComplete) || strings.HasPrefix(toComplete, "secret/") {
		secrets, err := clientset.CoreV1().Secrets(ns).List(ctx, metav1.ListOptions{})
		if err == nil {
			for _, s := range secrets.Items {
				name := "secret/" + s.Name
				if strings.HasPrefix(name, toComplete) {
					completions = append(completions, name)
				}
			}
		}
	}

	// If user hasn't typed a prefix or typed "c", offer configmaps
	if toComplete == "" || strings.HasPrefix("configmap/", toComplete) || strings.HasPrefix(toComplete, "configmap/") {
		configmaps, err := clientset.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{})
		if err == nil {
			for _, cm := range configmaps.Items {
				name := "configmap/" + cm.Name
				if strings.HasPrefix(name, toComplete) {
					completions = append(completions, name)
				}
			}
		}
	}

	return completions, cobra.ShellCompDirectiveNoFileComp
}
