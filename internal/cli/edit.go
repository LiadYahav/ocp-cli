package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newEditCommand() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "edit <resource-type> <resource-name>",
		Short: "Edit a resource",
		Long:  "Edit a resource using the default editor (EDITOR env var, defaults to vi).",
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return completeResourceTypes(cmd, toComplete)
			}
			if len(args) == 1 {
				return completeResourceNames(cmd, args[0], toComplete, namespace, false)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			resourceType := args[0]
			resourceName := args[1]
			ns := resolveNamespace(ctx, namespace, false)

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			return editResource(ctx, dynamicClient, resourceType, resourceName, ns, cmd.InOrStdin(), cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func editResource(ctx context.Context, dynamicClient dynamic.Interface, resourceType string, resourceName string, namespace string, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
	resolver, err := getResourceResolver(ctx)
	if err != nil {
		return fmt.Errorf("failed to get resource resolver: %w", err)
	}

	gvr, namespaced, err := resolver.resolveResource(resourceType)
	if err != nil {
		return fmt.Errorf("resource type %q not found: %w", resourceType, err)
	}

	var obj *unstructured.Unstructured
	if !namespaced {
		obj, err = dynamicClient.Resource(gvr).Get(ctx, resourceName, metav1.GetOptions{})
	} else {
		if namespace == "" {
			return fmt.Errorf("namespace is required for namespaced resource %q", resourceType)
		}
		obj, err = dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, resourceName, metav1.GetOptions{})
	}
	if err != nil {
		return fmt.Errorf("failed to get resource: %w", err)
	}

	yamlData, err := sigsyaml.Marshal(obj)
	if err != nil {
		return fmt.Errorf("failed to marshal resource: %w", err)
	}

	tmpFile, err := os.CreateTemp("", "ocp-edit-*.yaml")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	if _, err := tmpFile.Write(yamlData); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = "vi"
	}

	editCmd := exec.Command(editor, tmpFile.Name())
	editCmd.Stdin = stdin
	editCmd.Stdout = stdout
	editCmd.Stderr = stderr
	if err := editCmd.Run(); err != nil {
		return fmt.Errorf("editor exited with error: %w", err)
	}

	editedData, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		return fmt.Errorf("failed to read edited file: %w", err)
	}

	var editedObj unstructured.Unstructured
	if err := sigsyaml.Unmarshal(editedData, &editedObj); err != nil {
		return fmt.Errorf("failed to parse edited YAML: %w", err)
	}

	if !namespaced {
		_, err = dynamicClient.Resource(gvr).Update(ctx, &editedObj, metav1.UpdateOptions{})
	} else {
		_, err = dynamicClient.Resource(gvr).Namespace(namespace).Update(ctx, &editedObj, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("failed to update resource: %w", err)
	}

	fmt.Fprintf(stdout, "%s/%s edited\n", resourceType, resourceName)
	return nil
}
