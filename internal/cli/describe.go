package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newDescribeCommand() *cobra.Command {
	var namespace string
	var allNamespaces bool

	cmd := &cobra.Command{
		Use:   "describe <resource-type> <resource-name>",
		Short: "Show details of a specific resource",
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return completeResourceTypes(cmd, toComplete)
			}
			if len(args) == 1 {
				return completeResourceNames(cmd, args[0], toComplete, namespace, allNamespaces)
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
			ns := resolveNamespace(ctx, namespace, allNamespaces)

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			return describeResource(ctx, dynamicClient, resourceType, resourceName, ns, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "Describe resources in all namespaces")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func describeResource(ctx context.Context, dynamicClient dynamic.Interface, resourceType string, resourceName string, namespace string, out io.Writer) error {
	resolver, err := getResourceResolver(ctx)
	if err != nil {
		return fmt.Errorf("failed to get resource resolver: %w", err)
	}

	gvr, namespaced, err := resolver.resolveResource(resourceType)
	if err != nil {
		return fmt.Errorf("resource type %q not found: %w", resourceType, err)
	}

	var obj interface{}
	if !namespaced {
		obj, err = dynamicClient.Resource(gvr).Get(ctx, resourceName, metav1.GetOptions{})
	} else {
		if namespace == "" {
			return fmt.Errorf("namespace is required for namespaced resource %q", resourceType)
		}
		obj, err = dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, resourceName, metav1.GetOptions{})
	}

	if meta.IsNoMatchError(err) {
		return fmt.Errorf("resource type %q is not available in this cluster", resourceType)
	}
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("resource %q not found", resourceName)
	}
	if err != nil {
		return fmt.Errorf("failed to get resource: %w", err)
	}

	data, err := sigsyaml.Marshal(obj)
	if err != nil {
		return fmt.Errorf("failed to marshal resource: %w", err)
	}

	_, err = out.Write(data)
	return err
}
