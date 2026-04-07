package cli

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newPatchCommand() *cobra.Command {
	var namespace string
	var patchType string
	var patch string
	var patchFile string

	cmd := &cobra.Command{
		Use:   "patch <resource-type> <resource-name>",
		Short: "Update field(s) of a resource",
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

			if patch == "" && patchFile == "" {
				return fmt.Errorf("must specify either -p/--patch or --patch-file")
			}

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			return patchResource(ctx, dynamicClient, resourceType, resourceName, ns, patchType, patch, patchFile, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().StringVarP(&patch, "patch", "p", "", "The patch to be applied to the resource JSON file")
	cmd.Flags().StringVar(&patchFile, "patch-file", "", "The file containing the patch to be applied")
	cmd.Flags().StringVar(&patchType, "type", "strategic", "The type of patch (strategic, merge, json)")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	_ = cmd.RegisterFlagCompletionFunc("type", completePatchTypes)

	return cmd
}

func patchResource(ctx context.Context, dynamicClient dynamic.Interface, resourceType string, resourceName string, namespace string, patchType string, patch string, patchFile string, out io.Writer) error {
	resolver, err := getResourceResolver(ctx)
	if err != nil {
		return fmt.Errorf("failed to get resource resolver: %w", err)
	}

	gvr, namespaced, err := resolver.resolveResource(resourceType)
	if err != nil {
		return fmt.Errorf("resource type %q not found: %w", resourceType, err)
	}

	var patchData []byte
	if patchFile != "" {
		patchData, err = os.ReadFile(patchFile)
		if err != nil {
			return fmt.Errorf("failed to read patch file: %w", err)
		}
	} else {
		patchData = []byte(patch)
	}

	var pt types.PatchType
	switch patchType {
	case "strategic":
		pt = types.StrategicMergePatchType
	case "merge":
		pt = types.MergePatchType
	case "json":
		pt = types.JSONPatchType
	default:
		return fmt.Errorf("invalid patch type: %s (must be strategic, merge, or json)", patchType)
	}

	if !namespaced {
		_, err = dynamicClient.Resource(gvr).Patch(ctx, resourceName, pt, patchData, metav1.PatchOptions{})
	} else {
		if namespace == "" {
			return fmt.Errorf("namespace is required for namespaced resource %q", resourceType)
		}
		_, err = dynamicClient.Resource(gvr).Namespace(namespace).Patch(ctx, resourceName, pt, patchData, metav1.PatchOptions{})
	}

	if meta.IsNoMatchError(err) {
		return fmt.Errorf("resource type %q is not available in this cluster", resourceType)
	}
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("resource %q not found", resourceName)
	}
	if err != nil {
		return fmt.Errorf("failed to patch resource: %w", err)
	}

	fmt.Fprintf(out, "%s/%s patched\n", resourceType, resourceName)
	return nil
}
