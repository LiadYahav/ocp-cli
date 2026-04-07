package cli

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newScaleCommand() *cobra.Command {
	var namespace string
	var replicas int32

	cmd := &cobra.Command{
		Use:   "scale <resource-type> <resource-name> --replicas=<count>",
		Short: "Set a new size for a deployment, replica set, or stateful set",
		Args:  cobra.ExactArgs(2),
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return completeResourceTypes(cmd, toComplete)
			}
			if len(args) == 1 {
				return completeResourceNames(cmd, args[0], toComplete, namespace, false)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			if !cmd.Flags().Changed("replicas") {
				return fmt.Errorf("--replicas is required")
			}

			resourceType := args[0]
			resourceName := args[1]
			ns := resolveNamespace(ctx, namespace, false)

			resolver, err := getResourceResolver(ctx)
			if err != nil {
				return err
			}
			gvr, _, err := resolver.resolveResource(resourceType)
			if err != nil {
				return fmt.Errorf("resource type %q not found: %w", resourceType, err)
			}

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return err
			}

			patch, _ := json.Marshal(map[string]interface{}{
				"spec": map[string]interface{}{"replicas": replicas},
			})

			_, err = dynamicClient.Resource(gvr).Namespace(ns).Patch(ctx, resourceName, types.MergePatchType, patch, metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("failed to scale %s/%s: %w", resourceType, resourceName, err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "%s/%s scaled to %d\n", resourceType, resourceName, replicas)
			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	cmd.Flags().Int32Var(&replicas, "replicas", 0, "The new desired number of replicas")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}
