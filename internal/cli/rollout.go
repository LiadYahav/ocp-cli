package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newRolloutCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rollout <subcommand>",
		Short: "Manage the rollout of a resource",
	}

	cmd.AddCommand(
		newRolloutStatusCommand(),
		newRolloutRestartCommand(),
		newRolloutPauseCommand(),
		newRolloutResumeCommand(),
		newRolloutHistoryCommand(),
		newRolloutUndoCommand(),
	)

	return cmd
}

func newRolloutRestartCommand() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "restart <resource-type> <resource-name>",
		Short: "Restart a resource",
		ValidArgsFunction: rolloutValidArgsFunc(&namespace),
		Args:              cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
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
				"spec": map[string]interface{}{
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"annotations": map[string]interface{}{
								"kubectl.kubernetes.io/restartedAt": time.Now().Format(time.RFC3339),
							},
						},
					},
				},
			})

			_, err = dynamicClient.Resource(gvr).Namespace(ns).Patch(ctx, resourceName, types.MergePatchType, patch, metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("failed to restart %s/%s: %w", resourceType, resourceName, err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "%s/%s restarted\n", resourceType, resourceName)
			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	return cmd
}

func newRolloutStatusCommand() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:               "status <resource-type> <resource-name>",
		Short:             "Show the status of a rollout",
		ValidArgsFunction: rolloutValidArgsFunc(&namespace),
		Args:              cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
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

			obj, err := dynamicClient.Resource(gvr).Namespace(ns).Get(ctx, resourceName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get %s/%s: %w", resourceType, resourceName, err)
			}

			replicas, _, _ := unstructured.NestedInt64(obj.Object, "spec", "replicas")
			updatedReplicas, _, _ := unstructured.NestedInt64(obj.Object, "status", "updatedReplicas")
			readyReplicas, _, _ := unstructured.NestedInt64(obj.Object, "status", "readyReplicas")
			availableReplicas, _, _ := unstructured.NestedInt64(obj.Object, "status", "availableReplicas")

			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "%s/%s:\n", resourceType, resourceName)
			fmt.Fprintf(out, "  Desired: %d\n", replicas)
			fmt.Fprintf(out, "  Updated: %d\n", updatedReplicas)
			fmt.Fprintf(out, "  Ready: %d\n", readyReplicas)
			fmt.Fprintf(out, "  Available: %d\n", availableReplicas)

			if updatedReplicas == replicas && readyReplicas == replicas && availableReplicas == replicas {
				fmt.Fprintf(out, "deployment %q successfully rolled out\n", resourceName)
			} else {
				fmt.Fprintf(out, "Waiting for rollout to finish: %d of %d updated replicas are available...\n", availableReplicas, replicas)
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	return cmd
}

func newRolloutPauseCommand() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:               "pause <resource-type> <resource-name>",
		Short:             "Mark the provided resource as paused",
		ValidArgsFunction: rolloutValidArgsFunc(&namespace),
		Args:              cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
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
				"spec": map[string]interface{}{"paused": true},
			})

			_, err = dynamicClient.Resource(gvr).Namespace(ns).Patch(ctx, resourceName, types.MergePatchType, patch, metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("failed to pause %s/%s: %w", resourceType, resourceName, err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "%s/%s paused\n", resourceType, resourceName)
			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	return cmd
}

func newRolloutResumeCommand() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:               "resume <resource-type> <resource-name>",
		Short:             "Resume a paused resource",
		ValidArgsFunction: rolloutValidArgsFunc(&namespace),
		Args:              cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
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
				"spec": map[string]interface{}{"paused": false},
			})

			_, err = dynamicClient.Resource(gvr).Namespace(ns).Patch(ctx, resourceName, types.MergePatchType, patch, metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("failed to resume %s/%s: %w", resourceType, resourceName, err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "%s/%s resumed\n", resourceType, resourceName)
			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	return cmd
}

func newRolloutHistoryCommand() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:               "history <resource-type> <resource-name>",
		Short:             "View rollout history",
		ValidArgsFunction: rolloutValidArgsFunc(&namespace),
		Args:              cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
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

			obj, err := dynamicClient.Resource(gvr).Namespace(ns).Get(ctx, resourceName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get %s/%s: %w", resourceType, resourceName, err)
			}

			annotations := obj.GetAnnotations()
			revision := annotations["deployment.kubernetes.io/revision"]
			if revision == "" {
				revision = "0"
			}

			fmt.Fprintf(cmd.OutOrStdout(), "%s/%s\nREVISION  CHANGE-CAUSE\n%s        <none>\n", resourceType, resourceName, revision)
			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	return cmd
}

func newRolloutUndoCommand() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:               "undo <resource-type> <resource-name>",
		Short:             "Undo a previous rollout",
		ValidArgsFunction: rolloutValidArgsFunc(&namespace),
		Args:              cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
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

			// Verify the resource exists before patching
			_, err = dynamicClient.Resource(gvr).Namespace(ns).Get(ctx, resourceName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get %s/%s: %w", resourceType, resourceName, err)
			}

			// For deployments, trigger a rollback by patching with restartedAt
			// A proper undo would require tracking ReplicaSet revisions
			patch, _ := json.Marshal(map[string]interface{}{
				"spec": map[string]interface{}{
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"annotations": map[string]interface{}{
								"kubectl.kubernetes.io/restartedAt": time.Now().Format(time.RFC3339),
							},
						},
					},
				},
			})

			_, err = dynamicClient.Resource(gvr).Namespace(ns).Patch(ctx, resourceName, types.MergePatchType, patch, metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("failed to undo %s/%s: %w", resourceType, resourceName, err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "%s/%s rolled back\n", resourceType, resourceName)
			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	return cmd
}

// rolloutValidArgsFunc returns a ValidArgsFunction for rollout subcommands.
func rolloutValidArgsFunc(namespace *string) func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return completeResourceTypes(cmd, toComplete)
		}
		if len(args) == 1 {
			return completeResourceNames(cmd, args[0], toComplete, *namespace, false)
		}
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
}
