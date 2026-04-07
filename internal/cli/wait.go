package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newWaitCommand() *cobra.Command {
	var namespace string
	var forCondition string
	var timeout time.Duration

	cmd := &cobra.Command{
		Use:   "wait <resource-type> <resource-name> --for=<condition>",
		Short: "Wait for a specific condition on a resource",
		Long: `Wait for a specific condition on one or more resources.

Conditions:
  --for=condition=Ready      Wait until condition is True
  --for=delete               Wait until resource is deleted`,
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

			if forCondition == "" {
				return fmt.Errorf("--for is required")
			}

			resourceType := args[0]
			resourceName := args[1]
			ns := resolveNamespace(ctx, namespace, false)

			resolver, err := getResourceResolver(ctx)
			if err != nil {
				return err
			}
			gvr, namespaced, err := resolver.resolveResource(resourceType)
			if err != nil {
				return fmt.Errorf("resource type %q not found: %w", resourceType, err)
			}

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return err
			}

			timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			effectiveNS := ns
			if !namespaced {
				effectiveNS = ""
			}

			if forCondition == "delete" {
				opts := metav1.ListOptions{
					FieldSelector: "metadata.name=" + resourceName,
				}
				var watcher watch.Interface
				if effectiveNS == "" {
					watcher, err = dynamicClient.Resource(gvr).Watch(timeoutCtx, opts)
				} else {
					watcher, err = dynamicClient.Resource(gvr).Namespace(effectiveNS).Watch(timeoutCtx, opts)
				}
				if err != nil {
					return fmt.Errorf("failed to watch: %w", err)
				}
				defer watcher.Stop()

				for event := range watcher.ResultChan() {
					if event.Type == watch.Deleted {
						fmt.Fprintf(cmd.OutOrStdout(), "%s/%s condition met\n", resourceType, resourceName)
						return nil
					}
				}
				return fmt.Errorf("timed out waiting for %s/%s to be deleted", resourceType, resourceName)
			}

			// Wait for condition=<type>
			conditionType := strings.TrimPrefix(forCondition, "condition=")

			opts := metav1.ListOptions{
				FieldSelector: "metadata.name=" + resourceName,
			}
			var watcher watch.Interface
			if effectiveNS == "" {
				watcher, err = dynamicClient.Resource(gvr).Watch(timeoutCtx, opts)
			} else {
				watcher, err = dynamicClient.Resource(gvr).Namespace(effectiveNS).Watch(timeoutCtx, opts)
			}
			if err != nil {
				return fmt.Errorf("failed to watch: %w", err)
			}
			defer watcher.Stop()

			for event := range watcher.ResultChan() {
				obj, ok := event.Object.(*unstructured.Unstructured)
				if !ok {
					continue
				}

				conditions, found, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
				if !found {
					continue
				}

				for _, c := range conditions {
					cMap, ok := c.(map[string]interface{})
					if !ok {
						continue
					}
					cType, _, _ := unstructured.NestedString(cMap, "type")
					cStatus, _, _ := unstructured.NestedString(cMap, "status")
					if strings.EqualFold(cType, conditionType) && cStatus == "True" {
						fmt.Fprintf(cmd.OutOrStdout(), "%s/%s condition met\n", resourceType, resourceName)
						return nil
					}
				}
			}
			return fmt.Errorf("timed out waiting for condition %q on %s/%s", conditionType, resourceType, resourceName)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	cmd.Flags().StringVar(&forCondition, "for", "", "The condition to wait for (e.g., condition=Ready, delete)")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "The length of time to wait before giving up")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	_ = cmd.RegisterFlagCompletionFunc("for", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		conditions := []string{"delete", "condition=Ready", "condition=Available", "condition=Progressing", "condition=Complete"}
		return filterCompletions(conditions, toComplete), cobra.ShellCompDirectiveNoFileComp
	})

	return cmd
}
