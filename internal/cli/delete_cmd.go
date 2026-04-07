package cli

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

type resourceIdentifier struct {
	name      string
	namespace string
}

func newDeleteCommand() *cobra.Command {
	var namespace string
	var allNamespaces bool
	var selector string
	var force bool
	var maxConcurrency int

	cmd := &cobra.Command{
		Use:   "delete <resource-type> [resource-name...]",
		Short: "Delete resources",
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return completeResourceTypes(cmd, toComplete)
			}
			if len(args) == 1 {
				return completeResourceNames(cmd, args[0], toComplete, namespace, allNamespaces)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			resourceType := args[0]
			resourceNames := args[1:]
			ns := resolveNamespace(ctx, namespace, allNamespaces)

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			if maxConcurrency <= 0 {
				maxConcurrency = 10
			}

			return deleteResources(ctx, dynamicClient, resourceType, resourceNames, ns, allNamespaces, selector, force, maxConcurrency, cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "Delete resources in all namespaces")
	cmd.Flags().StringVarP(&selector, "selector", "l", "", "Selector (label query) to filter on")
	cmd.Flags().BoolVar(&force, "force", false, "Immediately remove resources from API and bypass graceful deletion")
	cmd.Flags().IntVar(&maxConcurrency, "max-concurrency", 10, "Maximum number of concurrent deletions")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func deleteResources(ctx context.Context, dynamicClient dynamic.Interface, resourceType string, resourceNames []string, namespace string, allNamespaces bool, selector string, force bool, maxConcurrency int, out io.Writer, errOut io.Writer) error {
	resolver, err := getResourceResolver(ctx)
	if err != nil {
		return fmt.Errorf("failed to get resource resolver: %w", err)
	}

	gvr, namespaced, err := resolver.resolveResource(resourceType)
	if err != nil {
		return fmt.Errorf("resource type %q not found: %w", resourceType, err)
	}

	var resourcesToDelete []resourceIdentifier

	if selector != "" {
		opts := metav1.ListOptions{LabelSelector: selector}
		var list *unstructured.UnstructuredList

		if !namespaced {
			list, err = dynamicClient.Resource(gvr).List(ctx, opts)
		} else if allNamespaces {
			list, err = dynamicClient.Resource(gvr).Namespace("").List(ctx, opts)
		} else {
			list, err = dynamicClient.Resource(gvr).Namespace(namespace).List(ctx, opts)
		}
		if err != nil {
			return fmt.Errorf("failed to list resources: %w", err)
		}

		for _, item := range list.Items {
			name, _, _ := unstructured.NestedString(item.Object, "metadata", "name")
			ns, _, _ := unstructured.NestedString(item.Object, "metadata", "namespace")
			resourcesToDelete = append(resourcesToDelete, resourceIdentifier{name: name, namespace: ns})
		}
	} else if len(resourceNames) > 0 {
		for _, name := range resourceNames {
			resourcesToDelete = append(resourcesToDelete, resourceIdentifier{name: name, namespace: namespace})
		}
	} else {
		return fmt.Errorf("must specify resource names or use --selector")
	}

	if len(resourcesToDelete) == 0 {
		fmt.Fprintf(out, "No resources found to delete.\n")
		return nil
	}

	type deleteResult struct {
		resource resourceIdentifier
		err      error
	}

	resourceChan := make(chan resourceIdentifier, len(resourcesToDelete))
	resultChan := make(chan deleteResult, len(resourcesToDelete))

	for _, res := range resourcesToDelete {
		resourceChan <- res
	}
	close(resourceChan)

	var wg sync.WaitGroup
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for res := range resourceChan {
				err := deleteSingleResource(ctx, dynamicClient, gvr, namespaced, res.name, res.namespace, force)
				resultChan <- deleteResult{resource: res, err: err}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var deletedCount, failedCount int
	for result := range resultChan {
		if result.err != nil {
			fmt.Fprintf(errOut, "Error deleting %s: %v\n", result.resource.name, result.err)
			failedCount++
		} else {
			fmt.Fprintf(out, "%s \"%s\" deleted\n", resourceType, result.resource.name)
			deletedCount++
		}
	}

	if failedCount > 0 {
		return fmt.Errorf("failed to delete %d resource(s)", failedCount)
	}
	return nil
}

func deleteSingleResource(ctx context.Context, dynamicClient dynamic.Interface, gvr schema.GroupVersionResource, namespaced bool, resourceName string, namespace string, force bool) error {
	opts := metav1.DeleteOptions{}
	if force {
		gracePeriod := int64(0)
		opts.GracePeriodSeconds = &gracePeriod
	}

	var err error
	if !namespaced {
		err = dynamicClient.Resource(gvr).Delete(ctx, resourceName, opts)
	} else {
		if namespace == "" {
			return fmt.Errorf("namespace is required for namespaced resource")
		}
		err = dynamicClient.Resource(gvr).Namespace(namespace).Delete(ctx, resourceName, opts)
	}

	if meta.IsNoMatchError(err) {
		return fmt.Errorf("resource type is not available in this cluster")
	}
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("resource %q not found", resourceName)
	}
	return err
}
