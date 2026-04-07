package cli

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newGetCommand() *cobra.Command {
	var output string
	var namespace string
	var allNamespaces bool
	var selector string
	var showLabels bool
	var fieldSelector string
	var sortBy string
	var watchFlag bool

	cmd := &cobra.Command{
		Use:   "get <resource-type> [resource-name]",
		Short: "Display one or many resources",
		Long: `Display one or many resources.

This command works with any resource type discovered in your cluster, including:
- Standard Kubernetes resources (pods, services, deployments, etc.)
- OpenShift resources (routes, buildconfigs, machineconfigpools, etc.)
- Custom Resource Definitions (CRDs) installed in your cluster

Supported output formats:
  - table (default): Server-side table format matching kubectl output
  - wide (-o wide): Extended table with additional columns
  - json (-o json): JSON format
  - yaml (-o yaml): YAML format
  - name (-o name): Resource names only
  - jsonpath (-o jsonpath='{.items[*].metadata.name}'): JSONPath expression
  - go-template (-o go-template='{{.metadata.name}}'): Go template
  - custom-columns (-o custom-columns=NAME:.metadata.name,STATUS:.status.phase)`,
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
			resourceName := ""
			if len(args) > 1 {
				resourceName = args[1]
			}

			if strings.EqualFold(resourceType, "all") {
				return getAllResources(ctx, namespace, allNamespaces, selector, output, showLabels, cmd.OutOrStdout())
			}

			ns := resolveNamespace(ctx, namespace, allNamespaces)

			return getResource(ctx, resourceType, resourceName, ns, allNamespaces, selector, fieldSelector, output, showLabels, sortBy, watchFlag, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "", "Output format (json, yaml, wide, name, jsonpath=..., go-template=..., custom-columns=...)")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "List resources in all namespaces")
	cmd.Flags().StringVarP(&selector, "selector", "l", "", "Selector (label query) to filter on")
	cmd.Flags().BoolVar(&showLabels, "show-labels", false, "Show all labels as the last column")
	cmd.Flags().StringVar(&fieldSelector, "field-selector", "", "Selector to filter on field (e.g., metadata.name=foo)")
	cmd.Flags().StringVar(&sortBy, "sort-by", "", "Sort list by a jsonpath expression (e.g., .metadata.creationTimestamp)")
	cmd.Flags().BoolVarP(&watchFlag, "watch", "w", false, "Watch for changes")

	_ = cmd.RegisterFlagCompletionFunc("output", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		formats := []string{"json", "yaml", "wide", "name", "jsonpath=", "go-template=", "custom-columns="}
		completions := make([]string, 0, len(formats))
		for _, f := range formats {
			if strings.HasPrefix(f, toComplete) {
				completions = append(completions, f)
			}
		}
		return completions, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
	})
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func getResource(ctx context.Context, resourceType, resourceName, namespace string, allNamespaces bool, selector, fieldSelector, output string, showLabels bool, sortBy string, watchFlag bool, out io.Writer) error {
	resolver, err := getResourceResolver(ctx)
	if err != nil {
		return fmt.Errorf("failed to get resource resolver: %w", err)
	}

	gvr, namespaced, err := resolver.resolveResource(resourceType)
	if err != nil {
		return fmt.Errorf("resource type %q not found: %w", resourceType, err)
	}

	format := parseOutputFormat(output)

	// Watch mode
	if watchFlag {
		return watchResource(ctx, gvr, namespaced, namespace, allNamespaces, selector, fieldSelector, format, showLabels, sortBy, out)
	}

	// Server-side table printing for table/wide output
	if format.isTableFormat() {
		ns := namespace
		if !namespaced {
			ns = ""
		} else if allNamespaces {
			ns = ""
		} else if ns == "" {
			ns = "default"
		}

		renderOpts := tableRenderOptions{
			wide:          format.kind == "wide",
			allNamespaces: allNamespaces,
			showLabels:    showLabels,
			sortBy:        sortBy,
		}

		// Try server-side table printing first
		printer, printerErr := newTablePrinter(ctx)
		if printerErr == nil {
			table, fetchErr := printer.fetchTable(ctx, gvr, ns, resourceName, selector, fieldSelector)
			if fetchErr == nil {
				return renderTable(table, out, renderOpts)
			}
		}

		// Fallback: fetch via dynamic client and build client-side table
		dynamicClient, err := kube.NewDynamicClient(ctx)
		if err != nil {
			return fmt.Errorf("failed to create dynamic client: %w", err)
		}

		if resourceName != "" {
			obj, err := getSingleResource(ctx, dynamicClient, gvr, resourceName, ns, namespaced)
			if err != nil {
				return err
			}
			table := buildFallbackTable([]map[string]interface{}{obj.Object}, namespaced)
			return renderTable(table, out, renderOpts)
		}

		opts := metav1.ListOptions{}
		if selector != "" {
			opts.LabelSelector = selector
		}
		if fieldSelector != "" {
			opts.FieldSelector = fieldSelector
		}

		var list *unstructured.UnstructuredList
		if !namespaced {
			list, err = dynamicClient.Resource(gvr).List(ctx, opts)
		} else if allNamespaces {
			list, err = dynamicClient.Resource(gvr).Namespace("").List(ctx, opts)
		} else {
			list, err = dynamicClient.Resource(gvr).Namespace(ns).List(ctx, opts)
		}
		if err != nil {
			return fmt.Errorf("failed to list resources: %w", err)
		}

		items := make([]map[string]interface{}, len(list.Items))
		for i, item := range list.Items {
			items[i] = item.Object
		}
		table := buildFallbackTable(items, namespaced)
		return renderTable(table, out, renderOpts)
	}

	// Non-table formats: use dynamic client
	dynamicClient, err := kube.NewDynamicClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	if resourceName != "" {
		obj, err := getSingleResource(ctx, dynamicClient, gvr, resourceName, namespace, namespaced)
		if err != nil {
			return err
		}
		return printFormattedObject(obj, format, out)
	}

	// List resources
	opts := metav1.ListOptions{}
	if selector != "" {
		opts.LabelSelector = selector
	}
	if fieldSelector != "" {
		opts.FieldSelector = fieldSelector
	}

	var list *unstructured.UnstructuredList
	if !namespaced {
		list, err = dynamicClient.Resource(gvr).List(ctx, opts)
	} else if allNamespaces {
		list, err = dynamicClient.Resource(gvr).Namespace("").List(ctx, opts)
	} else {
		ns := namespace
		if ns == "" {
			ns = "default"
		}
		list, err = dynamicClient.Resource(gvr).Namespace(ns).List(ctx, opts)
	}
	if err != nil {
		return fmt.Errorf("failed to list resources: %w", err)
	}

	return printFormattedList(list, format, out, allNamespaces)
}

func getAllResources(ctx context.Context, namespace string, allNamespaces bool, selector string, output string, showLabels bool, out io.Writer) error {
	commonResources := []string{
		"pods", "services", "deployments", "replicasets", "statefulsets",
		"daemonsets", "jobs", "configmaps", "secrets", "persistentvolumeclaims",
	}

	ns := namespace
	if allNamespaces {
		ns = ""
	} else if ns == "" {
		ns = resolveNamespace(ctx, namespace, false)
	}

	var allErrors []string
	hasOutput := false

	for _, resType := range commonResources {
		effectiveNS := ns
		if effectiveNS == "" && !allNamespaces {
			effectiveNS = "default"
		}

		err := getResource(ctx, resType, "", effectiveNS, allNamespaces, selector, "", output, showLabels, "", false, out)
		if err != nil {
			allErrors = append(allErrors, fmt.Sprintf("%s: %v", resType, err))
			continue
		}
		hasOutput = true
	}

	if !hasOutput && len(allErrors) > 0 {
		return fmt.Errorf("failed to list any resources: %s", strings.Join(allErrors, "; "))
	}

	if len(allErrors) > 0 {
		fmt.Fprintf(out, "\nWarnings:\n")
		for _, errMsg := range allErrors {
			fmt.Fprintf(out, "  %s\n", errMsg)
		}
	}

	return nil
}

func getSingleResource(ctx context.Context, dynamicClient dynamic.Interface, gvr schema.GroupVersionResource, resourceName string, namespace string, namespaced bool) (*unstructured.Unstructured, error) {
	var obj *unstructured.Unstructured
	var err error

	if !namespaced {
		obj, err = dynamicClient.Resource(gvr).Get(ctx, resourceName, metav1.GetOptions{})
	} else {
		if namespace == "" {
			return nil, fmt.Errorf("namespace is required for namespaced resource")
		}
		obj, err = dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, resourceName, metav1.GetOptions{})
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get resource %q: %w", resourceName, err)
	}

	return obj, nil
}

func listResourceNames(ctx context.Context, resourceType string, namespace string, allNamespaces bool) ([]string, error) {
	resolver, err := getResourceResolver(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get resource resolver: %w", err)
	}

	gvr, namespaced, err := resolver.resolveResource(resourceType)
	if err != nil {
		return nil, fmt.Errorf("resource type %q not found: %w", resourceType, err)
	}

	dynamicClient, err := kube.NewDynamicClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	var list *unstructured.UnstructuredList
	if !namespaced {
		list, err = dynamicClient.Resource(gvr).List(ctx, metav1.ListOptions{})
	} else if allNamespaces {
		list, err = dynamicClient.Resource(gvr).Namespace("").List(ctx, metav1.ListOptions{})
	} else {
		list, err = dynamicClient.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{})
	}
	if err != nil {
		return []string{}, nil
	}

	names := make([]string, 0, len(list.Items))
	for _, item := range list.Items {
		name, found, _ := unstructured.NestedString(item.Object, "metadata", "name")
		if found && name != "" {
			names = append(names, name)
		}
	}
	return names, nil
}

func watchResource(ctx context.Context, gvr schema.GroupVersionResource, namespaced bool, namespace string, allNamespaces bool, selector, fieldSelector string, format outputFormat, showLabels bool, sortBy string, out io.Writer) error {
	dynamicClient, err := kube.NewDynamicClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	opts := metav1.ListOptions{}
	if selector != "" {
		opts.LabelSelector = selector
	}
	if fieldSelector != "" {
		opts.FieldSelector = fieldSelector
	}

	// Print initial state using table printer
	if format.isTableFormat() {
		printer, tblErr := newTablePrinter(ctx)
		if tblErr == nil {
			ns := namespace
			if !namespaced || allNamespaces {
				ns = ""
			} else if ns == "" {
				ns = "default"
			}
			table, fetchErr := printer.fetchTable(ctx, gvr, ns, "", selector, fieldSelector)
			if fetchErr == nil {
				_ = renderTable(table, out, tableRenderOptions{
					wide:          format.kind == "wide",
					allNamespaces: allNamespaces,
					showLabels:    showLabels,
					sortBy:        sortBy,
				})
			}
		}
	}

	// Start watching
	var watcher watch.Interface
	if !namespaced {
		watcher, err = dynamicClient.Resource(gvr).Watch(ctx, opts)
	} else if allNamespaces {
		watcher, err = dynamicClient.Resource(gvr).Namespace("").Watch(ctx, opts)
	} else {
		ns := namespace
		if ns == "" {
			ns = "default"
		}
		watcher, err = dynamicClient.Resource(gvr).Namespace(ns).Watch(ctx, opts)
	}
	if err != nil {
		return fmt.Errorf("failed to watch resources: %w", err)
	}
	defer watcher.Stop()

	for event := range watcher.ResultChan() {
		obj, ok := event.Object.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		name, _, _ := unstructured.NestedString(obj.Object, "metadata", "name")
		ns, _, _ := unstructured.NestedString(obj.Object, "metadata", "namespace")

		timestamp := time.Now().Format("15:04:05")
		if allNamespaces && ns != "" {
			fmt.Fprintf(out, "%s  %s  %s/%s\n", timestamp, event.Type, ns, name)
		} else {
			fmt.Fprintf(out, "%s  %s  %s\n", timestamp, event.Type, name)
		}
	}

	return nil
}
