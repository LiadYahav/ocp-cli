// Portions of this file are based on code from Kubernetes kubectl
// under the Apache License 2.0.
// Source: https://github.com/kubernetes/kubectl
// See LICENSE and NOTICE files in the project root for full license information.

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/discovery"
	discoverycached "k8s.io/client-go/discovery/cached"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/restmapper"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

// resourceResolver provides dynamic resource discovery and resolution
type resourceResolver struct {
	discoveryClient discovery.DiscoveryInterface
	mapper          meta.RESTMapper
	resourceCache   map[string]*resourceInfo
	mu              sync.RWMutex
}

type resourceInfo struct {
	gvr           schema.GroupVersionResource
	namespaced    bool
	resourceName  string   // singular form
	resourceNames []string // plural forms and aliases
}

var (
	resolverCache = make(map[context.Context]*resourceResolver)
	resolverMu    sync.Mutex
)

// getResourceResolver returns a cached resource resolver for the context
func getResourceResolver(ctx context.Context) (*resourceResolver, error) {
	resolverMu.Lock()
	defer resolverMu.Unlock()

	if resolver, ok := resolverCache[ctx]; ok {
		return resolver, nil
	}

	discoveryClient, err := kube.NewDiscoveryClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create discovery client: %w", err)
	}

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(discoverycached.NewMemCacheClient(discoveryClient))

	var resolver *resourceResolver
	resolver = &resourceResolver{
		discoveryClient: discoveryClient,
		mapper:          mapper,
		resourceCache:   make(map[string]*resourceInfo),
	}

	// Cache resolver (note: in production, you might want to expire this cache)
	resolverCache[ctx] = resolver

	return resolver, nil
}

// discoverResources discovers all available resources from the API server
func (r *resourceResolver) discoverResources() ([]string, error) {
	r.mu.RLock()
	if len(r.resourceCache) > 0 {
		// Return cached resource names
		resources := make([]string, 0, len(r.resourceCache)*2) // Estimate: plural + aliases
		for _, info := range r.resourceCache {
			resources = append(resources, info.resourceNames...)
		}
		r.mu.RUnlock()
		return resources, nil
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock
	if len(r.resourceCache) > 0 {
		resources := make([]string, 0, len(r.resourceCache)*2)
		for _, info := range r.resourceCache {
			resources = append(resources, info.resourceNames...)
		}
		return resources, nil
	}

	// Discover all API resources
	_, apiResourceLists, err := r.discoveryClient.ServerGroupsAndResources()
	if err != nil {
		// If discovery fails, return empty list but don't fail completely
		// Some clusters may have partial discovery failures
		return []string{}, nil
	}

	resourceMap := make(map[string]*resourceInfo)

	for _, apiResourceList := range apiResourceLists {
		gv, err := schema.ParseGroupVersion(apiResourceList.GroupVersion)
		if err != nil {
			continue
		}

		for _, apiResource := range apiResourceList.APIResources {
			// Skip subresources (e.g., pods/status, pods/exec)
			if strings.Contains(apiResource.Name, "/") {
				continue
			}

			// Skip non-verb resources
			if !contains(apiResource.Verbs, "get") && !contains(apiResource.Verbs, "list") {
				continue
			}

			gvr := schema.GroupVersionResource{
				Group:    gv.Group,
				Version:  gv.Version,
				Resource: apiResource.Name,
			}

			// Use plural name as primary key
			pluralName := apiResource.Name
			resourceNames := []string{pluralName}

			// Add singular name if different
			if apiResource.SingularName != "" && apiResource.SingularName != pluralName {
				resourceNames = append(resourceNames, apiResource.SingularName)
			}

			// Add short names if available
			for _, shortName := range apiResource.ShortNames {
				resourceNames = append(resourceNames, shortName)
			}

			// Store or merge with existing entry
			if existing, exists := resourceMap[pluralName]; exists {
				// Merge resource names
				existing.resourceNames = append(existing.resourceNames, resourceNames...)
			} else {
				resourceMap[pluralName] = &resourceInfo{
					gvr:           gvr,
					namespaced:    apiResource.Namespaced,
					resourceName:  apiResource.SingularName,
					resourceNames: resourceNames,
				}
			}
		}
	}

	// Update cache
	r.resourceCache = resourceMap

	// Build resource list for completion
	resources := make([]string, 0, len(resourceMap)*2)
	for _, info := range resourceMap {
		resources = append(resources, info.resourceNames...)
	}

	return resources, nil
}

// resolveResource resolves a resource type (including aliases) to its GVR and scope
func (r *resourceResolver) resolveResource(resourceType string) (schema.GroupVersionResource, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Search cache for matching resource
	for _, info := range r.resourceCache {
		for _, name := range info.resourceNames {
			if strings.EqualFold(name, resourceType) {
				return info.gvr, info.namespaced, nil
			}
		}
	}

	// Try using REST mapper as fallback
	gvr, err := r.mapper.ResourceFor(schema.GroupVersionResource{Resource: resourceType})
	if err == nil {
		// Determine if namespaced by checking the mapper
		mapping, err := r.mapper.RESTMapping(schema.GroupKind{Group: gvr.Group, Kind: ""}, gvr.Version)
		if err == nil {
			return gvr, mapping.Scope.Name() == meta.RESTScopeNameNamespace, nil
		}
		return gvr, true, nil // Default to namespaced if we can't determine
	}

	return schema.GroupVersionResource{}, false, fmt.Errorf("resource type %q not found in cluster", resourceType)
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// completeResourceTypes returns discovered resource types for completion
func completeResourceTypes(cmd *cobra.Command, toComplete string) ([]string, cobra.ShellCompDirective) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	resolver, err := getResourceResolver(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}
	resources, err := resolver.discoverResources()
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}
	return filterCompletions(resources, toComplete), cobra.ShellCompDirectiveNoFileComp
}

// Commands are exported individually to be added directly to root

func newGetCommand() *cobra.Command {
	var output string
	var namespace string
	var allNamespaces bool
	var selector string
	var showLabels bool

	cmd := &cobra.Command{
		Use:   "get <resource-type> [resource-name]",
		Short: "Display one or many resources",
		Long: `Display one or many resources.

This command works with any resource type discovered in your cluster, including:
- Standard Kubernetes resources (pods, services, deployments, etc.)
- OpenShift resources (routes, buildconfigs, deploymentconfigs, etc.)
- Custom Resource Definitions (CRDs) installed in your cluster

Resource types are automatically discovered from the API server, so you can use
any resource type without hardcoding.

Examples:
  # List all pods
  ocp get pods

  # Get a specific pod
  ocp get pod my-pod

  # List all resources in all namespaces
  ocp get pods --all-namespaces

  # List resources with label selector
  ocp get pods -l app=myapp

  # Show pods with wide output (more columns)
  ocp get pods -owide

  # Show pods with labels
  ocp get pods --show-labels

  # List any custom resource (CRD)
  ocp get mycustomresources

  # Get a specific custom resource
  ocp get mycustomresource my-instance`,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				// Complete resource types using discovery
				ctx := cmd.Context()
				if ctx == nil {
					ctx = context.Background()
				}
				resolver, err := getResourceResolver(ctx)
				if err != nil {
					return nil, cobra.ShellCompDirectiveError
				}
				resources, err := resolver.discoverResources()
				if err != nil {
					return nil, cobra.ShellCompDirectiveError
				}
				return filterCompletions(resources, toComplete), cobra.ShellCompDirectiveNoFileComp
			}
			if len(args) == 1 {
				// Complete resource names for the given type
				return completeResourceNames(cmd, args[0], toComplete, namespace, allNamespaces)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Args: cobra.MinimumNArgs(1),
		Example: `  # List all pods
  ocp get pods

  # Get a specific deployment
  ocp get deployment my-app

  # List pods in all namespaces
  ocp get pods -A

  # List with label selector
  ocp get pods -l app=myapp

  # Show pods with wide output (includes node, IPs)
  ocp get pods -owide

  # Show pods with labels
  ocp get pods --show-labels

  # Output as JSON
  ocp get pods -o json

  # Output as YAML
  ocp get pods -o yaml

  # List any OpenShift resource (automatically discovered)
  ocp get routes
  ocp get buildconfigs
  ocp get deploymentconfigs
  ocp get imagestreams
  ocp get clusteroperators
  ocp get mcp

  # List any custom resource (CRD) - automatically discovered
  ocp get mycustomresources
  ocp get mycustomresource my-instance`,
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

			// Resolve namespace
			ns := resolveNamespace(ctx, namespace, allNamespaces)

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			return getResource(ctx, clientset, resourceType, resourceName, ns, allNamespaces, selector, output, showLabels, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "", "Output format (json, yaml, wide, name)")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "List resources in all namespaces")
	cmd.Flags().StringVarP(&selector, "selector", "l", "", "Selector (label query) to filter on")
	cmd.Flags().BoolVar(&showLabels, "show-labels", false, "When printing, show all labels as the last column")

	// Register flag completions
	_ = cmd.RegisterFlagCompletionFunc("output", completeOutputFormats)
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func newCreateCommand() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "create -f <file>",
		Short: "Create a resource from a file",
		Long: `Create a resource from a file or stdin.

This command works with any resource type discovered in your cluster, including:
- Standard Kubernetes resources
- OpenShift resources
- Custom Resource Definitions (CRDs)

Resources are created using the dynamic client, so any valid Kubernetes resource
definition will work.

Examples:
  # Create from a file
  ocp create -f deployment.yaml

  # Create from stdin
  cat deployment.yaml | ocp create -f -

  # Create multiple resources from multiple files
  ocp create -f file1.yaml -f file2.yaml

  # Create any OpenShift resource
  ocp create -f route.yaml
  ocp create -f buildconfig.yaml

  # Create any custom resource (CRD)
  ocp create -f mycustomresource.yaml`,
		Args: cobra.NoArgs,
		Example: `  # Create resources from a file
  ocp create -f deployment.yaml

  # Create from multiple files
  ocp create -f file1.yaml -f file2.yaml

  # Create from stdin
  cat route.yaml | ocp create -f -

  # Create OpenShift resources (automatically discovered)
  ocp create -f route.yaml
  ocp create -f buildconfig.yaml

  # Create any custom resource (CRD)
  ocp create -f mycustomresource.yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			fileFlag, err := cmd.Flags().GetStringSlice("filename")
			if err != nil {
				return err
			}

			if len(fileFlag) == 0 {
				cmd.PrintErrln("Error: must specify -f or --filename")
				cmd.PrintErrln()
				cmd.PrintErrln("Usage:")
				cmd.PrintErrln("  ocp create -f <file>")
				cmd.PrintErrln()
				cmd.PrintErrln("Examples:")
				cmd.PrintErrln("  ocp create -f deployment.yaml")
				cmd.PrintErrln("  ocp create -f file1.yaml -f file2.yaml")
				cmd.PrintErrln("  cat deployment.yaml | ocp create -f -")
				return fmt.Errorf("missing required flag: -f or --filename")
			}

			ns := resolveNamespace(ctx, namespace, false)

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			return createResources(ctx, clientset, dynamicClient, fileFlag, ns, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringSliceP("filename", "f", []string{}, "Filename, directory, or URL to files to use to create the resource")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")

	// Register flag completions
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func newEditCommand() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "edit <resource-type> <resource-name>",
		Short: "Edit a resource",
		Long: `Edit a resource using the default editor.

The editor is determined by the EDITOR environment variable, or defaults to vi.

This command works with any resource type discovered in your cluster, including:
- Standard Kubernetes resources
- OpenShift resources
- Custom Resource Definitions (CRDs)

Examples:
  # Edit a deployment
  ocp edit deployment my-app

  # Edit a pod
  ocp edit pod my-pod

  # Edit any OpenShift resource
  ocp edit route my-route

  # Edit any custom resource (CRD)
  ocp edit mycustomresource my-instance`,
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
		Example: `  # Edit a deployment
  ocp edit deployment my-app

  # Edit a pod
  ocp edit pod my-pod

  # Edit a Route (OpenShift)
  ocp edit route my-route

  # Edit a BuildConfig (OpenShift)
  ocp edit buildconfig my-build`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			resourceType := args[0]
			resourceName := args[1]
			ns := resolveNamespace(ctx, namespace, false)

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			return editResource(ctx, clientset, dynamicClient, resourceType, resourceName, ns, cmd.InOrStdin(), cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")

	// Register flag completions
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func newDeleteCommand() *cobra.Command {
	var namespace string
	var allNamespaces bool
	var selector string
	var force bool
	var maxConcurrency int

	cmd := &cobra.Command{
		Use:   "delete <resource-type> [resource-name]",
		Short: "Delete resources",
		Long: `Delete resources by resource type and name, or by selector.

This command works with any resource type discovered in your cluster, including:
- Standard Kubernetes resources
- OpenShift resources
- Custom Resource Definitions (CRDs)

Deletion operations on multiple resources are performed concurrently for better performance.

Examples:
  # Delete a pod
  ocp delete pod my-pod

  # Delete all pods matching a selector
  ocp delete pods -l app=myapp

  # Delete multiple resources (concurrent)
  ocp delete pod pod1 pod2 pod3

  # Delete any custom resource (CRD)
  ocp delete mycustomresource my-instance`,
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
		Example: `  # Delete a pod
  ocp delete pod my-pod

  # Delete all pods with a label
  ocp delete pods -l app=myapp

  # Delete multiple resources
  ocp delete pod pod1 pod2 pod3

  # Force delete (immediate removal)
  ocp delete pod my-pod --force

  # Delete OpenShift resources
  ocp delete route my-route
  ocp delete buildconfig my-build`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			resourceType := args[0]
			resourceNames := args[1:]

			ns := resolveNamespace(ctx, namespace, allNamespaces)

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			if maxConcurrency <= 0 {
				maxConcurrency = 10
			}

			return deleteResources(ctx, clientset, dynamicClient, resourceType, resourceNames, ns, allNamespaces, selector, force, maxConcurrency, cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "Delete resources in all namespaces")
	cmd.Flags().StringVarP(&selector, "selector", "l", "", "Selector (label query) to filter on")
	cmd.Flags().BoolVar(&force, "force", false, "Immediately remove resources from API and bypass graceful deletion")
	cmd.Flags().IntVar(&maxConcurrency, "max-concurrency", 10, "Maximum number of concurrent deletions")

	// Register flag completions
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func newDescribeCommand() *cobra.Command {
	var namespace string
	var allNamespaces bool

	cmd := &cobra.Command{
		Use:   "describe <resource-type> <resource-name>",
		Short: "Show details of a specific resource",
		Long: `Show details of a specific resource or group of resources.

This command works with any resource type discovered in your cluster, including:
- Standard Kubernetes resources
- OpenShift resources
- Custom Resource Definitions (CRDs)

Examples:
  # Describe a pod
  ocp describe pod my-pod

  # Describe a deployment
  ocp describe deployment my-app

  # Describe any OpenShift resource
  ocp describe route my-route

  # Describe any custom resource (CRD)
  ocp describe mycustomresource my-instance`,
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
		Example: `  # Describe a pod
  ocp describe pod my-pod

  # Describe a deployment
  ocp describe deployment my-app

  # Describe a node
  ocp describe node worker-0

  # Describe a service
  ocp describe service my-svc

  # Describe OpenShift resources
  ocp describe mcp worker
  ocp describe route my-route
  ocp describe buildconfig my-build
  ocp describe deploymentconfig my-app`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			resourceType := args[0]
			resourceName := args[1]
			ns := resolveNamespace(ctx, namespace, allNamespaces)

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			return describeResource(ctx, clientset, dynamicClient, resourceType, resourceName, ns, allNamespaces, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "Describe resources in all namespaces")

	// Register flag completions
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func newLogsCommand() *cobra.Command {
	var namespace string
	var container string
	var follow bool
	var previous bool
	var tailLines int
	var since time.Duration

	cmd := &cobra.Command{
		Use:   "logs <pod-name>",
		Short: "Print the logs for a container in a pod",
		Long: `Print the logs for a container in a pod.

Examples:
  # Get logs from a pod
  ocp logs my-pod

  # Follow logs
  ocp logs -f my-pod

  # Get logs from a specific container
  ocp logs my-pod -c my-container`,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return completePodNames(cmd, toComplete, namespace)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Args: cobra.ExactArgs(1),
		Example: `  # Get logs from a pod
  ocp logs my-pod

  # Follow logs
  ocp logs -f my-pod

  # Get last 100 lines
  ocp logs my-pod --tail=100

  # Get logs from a specific container
  ocp logs my-pod -c my-container

  # Get logs from previous container instance
  ocp logs my-pod --previous

  # Get logs since 5 minutes ago
  ocp logs my-pod --since=5m`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			podName := args[0]
			ns := resolveNamespace(ctx, namespace, false)

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			return getPodLogs(ctx, clientset, podName, ns, container, follow, previous, tailLines, since, cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().StringVarP(&container, "container", "c", "", "Print logs from this container")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Follow log output")
	cmd.Flags().BoolVar(&previous, "previous", false, "If true, print the logs for the previous instance of the container")
	cmd.Flags().IntVar(&tailLines, "tail", -1, "Lines of recent log file to display")
	cmd.Flags().DurationVar(&since, "since", 0, "Only return logs newer than a relative duration like 5s, 2m, or 3h")

	// Register flag completions
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func newApplyCommand() *cobra.Command {
	var namespace string
	var force bool

	cmd := &cobra.Command{
		Use:   "apply -f <file>",
		Short: "Apply a configuration to a resource",
		Long: `Apply a configuration to a resource by filename or stdin.

This command works with any resource type discovered in your cluster, including:
- Standard Kubernetes resources
- OpenShift resources
- Custom Resource Definitions (CRDs)

Resources are applied using the dynamic client, so any valid Kubernetes resource
definition will work. The command creates resources if they don't exist, or updates
them if they do.

Examples:
  # Apply from a file
  ocp apply -f deployment.yaml

  # Apply from stdin
  cat deployment.yaml | ocp apply -f -

  # Apply any OpenShift resource
  ocp apply -f route.yaml

  # Apply any custom resource (CRD)
  ocp apply -f mycustomresource.yaml`,
		Args: cobra.NoArgs,
		Example: `  # Apply resources from a file
  ocp apply -f deployment.yaml

  # Apply from multiple files
  ocp apply -f file1.yaml -f file2.yaml

  # Apply from stdin
  cat deployment.yaml | ocp apply -f -

  # Force apply (recreate if necessary)
  ocp apply -f deployment.yaml --force

  # Apply OpenShift resources
  ocp apply -f route.yaml
  ocp apply -f buildconfig.yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			fileFlag, err := cmd.Flags().GetStringSlice("filename")
			if err != nil {
				return err
			}

			if len(fileFlag) == 0 {
				cmd.PrintErrln("Error: must specify -f or --filename")
				cmd.PrintErrln()
				cmd.PrintErrln("Usage:")
				cmd.PrintErrln("  ocp apply -f <file>")
				cmd.PrintErrln()
				cmd.PrintErrln("Examples:")
				cmd.PrintErrln("  ocp apply -f deployment.yaml")
				cmd.PrintErrln("  ocp apply -f file1.yaml -f file2.yaml")
				cmd.PrintErrln("  cat deployment.yaml | ocp apply -f -")
				return fmt.Errorf("missing required flag: -f or --filename")
			}

			ns := resolveNamespace(ctx, namespace, false)

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			return applyResources(ctx, clientset, dynamicClient, fileFlag, ns, force, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringSliceP("filename", "f", []string{}, "Filename, directory, or URL to files to use to apply the resource")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().BoolVar(&force, "force", false, "Force apply, recreate resources if necessary")

	// Register flag completions
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func newPatchCommand() *cobra.Command {
	var namespace string
	var patchType string
	var patch string
	var patchFile string

	cmd := &cobra.Command{
		Use:   "patch <resource-type> <resource-name>",
		Short: "Update field(s) of a resource",
		Long: `Update field(s) of a resource using strategic merge patch, JSON merge patch, or JSON patch.

This command works with any resource type discovered in your cluster, including:
- Standard Kubernetes resources
- OpenShift resources
- Custom Resource Definitions (CRDs)

Examples:
  # Patch using JSON
  ocp patch pod my-pod -p '{"spec":{"containers":[{"name":"my-container","image":"new-image"}]}}'

  # Patch from a file
  ocp patch pod my-pod --patch-file patch.yaml

  # Patch any OpenShift resource
  ocp patch route my-route -p '{"spec":{"host":"new-host.example.com"}}'

  # Patch any custom resource (CRD)
  ocp patch mycustomresource my-instance -p '{"spec":{"replicas":3}}'`,
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
		Example: `  # Patch a pod using strategic merge patch (default)
  ocp patch pod my-pod -p '{"spec":{"containers":[{"name":"my-container","image":"new-image"}]}}'

  # Patch from file
  ocp patch pod my-pod --patch-file patch.yaml

  # Patch using JSON merge patch
  ocp patch pod my-pod --type=merge -p '{"metadata":{"labels":{"new":"label"}}}'

  # Patch using JSON patch
  ocp patch pod my-pod --type=json -p '[{"op":"replace","path":"/spec/containers/0/image","value":"new-image"}]'

  # Patch OpenShift resources
  ocp patch route my-route -p '{"spec":{"host":"new-host.example.com"}}'
  ocp patch buildconfig my-build --type=merge -p '{"spec":{"source":{"type":"Git"}}}'`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			resourceType := args[0]
			resourceName := args[1]
			ns := resolveNamespace(ctx, namespace, false)

			if patch == "" && patchFile == "" {
				cmd.PrintErrln("Error: must specify either -p/--patch or --patch-file")
				cmd.PrintErrln()
				cmd.PrintErrln("Usage:")
				cmd.PrintErrln("  ocp patch <resource-type> <resource-name> -p <patch-string>")
				cmd.PrintErrln("  ocp patch <resource-type> <resource-name> --patch-file <file>")
				cmd.PrintErrln()
				cmd.PrintErrln("Examples:")
				cmd.PrintErrln("  ocp patch pod my-pod -p '{\"spec\":{\"containers\":[{\"name\":\"my-container\",\"image\":\"new-image\"}]}}'")
				cmd.PrintErrln("  ocp patch pod my-pod --patch-file patch.yaml")
				return fmt.Errorf("missing required flag: -p/--patch or --patch-file")
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			return patchResource(ctx, clientset, dynamicClient, resourceType, resourceName, ns, patchType, patch, patchFile, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().StringVarP(&patch, "patch", "p", "", "The patch to be applied to the resource JSON file")
	cmd.Flags().StringVar(&patchFile, "patch-file", "", "The file containing the patch to be applied")
	cmd.Flags().StringVar(&patchType, "type", "strategic", "The type of patch being provided (strategic, merge, json)")

	// Register flag completions
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	_ = cmd.RegisterFlagCompletionFunc("type", completePatchTypes)

	return cmd
}

// Helper functions

func resolveNamespace(ctx context.Context, explicitNS string, allNamespaces bool) string {
	if allNamespaces {
		return ""
	}
	if explicitNS != "" {
		return explicitNS
	}
	return getCurrentNamespace(ctx)
}

// filterCompletions is now in completion.go - use that one

func completeResourceNames(cmd *cobra.Command, resourceType string, toComplete string, namespace string, allNamespaces bool) ([]string, cobra.ShellCompDirective) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	ns := resolveNamespace(ctx, namespace, allNamespaces)

	// Resource type resolution is handled by listResourceNames using discovery
	names, err := listResourceNames(ctx, clientset, resourceType, ns, allNamespaces)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	return filterCompletions(names, toComplete), cobra.ShellCompDirectiveNoFileComp
}

func completePodNames(cmd *cobra.Command, toComplete string, namespace string) ([]string, cobra.ShellCompDirective) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	ns := resolveNamespace(ctx, namespace, false)

	pods, err := clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	names := make([]string, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if strings.HasPrefix(pod.Name, toComplete) {
			names = append(names, pod.Name)
		}
	}

	return names, cobra.ShellCompDirectiveNoFileComp
}

// normalizeResourceType is now a wrapper that uses discovery-based resolution
// It returns the resource type string (for backward compatibility with existing code)
// but internally uses discovery to resolve aliases
// Note: This function now requires context. For backward compatibility, it returns the resource type as-is if discovery fails.
func normalizeResourceType(ctx context.Context, resourceType string) string {
	resolver, err := getResourceResolver(ctx)
	if err != nil {
		// If discovery fails, return as-is (backward compatibility)
		return resourceType
	}

	// Resolve to get the canonical resource name
	_, _, err = resolver.resolveResource(resourceType)
	if err != nil {
		// If resolution fails, return as-is
		return resourceType
	}

	// Find the primary (plural) name from cache
	resolver.mu.RLock()
	defer resolver.mu.RUnlock()

	for _, info := range resolver.resourceCache {
		for _, name := range info.resourceNames {
			if strings.EqualFold(name, resourceType) {
				// Return the plural form (first in resourceNames)
				if len(info.resourceNames) > 0 {
					return info.resourceNames[0]
				}
			}
		}
	}

	// If not found in cache, return as-is (discovery will handle it)
	return resourceType
}

// getOpenShiftGVR returns the GroupVersionResource for an OpenShift resource type
// Returns nil if the resource is not an OpenShift resource
func getOpenShiftGVR(resourceType string) *schema.GroupVersionResource {
	gvrMap := map[string]schema.GroupVersionResource{
		"routes":                       {Group: "route.openshift.io", Version: "v1", Resource: "routes"},
		"buildconfigs":                 {Group: "build.openshift.io", Version: "v1", Resource: "buildconfigs"},
		"builds":                       {Group: "build.openshift.io", Version: "v1", Resource: "builds"},
		"deploymentconfigs":            {Group: "apps.openshift.io", Version: "v1", Resource: "deploymentconfigs"},
		"imagestreams":                 {Group: "image.openshift.io", Version: "v1", Resource: "imagestreams"},
		"imagestreamtags":              {Group: "image.openshift.io", Version: "v1", Resource: "imagestreamtags"},
		"imagestreamimages":            {Group: "image.openshift.io", Version: "v1", Resource: "imagestreamimages"},
		"templates":                    {Group: "template.openshift.io", Version: "v1", Resource: "templates"},
		"projects":                     {Group: "project.openshift.io", Version: "v1", Resource: "projects"},
		"clusterresourcequotas":        {Group: "quota.openshift.io", Version: "v1", Resource: "clusterresourcequotas"},
		"securitycontextconstraints":   {Group: "security.openshift.io", Version: "v1", Resource: "securitycontextconstraints"},
		"networkattachmentdefinitions": {Group: "k8s.cni.cncf.io", Version: "v1", Resource: "networkattachmentdefinitions"},
		"clusteroperators":             {Group: "config.openshift.io", Version: "v1", Resource: "clusteroperators"},
		"clusterversions":              {Group: "config.openshift.io", Version: "v1", Resource: "clusterversions"},
		"machineconfigpools":           {Group: "machineconfiguration.openshift.io", Version: "v1", Resource: "machineconfigpools"},
	}

	if gvr, ok := gvrMap[resourceType]; ok {
		return &gvr
	}
	return nil
}

// isClusterScopedResource returns true if the resource is cluster-scoped
func isClusterScopedResource(resourceType string) bool {
	clusterScoped := map[string]bool{
		"nodes":                        true,
		"namespaces":                   true,
		"persistentvolumes":            true,
		"clusterroles":                 true,
		"clusterrolebindings":          true,
		"machineconfigpools":           true,
		"clusteroperators":             true,
		"clusterversions":              true,
		"clusterresourcequotas":        true,
		"securitycontextconstraints":   true,
		"networkattachmentdefinitions": true,
	}
	return clusterScoped[resourceType]
}

// listResourceNames lists resource names using discovery and dynamic client (fully generic)
func listResourceNames(ctx context.Context, clientset *kubernetes.Clientset, resourceType string, namespace string, allNamespaces bool) ([]string, error) {
	// Resolve resource type to GVR using discovery
	resolver, err := getResourceResolver(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get resource resolver: %w", err)
	}

	gvr, namespaced, err := resolver.resolveResource(resourceType)
	if err != nil {
		return nil, fmt.Errorf("resource type %q not found: %w", resourceType, err)
	}

	// Use dynamic client for all resources (fully generic)
	dynamicClient, err := kube.NewDynamicClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	var list *unstructured.UnstructuredList
	if !namespaced {
		// Cluster-scoped resource
		list, err = dynamicClient.Resource(gvr).List(ctx, metav1.ListOptions{})
	} else {
		// Namespace-scoped resource
		if allNamespaces {
			list, err = dynamicClient.Resource(gvr).Namespace("").List(ctx, metav1.ListOptions{})
		} else {
			list, err = dynamicClient.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{})
		}
	}

	if meta.IsNoMatchError(err) {
		// CRD not available - return empty list for completion
		return []string{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list resources: %w", err)
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

// Implementation functions

func getResource(ctx context.Context, clientset *kubernetes.Clientset, resourceType string, resourceName string, namespace string, allNamespaces bool, selector string, output string, showLabels bool, out io.Writer) error {
	// Use discovery to resolve resource type
	resolver, err := getResourceResolver(ctx)
	if err != nil {
		return fmt.Errorf("failed to get resource resolver: %w", err)
	}

	gvr, namespaced, err := resolver.resolveResource(resourceType)
	if err != nil {
		return fmt.Errorf("resource type %q not found: %w", resourceType, err)
	}

	// Use dynamic client for all resources (fully generic)
	dynamicClient, err := kube.NewDynamicClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	opts := metav1.ListOptions{}
	if selector != "" {
		opts.LabelSelector = selector
	}

	var list *unstructured.UnstructuredList
	if !namespaced {
		// Cluster-scoped resource
		list, err = dynamicClient.Resource(gvr).List(ctx, opts)
	} else {
		// Namespace-scoped resource
		if allNamespaces {
			list, err = dynamicClient.Resource(gvr).Namespace("").List(ctx, opts)
		} else {
			list, err = dynamicClient.Resource(gvr).Namespace(namespace).List(ctx, opts)
		}
	}

	if meta.IsNoMatchError(err) {
		return fmt.Errorf("resource type %q is not available in this cluster - the Custom Resource Definition (CRD) may not be installed", resourceType)
	}
	if err != nil {
		return fmt.Errorf("failed to list resources: %w", err)
	}

	// If resourceName specified, filter to that resource
	if resourceName != "" {
		found := false
		for _, item := range list.Items {
			name, _, _ := unstructured.NestedString(item.Object, "metadata", "name")
			if name == resourceName {
				return printResource(&item, output, out)
			}
		}
		if !found {
			return fmt.Errorf("%s %q not found", resourceType, resourceName)
		}
	}

	// Print list
	return printResourceList(list, output, out, allNamespaces, showLabels, resourceType)
}

// Helper function to get a single resource by name using discovery
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

	if meta.IsNoMatchError(err) {
		return nil, fmt.Errorf("resource type is not available in this cluster - the Custom Resource Definition (CRD) may not be installed")
	}
	if apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("resource %q not found", resourceName)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get resource: %w", err)
	}

	return obj, nil
}

// Implementation functions - All functions now use discovery-based generic resource handling
// All hardcoded switch statements have been replaced with generic discovery-based implementations

func printResource(obj runtime.Object, output string, out io.Writer) error {
	switch output {
	case "json":
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(obj)
	case "yaml":
		data, err := sigsyaml.Marshal(obj)
		if err != nil {
			return err
		}
		_, err = out.Write(data)
		return err
	case "name":
		metaObj, err := meta.Accessor(obj)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "%s\n", metaObj.GetName())
		return nil
	default:
		// Default table format
		data, err := sigsyaml.Marshal(obj)
		if err != nil {
			return err
		}
		_, err = out.Write(data)
		return err
	}
}

// createResources is defined later in the file (around line 3205)

// printResourceList is defined later in the file (around line 1725)

func printResourceList(list runtime.Object, output string, out io.Writer, allNamespaces bool, showLabels bool, resourceType string) error {
	switch output {
	case "json":
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(list)
	case "yaml":
		data, err := sigsyaml.Marshal(list)
		if err != nil {
			return err
		}
		_, err = out.Write(data)
		return err
	case "name":
		// Extract items and print names
		items, err := meta.ExtractList(list)
		if err != nil {
			return err
		}
		for _, item := range items {
			meta, err := meta.Accessor(item)
			if err != nil {
				continue
			}
			if allNamespaces {
				fmt.Fprintf(out, "%s/%s\n", meta.GetNamespace(), meta.GetName())
			} else {
				fmt.Fprintf(out, "%s\n", meta.GetName())
			}
		}
		return nil
	case "wide":
		// Wide output with more columns
		return printWideResourceList(list, out, allNamespaces, showLabels, resourceType)
	default:
		// Default table format
		return printDefaultResourceList(list, out, allNamespaces, showLabels, resourceType)
	}
}

func printDefaultResourceList(list runtime.Object, out io.Writer, allNamespaces bool, showLabels bool, resourceType string) error {
	items, err := meta.ExtractList(list)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		fmt.Fprintf(out, "No resources found.\n")
		return nil
	}

	// Use dynamic column width calculation like ocp node info
	switch resourceType {
	case "pods", "po":
		return printPodsTable(items, out, allNamespaces, showLabels, false)
	case "services", "svc":
		return printServicesTable(items, out, allNamespaces, showLabels, false)
	case "deployments", "deploy":
		return printDeploymentsTable(items, out, allNamespaces, showLabels, false)
	case "nodes", "no":
		return printNodesTable(items, out, showLabels, false)
	case "namespaces", "ns":
		return printNamespacesTable(items, out, showLabels, false)
	default:
		// Fallback to simple format
		return printSimpleTable(items, out, allNamespaces, showLabels)
	}
}

func printWideResourceList(list runtime.Object, out io.Writer, allNamespaces bool, showLabels bool, resourceType string) error {
	items, err := meta.ExtractList(list)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		fmt.Fprintf(out, "No resources found.\n")
		return nil
	}

	// Use dynamic column width calculation like ocp node info
	switch resourceType {
	case "pods", "po":
		return printPodsTable(items, out, allNamespaces, showLabels, true)
	case "services", "svc":
		return printServicesTable(items, out, allNamespaces, showLabels, true)
	case "deployments", "deploy":
		return printDeploymentsTable(items, out, allNamespaces, showLabels, true)
	case "nodes", "no":
		return printNodesTable(items, out, showLabels, true)
	case "namespaces", "ns":
		return printNamespacesTable(items, out, showLabels, true)
	default:
		// Fallback to simple format
		return printSimpleTable(items, out, allNamespaces, showLabels)
	}
}

func getReadyContainers(pod *corev1.Pod) int {
	ready := 0
	for _, status := range pod.Status.ContainerStatuses {
		if status.Ready {
			ready++
		}
	}
	return ready
}

// Table printing functions with dynamic column widths (like ocp node info)

func printPodsTable(items []runtime.Object, out io.Writer, allNamespaces bool, showLabels bool, wide bool) error {
	type podData struct {
		namespace string
		name      string
		ready     string
		status    string
		restarts  string
		age       string
		node      string
		ip        string
		nominated string
		labels    string
	}

	var podList []podData
	widths := map[string]int{
		"namespace": len("NAMESPACE"),
		"name":      len("NAME"),
		"ready":     len("READY"),
		"status":    len("STATUS"),
		"restarts":  len("RESTARTS"),
		"age":       len("AGE"),
	}

	if wide {
		widths["node"] = len("NODE")
		widths["ip"] = len("IP")
		widths["nominated"] = len("NOMINATED NODE")
	}
	if showLabels {
		widths["labels"] = len("LABELS")
	}

	// First pass: collect data and calculate widths
	for _, item := range items {
		pod, ok := item.(*corev1.Pod)
		if !ok {
			continue
		}

		ready := fmt.Sprintf("%d/%d", getReadyContainers(pod), len(pod.Spec.Containers))
		status := string(pod.Status.Phase)
		restarts := getPodRestarts(pod)
		age := formatAge(pod.CreationTimestamp.Time)

		data := podData{
			namespace: pod.Namespace,
			name:      pod.Name,
			ready:     ready,
			status:    status,
			restarts:  restarts,
			age:       age,
		}

		if wide {
			data.node = pod.Spec.NodeName
			if data.node == "" {
				data.node = "<none>"
			}
			data.ip = pod.Status.PodIP
			if data.ip == "" {
				data.ip = "<none>"
			}
			if pod.Spec.NodeName != "" {
				data.nominated = "<none>"
			} else {
				data.nominated = "<none>"
			}
		}

		if showLabels {
			labelPairs := make([]string, 0, len(pod.Labels))
			for k, v := range pod.Labels {
				labelPairs = append(labelPairs, fmt.Sprintf("%s=%s", k, v))
			}
			data.labels = strings.Join(labelPairs, ",")
			if data.labels == "" {
				data.labels = "<none>"
			}
		}

		podList = append(podList, data)

		// Update widths
		if len(data.namespace) > widths["namespace"] {
			widths["namespace"] = len(data.namespace)
		}
		if len(data.name) > widths["name"] {
			widths["name"] = len(data.name)
		}
		if len(data.ready) > widths["ready"] {
			widths["ready"] = len(data.ready)
		}
		if len(data.status) > widths["status"] {
			widths["status"] = len(data.status)
		}
		if len(data.restarts) > widths["restarts"] {
			widths["restarts"] = len(data.restarts)
		}
		if len(data.age) > widths["age"] {
			widths["age"] = len(data.age)
		}
		if wide {
			if len(data.node) > widths["node"] {
				widths["node"] = len(data.node)
			}
			if len(data.ip) > widths["ip"] {
				widths["ip"] = len(data.ip)
			}
			if len(data.nominated) > widths["nominated"] {
				widths["nominated"] = len(data.nominated)
			}
		}
		if showLabels {
			if len(data.labels) > widths["labels"] {
				widths["labels"] = len(data.labels)
			}
		}
	}

	if len(podList) == 0 {
		fmt.Fprintf(out, "No resources found.\n")
		return nil
	}

	// Build format string
	var formatParts []string
	var headerParts []string

	if allNamespaces {
		formatParts = append(formatParts, fmt.Sprintf("%%-%ds", widths["namespace"]))
		headerParts = append(headerParts, "NAMESPACE")
	}

	formatParts = append(formatParts,
		fmt.Sprintf("%%-%ds", widths["name"]),
		fmt.Sprintf("%%-%ds", widths["ready"]),
		fmt.Sprintf("%%-%ds", widths["status"]),
		fmt.Sprintf("%%-%ds", widths["restarts"]),
		fmt.Sprintf("%%-%ds", widths["age"]))
	headerParts = append(headerParts, "NAME", "READY", "STATUS", "RESTARTS", "AGE")

	if wide {
		formatParts = append(formatParts,
			fmt.Sprintf("%%-%ds", widths["ip"]),
			fmt.Sprintf("%%-%ds", widths["node"]),
			fmt.Sprintf("%%-%ds", widths["nominated"]))
		headerParts = append(headerParts, "IP", "NODE", "NOMINATED NODE")
	}

	if showLabels {
		formatParts = append(formatParts, fmt.Sprintf("%%-%ds", widths["labels"]))
		headerParts = append(headerParts, "LABELS")
	}

	dataFormat := strings.Join(formatParts, "   ") + "\n"

	// Print header
	fmt.Fprintf(out, dataFormat, toInterfaceSlice(headerParts)...)

	// Print each pod
	for _, data := range podList {
		var rowParts []interface{}
		if allNamespaces {
			rowParts = append(rowParts, data.namespace)
		}
		rowParts = append(rowParts, data.name, data.ready, data.status, data.restarts, data.age)
		if wide {
			rowParts = append(rowParts, data.ip, data.node, data.nominated)
		}
		if showLabels {
			rowParts = append(rowParts, data.labels)
		}
		fmt.Fprintf(out, dataFormat, rowParts...)
	}

	return nil
}

func printServicesTable(items []runtime.Object, out io.Writer, allNamespaces bool, showLabels bool, wide bool) error {
	type svcData struct {
		namespace  string
		name       string
		typeVal    string
		clusterIP  string
		externalIP string
		ports      string
		age        string
		selector   string
		labels     string
	}

	var svcList []svcData
	widths := map[string]int{
		"namespace":  len("NAMESPACE"),
		"name":       len("NAME"),
		"typeVal":    len("TYPE"),
		"clusterIP":  len("CLUSTER-IP"),
		"externalIP": len("EXTERNAL-IP"),
		"ports":      len("PORT(S)"),
		"age":        len("AGE"),
	}

	if wide {
		widths["selector"] = len("SELECTOR")
	}
	if showLabels {
		widths["labels"] = len("LABELS")
	}

	for _, item := range items {
		svc, ok := item.(*corev1.Service)
		if !ok {
			continue
		}

		ports := make([]string, 0, len(svc.Spec.Ports))
		for _, port := range svc.Spec.Ports {
			if port.NodePort > 0 {
				ports = append(ports, fmt.Sprintf("%d:%d/%s", port.Port, port.NodePort, port.Protocol))
			} else {
				ports = append(ports, fmt.Sprintf("%d/%s", port.Port, port.Protocol))
			}
		}
		portsStr := strings.Join(ports, ",")
		if portsStr == "" {
			portsStr = "<none>"
		}

		externalIP := "<none>"
		if len(svc.Spec.ExternalIPs) > 0 {
			externalIP = strings.Join(svc.Spec.ExternalIPs, ",")
		} else if svc.Spec.Type == corev1.ServiceTypeLoadBalancer && len(svc.Status.LoadBalancer.Ingress) > 0 {
			externalIP = svc.Status.LoadBalancer.Ingress[0].IP
			if externalIP == "" {
				externalIP = svc.Status.LoadBalancer.Ingress[0].Hostname
			}
		}

		selector := "<none>"
		if wide && len(svc.Spec.Selector) > 0 {
			selPairs := make([]string, 0, len(svc.Spec.Selector))
			for k, v := range svc.Spec.Selector {
				selPairs = append(selPairs, fmt.Sprintf("%s=%s", k, v))
			}
			selector = strings.Join(selPairs, ",")
		}

		labels := "<none>"
		if showLabels && len(svc.Labels) > 0 {
			labelPairs := make([]string, 0, len(svc.Labels))
			for k, v := range svc.Labels {
				labelPairs = append(labelPairs, fmt.Sprintf("%s=%s", k, v))
			}
			labels = strings.Join(labelPairs, ",")
		}

		data := svcData{
			namespace:  svc.Namespace,
			name:       svc.Name,
			typeVal:    string(svc.Spec.Type),
			clusterIP:  svc.Spec.ClusterIP,
			externalIP: externalIP,
			ports:      portsStr,
			age:        formatAge(svc.CreationTimestamp.Time),
			selector:   selector,
			labels:     labels,
		}

		svcList = append(svcList, data)

		// Update widths
		if len(data.namespace) > widths["namespace"] {
			widths["namespace"] = len(data.namespace)
		}
		if len(data.name) > widths["name"] {
			widths["name"] = len(data.name)
		}
		if len(data.typeVal) > widths["typeVal"] {
			widths["typeVal"] = len(data.typeVal)
		}
		if len(data.clusterIP) > widths["clusterIP"] {
			widths["clusterIP"] = len(data.clusterIP)
		}
		if len(data.externalIP) > widths["externalIP"] {
			widths["externalIP"] = len(data.externalIP)
		}
		if len(data.ports) > widths["ports"] {
			widths["ports"] = len(data.ports)
		}
		if len(data.age) > widths["age"] {
			widths["age"] = len(data.age)
		}
		if wide && len(data.selector) > widths["selector"] {
			widths["selector"] = len(data.selector)
		}
		if showLabels && len(data.labels) > widths["labels"] {
			widths["labels"] = len(data.labels)
		}
	}

	if len(svcList) == 0 {
		fmt.Fprintf(out, "No resources found.\n")
		return nil
	}

	// Build format string
	var formatParts []string
	var headerParts []string

	if allNamespaces {
		formatParts = append(formatParts, fmt.Sprintf("%%-%ds", widths["namespace"]))
		headerParts = append(headerParts, "NAMESPACE")
	}

	formatParts = append(formatParts,
		fmt.Sprintf("%%-%ds", widths["name"]),
		fmt.Sprintf("%%-%ds", widths["typeVal"]),
		fmt.Sprintf("%%-%ds", widths["clusterIP"]),
		fmt.Sprintf("%%-%ds", widths["externalIP"]),
		fmt.Sprintf("%%-%ds", widths["ports"]),
		fmt.Sprintf("%%-%ds", widths["age"]))
	headerParts = append(headerParts, "NAME", "TYPE", "CLUSTER-IP", "EXTERNAL-IP", "PORT(S)", "AGE")

	if wide {
		formatParts = append(formatParts, fmt.Sprintf("%%-%ds", widths["selector"]))
		headerParts = append(headerParts, "SELECTOR")
	}

	if showLabels {
		formatParts = append(formatParts, fmt.Sprintf("%%-%ds", widths["labels"]))
		headerParts = append(headerParts, "LABELS")
	}

	dataFormat := strings.Join(formatParts, "   ") + "\n"

	// Print header
	fmt.Fprintf(out, dataFormat, toInterfaceSlice(headerParts)...)

	// Print each service
	for _, data := range svcList {
		var rowParts []interface{}
		if allNamespaces {
			rowParts = append(rowParts, data.namespace)
		}
		rowParts = append(rowParts, data.name, data.typeVal, data.clusterIP, data.externalIP, data.ports, data.age)
		if wide {
			rowParts = append(rowParts, data.selector)
		}
		if showLabels {
			rowParts = append(rowParts, data.labels)
		}
		fmt.Fprintf(out, dataFormat, rowParts...)
	}

	return nil
}

func printDeploymentsTable(items []runtime.Object, out io.Writer, allNamespaces bool, showLabels bool, wide bool) error {
	type deployData struct {
		namespace  string
		name       string
		ready      string
		upToDate   string
		available  string
		age        string
		containers string
		images     string
		selector   string
		labels     string
	}

	var deployList []deployData
	widths := map[string]int{
		"namespace": len("NAMESPACE"),
		"name":      len("NAME"),
		"ready":     len("READY"),
		"upToDate":  len("UP-TO-DATE"),
		"available": len("AVAILABLE"),
		"age":       len("AGE"),
	}

	if wide {
		widths["containers"] = len("CONTAINERS")
		widths["images"] = len("IMAGES")
		widths["selector"] = len("SELECTOR")
	}
	if showLabels {
		widths["labels"] = len("LABELS")
	}

	for _, item := range items {
		deploy, ok := item.(*appsv1.Deployment)
		if !ok {
			continue
		}

		ready := fmt.Sprintf("%d/%d", deploy.Status.ReadyReplicas, deploy.Status.Replicas)
		upToDate := fmt.Sprintf("%d", deploy.Status.UpdatedReplicas)
		available := fmt.Sprintf("%d", deploy.Status.AvailableReplicas)

		containers := "<none>"
		images := "<none>"
		selector := "<none>"

		if wide {
			if len(deploy.Spec.Template.Spec.Containers) > 0 {
				containerNames := make([]string, 0, len(deploy.Spec.Template.Spec.Containers))
				imageNames := make([]string, 0, len(deploy.Spec.Template.Spec.Containers))
				for _, c := range deploy.Spec.Template.Spec.Containers {
					containerNames = append(containerNames, c.Name)
					imageNames = append(imageNames, c.Image)
				}
				containers = strings.Join(containerNames, ",")
				images = strings.Join(imageNames, ",")
			}

			if deploy.Spec.Selector != nil && len(deploy.Spec.Selector.MatchLabels) > 0 {
				selPairs := make([]string, 0, len(deploy.Spec.Selector.MatchLabels))
				for k, v := range deploy.Spec.Selector.MatchLabels {
					selPairs = append(selPairs, fmt.Sprintf("%s=%s", k, v))
				}
				selector = strings.Join(selPairs, ",")
			}
		}

		labels := "<none>"
		if showLabels && len(deploy.Labels) > 0 {
			labelPairs := make([]string, 0, len(deploy.Labels))
			for k, v := range deploy.Labels {
				labelPairs = append(labelPairs, fmt.Sprintf("%s=%s", k, v))
			}
			labels = strings.Join(labelPairs, ",")
		}

		data := deployData{
			namespace:  deploy.Namespace,
			name:       deploy.Name,
			ready:      ready,
			upToDate:   upToDate,
			available:  available,
			age:        formatAge(deploy.CreationTimestamp.Time),
			containers: containers,
			images:     images,
			selector:   selector,
			labels:     labels,
		}

		deployList = append(deployList, data)

		// Update widths
		if len(data.namespace) > widths["namespace"] {
			widths["namespace"] = len(data.namespace)
		}
		if len(data.name) > widths["name"] {
			widths["name"] = len(data.name)
		}
		if len(data.ready) > widths["ready"] {
			widths["ready"] = len(data.ready)
		}
		if len(data.upToDate) > widths["upToDate"] {
			widths["upToDate"] = len(data.upToDate)
		}
		if len(data.available) > widths["available"] {
			widths["available"] = len(data.available)
		}
		if len(data.age) > widths["age"] {
			widths["age"] = len(data.age)
		}
		if wide {
			if len(data.containers) > widths["containers"] {
				widths["containers"] = len(data.containers)
			}
			if len(data.images) > widths["images"] {
				widths["images"] = len(data.images)
			}
			if len(data.selector) > widths["selector"] {
				widths["selector"] = len(data.selector)
			}
		}
		if showLabels && len(data.labels) > widths["labels"] {
			widths["labels"] = len(data.labels)
		}
	}

	if len(deployList) == 0 {
		fmt.Fprintf(out, "No resources found.\n")
		return nil
	}

	// Build format string
	var formatParts []string
	var headerParts []string

	if allNamespaces {
		formatParts = append(formatParts, fmt.Sprintf("%%-%ds", widths["namespace"]))
		headerParts = append(headerParts, "NAMESPACE")
	}

	formatParts = append(formatParts,
		fmt.Sprintf("%%-%ds", widths["name"]),
		fmt.Sprintf("%%-%ds", widths["ready"]),
		fmt.Sprintf("%%-%ds", widths["upToDate"]),
		fmt.Sprintf("%%-%ds", widths["available"]),
		fmt.Sprintf("%%-%ds", widths["age"]))
	headerParts = append(headerParts, "NAME", "READY", "UP-TO-DATE", "AVAILABLE", "AGE")

	if wide {
		formatParts = append(formatParts,
			fmt.Sprintf("%%-%ds", widths["containers"]),
			fmt.Sprintf("%%-%ds", widths["images"]),
			fmt.Sprintf("%%-%ds", widths["selector"]))
		headerParts = append(headerParts, "CONTAINERS", "IMAGES", "SELECTOR")
	}

	if showLabels {
		formatParts = append(formatParts, fmt.Sprintf("%%-%ds", widths["labels"]))
		headerParts = append(headerParts, "LABELS")
	}

	dataFormat := strings.Join(formatParts, "   ") + "\n"

	// Print header
	fmt.Fprintf(out, dataFormat, toInterfaceSlice(headerParts)...)

	// Print each deployment
	for _, data := range deployList {
		var rowParts []interface{}
		if allNamespaces {
			rowParts = append(rowParts, data.namespace)
		}
		rowParts = append(rowParts, data.name, data.ready, data.upToDate, data.available, data.age)
		if wide {
			rowParts = append(rowParts, data.containers, data.images, data.selector)
		}
		if showLabels {
			rowParts = append(rowParts, data.labels)
		}
		fmt.Fprintf(out, dataFormat, rowParts...)
	}

	return nil
}

func printNodesTable(items []runtime.Object, out io.Writer, showLabels bool, wide bool) error {
	type nodeData struct {
		name             string
		status           string
		roles            string
		age              string
		version          string
		internalIP       string
		externalIP       string
		osImage          string
		kernelVersion    string
		containerRuntime string
		labels           string
	}

	var nodeList []nodeData
	widths := map[string]int{
		"name":    len("NAME"),
		"status":  len("STATUS"),
		"roles":   len("ROLES"),
		"age":     len("AGE"),
		"version": len("VERSION"),
	}

	if wide {
		widths["internalIP"] = len("INTERNAL-IP")
		widths["externalIP"] = len("EXTERNAL-IP")
		widths["osImage"] = len("OS-IMAGE")
		widths["kernelVersion"] = len("KERNEL-VERSION")
		widths["containerRuntime"] = len("CONTAINER-RUNTIME")
	}
	if showLabels {
		widths["labels"] = len("LABELS")
	}

	for _, item := range items {
		node, ok := item.(*corev1.Node)
		if !ok {
			continue
		}

		// Get node status
		var status string
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady {
				if condition.Status == corev1.ConditionTrue {
					status = "Ready"
				} else {
					status = "NotReady"
				}
				break
			}
		}
		if status == "" {
			status = "Unknown"
		}
		if node.Spec.Unschedulable {
			status += ", SchedulingDisabled"
		}

		// Get node roles
		var roles []string
		const rolePrefix = "node-role.kubernetes.io/"
		for label := range node.Labels {
			if strings.HasPrefix(label, rolePrefix) {
				role := label[len(rolePrefix):]
				if role != "" {
					roles = append(roles, role)
				}
			}
		}
		rolesStr := "<none>"
		if len(roles) > 0 {
			rolesStr = strings.Join(roles, ",")
		}

		// Get IPs
		var internalIP, externalIP string
		if wide {
			internalIP = "<none>"
			externalIP = "<none>"
			for _, addr := range node.Status.Addresses {
				if addr.Type == corev1.NodeInternalIP {
					internalIP = addr.Address
				}
				if addr.Type == corev1.NodeExternalIP {
					externalIP = addr.Address
				}
			}
		}

		data := nodeData{
			name:    node.Name,
			status:  status,
			roles:   rolesStr,
			age:     formatAge(node.CreationTimestamp.Time),
			version: node.Status.NodeInfo.KubeletVersion,
		}

		if wide {
			data.internalIP = internalIP
			data.externalIP = externalIP
			data.osImage = node.Status.NodeInfo.OSImage
			data.kernelVersion = node.Status.NodeInfo.KernelVersion
			data.containerRuntime = node.Status.NodeInfo.ContainerRuntimeVersion
		}

		if showLabels {
			labelPairs := make([]string, 0, len(node.Labels))
			for k, v := range node.Labels {
				labelPairs = append(labelPairs, fmt.Sprintf("%s=%s", k, v))
			}
			data.labels = strings.Join(labelPairs, ",")
			if data.labels == "" {
				data.labels = "<none>"
			}
		}

		nodeList = append(nodeList, data)

		// Update widths
		if len(data.name) > widths["name"] {
			widths["name"] = len(data.name)
		}
		if len(data.status) > widths["status"] {
			widths["status"] = len(data.status)
		}
		if len(data.roles) > widths["roles"] {
			widths["roles"] = len(data.roles)
		}
		if len(data.age) > widths["age"] {
			widths["age"] = len(data.age)
		}
		if len(data.version) > widths["version"] {
			widths["version"] = len(data.version)
		}
		if wide {
			if len(data.internalIP) > widths["internalIP"] {
				widths["internalIP"] = len(data.internalIP)
			}
			if len(data.externalIP) > widths["externalIP"] {
				widths["externalIP"] = len(data.externalIP)
			}
			if len(data.osImage) > widths["osImage"] {
				widths["osImage"] = len(data.osImage)
			}
			if len(data.kernelVersion) > widths["kernelVersion"] {
				widths["kernelVersion"] = len(data.kernelVersion)
			}
			if len(data.containerRuntime) > widths["containerRuntime"] {
				widths["containerRuntime"] = len(data.containerRuntime)
			}
		}
		if showLabels && len(data.labels) > widths["labels"] {
			widths["labels"] = len(data.labels)
		}
	}

	if len(nodeList) == 0 {
		fmt.Fprintf(out, "No resources found.\n")
		return nil
	}

	// Build format string
	var formatParts []string
	var headerParts []string

	formatParts = append(formatParts,
		fmt.Sprintf("%%-%ds", widths["name"]),
		fmt.Sprintf("%%-%ds", widths["status"]),
		fmt.Sprintf("%%-%ds", widths["roles"]),
		fmt.Sprintf("%%-%ds", widths["age"]),
		fmt.Sprintf("%%-%ds", widths["version"]))
	headerParts = append(headerParts, "NAME", "STATUS", "ROLES", "AGE", "VERSION")

	if wide {
		formatParts = append(formatParts,
			fmt.Sprintf("%%-%ds", widths["internalIP"]),
			fmt.Sprintf("%%-%ds", widths["externalIP"]),
			fmt.Sprintf("%%-%ds", widths["osImage"]),
			fmt.Sprintf("%%-%ds", widths["kernelVersion"]),
			fmt.Sprintf("%%-%ds", widths["containerRuntime"]))
		headerParts = append(headerParts, "INTERNAL-IP", "EXTERNAL-IP", "OS-IMAGE", "KERNEL-VERSION", "CONTAINER-RUNTIME")
	}

	if showLabels {
		formatParts = append(formatParts, fmt.Sprintf("%%-%ds", widths["labels"]))
		headerParts = append(headerParts, "LABELS")
	}

	dataFormat := strings.Join(formatParts, "   ") + "\n"

	// Print header
	fmt.Fprintf(out, dataFormat, toInterfaceSlice(headerParts)...)

	// Print each node
	for _, data := range nodeList {
		var rowParts []interface{}
		rowParts = append(rowParts, data.name, data.status, data.roles, data.age, data.version)
		if wide {
			rowParts = append(rowParts, data.internalIP, data.externalIP, data.osImage, data.kernelVersion, data.containerRuntime)
		}
		if showLabels {
			rowParts = append(rowParts, data.labels)
		}
		fmt.Fprintf(out, dataFormat, rowParts...)
	}

	return nil
}

func printNamespacesTable(items []runtime.Object, out io.Writer, showLabels bool, wide bool) error {
	type nsData struct {
		name   string
		status string
		age    string
		labels string
	}

	var nsList []nsData
	widths := map[string]int{
		"name":   len("NAME"),
		"status": len("STATUS"),
		"age":    len("AGE"),
	}

	if showLabels {
		widths["labels"] = len("LABELS")
	}

	for _, item := range items {
		ns, ok := item.(*corev1.Namespace)
		if !ok {
			continue
		}

		status := string(ns.Status.Phase)
		if status == "" {
			status = "Active"
		}

		labels := "<none>"
		if showLabels && len(ns.Labels) > 0 {
			labelPairs := make([]string, 0, len(ns.Labels))
			for k, v := range ns.Labels {
				labelPairs = append(labelPairs, fmt.Sprintf("%s=%s", k, v))
			}
			labels = strings.Join(labelPairs, ",")
		}

		data := nsData{
			name:   ns.Name,
			status: status,
			age:    formatAge(ns.CreationTimestamp.Time),
			labels: labels,
		}

		nsList = append(nsList, data)

		// Update widths
		if len(data.name) > widths["name"] {
			widths["name"] = len(data.name)
		}
		if len(data.status) > widths["status"] {
			widths["status"] = len(data.status)
		}
		if len(data.age) > widths["age"] {
			widths["age"] = len(data.age)
		}
		if showLabels && len(data.labels) > widths["labels"] {
			widths["labels"] = len(data.labels)
		}
	}

	if len(nsList) == 0 {
		fmt.Fprintf(out, "No resources found.\n")
		return nil
	}

	// Build format string
	var formatParts []string
	var headerParts []string

	formatParts = append(formatParts,
		fmt.Sprintf("%%-%ds", widths["name"]),
		fmt.Sprintf("%%-%ds", widths["status"]),
		fmt.Sprintf("%%-%ds", widths["age"]))
	headerParts = append(headerParts, "NAME", "STATUS", "AGE")

	if showLabels {
		formatParts = append(formatParts, fmt.Sprintf("%%-%ds", widths["labels"]))
		headerParts = append(headerParts, "LABELS")
	}

	dataFormat := strings.Join(formatParts, "   ") + "\n"

	// Print header
	fmt.Fprintf(out, dataFormat, toInterfaceSlice(headerParts)...)

	// Print each namespace
	for _, data := range nsList {
		var rowParts []interface{}
		rowParts = append(rowParts, data.name, data.status, data.age)
		if showLabels {
			rowParts = append(rowParts, data.labels)
		}
		fmt.Fprintf(out, dataFormat, rowParts...)
	}

	return nil
}

func printMCPTable(items []runtime.Object, out io.Writer, showLabels bool, wide bool) error {
	type mcpData struct {
		name                 string
		config               string
		updated              string
		updating             string
		degraded             string
		machineCount         string
		readyMachineCount    string
		updatedMachineCount  string
		degradedMachineCount string
		age                  string
		labels               string
	}

	var mcpList []mcpData
	widths := map[string]int{
		"name":                 len("NAME"),
		"config":               len("CONFIG"),
		"updated":              len("UPDATED"),
		"updating":             len("UPDATING"),
		"degraded":             len("DEGRADED"),
		"machineCount":         len("MACHINECOUNT"),
		"readyMachineCount":    len("READYMACHINECOUNT"),
		"updatedMachineCount":  len("UPDATEDMACHINECOUNT"),
		"degradedMachineCount": len("DEGRADEDMACHINECOUNT"),
		"age":                  len("AGE"),
	}

	if showLabels {
		widths["labels"] = len("LABELS")
	}

	for _, item := range items {
		mcp, ok := item.(*unstructured.Unstructured)
		if !ok {
			continue
		}

		name, _, _ := unstructured.NestedString(mcp.Object, "metadata", "name")
		creationTimestamp, _, _ := unstructured.NestedString(mcp.Object, "metadata", "creationTimestamp")

		// Get config from status.configuration.name
		config, _, _ := unstructured.NestedString(mcp.Object, "status", "configuration", "name")
		if config == "" {
			config = "<none>"
		}

		// Get conditions
		conditions, _, _ := unstructured.NestedSlice(mcp.Object, "status", "conditions")
		updated := "False"
		updating := "False"
		degraded := "False"

		for _, cond := range conditions {
			condMap, ok := cond.(map[string]interface{})
			if !ok {
				continue
			}
			condType, _, _ := unstructured.NestedString(condMap, "type")
			condStatus, _, _ := unstructured.NestedString(condMap, "status")
			if condStatus == "True" {
				switch condType {
				case "Updated":
					updated = "True"
				case "Updating":
					updating = "True"
				case "Degraded":
					degraded = "True"
				}
			}
		}

		// Get machine counts
		machineCount, _, _ := unstructured.NestedInt64(mcp.Object, "status", "machineCount")
		readyMachineCount, _, _ := unstructured.NestedInt64(mcp.Object, "status", "readyMachineCount")
		updatedMachineCount, _, _ := unstructured.NestedInt64(mcp.Object, "status", "updatedMachineCount")
		degradedMachineCount, _, _ := unstructured.NestedInt64(mcp.Object, "status", "degradedMachineCount")

		// Calculate age
		var age string
		if creationTimestamp != "" {
			createdTime, err := time.Parse(time.RFC3339, creationTimestamp)
			if err == nil {
				age = formatAge(createdTime)
			} else {
				age = "<unknown>"
			}
		} else {
			age = "<unknown>"
		}

		// Get labels if needed
		labels := "<none>"
		if showLabels {
			labelMap, _, _ := unstructured.NestedMap(mcp.Object, "metadata", "labels")
			if len(labelMap) > 0 {
				labelPairs := make([]string, 0, len(labelMap))
				for k, v := range labelMap {
					if vStr, ok := v.(string); ok {
						labelPairs = append(labelPairs, fmt.Sprintf("%s=%s", k, vStr))
					}
				}
				labels = strings.Join(labelPairs, ",")
			}
		}

		data := mcpData{
			name:                 name,
			config:               config,
			updated:              updated,
			updating:             updating,
			degraded:             degraded,
			machineCount:         fmt.Sprintf("%d", machineCount),
			readyMachineCount:    fmt.Sprintf("%d", readyMachineCount),
			updatedMachineCount:  fmt.Sprintf("%d", updatedMachineCount),
			degradedMachineCount: fmt.Sprintf("%d", degradedMachineCount),
			age:                  age,
			labels:               labels,
		}

		mcpList = append(mcpList, data)

		// Update widths
		if len(data.name) > widths["name"] {
			widths["name"] = len(data.name)
		}
		if len(data.config) > widths["config"] {
			widths["config"] = len(data.config)
		}
		if len(data.updated) > widths["updated"] {
			widths["updated"] = len(data.updated)
		}
		if len(data.updating) > widths["updating"] {
			widths["updating"] = len(data.updating)
		}
		if len(data.degraded) > widths["degraded"] {
			widths["degraded"] = len(data.degraded)
		}
		if len(data.machineCount) > widths["machineCount"] {
			widths["machineCount"] = len(data.machineCount)
		}
		if len(data.readyMachineCount) > widths["readyMachineCount"] {
			widths["readyMachineCount"] = len(data.readyMachineCount)
		}
		if len(data.updatedMachineCount) > widths["updatedMachineCount"] {
			widths["updatedMachineCount"] = len(data.updatedMachineCount)
		}
		if len(data.degradedMachineCount) > widths["degradedMachineCount"] {
			widths["degradedMachineCount"] = len(data.degradedMachineCount)
		}
		if len(data.age) > widths["age"] {
			widths["age"] = len(data.age)
		}
		if showLabels && len(data.labels) > widths["labels"] {
			widths["labels"] = len(data.labels)
		}
	}

	if len(mcpList) == 0 {
		fmt.Fprintf(out, "No resources found.\n")
		return nil
	}

	// Build format string
	var formatParts []string
	var headerParts []string

	formatParts = append(formatParts,
		fmt.Sprintf("%%-%ds", widths["name"]),
		fmt.Sprintf("%%-%ds", widths["config"]),
		fmt.Sprintf("%%-%ds", widths["updated"]),
		fmt.Sprintf("%%-%ds", widths["updating"]),
		fmt.Sprintf("%%-%ds", widths["degraded"]),
		fmt.Sprintf("%%-%ds", widths["machineCount"]),
		fmt.Sprintf("%%-%ds", widths["readyMachineCount"]),
		fmt.Sprintf("%%-%ds", widths["updatedMachineCount"]),
		fmt.Sprintf("%%-%ds", widths["degradedMachineCount"]),
		fmt.Sprintf("%%-%ds", widths["age"]))
	headerParts = append(headerParts, "NAME", "CONFIG", "UPDATED", "UPDATING", "DEGRADED", "MACHINECOUNT", "READYMACHINECOUNT", "UPDATEDMACHINECOUNT", "DEGRADEDMACHINECOUNT", "AGE")

	if showLabels {
		formatParts = append(formatParts, fmt.Sprintf("%%-%ds", widths["labels"]))
		headerParts = append(headerParts, "LABELS")
	}

	dataFormat := strings.Join(formatParts, "   ") + "\n"

	// Print header
	fmt.Fprintf(out, dataFormat, toInterfaceSlice(headerParts)...)

	// Print each MCP
	for _, data := range mcpList {
		var rowParts []interface{}
		rowParts = append(rowParts, data.name, data.config, data.updated, data.updating, data.degraded,
			data.machineCount, data.readyMachineCount, data.updatedMachineCount, data.degradedMachineCount, data.age)
		if showLabels {
			rowParts = append(rowParts, data.labels)
		}
		fmt.Fprintf(out, dataFormat, rowParts...)
	}

	return nil
}

func printRoutesTable(items []runtime.Object, out io.Writer, showLabels bool, allNamespaces bool, wide bool) error {
	type routeData struct {
		namespace   string
		name        string
		host        string
		path        string
		services    string
		port        string
		termination string
		wildcard    string
		age         string
		labels      string
	}

	var routeList []routeData
	widths := map[string]int{
		"name":        len("NAME"),
		"host":        len("HOST/PORT"),
		"path":        len("PATH"),
		"services":    len("SERVICES"),
		"port":        len("PORT"),
		"termination": len("TERMINATION"),
		"wildcard":    len("WILDCARD"),
		"age":         len("AGE"),
	}

	if allNamespaces {
		widths["namespace"] = len("NAMESPACE")
	}

	if showLabels {
		widths["labels"] = len("LABELS")
	}

	for _, item := range items {
		route, ok := item.(*unstructured.Unstructured)
		if !ok {
			continue
		}

		name, _, _ := unstructured.NestedString(route.Object, "metadata", "name")
		namespace, _, _ := unstructured.NestedString(route.Object, "metadata", "namespace")
		creationTimestamp, _, _ := unstructured.NestedString(route.Object, "metadata", "creationTimestamp")

		// Get host from spec.host
		host, _, _ := unstructured.NestedString(route.Object, "spec", "host")
		if host == "" {
			host = "<none>"
		}

		// Get path from spec.path
		path, _, _ := unstructured.NestedString(route.Object, "spec", "path")
		if path == "" {
			path = "<none>"
		}

		// Get service from spec.to.name
		services, _, _ := unstructured.NestedString(route.Object, "spec", "to", "name")
		if services == "" {
			services = "<none>"
		}

		// Get port from spec.port.targetPort
		var port string
		if portVal, found, _ := unstructured.NestedFieldNoCopy(route.Object, "spec", "port", "targetPort"); found {
			if portStr, ok := portVal.(string); ok {
				port = portStr
			} else if portInt, ok := portVal.(int64); ok {
				port = fmt.Sprintf("%d", portInt)
			} else {
				port = "<none>"
			}
		} else {
			port = "<none>"
		}

		// Get termination from spec.tls.termination
		termination, _, _ := unstructured.NestedString(route.Object, "spec", "tls", "termination")
		if termination == "" {
			termination = "<none>"
		}

		// Get wildcard from spec.wildcardPolicy
		wildcard, _, _ := unstructured.NestedString(route.Object, "spec", "wildcardPolicy")
		if wildcard == "" {
			wildcard = "<none>"
		}

		// Calculate age
		var age string
		if creationTimestamp != "" {
			createdTime, err := time.Parse(time.RFC3339, creationTimestamp)
			if err == nil {
				age = formatAge(createdTime)
			} else {
				age = "<unknown>"
			}
		} else {
			age = "<unknown>"
		}

		// Get labels if needed
		labels := "<none>"
		if showLabels {
			labelMap, _, _ := unstructured.NestedMap(route.Object, "metadata", "labels")
			if len(labelMap) > 0 {
				labelPairs := make([]string, 0, len(labelMap))
				for k, v := range labelMap {
					if vStr, ok := v.(string); ok {
						labelPairs = append(labelPairs, fmt.Sprintf("%s=%s", k, vStr))
					}
				}
				labels = strings.Join(labelPairs, ",")
			}
		}

		data := routeData{
			namespace:   namespace,
			name:        name,
			host:        host,
			path:        path,
			services:    services,
			port:        port,
			termination: termination,
			wildcard:    wildcard,
			age:         age,
			labels:      labels,
		}

		routeList = append(routeList, data)

		// Update widths
		if allNamespaces && len(data.namespace) > widths["namespace"] {
			widths["namespace"] = len(data.namespace)
		}
		if len(data.name) > widths["name"] {
			widths["name"] = len(data.name)
		}
		if len(data.host) > widths["host"] {
			widths["host"] = len(data.host)
		}
		if len(data.path) > widths["path"] {
			widths["path"] = len(data.path)
		}
		if len(data.services) > widths["services"] {
			widths["services"] = len(data.services)
		}
		if len(data.port) > widths["port"] {
			widths["port"] = len(data.port)
		}
		if len(data.termination) > widths["termination"] {
			widths["termination"] = len(data.termination)
		}
		if len(data.wildcard) > widths["wildcard"] {
			widths["wildcard"] = len(data.wildcard)
		}
		if len(data.age) > widths["age"] {
			widths["age"] = len(data.age)
		}
		if showLabels && len(data.labels) > widths["labels"] {
			widths["labels"] = len(data.labels)
		}
	}

	if len(routeList) == 0 {
		fmt.Fprintf(out, "No resources found.\n")
		return nil
	}

	// Build format string
	var formatParts []string
	var headerParts []string

	if allNamespaces {
		formatParts = append(formatParts, fmt.Sprintf("%%-%ds", widths["namespace"]))
		headerParts = append(headerParts, "NAMESPACE")
	}

	formatParts = append(formatParts,
		fmt.Sprintf("%%-%ds", widths["name"]),
		fmt.Sprintf("%%-%ds", widths["host"]),
		fmt.Sprintf("%%-%ds", widths["path"]),
		fmt.Sprintf("%%-%ds", widths["services"]),
		fmt.Sprintf("%%-%ds", widths["port"]),
		fmt.Sprintf("%%-%ds", widths["termination"]),
		fmt.Sprintf("%%-%ds", widths["wildcard"]),
		fmt.Sprintf("%%-%ds", widths["age"]))

	headerParts = append(headerParts, "NAME", "HOST/PORT", "PATH", "SERVICES", "PORT", "TERMINATION", "WILDCARD", "AGE")

	if showLabels {
		formatParts = append(formatParts, fmt.Sprintf("%%-%ds", widths["labels"]))
		headerParts = append(headerParts, "LABELS")
	}

	dataFormat := strings.Join(formatParts, "   ") + "\n"

	// Print header
	fmt.Fprintf(out, dataFormat, toInterfaceSlice(headerParts)...)

	// Print each route
	for _, data := range routeList {
		var rowParts []interface{}
		if allNamespaces {
			rowParts = append(rowParts, data.namespace)
		}
		rowParts = append(rowParts, data.name, data.host, data.path, data.services, data.port, data.termination, data.wildcard, data.age)
		if showLabels {
			rowParts = append(rowParts, data.labels)
		}
		fmt.Fprintf(out, dataFormat, rowParts...)
	}

	return nil
}

func printSimpleTable(items []runtime.Object, out io.Writer, allNamespaces bool, showLabels bool) error {
	// Fallback simple table
	if len(items) == 0 {
		fmt.Fprintf(out, "No resources found.\n")
		return nil
	}

	var formatParts []string
	var headerParts []string

	if allNamespaces {
		formatParts = append(formatParts, "%-20s")
		headerParts = append(headerParts, "NAMESPACE")
	}

	formatParts = append(formatParts, "%-50s", "%-10s", "%-10s")
	headerParts = append(headerParts, "NAME", "READY", "STATUS")

	if showLabels {
		formatParts = append(formatParts, "%-50s")
		headerParts = append(headerParts, "LABELS")
	}

	dataFormat := strings.Join(formatParts, "   ") + "\n"

	fmt.Fprintf(out, dataFormat, toInterfaceSlice(headerParts)...)

	for _, item := range items {
		meta, err := meta.Accessor(item)
		if err != nil {
			continue
		}

		var rowParts []interface{}
		if allNamespaces {
			rowParts = append(rowParts, meta.GetNamespace())
		}
		rowParts = append(rowParts, meta.GetName(), "-", "Unknown")
		if showLabels {
			labelPairs := make([]string, 0, len(meta.GetLabels()))
			for k, v := range meta.GetLabels() {
				labelPairs = append(labelPairs, fmt.Sprintf("%s=%s", k, v))
			}
			labels := strings.Join(labelPairs, ",")
			if labels == "" {
				labels = "<none>"
			}
			rowParts = append(rowParts, labels)
		}
		fmt.Fprintf(out, dataFormat, rowParts...)
	}

	return nil
}

func toInterfaceSlice(strs []string) []interface{} {
	result := make([]interface{}, len(strs))
	for i, s := range strs {
		result[i] = s
	}
	return result
}

// getPodRestarts is defined in node.go - using that one

func createResources(ctx context.Context, clientset *kubernetes.Clientset, dynamicClient dynamic.Interface, files []string, namespace string, out io.Writer) error {
	var createdCount int
	var failedCount int
	var failedResources []string

	for _, file := range files {
		func() {
			var reader io.Reader
			var f *os.File
			if file == "-" {
				reader = os.Stdin
			} else {
				var err error
				f, err = os.Open(file)
				if err != nil {
					fmt.Fprintf(out, "✗ Error opening file %s: %v\n", file, err)
					failedCount++
					failedResources = append(failedResources, file)
					return
				}
				defer f.Close()
				reader = f
			}

			decoder := yaml.NewYAMLOrJSONDecoder(reader, 4096)
			for {
				var obj unstructured.Unstructured
				if err := decoder.Decode(&obj); err != nil {
					if err == io.EOF {
						break
					}
					fmt.Fprintf(out, "✗ Error decoding resource from %s: %v\n", file, err)
					failedCount++
					failedResources = append(failedResources, file)
					continue
				}

				if obj.Object == nil {
					continue
				}

				// Override namespace if specified
				if namespace != "" {
					obj.SetNamespace(namespace)
				}

				// Use dynamic client to create
				gvr := schema.GroupVersionResource{
					Group:    obj.GroupVersionKind().Group,
					Version:  obj.GroupVersionKind().Version,
					Resource: strings.ToLower(obj.GetKind()) + "s",
				}

				ns := obj.GetNamespace()
				if ns == "" {
					ns = "default"
				}

				resourceClient := dynamicClient.Resource(gvr).Namespace(ns)
				_, err := resourceClient.Create(ctx, &obj, metav1.CreateOptions{})
				if err != nil {
					fmt.Fprintf(out, "✗ Error creating %s/%s: %v\n", obj.GetKind(), obj.GetName(), err)
					failedCount++
					failedResources = append(failedResources, fmt.Sprintf("%s/%s", obj.GetKind(), obj.GetName()))
					continue
				}

				fmt.Fprintf(out, "✓ Created %s/%s\n", obj.GetKind(), obj.GetName())
				createdCount++
			}
		}()
	}

	fmt.Fprintf(out, "\n=== Summary ===\n")
	fmt.Fprintf(out, "Created: %d\n", createdCount)
	fmt.Fprintf(out, "Failed: %d\n", failedCount)

	if failedCount > 0 {
		if len(failedResources) > 0 {
			fmt.Fprintf(out, "\n✗ Failed resources:\n")
			for _, res := range failedResources {
				fmt.Fprintf(out, "  - %s\n", res)
			}
		}
		return fmt.Errorf("failed to create %d resource(s)", failedCount)
	}

	return nil
}

func editResource(ctx context.Context, clientset *kubernetes.Clientset, dynamicClient dynamic.Interface, resourceType string, resourceName string, namespace string, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
	// Use discovery to resolve resource type
	resolver, err := getResourceResolver(ctx)
	if err != nil {
		return fmt.Errorf("failed to get resource resolver: %w", err)
	}

	gvr, namespaced, err := resolver.resolveResource(resourceType)
	if err != nil {
		return fmt.Errorf("resource type %q not found: %w", resourceType, err)
	}

	// Get the resource using dynamic client (fully generic)
	var obj *unstructured.Unstructured
	if !namespaced {
		// Cluster-scoped resource
		obj, err = dynamicClient.Resource(gvr).Get(ctx, resourceName, metav1.GetOptions{})
	} else {
		// Namespace-scoped resource
		if namespace == "" {
			return fmt.Errorf("namespace is required for namespaced resource %q", resourceType)
		}
		obj, err = dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, resourceName, metav1.GetOptions{})
	}

	if meta.IsNoMatchError(err) {
		return fmt.Errorf("resource type %q is not available in this cluster - the Custom Resource Definition (CRD) may not be installed", resourceType)
	}
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("resource %q not found", resourceName)
	}
	if err != nil {
		return fmt.Errorf("failed to get resource: %w", err)
	}

	// Convert to YAML
	yamlData, err := sigsyaml.Marshal(obj)
	if err != nil {
		return fmt.Errorf("failed to marshal resource: %w", err)
	}

	// Write to temp file
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

	// Determine editor
	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = "vi"
	}

	// Open editor
	editCmd := exec.Command(editor, tmpFile.Name())
	editCmd.Stdin = stdin
	editCmd.Stdout = stdout
	editCmd.Stderr = stderr
	if err := editCmd.Run(); err != nil {
		return fmt.Errorf("editor exited with error: %w", err)
	}

	// Read edited file
	editedData, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		return fmt.Errorf("failed to read edited file: %w", err)
	}

	// Parse and apply
	var editedObj unstructured.Unstructured
	if err := sigsyaml.Unmarshal(editedData, &editedObj); err != nil {
		return fmt.Errorf("failed to parse edited YAML: %w", err)
	}

	// Update the resource using dynamic client (fully generic)
	if !namespaced {
		// Cluster-scoped resource
		_, err = dynamicClient.Resource(gvr).Update(ctx, &editedObj, metav1.UpdateOptions{})
	} else {
		// Namespace-scoped resource
		_, err = dynamicClient.Resource(gvr).Namespace(namespace).Update(ctx, &editedObj, metav1.UpdateOptions{})
	}

	if meta.IsNoMatchError(err) {
		return fmt.Errorf("resource type %q is not available in this cluster - the Custom Resource Definition (CRD) may not be installed", resourceType)
	}
	if err != nil {
		return fmt.Errorf("failed to update resource: %w", err)
	}

	fmt.Fprintf(stdout, "✓ %s/%s edited\n", resourceType, resourceName)
	return nil
}

func deleteResources(ctx context.Context, clientset *kubernetes.Clientset, dynamicClient dynamic.Interface, resourceType string, resourceNames []string, namespace string, allNamespaces bool, selector string, force bool, maxConcurrency int, out io.Writer, errOut io.Writer) error {
	// Use discovery to resolve resource type
	resolver, err := getResourceResolver(ctx)
	if err != nil {
		return fmt.Errorf("failed to get resource resolver: %w", err)
	}

	gvr, namespaced, err := resolver.resolveResource(resourceType)
	if err != nil {
		return fmt.Errorf("resource type %q not found: %w", resourceType, err)
	}

	var resourcesToDelete []resourceIdentifier

	// If selector is provided, list resources by selector using dynamic client (fully generic)
	if selector != "" {
		opts := metav1.ListOptions{LabelSelector: selector}
		var list *unstructured.UnstructuredList

		if !namespaced {
			// Cluster-scoped resource
			list, err = dynamicClient.Resource(gvr).List(ctx, opts)
		} else {
			// Namespace-scoped resource
			if allNamespaces {
				list, err = dynamicClient.Resource(gvr).Namespace("").List(ctx, opts)
			} else {
				list, err = dynamicClient.Resource(gvr).Namespace(namespace).List(ctx, opts)
			}
		}

		if meta.IsNoMatchError(err) {
			return fmt.Errorf("resource type %q is not available in this cluster - the Custom Resource Definition (CRD) may not be installed", resourceType)
		}
		if err != nil {
			return fmt.Errorf("failed to list resources: %w", err)
		}

		for _, item := range list.Items {
			name, _, _ := unstructured.NestedString(item.Object, "metadata", "name")
			ns, _, _ := unstructured.NestedString(item.Object, "metadata", "namespace")
			resourcesToDelete = append(resourcesToDelete, resourceIdentifier{
				name:      name,
				namespace: ns,
			})
		}
	} else if len(resourceNames) > 0 {
		// Delete specific resources
		for _, name := range resourceNames {
			resourcesToDelete = append(resourcesToDelete, resourceIdentifier{
				name:      name,
				namespace: namespace,
			})
		}
	} else {
		return fmt.Errorf("must specify resource names or use --selector")
	}

	if len(resourcesToDelete) == 0 {
		fmt.Fprintf(out, "No resources found to delete.\n")
		return nil
	}

	// Use worker pool for concurrent deletion
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

	var deletedCount int
	var failedCount int
	var failedResources []string

	for result := range resultChan {
		if result.err != nil {
			fmt.Fprintf(errOut, "✗ Failed to delete %s/%s: %v\n", result.resource.namespace, result.resource.name, result.err)
			failedCount++
			failedResources = append(failedResources, fmt.Sprintf("%s/%s", result.resource.namespace, result.resource.name))
		} else {
			fmt.Fprintf(out, "✓ Deleted %s/%s\n", result.resource.namespace, result.resource.name)
			deletedCount++
		}
	}

	fmt.Fprintf(out, "\n=== Summary ===\n")
	fmt.Fprintf(out, "Deleted: %d\n", deletedCount)
	fmt.Fprintf(out, "Failed: %d\n", failedCount)

	if failedCount > 0 {
		fmt.Fprintf(errOut, "\n✗ Failed to delete:\n")
		for _, res := range failedResources {
			fmt.Fprintf(errOut, "  - %s\n", res)
		}
		return fmt.Errorf("failed to delete %d resource(s)", failedCount)
	}

	return nil
}

type resourceIdentifier struct {
	name      string
	namespace string
}

// deleteSingleResource deletes a single resource using dynamic client (fully generic)
func deleteSingleResource(ctx context.Context, dynamicClient dynamic.Interface, gvr schema.GroupVersionResource, namespaced bool, resourceName string, namespace string, force bool) error {
	opts := metav1.DeleteOptions{}
	if force {
		gracePeriod := int64(0)
		opts.GracePeriodSeconds = &gracePeriod
	}

	var err error
	if !namespaced {
		// Cluster-scoped resource
		err = dynamicClient.Resource(gvr).Delete(ctx, resourceName, opts)
	} else {
		// Namespace-scoped resource
		if namespace == "" {
			return fmt.Errorf("namespace is required for namespaced resource")
		}
		err = dynamicClient.Resource(gvr).Namespace(namespace).Delete(ctx, resourceName, opts)
	}

	if meta.IsNoMatchError(err) {
		return fmt.Errorf("resource type is not available in this cluster - the Custom Resource Definition (CRD) may not be installed")
	}
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("resource %q not found", resourceName)
	}
	return err
}

func describeResource(ctx context.Context, clientset *kubernetes.Clientset, dynamicClient dynamic.Interface, resourceType string, resourceName string, namespace string, allNamespaces bool, out io.Writer) error {
	// Use discovery to resolve resource type
	resolver, err := getResourceResolver(ctx)
	if err != nil {
		return fmt.Errorf("failed to get resource resolver: %w", err)
	}

	gvr, namespaced, err := resolver.resolveResource(resourceType)
	if err != nil {
		return fmt.Errorf("resource type %q not found: %w", resourceType, err)
	}

	// Get the resource using dynamic client (fully generic)
	var obj *unstructured.Unstructured
	if !namespaced {
		// Cluster-scoped resource
		obj, err = dynamicClient.Resource(gvr).Get(ctx, resourceName, metav1.GetOptions{})
	} else {
		// Namespace-scoped resource
		if namespace == "" {
			return fmt.Errorf("namespace is required for namespaced resource %q", resourceType)
		}
		obj, err = dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, resourceName, metav1.GetOptions{})
	}

	if meta.IsNoMatchError(err) {
		return fmt.Errorf("resource type %q is not available in this cluster - the Custom Resource Definition (CRD) may not be installed", resourceType)
	}
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("resource %q not found", resourceName)
	}
	if err != nil {
		return fmt.Errorf("failed to get resource: %w", err)
	}

	// Print detailed information
	data, err := sigsyaml.Marshal(obj)
	if err != nil {
		return fmt.Errorf("failed to marshal resource: %w", err)
	}

	_, err = out.Write(data)
	return err
}

func getPodLogs(ctx context.Context, clientset *kubernetes.Clientset, podName string, namespace string, container string, follow bool, previous bool, tailLines int, since time.Duration, out io.Writer, errOut io.Writer) error {
	opts := &corev1.PodLogOptions{
		Follow:    follow,
		Previous:  previous,
		Container: container,
	}

	if tailLines > 0 {
		tail := int64(tailLines)
		opts.TailLines = &tail
	}

	if since > 0 {
		sinceTime := metav1.NewTime(time.Now().Add(-since))
		opts.SinceTime = &sinceTime
	}

	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		return fmt.Errorf("failed to get logs: %w", err)
	}
	defer stream.Close()

	_, err = io.Copy(out, stream)
	return err
}

func applyResources(ctx context.Context, clientset *kubernetes.Clientset, dynamicClient dynamic.Interface, files []string, namespace string, force bool, out io.Writer) error {
	var appliedCount int
	var failedCount int
	var failedResources []string

	for _, file := range files {
		func() {
			var reader io.Reader
			var f *os.File
			if file == "-" {
				reader = os.Stdin
			} else {
				var err error
				f, err = os.Open(file)
				if err != nil {
					fmt.Fprintf(out, "✗ Error opening file %s: %v\n", file, err)
					failedCount++
					failedResources = append(failedResources, file)
					return
				}
				defer f.Close()
				reader = f
			}

			decoder := yaml.NewYAMLOrJSONDecoder(reader, 4096)
			for {
				var obj unstructured.Unstructured
				if err := decoder.Decode(&obj); err != nil {
					if err == io.EOF {
						break
					}
					fmt.Fprintf(out, "✗ Error decoding resource from %s: %v\n", file, err)
					failedCount++
					failedResources = append(failedResources, file)
					continue
				}

				if obj.Object == nil {
					continue
				}

				// Override namespace if specified
				if namespace != "" {
					obj.SetNamespace(namespace)
				}

				// Try to get existing resource
				gvr := schema.GroupVersionResource{
					Group:    obj.GroupVersionKind().Group,
					Version:  obj.GroupVersionKind().Version,
					Resource: strings.ToLower(obj.GetKind()) + "s",
				}

				ns := obj.GetNamespace()
				if ns == "" {
					ns = "default"
				}

				resourceClient := dynamicClient.Resource(gvr).Namespace(ns)
				_, err := resourceClient.Get(ctx, obj.GetName(), metav1.GetOptions{})
				if err != nil {
					if apierrors.IsNotFound(err) {
						// Create
						_, err = resourceClient.Create(ctx, &obj, metav1.CreateOptions{})
						if err != nil {
							fmt.Fprintf(out, "✗ Error creating %s/%s: %v\n", obj.GetKind(), obj.GetName(), err)
							failedCount++
							failedResources = append(failedResources, fmt.Sprintf("%s/%s", obj.GetKind(), obj.GetName()))
							continue
						}
						fmt.Fprintf(out, "✓ Created %s/%s\n", obj.GetKind(), obj.GetName())
					} else {
						fmt.Fprintf(out, "✗ Error getting %s/%s: %v\n", obj.GetKind(), obj.GetName(), err)
						failedCount++
						failedResources = append(failedResources, fmt.Sprintf("%s/%s", obj.GetKind(), obj.GetName()))
						continue
					}
				} else {
					// Update
					_, err = resourceClient.Update(ctx, &obj, metav1.UpdateOptions{})
					if err != nil {
						fmt.Fprintf(out, "✗ Error updating %s/%s: %v\n", obj.GetKind(), obj.GetName(), err)
						failedCount++
						failedResources = append(failedResources, fmt.Sprintf("%s/%s", obj.GetKind(), obj.GetName()))
						continue
					}
					fmt.Fprintf(out, "✓ Updated %s/%s\n", obj.GetKind(), obj.GetName())
				}
				appliedCount++
			}
		}()
	}

	fmt.Fprintf(out, "\n=== Summary ===\n")
	fmt.Fprintf(out, "Applied: %d\n", appliedCount)
	fmt.Fprintf(out, "Failed: %d\n", failedCount)

	if failedCount > 0 {
		if len(failedResources) > 0 {
			fmt.Fprintf(out, "\n✗ Failed resources:\n")
			for _, res := range failedResources {
				fmt.Fprintf(out, "  - %s\n", res)
			}
		}
		return fmt.Errorf("failed to apply %d resource(s)", failedCount)
	}

	return nil
}

func patchResource(ctx context.Context, clientset *kubernetes.Clientset, dynamicClient dynamic.Interface, resourceType string, resourceName string, namespace string, patchType string, patch string, patchFile string, out io.Writer) error {
	// Use discovery to resolve resource type
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
	} else if patch != "" {
		patchData = []byte(patch)
	} else {
		return fmt.Errorf("must specify either -p/--patch or --patch-file")
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

	// Patch the resource using dynamic client (fully generic)
	if !namespaced {
		// Cluster-scoped resource
		_, err = dynamicClient.Resource(gvr).Patch(ctx, resourceName, pt, patchData, metav1.PatchOptions{})
	} else {
		// Namespace-scoped resource
		if namespace == "" {
			return fmt.Errorf("namespace is required for namespaced resource %q", resourceType)
		}
		_, err = dynamicClient.Resource(gvr).Namespace(namespace).Patch(ctx, resourceName, pt, patchData, metav1.PatchOptions{})
	}

	if meta.IsNoMatchError(err) {
		return fmt.Errorf("resource type %q is not available in this cluster - the Custom Resource Definition (CRD) may not be installed", resourceType)
	}
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("resource %q not found", resourceName)
	}
	if err != nil {
		return fmt.Errorf("failed to patch resource: %w", err)
	}

	fmt.Fprintf(out, "✓ %s/%s patched\n", resourceType, resourceName)
	return nil
}

func newAnnotateCommand() *cobra.Command {
	var namespace string
	var allNamespaces bool
	var resourceNames []string
	var maxConcurrency int

	cmd := &cobra.Command{
		Use:   "annotate <resource-type> <resource-name> <annotation> [flags]",
		Short: "Update the annotations on one or more resources",
		Long: `Update annotations on one or more resources.

This command works with any resource type discovered in your cluster, including:
- Standard Kubernetes resources
- OpenShift resources
- Custom Resource Definitions (CRDs)

Annotations should be in the format "key=value" (to add/update) or "key" (to remove).
Multiple annotations can be specified separated by commas (no spaces).

Operations on multiple resources are performed concurrently for better performance.

Examples:
  # Add an annotation to a pod
  ocp annotate pod my-pod description="My pod"

  # Add multiple annotations
  ocp annotate pod my-pod key1=value1,key2=value2

  # Remove an annotation
  ocp annotate pod my-pod description

  # Annotate multiple resources (concurrent)
  ocp annotate pod pod1 pod2 pod3 key=value

  # Annotate any OpenShift resource
  ocp annotate route my-route description="My route"

  # Annotate any custom resource (CRD)
  ocp annotate mycustomresource my-instance version=1.0.0`,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return completeResourceTypes(cmd, toComplete)
			}
			if len(args) == 1 {
				return completeResourceNames(cmd, args[0], toComplete, namespace, allNamespaces)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Args: cobra.MinimumNArgs(3),
		Example: `  # Add an annotation to a pod
  ocp annotate pod my-pod description="My pod"

  # Add multiple annotations
  ocp annotate pod my-pod key1=value1,key2=value2,key3=value3

  # Remove an annotation
  ocp annotate pod my-pod description

  # Annotate multiple pods
  ocp annotate pod pod1 pod2 pod3 environment=production

  # Annotate a deployment
  ocp annotate deployment my-app version=1.0.0

  # Annotate OpenShift resources
  ocp annotate route my-route description="My route"
  ocp annotate buildconfig my-build build.openshift.io/source-location="git://example.com/repo"`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			resourceType := args[0]
			resourceNames = args[1 : len(args)-1]
			annotationsStr := args[len(args)-1]

			if len(resourceNames) == 0 {
				return fmt.Errorf("must specify at least one resource name")
			}

			ns := resolveNamespace(ctx, namespace, allNamespaces)

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			if maxConcurrency <= 0 {
				maxConcurrency = 10
			}

			return annotateResources(ctx, clientset, dynamicClient, resourceType, resourceNames, ns, allNamespaces, annotationsStr, maxConcurrency, cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "Annotate resources in all namespaces")
	cmd.Flags().IntVar(&maxConcurrency, "max-concurrency", 10, "Maximum number of concurrent annotation operations")

	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func newLabelCommand() *cobra.Command {
	var namespace string
	var allNamespaces bool
	var resourceNames []string
	var maxConcurrency int

	cmd := &cobra.Command{
		Use:   "label <resource-type> <resource-name> <label> [flags]",
		Short: "Update the labels on one or more resources",
		Long: `Update labels on one or more resources.

This command works with any resource type discovered in your cluster, including:
- Standard Kubernetes resources
- OpenShift resources
- Custom Resource Definitions (CRDs)

Labels should be in the format "key=value" (to add/update) or "key-" (to remove).
Multiple labels can be specified separated by commas (no spaces).

Operations on multiple resources are performed concurrently for better performance.

Examples:
  # Add a label to a pod
  ocp label pod my-pod environment=production

  # Add multiple labels
  ocp label pod my-pod key1=value1,key2=value2

  # Remove a label
  ocp label pod my-pod environment-

  # Label multiple resources (concurrent)
  ocp label pod pod1 pod2 pod3 team=platform

  # Label any OpenShift resource
  ocp label route my-route app=myapp

  # Label any custom resource (CRD)
  ocp label mycustomresource my-instance version=1.0.0`,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return completeResourceTypes(cmd, toComplete)
			}
			if len(args) == 1 {
				return completeResourceNames(cmd, args[0], toComplete, namespace, allNamespaces)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Args: cobra.MinimumNArgs(3),
		Example: `  # Add a label to a pod
  ocp label pod my-pod environment=production

  # Add multiple labels
  ocp label pod my-pod key1=value1,key2=value2,key3=value3

  # Remove a label
  ocp label pod my-pod environment-

  # Label multiple pods
  ocp label pod pod1 pod2 pod3 team=platform

  # Label a deployment
  ocp label deployment my-app version=1.0.0

  # Label OpenShift resources
  ocp label route my-route app=myapp
  ocp label buildconfig my-build build.openshift.io/source-location=git`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			resourceType := args[0]
			resourceNames = args[1 : len(args)-1]
			labelsStr := args[len(args)-1]

			if len(resourceNames) == 0 {
				return fmt.Errorf("must specify at least one resource name")
			}

			ns := resolveNamespace(ctx, namespace, allNamespaces)

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			if maxConcurrency <= 0 {
				maxConcurrency = 10
			}

			return labelResources(ctx, clientset, dynamicClient, resourceType, resourceNames, ns, allNamespaces, labelsStr, maxConcurrency, cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "Label resources in all namespaces")
	cmd.Flags().IntVar(&maxConcurrency, "max-concurrency", 10, "Maximum number of concurrent label operations")

	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func annotateResources(ctx context.Context, clientset *kubernetes.Clientset, dynamicClient dynamic.Interface, resourceType string, resourceNames []string, namespace string, allNamespaces bool, annotationsStr string, maxConcurrency int, out io.Writer, errOut io.Writer) error {
	resourceType = normalizeResourceType(ctx, resourceType)

	// Parse comma-separated annotations
	annotationParts := strings.Split(annotationsStr, ",")
	var patchOps []string
	var needsAnnotationsInit bool
	var addedKeys []string
	var removedKeys []string

	// Build patch operations
	for _, annotation := range annotationParts {
		annotation = strings.TrimSpace(annotation)
		if annotation == "" {
			continue
		}

		// Parse annotation key=value or key (remove)
		parts := strings.SplitN(annotation, "=", 2)
		key := parts[0]
		var value string
		var remove bool

		if len(parts) == 2 {
			value = parts[1]
			remove = false
		} else {
			remove = true
		}

		if remove {
			// Remove annotation
			escapedKey := strings.ReplaceAll(key, "/", "~1")
			patchOps = append(patchOps, fmt.Sprintf(`{"op":"remove","path":"/metadata/annotations/%s"}`, escapedKey))
			removedKeys = append(removedKeys, key)
		} else {
			// Add or update annotation
			if !needsAnnotationsInit {
				needsAnnotationsInit = true
			}
			escapedKey := strings.ReplaceAll(key, "/", "~1")
			// JSON escape the value
			jsonEscapedValue, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("failed to escape annotation value for key %q: %w", key, err)
			}
			// Remove surrounding quotes from marshaled JSON string
			jsonValueStr := string(jsonEscapedValue[1 : len(jsonEscapedValue)-1])
			patchOps = append(patchOps, fmt.Sprintf(`{"op":"add","path":"/metadata/annotations/%s","value":"%s"}`, escapedKey, jsonValueStr))
			addedKeys = append(addedKeys, key)
		}
	}

	if len(patchOps) == 0 {
		return fmt.Errorf("no valid annotations provided")
	}

	// Build patch with initialization if needed
	var patch string
	if needsAnnotationsInit {
		patch = fmt.Sprintf(`[{"op":"add","path":"/metadata/annotations","value":{}},%s]`, strings.Join(patchOps, ","))
	} else {
		patch = fmt.Sprintf(`[%s]`, strings.Join(patchOps, ","))
	}

	// Use worker pool for concurrent annotation
	type annotateResult struct {
		resourceName string
		namespace    string
		err          error
	}

	// Normalize resource type once using discovery
	normalizedResourceType := normalizeResourceType(ctx, resourceType)

	// Determine if cluster-scoped using discovery
	var isClusterScoped bool
	resolver, err := getResourceResolver(ctx)
	if err == nil {
		_, namespaced, err := resolver.resolveResource(normalizedResourceType)
		if err == nil {
			isClusterScoped = !namespaced
		}
	}

	resourceChan := make(chan string, len(resourceNames))
	resultChan := make(chan annotateResult, len(resourceNames))

	for _, name := range resourceNames {
		resourceChan <- name
	}
	close(resourceChan)

	var wg sync.WaitGroup
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for resourceName := range resourceChan {
				// Use empty namespace for cluster-scoped resources
				ns := namespace
				if isClusterScoped {
					ns = ""
				}
				err := annotateSingleResource(ctx, clientset, dynamicClient, normalizedResourceType, resourceName, ns, patch)
				resultChan <- annotateResult{resourceName: resourceName, namespace: ns, err: err}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var annotatedCount int
	var failedCount int
	var failedResources []string

	for result := range resultChan {
		if result.err != nil {
			if result.namespace != "" {
				fmt.Fprintf(errOut, "✗ Failed to annotate %s/%s: %v\n", result.namespace, result.resourceName, result.err)
				failedResources = append(failedResources, fmt.Sprintf("%s/%s", result.namespace, result.resourceName))
			} else {
				fmt.Fprintf(errOut, "✗ Failed to annotate %s: %v\n", result.resourceName, result.err)
				failedResources = append(failedResources, result.resourceName)
			}
			failedCount++
		} else {
			if result.namespace != "" {
				fmt.Fprintf(out, "✓ Annotated %s/%s\n", result.namespace, result.resourceName)
			} else {
				fmt.Fprintf(out, "✓ Annotated %s\n", result.resourceName)
			}
			annotatedCount++
		}
	}

	fmt.Fprintf(out, "\n=== Summary ===\n")
	fmt.Fprintf(out, "Annotated: %d\n", annotatedCount)
	fmt.Fprintf(out, "Failed: %d\n", failedCount)
	if len(addedKeys) > 0 {
		fmt.Fprintf(out, "Annotations added/updated: %v\n", addedKeys)
	}
	if len(removedKeys) > 0 {
		fmt.Fprintf(out, "Annotations removed: %v\n", removedKeys)
	}

	if failedCount > 0 {
		fmt.Fprintf(errOut, "\n✗ Failed to annotate:\n")
		for _, res := range failedResources {
			fmt.Fprintf(errOut, "  - %s\n", res)
		}
		return fmt.Errorf("failed to annotate %d resource(s)", failedCount)
	}

	return nil
}

func annotateSingleResource(ctx context.Context, clientset *kubernetes.Clientset, dynamicClient dynamic.Interface, resourceType string, resourceName string, namespace string, patch string) error {
	resourceType = normalizeResourceType(ctx, resourceType)
	patchData := []byte(patch)

	switch resourceType {
	case "pods", "po":
		_, err := clientset.CoreV1().Pods(namespace).Patch(ctx, resourceName, types.JSONPatchType, patchData, metav1.PatchOptions{})
		return err
	case "services", "svc":
		_, err := clientset.CoreV1().Services(namespace).Patch(ctx, resourceName, types.JSONPatchType, patchData, metav1.PatchOptions{})
		return err
	case "deployments", "deploy":
		_, err := clientset.AppsV1().Deployments(namespace).Patch(ctx, resourceName, types.JSONPatchType, patchData, metav1.PatchOptions{})
		return err
	case "nodes", "no":
		_, err := clientset.CoreV1().Nodes().Patch(ctx, resourceName, types.JSONPatchType, patchData, metav1.PatchOptions{})
		return err
	case "namespaces", "ns":
		_, err := clientset.CoreV1().Namespaces().Patch(ctx, resourceName, types.JSONPatchType, patchData, metav1.PatchOptions{})
		return err
	default:
		// Try OpenShift resources
		gvr := getOpenShiftGVR(resourceType)
		if gvr != nil {
			if isClusterScopedResource(resourceType) {
				_, err := dynamicClient.Resource(*gvr).Patch(ctx, resourceName, types.JSONPatchType, patchData, metav1.PatchOptions{})
				if meta.IsNoMatchError(err) {
					return fmt.Errorf("resource type %q is not available in this cluster - the Custom Resource Definition (CRD) may not be installed. Ensure you're connected to an OpenShift cluster or that the required operator is installed", resourceType)
				}
				return err
			} else {
				_, err := dynamicClient.Resource(*gvr).Namespace(namespace).Patch(ctx, resourceName, types.JSONPatchType, patchData, metav1.PatchOptions{})
				if meta.IsNoMatchError(err) {
					return fmt.Errorf("resource type %q is not available in this cluster - the Custom Resource Definition (CRD) may not be installed. Ensure you're connected to an OpenShift cluster or that the required operator is installed", resourceType)
				}
				return err
			}
		}
		return fmt.Errorf("resource type %q not yet implemented for annotate command", resourceType)
	}
}

func labelResources(ctx context.Context, clientset *kubernetes.Clientset, dynamicClient dynamic.Interface, resourceType string, resourceNames []string, namespace string, allNamespaces bool, labelsStr string, maxConcurrency int, out io.Writer, errOut io.Writer) error {
	resourceType = normalizeResourceType(ctx, resourceType)

	// Parse comma-separated labels
	labelParts := strings.Split(labelsStr, ",")
	var patchOps []string
	var needsLabelsInit bool
	var addedKeys []string
	var removedKeys []string

	// Build patch operations
	for _, label := range labelParts {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}

		// Parse label key=value or key- (remove)
		var key, value string
		var remove bool

		if strings.HasSuffix(label, "-") {
			key = strings.TrimSuffix(label, "-")
			remove = true
		} else {
			parts := strings.SplitN(label, "=", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid label format: expected key=value or key-, got %q", label)
			}
			key = parts[0]
			value = parts[1]
			remove = false
		}

		if remove {
			// Remove label
			escapedKey := strings.ReplaceAll(key, "/", "~1")
			patchOps = append(patchOps, fmt.Sprintf(`{"op":"remove","path":"/metadata/labels/%s"}`, escapedKey))
			removedKeys = append(removedKeys, key)
		} else {
			// Add or update label
			if !needsLabelsInit {
				needsLabelsInit = true
			}
			escapedKey := strings.ReplaceAll(key, "/", "~1")
			// JSON escape the value
			jsonEscapedValue, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("failed to escape label value for key %q: %w", key, err)
			}
			// Remove surrounding quotes from marshaled JSON string
			jsonValueStr := string(jsonEscapedValue[1 : len(jsonEscapedValue)-1])
			patchOps = append(patchOps, fmt.Sprintf(`{"op":"add","path":"/metadata/labels/%s","value":"%s"}`, escapedKey, jsonValueStr))
			addedKeys = append(addedKeys, key)
		}
	}

	if len(patchOps) == 0 {
		return fmt.Errorf("no valid labels provided")
	}

	// Build patch with initialization if needed
	var patch string
	if needsLabelsInit {
		patch = fmt.Sprintf(`[{"op":"add","path":"/metadata/labels","value":{}},%s]`, strings.Join(patchOps, ","))
	} else {
		patch = fmt.Sprintf(`[%s]`, strings.Join(patchOps, ","))
	}

	// Use worker pool for concurrent labeling
	type labelResult struct {
		resourceName string
		namespace    string
		err          error
	}

	// Normalize resource type once using discovery
	normalizedResourceType := normalizeResourceType(ctx, resourceType)

	// Determine if cluster-scoped using discovery
	var isClusterScoped bool
	resolver, err := getResourceResolver(ctx)
	if err == nil {
		_, namespaced, err := resolver.resolveResource(normalizedResourceType)
		if err == nil {
			isClusterScoped = !namespaced
		}
	}

	resourceChan := make(chan string, len(resourceNames))
	resultChan := make(chan labelResult, len(resourceNames))

	for _, name := range resourceNames {
		resourceChan <- name
	}
	close(resourceChan)

	var wg sync.WaitGroup
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for resourceName := range resourceChan {
				// Use empty namespace for cluster-scoped resources
				ns := namespace
				if isClusterScoped {
					ns = ""
				}
				err := labelSingleResource(ctx, clientset, dynamicClient, normalizedResourceType, resourceName, ns, patch)
				resultChan <- labelResult{resourceName: resourceName, namespace: ns, err: err}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var labeledCount int
	var failedCount int
	var failedResources []string

	for result := range resultChan {
		if result.err != nil {
			if result.namespace != "" {
				fmt.Fprintf(errOut, "✗ Failed to label %s/%s: %v\n", result.namespace, result.resourceName, result.err)
				failedResources = append(failedResources, fmt.Sprintf("%s/%s", result.namespace, result.resourceName))
			} else {
				fmt.Fprintf(errOut, "✗ Failed to label %s: %v\n", result.resourceName, result.err)
				failedResources = append(failedResources, result.resourceName)
			}
			failedCount++
		} else {
			if result.namespace != "" {
				fmt.Fprintf(out, "✓ Labeled %s/%s\n", result.namespace, result.resourceName)
			} else {
				fmt.Fprintf(out, "✓ Labeled %s\n", result.resourceName)
			}
			labeledCount++
		}
	}

	fmt.Fprintf(out, "\n=== Summary ===\n")
	fmt.Fprintf(out, "Labeled: %d\n", labeledCount)
	fmt.Fprintf(out, "Failed: %d\n", failedCount)
	if len(addedKeys) > 0 {
		fmt.Fprintf(out, "Labels added/updated: %v\n", addedKeys)
	}
	if len(removedKeys) > 0 {
		fmt.Fprintf(out, "Labels removed: %v\n", removedKeys)
	}

	if failedCount > 0 {
		fmt.Fprintf(errOut, "\n✗ Failed to label:\n")
		for _, res := range failedResources {
			fmt.Fprintf(errOut, "  - %s\n", res)
		}
		return fmt.Errorf("failed to label %d resource(s)", failedCount)
	}

	return nil
}

func labelSingleResource(ctx context.Context, clientset *kubernetes.Clientset, dynamicClient dynamic.Interface, resourceType string, resourceName string, namespace string, patch string) error {
	resourceType = normalizeResourceType(ctx, resourceType)
	patchData := []byte(patch)

	switch resourceType {
	case "pods", "po":
		_, err := clientset.CoreV1().Pods(namespace).Patch(ctx, resourceName, types.JSONPatchType, patchData, metav1.PatchOptions{})
		return err
	case "services", "svc":
		_, err := clientset.CoreV1().Services(namespace).Patch(ctx, resourceName, types.JSONPatchType, patchData, metav1.PatchOptions{})
		return err
	case "deployments", "deploy":
		_, err := clientset.AppsV1().Deployments(namespace).Patch(ctx, resourceName, types.JSONPatchType, patchData, metav1.PatchOptions{})
		return err
	case "nodes", "no":
		_, err := clientset.CoreV1().Nodes().Patch(ctx, resourceName, types.JSONPatchType, patchData, metav1.PatchOptions{})
		return err
	case "namespaces", "ns":
		_, err := clientset.CoreV1().Namespaces().Patch(ctx, resourceName, types.JSONPatchType, patchData, metav1.PatchOptions{})
		return err
	default:
		// Try OpenShift resources
		gvr := getOpenShiftGVR(resourceType)
		if gvr != nil {
			if isClusterScopedResource(resourceType) {
				_, err := dynamicClient.Resource(*gvr).Patch(ctx, resourceName, types.JSONPatchType, patchData, metav1.PatchOptions{})
				if meta.IsNoMatchError(err) {
					return fmt.Errorf("resource type %q is not available in this cluster - the Custom Resource Definition (CRD) may not be installed. Ensure you're connected to an OpenShift cluster or that the required operator is installed", resourceType)
				}
				return err
			} else {
				_, err := dynamicClient.Resource(*gvr).Namespace(namespace).Patch(ctx, resourceName, types.JSONPatchType, patchData, metav1.PatchOptions{})
				if meta.IsNoMatchError(err) {
					return fmt.Errorf("resource type %q is not available in this cluster - the Custom Resource Definition (CRD) may not be installed. Ensure you're connected to an OpenShift cluster or that the required operator is installed", resourceType)
				}
				return err
			}
		}
		return fmt.Errorf("resource type %q not yet implemented for label command", resourceType)
	}
}
