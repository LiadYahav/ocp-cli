package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newSetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set",
		Short: "Commands that help set specific features on objects",
	}

	cmd.AddCommand(
		newSetEnvCommand(),
		newSetImageCommand(),
		newSetResourcesCommand(),
		newSetServiceAccountCommand(),
	)

	return cmd
}

func newSetEnvCommand() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "env <resource-type>/<name> KEY=VALUE [KEY=VALUE...]",
		Short: "Update environment variables on a resource",
		Long: `Set environment variables on a deployment, statefulset, or other pod template resource.

Examples:
  # Set env vars on a deployment
  ocp set env deployment/myapp DATABASE_URL=postgres://... LOG_LEVEL=debug

  # Remove an env var (use KEY-)
  ocp set env deploy/myapp LOG_LEVEL-`,
		ValidArgsFunction: completeResourceTargetArgs,
		Args:              cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			ns := resolveNamespace(ctx, namespace, false)
			out := cmd.OutOrStdout()

			target := args[0]
			envArgs := args[1:]

			resourceType, resourceName, err := parseResourceTarget(target)
			if err != nil {
				return err
			}

			// Build env var patch
			envVars := make([]map[string]interface{}, 0)
			removeVars := make([]string, 0)

			for _, arg := range envArgs {
				if strings.HasSuffix(arg, "-") {
					removeVars = append(removeVars, strings.TrimSuffix(arg, "-"))
					continue
				}

				parts := strings.SplitN(arg, "=", 2)
				if len(parts) != 2 {
					return fmt.Errorf("invalid env var format %q (expected KEY=VALUE)", arg)
				}

				envVars = append(envVars, map[string]interface{}{
					"name":  parts[0],
					"value": parts[1],
				})
			}

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

			// Get existing resource to merge env vars
			existing, err := dynamicClient.Resource(gvr).Namespace(ns).Get(ctx, resourceName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get %s/%s: %w", resourceType, resourceName, err)
			}

			// Get existing containers' env vars
			containers, found, err := unstructuredNestedSlice(existing.Object, "spec", "template", "spec", "containers")
			if err != nil || !found || len(containers) == 0 {
				return fmt.Errorf("no containers found in %s/%s", resourceType, resourceName)
			}

			// Update first container's env vars
			container := containers[0].(map[string]interface{})
			existingEnv, _, _ := unstructuredNestedSlice(container, "env")

			// Remove vars
			if len(removeVars) > 0 {
				removeSet := make(map[string]bool)
				for _, v := range removeVars {
					removeSet[v] = true
				}
				newEnv := make([]interface{}, 0)
				for _, e := range existingEnv {
					if envMap, ok := e.(map[string]interface{}); ok {
						if name, ok := envMap["name"].(string); ok && !removeSet[name] {
							newEnv = append(newEnv, e)
						}
					}
				}
				existingEnv = newEnv
			}

			// Add/update vars
			for _, newVar := range envVars {
				name := newVar["name"].(string)
				found := false
				for i, e := range existingEnv {
					if envMap, ok := e.(map[string]interface{}); ok {
						if envMap["name"] == name {
							existingEnv[i] = newVar
							found = true
							break
						}
					}
				}
				if !found {
					existingEnv = append(existingEnv, newVar)
				}
			}

			// Build patch
			container["env"] = existingEnv
			containers[0] = container

			patch := map[string]interface{}{
				"spec": map[string]interface{}{
					"template": map[string]interface{}{
						"spec": map[string]interface{}{
							"containers": containers,
						},
					},
				},
			}

			patchBytes, _ := json.Marshal(patch)
			_, err = dynamicClient.Resource(gvr).Namespace(ns).Patch(ctx, resourceName, types.MergePatchType, patchBytes, metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("failed to update env: %w", err)
			}

			fmt.Fprintf(out, "%s/%s env updated\n", resourceType, resourceName)
			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	return cmd
}

func newSetImageCommand() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "image <resource-type>/<name> <container>=<image>",
		Short: "Update container image(s) on a resource",
		Long: `Update container images on a deployment, statefulset, or other pod template resource.

Examples:
  # Set image for a specific container
  ocp set image deployment/myapp web=nginx:1.25

  # Set image for all containers
  ocp set image deploy/myapp *=myregistry/myimage:v2`,
		ValidArgsFunction: completeResourceTargetArgs,
		Args:              cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			ns := resolveNamespace(ctx, namespace, false)
			out := cmd.OutOrStdout()

			target := args[0]
			imageArgs := args[1:]

			resourceType, resourceName, err := parseResourceTarget(target)
			if err != nil {
				return err
			}

			// Parse container=image pairs
			imageUpdates := make(map[string]string)
			for _, arg := range imageArgs {
				parts := strings.SplitN(arg, "=", 2)
				if len(parts) != 2 {
					return fmt.Errorf("invalid format %q (expected CONTAINER=IMAGE)", arg)
				}
				imageUpdates[parts[0]] = parts[1]
			}

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

			existing, err := dynamicClient.Resource(gvr).Namespace(ns).Get(ctx, resourceName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get %s/%s: %w", resourceType, resourceName, err)
			}

			containers, found, err := unstructuredNestedSlice(existing.Object, "spec", "template", "spec", "containers")
			if err != nil || !found || len(containers) == 0 {
				return fmt.Errorf("no containers found in %s/%s", resourceType, resourceName)
			}

			for i, c := range containers {
				container := c.(map[string]interface{})
				name, _ := container["name"].(string)

				if newImage, ok := imageUpdates["*"]; ok {
					container["image"] = newImage
				} else if newImage, ok := imageUpdates[name]; ok {
					container["image"] = newImage
				}
				containers[i] = container
			}

			patch := map[string]interface{}{
				"spec": map[string]interface{}{
					"template": map[string]interface{}{
						"spec": map[string]interface{}{
							"containers": containers,
						},
					},
				},
			}

			patchBytes, _ := json.Marshal(patch)
			_, err = dynamicClient.Resource(gvr).Namespace(ns).Patch(ctx, resourceName, types.MergePatchType, patchBytes, metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("failed to update image: %w", err)
			}

			fmt.Fprintf(out, "%s/%s image updated\n", resourceType, resourceName)
			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	return cmd
}

func newSetResourcesCommand() *cobra.Command {
	var namespace string
	var limits string
	var requests string

	cmd := &cobra.Command{
		Use:   "resources <resource-type>/<name>",
		Short: "Update resource requests/limits on a resource",
		Long: `Set CPU and memory requests/limits on containers.

Examples:
  # Set limits
  ocp set resources deployment/myapp --limits=cpu=200m,memory=512Mi

  # Set requests
  ocp set resources deploy/myapp --requests=cpu=100m,memory=256Mi

  # Set both
  ocp set resources deploy/myapp --limits=cpu=200m,memory=512Mi --requests=cpu=100m,memory=256Mi`,
		ValidArgsFunction: completeResourceTargetArgs,
		Args:              cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			ns := resolveNamespace(ctx, namespace, false)
			out := cmd.OutOrStdout()

			target := args[0]
			resourceType, resourceName, err := parseResourceTarget(target)
			if err != nil {
				return err
			}

			if limits == "" && requests == "" {
				return fmt.Errorf("at least one of --limits or --requests is required")
			}

			resourceSpec := make(map[string]interface{})
			if limits != "" {
				resourceSpec["limits"] = parseResourceSpec(limits)
			}
			if requests != "" {
				resourceSpec["requests"] = parseResourceSpec(requests)
			}

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

			existing, err := dynamicClient.Resource(gvr).Namespace(ns).Get(ctx, resourceName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get %s/%s: %w", resourceType, resourceName, err)
			}

			containers, found, err := unstructuredNestedSlice(existing.Object, "spec", "template", "spec", "containers")
			if err != nil || !found || len(containers) == 0 {
				return fmt.Errorf("no containers found in %s/%s", resourceType, resourceName)
			}

			// Update all containers
			for i, c := range containers {
				container := c.(map[string]interface{})
				container["resources"] = resourceSpec
				containers[i] = container
			}

			patch := map[string]interface{}{
				"spec": map[string]interface{}{
					"template": map[string]interface{}{
						"spec": map[string]interface{}{
							"containers": containers,
						},
					},
				},
			}

			patchBytes, _ := json.Marshal(patch)
			_, err = dynamicClient.Resource(gvr).Namespace(ns).Patch(ctx, resourceName, types.MergePatchType, patchBytes, metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("failed to update resources: %w", err)
			}

			fmt.Fprintf(out, "%s/%s resources updated\n", resourceType, resourceName)
			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	cmd.Flags().StringVar(&limits, "limits", "", "Resource limits (e.g. cpu=200m,memory=512Mi)")
	cmd.Flags().StringVar(&requests, "requests", "", "Resource requests (e.g. cpu=100m,memory=256Mi)")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	return cmd
}

func newSetServiceAccountCommand() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "serviceaccount <resource-type>/<name> <service-account>",
		Short: "Update the service account of a resource",
		Long: `Set the service account on a deployment, statefulset, or other pod template resource.

Examples:
  ocp set serviceaccount deployment/myapp my-service-account`,
		Aliases:           []string{"sa"},
		ValidArgsFunction: completeResourceTargetArgs,
		Args:              cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			ns := resolveNamespace(ctx, namespace, false)
			out := cmd.OutOrStdout()

			target := args[0]
			serviceAccount := args[1]

			resourceType, resourceName, err := parseResourceTarget(target)
			if err != nil {
				return err
			}

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

			patch := map[string]interface{}{
				"spec": map[string]interface{}{
					"template": map[string]interface{}{
						"spec": map[string]interface{}{
							"serviceAccountName": serviceAccount,
						},
					},
				},
			}

			patchBytes, _ := json.Marshal(patch)
			_, err = dynamicClient.Resource(gvr).Namespace(ns).Patch(ctx, resourceName, types.MergePatchType, patchBytes, metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("failed to update service account: %w", err)
			}

			fmt.Fprintf(out, "%s/%s serviceaccount updated to %q\n", resourceType, resourceName, serviceAccount)
			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	return cmd
}

// parseResourceTarget parses "type/name" format
func parseResourceTarget(target string) (resourceType, resourceName string, err error) {
	parts := strings.SplitN(target, "/", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("expected TYPE/NAME format (e.g. deployment/myapp)")
	}
	return parts[0], parts[1], nil
}

// parseResourceSpec parses "cpu=200m,memory=512Mi" format
func parseResourceSpec(spec string) map[string]interface{} {
	result := make(map[string]interface{})
	for _, pair := range strings.Split(spec, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) == 2 {
			result[parts[0]] = parts[1]
		}
	}
	return result
}

// unstructuredNestedSlice retrieves a nested slice from an unstructured object
func unstructuredNestedSlice(obj map[string]interface{}, fields ...string) ([]interface{}, bool, error) {
	current := obj
	for i, field := range fields {
		if i == len(fields)-1 {
			val, ok := current[field]
			if !ok {
				return nil, false, nil
			}
			slice, ok := val.([]interface{})
			if !ok {
				return nil, false, fmt.Errorf("field %q is not a slice", field)
			}
			return slice, true, nil
		}

		next, ok := current[field]
		if !ok {
			return nil, false, nil
		}
		nextMap, ok := next.(map[string]interface{})
		if !ok {
			return nil, false, fmt.Errorf("field %q is not an object", field)
		}
		current = nextMap
	}
	return nil, false, nil
}

// completeResourceTargetArgs completes TYPE/NAME for set subcommands
func completeResourceTargetArgs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) > 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	// If user has typed something with a slash, complete the name part
	if strings.Contains(toComplete, "/") {
		parts := strings.SplitN(toComplete, "/", 2)
		ns, _ := cmd.Flags().GetString("namespace")
		names, directive := completeResourceNames(cmd, parts[0], parts[1], ns, false)
		// Prefix names with the resource type
		prefixed := make([]string, 0, len(names))
		for _, n := range names {
			prefixed = append(prefixed, parts[0]+"/"+n)
		}
		return prefixed, directive
	}

	// Otherwise complete resource types with trailing slash
	types := []string{
		"deployment/", "deploy/",
		"statefulset/", "sts/",
		"daemonset/", "ds/",
		"replicaset/", "rs/",
	}
	return filterCompletions(types, toComplete), cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
}
