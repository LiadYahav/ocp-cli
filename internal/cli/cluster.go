package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	unstructuredhelpers "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Display cluster information",
		Long: `Display information about the current Kubernetes cluster.

Examples:
  ocp cluster info
  ocp cluster version
  ocp cluster configure-dns`,
	}

	cmd.AddCommand(
		newClusterInfoCommand(),
		newClusterVersionCommand(),
		newClusterConfigureDNSCommand(),
		newClusterWatchCommand(),
	)

	return cmd
}

func newClusterInfoCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "info",
		Short: "Display cluster information",
		Example: `  # Show summary information about the current cluster
  ocp cluster info`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			// Get cluster context information
			contextName, clusterName, err := kube.GetCurrentContext(ctx)
			if err != nil {
				return fmt.Errorf("failed to get current context: %w", err)
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			// Fetch all data concurrently for better performance
			var nodes *corev1.NodeList
			var namespaces *corev1.NamespaceList
			var pods *corev1.PodList
			var nodesErr, namespacesErr, podsErr error

			var wg sync.WaitGroup
			wg.Add(3)

			// Get nodes
			go func() {
				defer wg.Done()
				nodes, nodesErr = clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			}()

			// Get namespaces
			go func() {
				defer wg.Done()
				namespaces, namespacesErr = clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
			}()

			// Get all pods with pagination support for large clusters
			go func() {
				defer wg.Done()
				// Use pagination for better performance on large clusters
				opts := metav1.ListOptions{Limit: 500}
				var allPods []corev1.Pod
				for {
					list, err := clientset.CoreV1().Pods("").List(ctx, opts)
					if err != nil {
						podsErr = err
						return
					}
					allPods = append(allPods, list.Items...)
					if list.Continue == "" {
						break
					}
					opts.Continue = list.Continue
				}
				pods = &corev1.PodList{Items: allPods}
			}()

			wg.Wait()

			if nodesErr != nil {
				return fmt.Errorf("failed to list nodes: %w", nodesErr)
			}
			if namespacesErr != nil {
				return fmt.Errorf("failed to list namespaces: %w", namespacesErr)
			}
			if podsErr != nil {
				return fmt.Errorf("failed to list pods: %w", podsErr)
			}

			fmt.Fprintln(cmd.OutOrStdout(), "Cluster Information:")
			fmt.Fprintf(cmd.OutOrStdout(), "  Cluster:    %s\n", clusterName)
			fmt.Fprintf(cmd.OutOrStdout(), "  Context:    %s\n", contextName)
			fmt.Fprintf(cmd.OutOrStdout(), "  Nodes:      %d\n", len(nodes.Items))
			fmt.Fprintf(cmd.OutOrStdout(), "  Namespaces: %d\n", len(namespaces.Items))
			fmt.Fprintf(cmd.OutOrStdout(), "  Pods:       %d\n", len(pods.Items))

			if consoleURL, err := getConsoleURL(ctx); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "warning: failed to fetch console route: %v\n", err)
			} else if consoleURL != "" {
				fmt.Fprintf(cmd.OutOrStdout(), "  Console:    %s\n", consoleURL)
			}

			return nil
		},
	}
}

func newClusterVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Display cluster version information",
		Example: `  # Show the cluster's Kubernetes version details
  ocp cluster version`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			version, err := clientset.Discovery().ServerVersion()
			if err != nil {
				return fmt.Errorf("failed to get server version: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Kubernetes Version: %s\n", version.GitVersion)
			fmt.Fprintf(cmd.OutOrStdout(), "Platform:           %s\n", version.Platform)
			fmt.Fprintf(cmd.OutOrStdout(), "Build Date:         %s\n", version.BuildDate)

			return nil
		},
	}
}

func newClusterWatchCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "watch",
		Short: "Watch important cluster resources (auto-detects OpenShift vs Kubernetes)",
		Long: `Continuously watch and display important cluster resources, refreshing every 1 second.

The command automatically detects the cluster distribution:
- OpenShift: Shows ClusterOperators, ClusterVersions, and MachineConfigPools
- Kubernetes: Shows Nodes, Namespaces summary, and Pods summary

Changes are highlighted in yellow for easy identification.`,
		Example: `  # Watch cluster resources (auto-detects distribution)
  ocp cluster watch`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			return watchClusterResources(ctx, cmd.OutOrStdout())
		},
	}
}

// detectClusterDistribution detects if the cluster is OpenShift or vanilla Kubernetes
func detectClusterDistribution(ctx context.Context) (string, error) {
	dynamicClient, err := kube.NewDynamicClient(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to create dynamic client: %w", err)
	}

	// Check for OpenShift-specific resources
	openshiftGVRs := []schema.GroupVersionResource{
		{Group: "config.openshift.io", Version: "v1", Resource: "clusteroperators"},
		{Group: "config.openshift.io", Version: "v1", Resource: "clusterversions"},
		{Group: "route.openshift.io", Version: "v1", Resource: "routes"},
	}

	for _, gvr := range openshiftGVRs {
		_, err := dynamicClient.Resource(gvr).List(ctx, metav1.ListOptions{Limit: 1})
		if err == nil {
			return "openshift", nil
		}
		if !meta.IsNoMatchError(err) {
			// If it's not a "not found" error, it might be a permission issue
			// but we'll assume it's OpenShift if we can query the API group
			return "openshift", nil
		}
	}

	return "kubernetes", nil
}

// watchClusterResources continuously watches and displays important cluster resources
func watchClusterResources(ctx context.Context, out io.Writer) error {
	dynamicClient, err := kube.NewDynamicClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	// Detect cluster distribution
	distro, err := detectClusterDistribution(ctx)
	if err != nil {
		return fmt.Errorf("failed to detect cluster distribution: %w", err)
	}

	// Define resources to watch based on cluster distribution
	var resources []struct {
		name string
		gvr  schema.GroupVersionResource
	}

	if distro == "openshift" {
		// OpenShift-specific resources
		resources = []struct {
			name string
			gvr  schema.GroupVersionResource
		}{
			{"clusteroperators", schema.GroupVersionResource{Group: "config.openshift.io", Version: "v1", Resource: "clusteroperators"}},
			{"clusterversions", schema.GroupVersionResource{Group: "config.openshift.io", Version: "v1", Resource: "clusterversions"}},
			{"machineconfigpools", schema.GroupVersionResource{Group: "machineconfiguration.openshift.io", Version: "v1", Resource: "machineconfigpools"}},
		}
	} else {
		// Vanilla Kubernetes - show important cluster resources
		clientset, err := kube.NewClientset(ctx)
		if err != nil {
			return fmt.Errorf("failed to create Kubernetes client: %w", err)
		}

		// For vanilla K8s, we'll show nodes, namespaces summary, and pods summary
		// We'll use a different approach - fetch data and display it
		return watchKubernetesResources(ctx, clientset, out)
	}

	var lastOutput string
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Clear screen and hide cursor
	fmt.Fprint(out, "\033[2J\033[H\033[?25l")
	defer fmt.Fprint(out, "\033[?25h") // Show cursor on exit

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			var output strings.Builder

			// Fetch and display each resource type
			for _, res := range resources {
				list, err := dynamicClient.Resource(res.gvr).List(ctx, metav1.ListOptions{})
				if err != nil {
					// If resource is not available (CRD not installed), skip it
					if meta.IsNoMatchError(err) {
						continue
					}
					fmt.Fprintf(&output, "Error fetching %s: %v\n", res.name, err)
					continue
				}

				// Print header for this resource type
				fmt.Fprintf(&output, "\n=== %s ===\n", strings.ToUpper(res.name))

				// Print table header
				if res.name == "clusteroperators" {
					fmt.Fprintf(&output, "%-50s %-15s %-12s %-12s %-12s\n", "NAME", "VERSION", "AVAILABLE", "PROGRESSING", "DEGRADED")
				} else if res.name == "clusterversions" {
					fmt.Fprintf(&output, "%-20s %-30s %-12s %-12s\n", "NAME", "VERSION", "AVAILABLE", "PROGRESSING")
				} else if res.name == "machineconfigpools" {
					fmt.Fprintf(&output, "%-25s %-50s %-10s %-10s %-10s\n", "NAME", "CONFIG", "UPDATED", "UPDATING", "DEGRADED")
				}

				// Print each item
				for _, item := range list.Items {
					name, _, _ := unstructuredhelpers.NestedString(item.Object, "metadata", "name")

					if res.name == "clusteroperators" {
						// Try to find version
						version := "<none>"
						if versions, found, _ := unstructuredhelpers.NestedSlice(item.Object, "status", "versions"); found {
							for _, v := range versions {
								if vMap, ok := v.(map[string]interface{}); ok {
									// Prefer "operator" version, or take the first one
									vName, _, _ := unstructuredhelpers.NestedString(vMap, "name")
									vVer, _, _ := unstructuredhelpers.NestedString(vMap, "version")
									if vName == "operator" {
										version = vVer
										break
									}
									if version == "<none>" {
										version = vVer
									}
								}
							}
						}
						
						conditions, _, _ := unstructuredhelpers.NestedSlice(item.Object, "status", "conditions")
						available := "Unknown"
						progressing := "Unknown"
						degraded := "Unknown"

						for _, cond := range conditions {
							condMap, ok := cond.(map[string]interface{})
							if !ok {
								continue
							}
							condType, _, _ := unstructuredhelpers.NestedString(condMap, "type")
							condStatus, _, _ := unstructuredhelpers.NestedString(condMap, "status")
							if condType == "Available" {
								available = condStatus
							}
							if condType == "Progressing" {
								progressing = condStatus
							}
							if condType == "Degraded" {
								degraded = condStatus
							}
						}

						fmt.Fprintf(&output, "%-50s %-15s %-12s %-12s %-12s\n", name, version, available, progressing, degraded)
					} else if res.name == "clusterversions" {
						version, _, _ := unstructuredhelpers.NestedString(item.Object, "status", "desired", "version")
						if version == "" {
							// Try history if desired is empty
							if history, found, _ := unstructuredhelpers.NestedSlice(item.Object, "status", "history"); found && len(history) > 0 {
								if last, ok := history[0].(map[string]interface{}); ok {
									version, _, _ = unstructuredhelpers.NestedString(last, "version")
								}
							}
						}
						
						conditions, _, _ := unstructuredhelpers.NestedSlice(item.Object, "status", "conditions")
						available := "Unknown"
						progressing := "Unknown"

						for _, cond := range conditions {
							condMap, ok := cond.(map[string]interface{})
							if !ok {
								continue
							}
							condType, _, _ := unstructuredhelpers.NestedString(condMap, "type")
							condStatus, _, _ := unstructuredhelpers.NestedString(condMap, "status")
							if condType == "Available" {
								available = condStatus
							}
							if condType == "Progressing" {
								progressing = condStatus
							}
						}

					if version == "" {
						version = "<none>"
					}
						fmt.Fprintf(&output, "%-20s %-30s %-12s %-12s\n", name, version, available, progressing)
					} else if res.name == "machineconfigpools" {
						config, _, _ := unstructuredhelpers.NestedString(item.Object, "status", "configuration", "name")
						conditions, _, _ := unstructuredhelpers.NestedSlice(item.Object, "status", "conditions")
						updated := "False"
						updating := "False"
						degraded := "False"

						for _, cond := range conditions {
							condMap, ok := cond.(map[string]interface{})
							if !ok {
								continue
							}
							condType, _, _ := unstructuredhelpers.NestedString(condMap, "type")
							condStatus, _, _ := unstructuredhelpers.NestedString(condMap, "status")
							
							if condStatus == "True" {
								if condType == "Updated" {
									updated = "True"
								}
								if condType == "Updating" {
									updating = "True"
								}
								if condType == "Degraded" {
									degraded = "True"
								}
							}
						}

					if config == "" {
						config = "<none>"
					}
						fmt.Fprintf(&output, "%-25s %-50s %-10s %-10s %-10s\n", name, config, updated, updating, degraded)
					}
				}
			}

			currentOutput := output.String()

			// Clear screen and move cursor to top
			fmt.Fprint(out, "\033[2J\033[H")

			// Print timestamp
			fmt.Fprintf(out, "Every 1.0s: ocp cluster watch (%s)  %s\n\n", distro, time.Now().Format("Mon Jan 2 15:04:05 2006"))

			// Print output with diff highlighting if previous output exists
			if lastOutput != "" {
				printDiff(lastOutput, currentOutput, out)
			} else {
				fmt.Fprint(out, currentOutput)
			}

			lastOutput = currentOutput
		}
	}
}

// watchKubernetesResources watches important resources for vanilla Kubernetes clusters
func watchKubernetesResources(ctx context.Context, clientset *kubernetes.Clientset, out io.Writer) error {
	var lastOutput string
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Clear screen and hide cursor
	fmt.Fprint(out, "\033[2J\033[H\033[?25l")
	defer fmt.Fprint(out, "\033[?25h") // Show cursor on exit

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			var output strings.Builder

			// Get nodes
			nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if err == nil {
				fmt.Fprintf(&output, "\n=== NODES ===\n")
				fmt.Fprintf(&output, "%-50s %-15s %-15s %-15s\n", "NAME", "STATUS", "ROLES", "AGE")
				for _, node := range nodes.Items {
					status := "Unknown"
					for _, cond := range node.Status.Conditions {
						if cond.Type == corev1.NodeReady {
							if cond.Status == corev1.ConditionTrue {
								status = "Ready"
							} else {
								status = "NotReady"
							}
							break
						}
					}
					roles := ""
					for key := range node.Labels {
						if strings.HasPrefix(key, "node-role.kubernetes.io/") {
							role := strings.TrimPrefix(key, "node-role.kubernetes.io/")
							if roles != "" {
								roles += ","
							}
							roles += role
						}
					}
					if roles == "" {
						roles = "<none>"
					}
					age := formatAgeHelper(node.CreationTimestamp.Time)
					fmt.Fprintf(&output, "%-50s %-15s %-15s %-15s\n", node.Name, status, roles, age)
				}
			}

			// Get namespaces summary
			namespaces, err := clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
			if err == nil {
				fmt.Fprintf(&output, "\n=== NAMESPACES ===\n")
				fmt.Fprintf(&output, "Total: %d\n", len(namespaces.Items))
			}

			// Get pods summary
			pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
			if err == nil {
				fmt.Fprintf(&output, "\n=== PODS ===\n")
				running := 0
				pending := 0
				failed := 0
				succeeded := 0
				for _, pod := range pods.Items {
					switch pod.Status.Phase {
					case corev1.PodRunning:
						running++
					case corev1.PodPending:
						pending++
					case corev1.PodFailed:
						failed++
					case corev1.PodSucceeded:
						succeeded++
					}
				}
				fmt.Fprintf(&output, "Total: %d (Running: %d, Pending: %d, Failed: %d, Succeeded: %d)\n",
					len(pods.Items), running, pending, failed, succeeded)
			}

			currentOutput := output.String()

			// Clear screen and move cursor to top
			fmt.Fprint(out, "\033[2J\033[H")

			// Print timestamp
			fmt.Fprintf(out, "Every 1.0s: ocp cluster watch (kubernetes)  %s\n\n", time.Now().Format("Mon Jan 2 15:04:05 2006"))

			// Print output with diff highlighting if previous output exists
			if lastOutput != "" {
				printDiff(lastOutput, currentOutput, out)
			} else {
				fmt.Fprint(out, currentOutput)
			}

			lastOutput = currentOutput
		}
	}
}

// formatAgeHelper formats a time duration as a human-readable age string
func formatAgeHelper(t time.Time) string {
	duration := time.Since(t)
	if duration < time.Minute {
		return fmt.Sprintf("%ds", int(duration.Seconds()))
	}
	if duration < time.Hour {
		return fmt.Sprintf("%dm", int(duration.Minutes()))
	}
	if duration < 24*time.Hour {
		return fmt.Sprintf("%dh", int(duration.Hours()))
	}
	days := int(duration.Hours() / 24)
	if days < 365 {
		return fmt.Sprintf("%dd", days)
	}
	years := days / 365
	remainingDays := days % 365
	if remainingDays == 0 {
		return fmt.Sprintf("%dy", years)
	}
	return fmt.Sprintf("%dy%dd", years, remainingDays)
}

// printDiff highlights differences between old and new output (similar to watch -d)
func printDiff(old, new string, out io.Writer) {
	oldLines := strings.Split(old, "\n")
	newLines := strings.Split(new, "\n")

	maxLen := len(oldLines)
	if len(newLines) > maxLen {
		maxLen = len(newLines)
	}

	for i := 0; i < maxLen; i++ {
		var oldLine, newLine string
		if i < len(oldLines) {
			oldLine = oldLines[i]
		}
		if i < len(newLines) {
			newLine = newLines[i]
		}

		if oldLine != newLine {
			// Highlight changed lines
			fmt.Fprintf(out, "\033[1;33m%s\033[0m\n", newLine) // Yellow for changes
		} else {
			fmt.Fprintf(out, "%s\n", newLine)
		}
	}
}

func newClusterConfigureDNSCommand() *cobra.Command {
	var user string
	var identityFile string
	var maxConcurrency int
	var maxRetries int
	var confirm bool
	var resolveDomain string
	var skipValidation bool

	cmd := &cobra.Command{
		Use:   "configure-dns <nameservers>",
		Short: "Configure node DNS settings",
		Long: `Configure DNS servers on every node via NetworkManager.

**RISK**: This command modifies live networking on all nodes. Ensure you have
         console access before proceeding.

**WARNING**: This is a destructive operation that modifies system networking.
Use --confirm to proceed without prompting.`,
		Example: `  # Prepend a single nameserver while keeping existing entries
  ocp cluster configure-dns 1.1.1.1

  # Override DNS on all nodes with two nameservers
  ocp cluster configure-dns 8.8.8.8,8.8.4.4

  # Configure with custom concurrency and retries
  ocp cluster configure-dns 8.8.8.8 --max-concurrency 10 --max-retries 5

  # Validate nameserver by resolving a specific domain instead of example.com
  ocp cluster configure-dns 10.0.0.1 --resolve-domain mycompany.local

  # Skip nameserver validation entirely
  ocp cluster configure-dns 10.0.0.1 --skip-validation`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := ensureContext(cmd.Context())
			
			// Require confirmation for destructive operation
			if !confirm {
				fmt.Fprintf(cmd.ErrOrStderr(), "WARNING: This will modify DNS settings on ALL nodes in the cluster\n")
				fmt.Fprintf(cmd.ErrOrStderr(), "Ensure you have console access before proceeding\n")
				fmt.Fprintf(cmd.ErrOrStderr(), "Use --confirm to proceed without this prompt\n")
				return fmt.Errorf("confirmation required for destructive operation. Use --confirm to proceed")
			}
			
			// Add timeout for DNS configuration operation
			opCtx, cancel := withTimeout(ctx, LongTimeout)
			defer cancel()

			nameservers, err := parseNameservers(args[0])
			if err != nil {
				return formatErrorWithContext(err, "parseNameservers", "", "", "")
			}

			if !skipValidation {
				domain := "example.com"
				if resolveDomain != "" {
					domain = resolveDomain
				}
				if err := validateNameserversWithDomain(opCtx, nameservers, domain); err != nil {
					return formatErrorWithContext(err, "validateNameservers", "", "", "")
				}
			}

			clientset, err := kube.NewClientset(opCtx)
			if err != nil {
				return formatErrorWithContext(err, "NewClientset", "", "", "")
			}

			nodes, err := clientset.CoreV1().Nodes().List(opCtx, metav1.ListOptions{})
			if err != nil {
				return formatErrorWithContext(err, "ListNodes", "nodes", "", "")
			}

			if len(nodes.Items) == 0 {
				return errors.New("no nodes found in the cluster")
			}

			mode := dnsModeOverride
			if len(nameservers) == 1 {
				mode = dnsModeAppend
			}

			// Set defaults
			if maxConcurrency <= 0 {
				maxConcurrency = 10
			}
			if maxRetries <= 0 {
				maxRetries = 3
			}

			// Use a worker pool pattern for concurrency
			type nodeResult struct {
				nodeName string
				err      error
			}

			nodeChan := make(chan string, len(nodes.Items))
			resultChan := make(chan nodeResult, len(nodes.Items))

			// Send all node names to the channel
			for _, node := range nodes.Items {
				nodeChan <- node.Name
			}
			close(nodeChan)

			// Start worker goroutines
			var wg sync.WaitGroup
			for i := 0; i < maxConcurrency; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for nodeName := range nodeChan {
						fmt.Fprintf(cmd.OutOrStdout(), "Configuring DNS on %s...\n", nodeName)
						script := buildDNSConfigureScript(nameservers, mode)
						err := runSSHCommandWithRetry(opCtx, user, identityFile, nodeName, []string{script}, cmd, maxRetries, time.Second)
						if err != nil {
							err = formatErrorWithContext(err, "configureDNS", "node", nodeName, "")
						}
						resultChan <- nodeResult{nodeName: nodeName, err: err}
					}
				}()
			}

			// Wait for all workers to finish
			go func() {
				wg.Wait()
				close(resultChan)
			}()

			// Collect results and report errors
			var failedNodes []string
			var successCount int
			for result := range resultChan {
				if result.err != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "✗ Error: failed to configure DNS on node %s: %v\n", result.nodeName, result.err)
					failedNodes = append(failedNodes, result.nodeName)
				} else {
					successCount++
					fmt.Fprintf(cmd.OutOrStdout(), "✓ Successfully configured DNS on %s\n", result.nodeName)
				}
			}

			// Print summary
			fmt.Fprintf(cmd.OutOrStdout(), "\n=== Summary ===\n")
			fmt.Fprintf(cmd.OutOrStdout(), "Total nodes: %d\n", len(nodes.Items))
			fmt.Fprintf(cmd.OutOrStdout(), "Successful: %d\n", successCount)
			fmt.Fprintf(cmd.OutOrStdout(), "Failed: %d\n", len(failedNodes))

			if len(failedNodes) > 0 {
				fmt.Fprintf(cmd.ErrOrStderr(), "\n✗ Failed nodes:\n")
				for _, nodeName := range failedNodes {
					fmt.Fprintf(cmd.ErrOrStderr(), "  - %s\n", nodeName)
				}
				return fmt.Errorf("failed to configure DNS on %d node(s) out of %d total", len(failedNodes), len(nodes.Items))
			}

			fmt.Fprintf(cmd.OutOrStdout(), "\n✓ DNS configuration updated successfully on all %d node(s).\n", successCount)
			return nil
		},
	}

	cmd.Flags().StringVarP(&user, "user", "u", "core", "Username for SSH connection")
	cmd.Flags().StringVarP(&identityFile, "identity", "i", "", "Path to private key file for SSH authentication (default: ~/.ssh/id_rsa_ocp if exists)")
	cmd.Flags().IntVar(&maxConcurrency, "max-concurrency", 10, "Maximum number of concurrent SSH connections")
	cmd.Flags().IntVar(&maxRetries, "max-retries", 3, "Maximum number of retry attempts per node")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "Confirm destructive operation without prompting")
	cmd.Flags().StringVar(&resolveDomain, "resolve-domain", "", "Domain to resolve when validating nameservers (default: example.com)")
	cmd.Flags().BoolVar(&skipValidation, "skip-validation", false, "Skip nameserver validation (use with caution)")

	return cmd
}

func getConsoleURL(ctx context.Context) (string, error) {
	dynamicClient, err := kube.NewDynamicClient(ctx)
	if err != nil {
		return "", err
	}

	gvr := schema.GroupVersionResource{
		Group:    "route.openshift.io",
		Version:  "v1",
		Resource: "routes",
	}

	route, err := dynamicClient.Resource(gvr).Namespace("openshift-console").Get(ctx, "console", metav1.GetOptions{})
	if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	host, found, _ := unstructuredhelpers.NestedString(route.Object, "spec", "host")
	if !found || host == "" {
		if ingress, found, _ := unstructuredhelpers.NestedSlice(route.Object, "status", "ingress"); found {
			for _, entry := range ingress {
				if m, ok := entry.(map[string]interface{}); ok {
					if candidate, ok := m["host"].(string); ok && candidate != "" {
						host = candidate
						break
					}
				}
			}
		}
	}

	if host == "" {
		return "", nil
	}

	return fmt.Sprintf("https://%s/", strings.TrimSuffix(host, "/")), nil
}

type dnsMode string

const (
	dnsModeAppend   dnsMode = "append"
	dnsModeOverride dnsMode = "override"
)

func parseNameservers(arg string) ([]string, error) {
	var result []string
	for _, part := range strings.Split(arg, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// Validate IP address format
		if ip := net.ParseIP(part); ip == nil {
			return nil, fmt.Errorf("invalid IP address: %q", part)
		}
		// Sanitize: ensure no shell metacharacters
		if strings.ContainsAny(part, ";&|`$(){}[]<>\"'\\\n\r\t") {
			return nil, fmt.Errorf("invalid characters in nameserver: %q", part)
		}
		result = append(result, part)
	}

	if len(result) == 0 {
		return nil, errors.New("no nameservers provided")
	}

	return result, nil
}

func validateNameservers(ctx context.Context, nameservers []string) error {
	return validateNameserversWithDomain(ctx, nameservers, "example.com")
}

func validateNameserversWithDomain(ctx context.Context, nameservers []string, domain string) error {
	for _, ns := range nameservers {
		if ip := net.ParseIP(ns); ip == nil {
			return fmt.Errorf("invalid nameserver IP: %s", ns)
		}

		if err := queryNameserverWithDomain(ctx, ns, domain); err != nil {
			return fmt.Errorf("nameserver %s failed validation (resolving %s): %w", ns, domain, err)
		}
	}

	return nil
}

func queryNameserver(ctx context.Context, nameserver string) error {
	return queryNameserverWithDomain(ctx, nameserver, "example.com")
}

func queryNameserverWithDomain(ctx context.Context, nameserver, domain string) error {
	lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(lookupCtx, "nslookup", domain, nameserver)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("nslookup failed: %w", err)
	}

	return nil
}

func buildDNSConfigureScript(nameservers []string, mode dnsMode) string {
	// Nameservers are already validated in parseNameservers
	// Join with comma - safe because IPs are validated
	joined := strings.Join(nameservers, ",")
	
	// Build script with proper escaping
	// Use single quotes around the DNS value to prevent injection
	body := fmt.Sprintf(`set -euo pipefail
conn=$(nmcli -t -f NAME connection show --active | head -n1)
if [ -z "$conn" ]; then
  echo "No active NetworkManager connection found" >&2
  exit 1
fi
current=$(nmcli -g ipv4.dns connection show "$conn" | tr -d "\r")
newdns='%s'
if [ "%s" = "append" ] && [ -n "$current" ]; then
  newdns="${newdns},${current}"
fi
nmcli connection modify "$conn" ipv4.dns "$newdns"
nmcli connection modify "$conn" ipv4.ignore-auto-dns yes
nmcli connection up "$conn" >/dev/null 2>&1 || true
systemctl restart NetworkManager
`, joined, mode)

	// Escape single quotes in the script body for safe passing to bash -c
	escapedBody := strings.ReplaceAll(body, "'", "'\"'\"'")
	return fmt.Sprintf("sudo bash -c '%s'", escapedBody)
}
