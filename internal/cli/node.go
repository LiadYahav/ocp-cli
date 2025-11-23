package cli

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

const (
	maintenanceAnnotationKey   = "node.dana.io/reason"
	maintenanceAnnotationValue = "Maintenance"
)

func newNodeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Work with nodes in the cluster",
	}

	cmd.AddCommand(
		newNodeListCommand(),
		newNodeRebootCommand(),
		newNodeCordonCommand(),
		newNodeDrainCommand(),
		newNodeUncordonCommand(),
		newNodeGetPodsCommand(),
	)

	return cmd
}

func newNodeListCommand() *cobra.Command {
	var schedulingDisabled bool
	var schedulable bool

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List nodes in the cluster",
		Long: `List nodes in the cluster. Use flags to filter by scheduling state:
  --scheduling-disabled, -d  Show only nodes that are in SchedulingDisabled state
  --schedulable, -s            Show only nodes that are schedulable (not in SchedulingDisabled state)`,
		Example: `  # List all nodes
  ocp node list

  # List only nodes that are SchedulingDisabled
  ocp node list --scheduling-disabled
  ocp node list -d

  # List only schedulable nodes
  ocp node list --schedulable
  ocp node list -s`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if err != nil {
				return fmt.Errorf("failed to list nodes: %w", err)
			}

			// Filter nodes based on flags - iterate once and print directly
			for _, node := range nodes.Items {
				isDisabled := node.Spec.Unschedulable

				if schedulingDisabled && !isDisabled {
					continue
				}
				if schedulable && isDisabled {
					continue
				}

				fmt.Fprintln(cmd.OutOrStdout(), node.Name)
			}

			return nil
		},
	}

	cmd.Flags().BoolVarP(&schedulingDisabled, "scheduling-disabled", "d", false, "Show only nodes that are in SchedulingDisabled state")
	cmd.Flags().BoolVarP(&schedulable, "schedulable", "s", false, "Show only nodes that are schedulable (not in SchedulingDisabled state)")

	return cmd
}

func newNodeRebootCommand() *cobra.Command {
	var user string
	var identityFile string
	var maxRetries int

	cmd := &cobra.Command{
		Use:               "reboot <node-name>",
		ValidArgsFunction: completeNodeNames,
		Short:             "Reboot a node via SSH",
		Long: `Reboot a node by SSHing into it and running 'sudo reboot'.
This command connects to the specified node by name and executes the reboot command.

The command will automatically retry failed SSH connections (default: 3 retries).
Use --max-retries to customize the number of retry attempts.`,
		Args: cobra.ExactArgs(1),
		Example: `  # Reboot a specific node
  ocp node reboot worker-2

  # Reboot with custom retry count
  ocp node reboot worker-2 --max-retries 5`,
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeName := args[0]

			// Use retry logic for SSH commands
			if maxRetries <= 0 {
				maxRetries = 3
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Rebooting node %s...\n", nodeName)
			err := runSSHCommandWithRetry(cmd.Context(), user, identityFile, nodeName, []string{"sudo reboot"}, cmd, maxRetries, time.Second)
			if err != nil {
				return fmt.Errorf("failed to reboot node %s: %w", nodeName, err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Reboot command sent successfully to node %s\n", nodeName)
			return nil
		},
	}

	cmd.Flags().StringVarP(&user, "user", "u", "core", "Username for SSH connection")
	cmd.Flags().StringVarP(&identityFile, "identity", "i", "", "Path to private key file for SSH authentication (default: ~/.ssh/id_rsa_ocp if exists)")
	cmd.Flags().IntVar(&maxRetries, "max-retries", 3, "Maximum number of retry attempts for SSH connection")

	return cmd
}

func newNodeCordonCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "cordon <node-name>",
		ValidArgsFunction: completeSchedulableNodes,
		Short:             "Mark node as unschedulable",
		Long: `Mark a node as unschedulable to prevent new pods from being scheduled on it.
The command will automatically add the annotation "node.dana.io/reason: Maintenance" before cordoning.`,
		Args: cobra.ExactArgs(1),
		Example: `  # Prevent new pods from scheduling on a node (annotation will be added automatically)
  ocp node cordon master-0`,
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeName := args[0]

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			node, err := findNodeByName(ctx, nodeName)
			if err != nil {
				return err
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			// Add maintenance annotation if it doesn't exist and cordon in a single update
			needsUpdate := false
			if node.Annotations == nil {
				node.Annotations = make(map[string]string)
			}
			if node.Annotations[maintenanceAnnotationKey] != maintenanceAnnotationValue {
				node.Annotations[maintenanceAnnotationKey] = maintenanceAnnotationValue
				needsUpdate = true
			}
			
			// Cordon the node
			if !node.Spec.Unschedulable {
				node.Spec.Unschedulable = true
				needsUpdate = true
			}
			
			if needsUpdate {
				_, err = clientset.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
				if err != nil {
					return fmt.Errorf("failed to update node %q: %w", node.Name, err)
				}
				if node.Annotations[maintenanceAnnotationKey] == maintenanceAnnotationValue {
					fmt.Fprintf(cmd.OutOrStdout(), "Added maintenance annotation to node %q\n", node.Name)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Node %q marked as unschedulable\n", node.Name)
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "Node %q is already unschedulable\n", node.Name)
			}
			
			return nil
		},
	}
	return cmd
}

func newNodeDrainCommand() *cobra.Command {
	var maxConcurrency int
	var confirm bool

	cmd := &cobra.Command{
		Use:               "drain <node-name>",
		ValidArgsFunction: completeSchedulableNodes,
		Short:             "Drain a node (cordon + evict pods)",
		Long: `Drain a node by marking it as unschedulable and evicting all pods.
The command will automatically add the annotation "node.dana.io/reason: Maintenance" before draining.

**WARNING**: This is a destructive operation that will evict all pods from the node.
Use --confirm to proceed without prompting.

Pod evictions are performed concurrently for better performance. Use --max-concurrency
to control the number of concurrent evictions (default: 5).`,
		Args: cobra.ExactArgs(1),
		Example: `  # Drain a node (annotation will be added automatically)
  ocp node drain worker-3 --confirm

  # Drain with custom concurrency
  ocp node drain worker-3 --max-concurrency 10 --confirm`,
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeName := args[0]

			ctx := ensureContext(cmd.Context())
			
			// Require confirmation for destructive operation
			if !confirm {
				fmt.Fprintf(cmd.ErrOrStderr(), "WARNING: This will evict all pods from node %q\n", nodeName)
				fmt.Fprintf(cmd.ErrOrStderr(), "Use --confirm to proceed without this prompt\n")
				return fmt.Errorf("confirmation required for destructive operation. Use --confirm to proceed")
			}

			node, err := findNodeByName(ctx, nodeName)
			if err != nil {
				return err
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			// Add maintenance annotation if it doesn't exist and cordon in a single update
			needsUpdate := false
			if node.Annotations == nil {
				node.Annotations = make(map[string]string)
			}
			if node.Annotations[maintenanceAnnotationKey] != maintenanceAnnotationValue {
				node.Annotations[maintenanceAnnotationKey] = maintenanceAnnotationValue
				needsUpdate = true
			}
			
			// Cordon the node
			if !node.Spec.Unschedulable {
				node.Spec.Unschedulable = true
				needsUpdate = true
			}
			
			if needsUpdate {
				_, err = clientset.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
				if err != nil {
					return fmt.Errorf("failed to update node %q: %w", node.Name, err)
				}
				if node.Annotations[maintenanceAnnotationKey] == maintenanceAnnotationValue {
					fmt.Fprintf(cmd.OutOrStdout(), "Added maintenance annotation to node %q\n", node.Name)
				}
			}

			// Evict all pods
			pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
				FieldSelector: fmt.Sprintf("spec.nodeName=%s", node.Name),
			})
			if err != nil {
				return fmt.Errorf("failed to list pods on node %q: %w", node.Name, err)
			}

			// Set default concurrency
			if maxConcurrency <= 0 {
				maxConcurrency = 5
			}

			// Filter out DaemonSet pods first
			type podToEvict struct {
				namespace string
				name      string
			}
			var podsToEvict []podToEvict
			var skippedCount int

			for _, pod := range pods.Items {
				// Skip DaemonSet pods
				if pod.OwnerReferences != nil {
					isDaemonSet := false
					for _, ref := range pod.OwnerReferences {
						if ref.Kind == "DaemonSet" {
							isDaemonSet = true
							break
						}
					}
					if isDaemonSet {
						fmt.Fprintf(cmd.OutOrStdout(), "⊘ Skipping DaemonSet pod %s/%s\n", pod.Namespace, pod.Name)
						skippedCount++
						continue
					}
				}
				podsToEvict = append(podsToEvict, podToEvict{namespace: pod.Namespace, name: pod.Name})
			}

			// Use worker pool pattern for concurrent evictions
			type evictionResult struct {
				podNamespace string
				podName      string
				err          error
			}

			podChan := make(chan podToEvict, len(podsToEvict))
			resultChan := make(chan evictionResult, len(podsToEvict))

			// Send all pods to evict to the channel
			for _, pod := range podsToEvict {
				podChan <- pod
			}
			close(podChan)

			// Start worker goroutines
			var wg sync.WaitGroup
			for i := 0; i < maxConcurrency; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for pod := range podChan {
						eviction := &policyv1.Eviction{
							ObjectMeta: metav1.ObjectMeta{
								Name:      pod.name,
								Namespace: pod.namespace,
							},
						}

						err := clientset.CoreV1().Pods(pod.namespace).EvictV1(ctx, eviction)
						resultChan <- evictionResult{
							podNamespace: pod.namespace,
							podName:      pod.name,
							err:          err,
						}
					}
				}()
			}

			// Wait for all workers to finish
			go func() {
				wg.Wait()
				close(resultChan)
			}()

			// Collect results
			var evictedCount int
			var failedCount int
			var failedPods []string

			for result := range resultChan {
				if result.err != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "✗ Failed to evict pod %s/%s: %v\n", result.podNamespace, result.podName, result.err)
					failedCount++
					failedPods = append(failedPods, fmt.Sprintf("%s/%s", result.podNamespace, result.podName))
				} else {
					fmt.Fprintf(cmd.OutOrStdout(), "✓ Evicted pod %s/%s\n", result.podNamespace, result.podName)
					evictedCount++
				}
			}

			// Print summary
			fmt.Fprintf(cmd.OutOrStdout(), "\n=== Summary ===\n")
			fmt.Fprintf(cmd.OutOrStdout(), "Node: %s\n", node.Name)
			fmt.Fprintf(cmd.OutOrStdout(), "Total pods found: %d\n", len(pods.Items))
			fmt.Fprintf(cmd.OutOrStdout(), "Pods evicted: %d\n", evictedCount)
			fmt.Fprintf(cmd.OutOrStdout(), "Pods skipped (DaemonSet): %d\n", skippedCount)
			fmt.Fprintf(cmd.OutOrStdout(), "Pods failed to evict: %d\n", failedCount)

			if failedCount > 0 {
				fmt.Fprintf(cmd.ErrOrStderr(), "\n✗ Failed to evict pods:\n")
				for _, podName := range failedPods {
					fmt.Fprintf(cmd.ErrOrStderr(), "  - %s\n", podName)
				}
				return fmt.Errorf("drain completed with %d failed pod eviction(s)", failedCount)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "\n✓ Node %q drained successfully\n", node.Name)
			return nil
		},
	}

	cmd.Flags().IntVar(&maxConcurrency, "max-concurrency", 5, "Maximum number of concurrent pod evictions")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "Confirm destructive operation without prompting")

	return cmd
}

func newNodeUncordonCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "uncordon <node-name>",
		ValidArgsFunction: completeUnschedulableNodes,
		Short:             "Mark a node as schedulable",
		Long: `Mark a node as schedulable, allowing new pods to be scheduled on it.
The command will automatically remove the annotation "node.dana.io/reason" before uncordoning.`,
		Args: cobra.ExactArgs(1),
		Example: `  # Uncordon a node (annotation will be removed automatically)
  ocp node uncordon worker-3`,
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeName := args[0]

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			node, err := findNodeByName(ctx, nodeName)
			if err != nil {
				return err
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			// Remove maintenance annotation if present
			if node.Annotations != nil && node.Annotations[maintenanceAnnotationKey] != "" {
				escapedKey := strings.ReplaceAll(maintenanceAnnotationKey, "/", "~1")
				patch := fmt.Sprintf(`[{"op":"remove","path":"/metadata/annotations/%s"}]`, escapedKey)
				_, err = clientset.CoreV1().Nodes().Patch(ctx, node.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
				if err != nil {
					return fmt.Errorf("failed to remove annotation from node %q: %w", node.Name, err)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Removed maintenance annotation from node %q\n", node.Name)
			}

			// Get fresh node copy for uncordon
			node, err = clientset.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get node %q: %w", node.Name, err)
			}

			// Uncordon the node
			node.Spec.Unschedulable = false
			_, err = clientset.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("failed to uncordon node %q: %w", node.Name, err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Node %q marked as schedulable\n", node.Name)
			return nil
		},
	}
	return cmd
}

func newNodeGetPodsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "getpods <node-name>",
		Short: "List all pods running on a specific node",
		Long: `List all pods running on a specific node with detailed information including
namespace, name, ready state, status, restarts, and age.

Examples:
  ocp node getpods worker-0
  ocp node getpods master-1`,
		Args: cobra.ExactArgs(1),
		Example: `  # List all pods on a specific node
  ocp node getpods worker-0`,
		ValidArgsFunction: completeNodeNames,
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeName := args[0]

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			// Get pods on this node (FieldSelector will fail fast if node doesn't exist)
			pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
				FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
			})
			if err != nil {
				return fmt.Errorf("failed to list pods on node %q: %w", nodeName, err)
			}

			if len(pods.Items) == 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "No pods found on node %q\n", nodeName)
				return nil
			}

			// Calculate column widths
			type podData struct {
				namespace string
				name      string
				ready     string
				status    string
				restarts  string
				age       string
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

			for _, pod := range pods.Items {
				ready := getPodReady(&pod)
				status := string(pod.Status.Phase)
				restarts := getPodRestarts(&pod)
				age := formatAge(pod.CreationTimestamp.Time)

				data := podData{
					namespace: pod.Namespace,
					name:      pod.Name,
					ready:     ready,
					status:    status,
					restarts:  restarts,
					age:       age,
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
			}

			// Build format string for data and headers (left-aligned)
			dataFormat := fmt.Sprintf("%%-%ds   %%-%ds   %%-%ds   %%-%ds   %%-%ds   %%-%ds\n",
				widths["namespace"], widths["name"], widths["ready"], widths["status"], widths["restarts"], widths["age"])

			// Print header with left alignment
			fmt.Fprintf(cmd.OutOrStdout(), dataFormat,
				"NAMESPACE", "NAME", "READY", "STATUS", "RESTARTS", "AGE")

			// Print each pod
			for _, data := range podList {
				fmt.Fprintf(cmd.OutOrStdout(), dataFormat,
					data.namespace, data.name, data.ready, data.status, data.restarts, data.age)
			}

			// Print summary
			fmt.Fprintf(cmd.OutOrStdout(), "\n=== Summary ===\n")
			fmt.Fprintf(cmd.OutOrStdout(), "Node: %s\n", nodeName)
			fmt.Fprintf(cmd.OutOrStdout(), "Total pods: %d\n", len(pods.Items))

			return nil
		},
	}
	return cmd
}

// getPodReady returns the ready state in format "ready/total"
func getPodReady(pod *corev1.Pod) string {
	total := len(pod.Spec.Containers)
	ready := 0

	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.Ready {
			ready++
		}
	}

	return fmt.Sprintf("%d/%d", ready, total)
}

// findNodeByName finds a node by exact name match
func findNodeByName(ctx context.Context, nodeName string) (*corev1.Node, error) {
	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get node %q: %w", nodeName, err)
	}

	return node, nil
}
