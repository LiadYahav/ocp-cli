package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"

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
		newNodeInfoCommand(),
		newNodeDescribeCommand(),
		newNodeYamlCommand(),
		newNodeRebootCommand(),
		newNodeCordonCommand(),
		newNodeDrainCommand(),
		newNodeUncordonCommand(),
		newNodeAnnotateCommand(),
		newNodeLabelCommand(),
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

func newNodeInfoCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "info",
		Short: "Display detailed information about all nodes",
		Long:  `Display a table with detailed information about all nodes including status, roles, IPs, versions, and system info.`,
		Example: `  # Show a wide table of node details
  ocp node info`,
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

			if len(nodes.Items) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "No nodes found")
				return nil
			}

			// Calculate dynamic column widths based on actual data
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
			}

			var nodeList []nodeData
			widths := map[string]int{
				"name":             len("NAME"),
				"status":           len("STATUS"),
				"roles":            len("ROLES"),
				"age":              len("AGE"),
				"version":          len("VERSION"),
				"internalIP":       len("INTERNAL-IP"),
				"externalIP":       len("EXTERNAL-IP"),
				"osImage":          len("OS-IMAGE"),
				"kernelVersion":    len("KERNEL-VERSION"),
				"containerRuntime": len("CONTAINER-RUNTIME"),
			}

			// First pass: collect data and calculate widths
			for _, node := range nodes.Items {
				data := nodeData{
					name:             node.Name,
					status:           getNodeStatus(&node),
					roles:            getNodeRoles(&node),
					age:              formatAge(node.CreationTimestamp.Time),
					version:          node.Status.NodeInfo.KubeletVersion,
					internalIP:       getNodeInternalIP(&node),
					externalIP:       getNodeExternalIP(&node),
					osImage:          node.Status.NodeInfo.OSImage,
					kernelVersion:    node.Status.NodeInfo.KernelVersion,
					containerRuntime: node.Status.NodeInfo.ContainerRuntimeVersion,
				}
				nodeList = append(nodeList, data)

				// Update max widths
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

			// Build format string for data and headers (left-aligned)
			dataFormat := fmt.Sprintf("%%-%ds   %%-%ds   %%-%ds   %%-%ds   %%-%ds   %%-%ds   %%-%ds   %%-%ds   %%-%ds   %%-%ds\n",
				widths["name"], widths["status"], widths["roles"], widths["age"], widths["version"],
				widths["internalIP"], widths["externalIP"], widths["osImage"], widths["kernelVersion"], widths["containerRuntime"])

			// Print header with left alignment
			fmt.Fprintf(cmd.OutOrStdout(), dataFormat,
				"NAME", "STATUS", "ROLES", "AGE", "VERSION",
				"INTERNAL-IP", "EXTERNAL-IP", "OS-IMAGE", "KERNEL-VERSION", "CONTAINER-RUNTIME")

			// Print each node
			for _, data := range nodeList {
				fmt.Fprintf(cmd.OutOrStdout(), dataFormat,
					data.name, data.status, data.roles, data.age, data.version,
					data.internalIP, data.externalIP, data.osImage, data.kernelVersion, data.containerRuntime)
			}

			return nil
		},
	}
}

func newNodeDescribeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "describe <node-name>",
		Short: "Show detailed information about a node",
		Args:  cobra.ExactArgs(1),
		Example: `  # Describe a specific node
  ocp node describe master-0`,
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

			node, err := findNodeByName(ctx, nodeName)
			if err != nil {
				return err
			}

			// Get pods on this node
			pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
				FieldSelector: fmt.Sprintf("spec.nodeName=%s", node.Name),
			})
			if err != nil {
				return fmt.Errorf("failed to list pods on node %q: %w", node.Name, err)
			}

			// Print node information
			fmt.Fprintf(cmd.OutOrStdout(), "Name:               %s\n", node.Name)
			fmt.Fprintf(cmd.OutOrStdout(), "Roles:              %s\n", getNodeRoles(node))
			fmt.Fprintf(cmd.OutOrStdout(), "Status:             %s\n", getNodeStatus(node))
			fmt.Fprintf(cmd.OutOrStdout(), "Unschedulable:      %t\n", node.Spec.Unschedulable)
			fmt.Fprintf(cmd.OutOrStdout(), "CreationTimestamp:  %s\n", node.CreationTimestamp.Format(time.RFC3339))

			// Labels
			if len(node.Labels) > 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "Labels:")
				for k, v := range node.Labels {
					fmt.Fprintf(cmd.OutOrStdout(), "  %s=%s\n", k, v)
				}
			}

			// Annotations
			if len(node.Annotations) > 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "Annotations:")
				for k, v := range node.Annotations {
					fmt.Fprintf(cmd.OutOrStdout(), "  %s=%s\n", k, v)
				}
			}

			// Addresses
			if len(node.Status.Addresses) > 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "Addresses:")
				for _, addr := range node.Status.Addresses {
					fmt.Fprintf(cmd.OutOrStdout(), "  %s: %s\n", addr.Type, addr.Address)
				}
			}

			// Capacity
			fmt.Fprintln(cmd.OutOrStdout(), "Capacity:")
			fmt.Fprintf(cmd.OutOrStdout(), "  cpu:     %s\n", node.Status.Capacity.Cpu().String())
			fmt.Fprintf(cmd.OutOrStdout(), "  memory:  %s\n", node.Status.Capacity.Memory().String())
			fmt.Fprintf(cmd.OutOrStdout(), "  pods:    %s\n", node.Status.Capacity.Pods().String())

			// Allocatable
			fmt.Fprintln(cmd.OutOrStdout(), "Allocatable:")
			fmt.Fprintf(cmd.OutOrStdout(), "  cpu:     %s\n", node.Status.Allocatable.Cpu().String())
			fmt.Fprintf(cmd.OutOrStdout(), "  memory:  %s\n", node.Status.Allocatable.Memory().String())
			fmt.Fprintf(cmd.OutOrStdout(), "  pods:    %s\n", node.Status.Allocatable.Pods().String())

			// System Info
			fmt.Fprintln(cmd.OutOrStdout(), "System Info:")
			fmt.Fprintf(cmd.OutOrStdout(), "  OS Image:              %s\n", node.Status.NodeInfo.OSImage)
			fmt.Fprintf(cmd.OutOrStdout(), "  Kernel Version:        %s\n", node.Status.NodeInfo.KernelVersion)
			fmt.Fprintf(cmd.OutOrStdout(), "  Container Runtime:     %s\n", node.Status.NodeInfo.ContainerRuntimeVersion)
			fmt.Fprintf(cmd.OutOrStdout(), "  Kubelet Version:       %s\n", node.Status.NodeInfo.KubeletVersion)
			fmt.Fprintf(cmd.OutOrStdout(), "  Kube-Proxy Version:    %s\n", node.Status.NodeInfo.KubeProxyVersion)

			// Pods
			fmt.Fprintf(cmd.OutOrStdout(), "\nPods: (%d total)\n", len(pods.Items))
			for _, pod := range pods.Items {
				fmt.Fprintf(cmd.OutOrStdout(), "  %s/%s (%s)\n", pod.Namespace, pod.Name, pod.Status.Phase)
			}

			return nil
		},
	}
	return cmd
}

func newNodeYamlCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "yaml <node-name>",
		Short:             "Display node information in YAML format",
		ValidArgsFunction: completeNodeNames,
		Args:              cobra.ExactArgs(1),
		Example: `  # Output the node definition in YAML
  ocp node yaml worker-1`,
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

			// Convert to YAML
			yamlData, err := yaml.Marshal(node)
			if err != nil {
				return fmt.Errorf("failed to marshal node to YAML: %w", err)
			}

			fmt.Fprint(cmd.OutOrStdout(), string(yamlData))
			return nil
		},
	}
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
		ValidArgsFunction: completeNodeNames,
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

			// Add maintenance annotation if it doesn't exist
			if node.Annotations == nil {
				node.Annotations = make(map[string]string)
			}
			if node.Annotations[maintenanceAnnotationKey] != maintenanceAnnotationValue {
				node.Annotations[maintenanceAnnotationKey] = maintenanceAnnotationValue
				_, err = clientset.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
				if err != nil {
					return fmt.Errorf("failed to add maintenance annotation to node %q: %w", node.Name, err)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Added maintenance annotation to node %q\n", node.Name)
			}

			// Get fresh node data after annotation update
			node, err = clientset.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get node %q: %w", node.Name, err)
			}

			// Cordon the node
			node.Spec.Unschedulable = true
			_, err = clientset.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("failed to cordon node %q: %w", node.Name, err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Node %q marked as unschedulable\n", node.Name)
			return nil
		},
	}
	return cmd
}

func newNodeDrainCommand() *cobra.Command {
	var maxConcurrency int

	cmd := &cobra.Command{
		Use:               "drain <node-name>",
		ValidArgsFunction: completeNodeNames,
		Short:             "Drain a node (cordon + evict pods)",
		Long: `Drain a node by marking it as unschedulable and evicting all pods.
The command will automatically add the annotation "node.dana.io/reason: Maintenance" before draining.

Pod evictions are performed concurrently for better performance. Use --max-concurrency
to control the number of concurrent evictions (default: 5).`,
		Args: cobra.ExactArgs(1),
		Example: `  # Drain a node (annotation will be added automatically)
  ocp node drain worker-3

  # Drain with custom concurrency
  ocp node drain worker-3 --max-concurrency 10`,
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

			// Add maintenance annotation if it doesn't exist
			if node.Annotations == nil {
				node.Annotations = make(map[string]string)
			}
			if node.Annotations[maintenanceAnnotationKey] != maintenanceAnnotationValue {
				node.Annotations[maintenanceAnnotationKey] = maintenanceAnnotationValue
				_, err = clientset.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
				if err != nil {
					return fmt.Errorf("failed to add maintenance annotation to node %q: %w", node.Name, err)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Added maintenance annotation to node %q\n", node.Name)
			}

			// Get fresh node data after annotation update
			node, err = clientset.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get node %q: %w", node.Name, err)
			}

			// Cordon the node
			node.Spec.Unschedulable = true
			_, err = clientset.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("failed to cordon node %q: %w", node.Name, err)
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

	return cmd
}

func newNodeUncordonCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "uncordon <node-name>",
		Short: "Mark a node as schedulable",
		Long: `Mark a node as schedulable, allowing new pods to be scheduled on it.
The command will automatically remove the annotation "node.dana.io/reason" before uncordoning.`,
		ValidArgsFunction: completeNodeNames,
		Args:              cobra.ExactArgs(1),
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

func newNodeAnnotateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "annotate <node-name> <annotation>",
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			// Complete node names for the first argument
			if len(args) == 0 {
				return completeNodeNames(cmd, args, toComplete)
			}
			// No completion for annotation argument
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Short: "Add or update an annotation on a node",
		Long: `Add or update annotations on a node by name.
Annotations should be in the format "key=value" or "key" (to remove).
Multiple annotations can be specified separated by commas (no spaces).

Examples:
  ocp node annotate node-24 node.dana.io/reason=Maintenance
  ocp node annotate node-24 node.dana.io/reason
  ocp node annotate node-24 key1=value1,key2=value2,key3=value3`,
		Args: cobra.ExactArgs(2),
		Example: `  # Add a single annotation
  ocp node annotate worker-2 purpose=maintenance

  # Add multiple annotations at once
  ocp node annotate worker-2 purpose=maintenance,environment=prod,team=platform

  # Remove an annotation
  ocp node annotate worker-2 purpose`,
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeName := args[0]
			annotationsStr := args[1]

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

			// Parse comma-separated annotations
			annotationParts := strings.Split(annotationsStr, ",")
			var patchOps []string
			var needsAnnotationsInit bool
			var addedKeys []string
			var removedKeys []string

			for _, annotation := range annotationParts {
				annotation = strings.TrimSpace(annotation)
				if annotation == "" {
					continue
				}

				// Parse annotation key=value or key
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
					if node.Annotations == nil || node.Annotations[key] == "" {
						fmt.Fprintf(cmd.OutOrStdout(), "Annotation %q not found on node %q\n", key, node.Name)
						continue
					}
					escapedKey := strings.ReplaceAll(key, "/", "~1")
					patchOps = append(patchOps, fmt.Sprintf(`{"op":"remove","path":"/metadata/annotations/%s"}`, escapedKey))
					removedKeys = append(removedKeys, key)
				} else {
					// Add or update annotation
					if node.Annotations == nil && !needsAnnotationsInit {
						needsAnnotationsInit = true
					}
					escapedKey := strings.ReplaceAll(key, "/", "~1")
					// JSON escape the value for the JSON string literal
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
				return nil
			}

			// Build patch with initialization if needed
			var patch string
			if needsAnnotationsInit {
				patch = fmt.Sprintf(`[{"op":"add","path":"/metadata/annotations","value":{}},%s]`, strings.Join(patchOps, ","))
			} else {
				patch = fmt.Sprintf(`[%s]`, strings.Join(patchOps, ","))
			}

			_, err = clientset.CoreV1().Nodes().Patch(ctx, node.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("failed to annotate node %q: %w", node.Name, err)
			}

			// Print results
			if len(removedKeys) > 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "✓ Removed annotation(s) %v from node %q\n", removedKeys, node.Name)
			}
			if len(addedKeys) > 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "✓ Added annotation(s) %v to node %q\n", addedKeys, node.Name)
			}

			// Print summary
			fmt.Fprintf(cmd.OutOrStdout(), "\n=== Summary ===\n")
			fmt.Fprintf(cmd.OutOrStdout(), "Node: %s\n", node.Name)
			fmt.Fprintf(cmd.OutOrStdout(), "Annotations added/updated: %d\n", len(addedKeys))
			fmt.Fprintf(cmd.OutOrStdout(), "Annotations removed: %d\n", len(removedKeys))
			fmt.Fprintf(cmd.OutOrStdout(), "Total operations: %d\n", len(addedKeys)+len(removedKeys))
			if len(addedKeys) > 0 || len(removedKeys) > 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "\n✓ Annotation operation completed successfully\n")
			}

			return nil
		},
	}
	return cmd
}

func newNodeLabelCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "label <node-name> <label>",
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			// Complete node names for the first argument
			if len(args) == 0 {
				return completeNodeNames(cmd, args, toComplete)
			}
			// No completion for label argument
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Short: "Add or update a label on a node",
		Long: `Add or update labels on a node by name.
Labels should be in the format "key=value" or "key-" (to remove).
Multiple labels can be specified separated by commas (no spaces).

Examples:
  ocp node label node-24 environment=production
  ocp node label node-24 environment-
  ocp node label node-24 key1=value1,key2=value2,key3-`,
		Args: cobra.ExactArgs(2),
		Example: `  # Add or update a single label
  ocp node label worker-1 zone=us-east-1a

  # Add multiple labels at once
  ocp node label worker-1 zone=us-east-1a,environment=prod,team=platform

  # Remove a label
  ocp node label worker-1 zone-`,
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeName := args[0]
			labelsStr := args[1]

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

			// Parse comma-separated labels
			labelParts := strings.Split(labelsStr, ",")
			var patchOps []string
			var needsLabelsInit bool
			var addedKeys []string
			var removedKeys []string

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
					if node.Labels == nil || node.Labels[key] == "" {
						fmt.Fprintf(cmd.OutOrStdout(), "Label %q not found on node %q\n", key, node.Name)
						continue
					}
					escapedKey := strings.ReplaceAll(key, "/", "~1")
					patchOps = append(patchOps, fmt.Sprintf(`{"op":"remove","path":"/metadata/labels/%s"}`, escapedKey))
					removedKeys = append(removedKeys, key)
				} else {
					// Add or update label
					if node.Labels == nil && !needsLabelsInit {
						needsLabelsInit = true
					}
					// JSON Pointer escape the key for the path
					escapedKey := strings.ReplaceAll(key, "/", "~1")
					// JSON escape the value for the JSON string literal
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
				return nil
			}

			// Build patch with initialization if needed
			var patch string
			if needsLabelsInit {
				patch = fmt.Sprintf(`[{"op":"add","path":"/metadata/labels","value":{}},%s]`, strings.Join(patchOps, ","))
			} else {
				patch = fmt.Sprintf(`[%s]`, strings.Join(patchOps, ","))
			}

			_, err = clientset.CoreV1().Nodes().Patch(ctx, node.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("failed to label node %q: %w", node.Name, err)
			}

			// Print results
			if len(removedKeys) > 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "✓ Removed label(s) %v from node %q\n", removedKeys, node.Name)
			}
			if len(addedKeys) > 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "✓ Added label(s) %v to node %q\n", addedKeys, node.Name)
			}

			// Print summary
			fmt.Fprintf(cmd.OutOrStdout(), "\n=== Summary ===\n")
			fmt.Fprintf(cmd.OutOrStdout(), "Node: %s\n", node.Name)
			fmt.Fprintf(cmd.OutOrStdout(), "Labels added/updated: %d\n", len(addedKeys))
			fmt.Fprintf(cmd.OutOrStdout(), "Labels removed: %d\n", len(removedKeys))
			fmt.Fprintf(cmd.OutOrStdout(), "Total operations: %d\n", len(addedKeys)+len(removedKeys))
			if len(addedKeys) > 0 || len(removedKeys) > 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "\n✓ Label operation completed successfully\n")
			}

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

// getPodRestarts returns the total number of restarts across all containers
func getPodRestarts(pod *corev1.Pod) string {
	totalRestarts := int32(0)
	for _, containerStatus := range pod.Status.ContainerStatuses {
		totalRestarts += containerStatus.RestartCount
	}
	return fmt.Sprintf("%d", totalRestarts)
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

// getNodeRoles returns a comma-separated list of node roles
func getNodeRoles(node *corev1.Node) string {
	if len(node.Labels) == 0 {
		return "<none>"
	}

	const rolePrefix = "node-role.kubernetes.io/"
	var roles []string
	// Pre-allocate slice with estimated capacity
	roles = make([]string, 0, 3) // Most nodes have 1-3 roles

	for label := range node.Labels {
		if strings.HasPrefix(label, rolePrefix) {
			role := label[len(rolePrefix):]
			if role != "" {
				roles = append(roles, role)
			}
		}
	}

	if len(roles) == 0 {
		return "<none>"
	}
	return strings.Join(roles, ",")
}

// getNodeStatus returns the status of the node (Ready/NotReady)
func getNodeStatus(node *corev1.Node) string {
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

	// Add SchedulingDisabled if node is unschedulable
	if node.Spec.Unschedulable {
		status += ", SchedulingDisabled"
	}

	return status
}

// getNodeInternalIP returns the internal IP address of the node
func getNodeInternalIP(node *corev1.Node) string {
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP {
			return addr.Address
		}
	}
	return "<none>"
}

// getNodeExternalIP returns the external IP address of the node
func getNodeExternalIP(node *corev1.Node) string {
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeExternalIP {
			return addr.Address
		}
	}
	return "<none>"
}

// formatAge returns a human-readable age string
func formatAge(t time.Time) string {
	duration := time.Since(t)

	days := int(duration.Hours() / 24)
	if days > 0 {
		return fmt.Sprintf("%dd", days)
	}

	hours := int(duration.Hours())
	if hours > 0 {
		return fmt.Sprintf("%dh", hours)
	}

	minutes := int(duration.Minutes())
	if minutes > 0 {
		return fmt.Sprintf("%dm", minutes)
	}

	return fmt.Sprintf("%ds", int(duration.Seconds()))
}
