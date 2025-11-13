package cli

import (
	"context"
	"fmt"
	"regexp"
	"strings"
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
	)

	return cmd
}

func newNodeListCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List nodes in the cluster",
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

			for _, node := range nodes.Items {
				fmt.Fprintln(cmd.OutOrStdout(), node.Name)
			}

			return nil
		},
	}
}

func newNodeInfoCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "info",
		Short: "Display detailed information about all nodes",
		Long:  `Display a table with detailed information about all nodes including status, roles, IPs, versions, and system info.`,
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

			// Build format string for data (left-aligned)
			dataFormat := fmt.Sprintf("%%-%ds   %%-%ds   %%-%ds   %%-%ds   %%-%ds   %%-%ds   %%-%ds   %%-%ds   %%-%ds   %%-%ds\n",
				widths["name"], widths["status"], widths["roles"], widths["age"], widths["version"],
				widths["internalIP"], widths["externalIP"], widths["osImage"], widths["kernelVersion"], widths["containerRuntime"])

			// Print header with center alignment
			fmt.Fprintf(cmd.OutOrStdout(), "%s   %s   %s   %s   %s   %s   %s   %s   %s   %s\n",
				centerPad("NAME", widths["name"]),
				centerPad("STATUS", widths["status"]),
				centerPad("ROLES", widths["roles"]),
				centerPad("AGE", widths["age"]),
				centerPad("VERSION", widths["version"]),
				centerPad("INTERNAL-IP", widths["internalIP"]),
				centerPad("EXTERNAL-IP", widths["externalIP"]),
				centerPad("OS-IMAGE", widths["osImage"]),
				centerPad("KERNEL-VERSION", widths["kernelVersion"]),
				centerPad("CONTAINER-RUNTIME", widths["containerRuntime"]))

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
	return &cobra.Command{
		Use:   "describe <node-pattern>",
		Short: "Show detailed information about a node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pattern := args[0]

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			node, err := findNodeByPattern(ctx, pattern)
			if err != nil {
				return err
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			// Get fresh node data
			node, err = clientset.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get node %q: %w", node.Name, err)
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
}

func newNodeYamlCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "yaml <node-pattern>",
		Short: "Display node information in YAML format",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pattern := args[0]

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			node, err := findNodeByPattern(ctx, pattern)
			if err != nil {
				return err
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			// Get fresh node data
			node, err = clientset.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get node %q: %w", node.Name, err)
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
}

func newNodeRebootCommand() *cobra.Command {
	var user string
	var identityFile string

	cmd := &cobra.Command{
		Use:   "reboot <node-pattern>",
		Short: "Reboot a node via SSH",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pattern := args[0]
			return runSSHCommand(cmd.Context(), user, identityFile, pattern, []string{"sudo reboot"}, cmd)
		},
	}

	cmd.Flags().StringVarP(&user, "user", "u", "core", "Username for SSH connection")
	cmd.Flags().StringVarP(&identityFile, "identity", "i", "", "Path to private key file for SSH authentication (default: ~/.ssh/id_rsa_ocp if exists)")

	return cmd
}

func newNodeCordonCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "cordon <node-pattern>",
		Short: "Mark node as unschedulable",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pattern := args[0]

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			node, err := findNodeByPattern(ctx, pattern)
			if err != nil {
				return err
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			node.Spec.Unschedulable = true
			_, err = clientset.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("failed to cordon node %q: %w", node.Name, err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Node %q marked as unschedulable\n", node.Name)
			return nil
		},
	}
}

func newNodeDrainCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "drain <node-pattern>",
		Short: "Drain a node (cordon + evict pods)",
		Long: `Drain a node by marking it as unschedulable and evicting all pods.
The node must have the annotation "node.dana.io/reason: Maintenance" before draining.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pattern := args[0]

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			node, err := findNodeByPattern(ctx, pattern)
			if err != nil {
				return err
			}

			// Check for required annotation
			if node.Annotations == nil || node.Annotations[maintenanceAnnotationKey] != maintenanceAnnotationValue {
				return fmt.Errorf("node %q must have annotation %q=%q before draining", node.Name, maintenanceAnnotationKey, maintenanceAnnotationValue)
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
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
						fmt.Fprintf(cmd.OutOrStdout(), "Skipping DaemonSet pod %s/%s\n", pod.Namespace, pod.Name)
						continue
					}
				}

				eviction := &policyv1.Eviction{
					ObjectMeta: metav1.ObjectMeta{
						Name:      pod.Name,
						Namespace: pod.Namespace,
					},
				}

				err := clientset.CoreV1().Pods(pod.Namespace).EvictV1(ctx, eviction)
				if err != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "Warning: failed to evict pod %s/%s: %v\n", pod.Namespace, pod.Name, err)
				} else {
					fmt.Fprintf(cmd.OutOrStdout(), "Evicted pod %s/%s\n", pod.Namespace, pod.Name)
				}
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Node %q drained successfully\n", node.Name)
			return nil
		},
	}
}

func newNodeUncordonCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "uncordon <node-pattern>",
		Short: "Mark node as schedulable and remove maintenance annotation",
		Long:  `Mark a node as schedulable and remove the "node.dana.io/reason" annotation if present.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pattern := args[0]

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			node, err := findNodeByPattern(ctx, pattern)
			if err != nil {
				return err
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			// Remove maintenance annotation if present
			if node.Annotations != nil && node.Annotations[maintenanceAnnotationKey] != "" {
				patch := fmt.Sprintf(`[{"op":"remove","path":"/metadata/annotations/%s"}]`, strings.ReplaceAll(maintenanceAnnotationKey, "/", "~1"))
				_, err = clientset.CoreV1().Nodes().Patch(ctx, node.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
				if err != nil {
					return fmt.Errorf("failed to remove annotation from node %q: %w", node.Name, err)
				}
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
}

func newNodeAnnotateCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "annotate <node-pattern> <annotation>",
		Short: "Add or update an annotation on a node",
		Long: `Add or update an annotation on a node matching the pattern.
The annotation should be in the format "key=value" or "key" (to remove).

Examples:
  ocp node annotate node-24 node.dana.io/reason=Maintenance
  ocp node annotate node-24 node.dana.io/reason`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			pattern := args[0]
			annotation := args[1]

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			node, err := findNodeByPattern(ctx, pattern)
			if err != nil {
				return err
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
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

			// Build patch
			var patch string
			if remove {
				// Remove annotation
				if node.Annotations == nil || node.Annotations[key] == "" {
					fmt.Fprintf(cmd.OutOrStdout(), "Annotation %q not found on node %q\n", key, node.Name)
					return nil
				}
				escapedKey := strings.ReplaceAll(key, "/", "~1")
				patch = fmt.Sprintf(`[{"op":"remove","path":"/metadata/annotations/%s"}]`, escapedKey)
			} else {
				// Add or update annotation
				escapedKey := strings.ReplaceAll(key, "/", "~1")
				escapedValue := strings.ReplaceAll(value, "~", "~0")
				escapedValue = strings.ReplaceAll(escapedValue, "/", "~1")
				if node.Annotations == nil {
					patch = fmt.Sprintf(`[{"op":"add","path":"/metadata/annotations","value":{}},{"op":"add","path":"/metadata/annotations/%s","value":"%s"}]`, escapedKey, escapedValue)
				} else {
					patch = fmt.Sprintf(`[{"op":"add","path":"/metadata/annotations/%s","value":"%s"}]`, escapedKey, escapedValue)
				}
			}

			_, err = clientset.CoreV1().Nodes().Patch(ctx, node.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("failed to annotate node %q: %w", node.Name, err)
			}

			if remove {
				fmt.Fprintf(cmd.OutOrStdout(), "Removed annotation %q from node %q\n", key, node.Name)
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "Added annotation %q=%q to node %q\n", key, value, node.Name)
			}

			return nil
		},
	}
}

func newNodeLabelCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "label <node-pattern> <label>",
		Short: "Add or update a label on a node",
		Long: `Add or update a label on a node matching the pattern.
The label should be in the format "key=value" or "key-" (to remove).

Examples:
  ocp node label node-24 environment=production
  ocp node label node-24 environment-`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			pattern := args[0]
			label := args[1]

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			node, err := findNodeByPattern(ctx, pattern)
			if err != nil {
				return err
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
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

			// Build patch
			var patch string
			if remove {
				// Remove label
				if node.Labels == nil || node.Labels[key] == "" {
					fmt.Fprintf(cmd.OutOrStdout(), "Label %q not found on node %q\n", key, node.Name)
					return nil
				}
				escapedKey := strings.ReplaceAll(key, "/", "~1")
				patch = fmt.Sprintf(`[{"op":"remove","path":"/metadata/labels/%s"}]`, escapedKey)
			} else {
				// Add or update label
				escapedKey := strings.ReplaceAll(key, "/", "~1")
				escapedValue := strings.ReplaceAll(value, "~", "~0")
				escapedValue = strings.ReplaceAll(escapedValue, "/", "~1")
				if node.Labels == nil {
					patch = fmt.Sprintf(`[{"op":"add","path":"/metadata/labels","value":{}},{"op":"add","path":"/metadata/labels/%s","value":"%s"}]`, escapedKey, escapedValue)
				} else {
					patch = fmt.Sprintf(`[{"op":"add","path":"/metadata/labels/%s","value":"%s"}]`, escapedKey, escapedValue)
				}
			}

			_, err = clientset.CoreV1().Nodes().Patch(ctx, node.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("failed to label node %q: %w", node.Name, err)
			}

			if remove {
				fmt.Fprintf(cmd.OutOrStdout(), "Removed label %q from node %q\n", key, node.Name)
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "Added label %q=%q to node %q\n", key, value, node.Name)
			}

			return nil
		},
	}
}

// findNodeByPattern finds a node matching the regex pattern
func findNodeByPattern(ctx context.Context, pattern string) (*corev1.Node, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid node pattern %q: %w", pattern, err)
	}

	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}

	var matches []*corev1.Node
	for i := range nodes.Items {
		if re.MatchString(nodes.Items[i].Name) {
			matches = append(matches, &nodes.Items[i])
		}
	}

	switch len(matches) {
	case 0:
		return nil, fmt.Errorf("no node matched pattern %q", pattern)
	case 1:
		return matches[0], nil
	default:
		var names []string
		for _, node := range matches {
			names = append(names, node.Name)
		}
		return nil, fmt.Errorf("pattern matched multiple nodes: %s", strings.Join(names, ", "))
	}
}

// getNodeRoles returns a comma-separated list of node roles
func getNodeRoles(node *corev1.Node) string {
	var roles []string
	for label := range node.Labels {
		if strings.HasPrefix(label, "node-role.kubernetes.io/") {
			role := strings.TrimPrefix(label, "node-role.kubernetes.io/")
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
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			if condition.Status == corev1.ConditionTrue {
				return "Ready"
			}
			return "NotReady"
		}
	}
	return "Unknown"
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

// truncate truncates a string to the specified length
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

// centerPad centers a string within a given width by padding with spaces
func centerPad(s string, width int) string {
	if len(s) >= width {
		return s
	}
	totalPad := width - len(s)
	leftPad := totalPad / 2
	rightPad := totalPad - leftPad
	return strings.Repeat(" ", leftPad) + s + strings.Repeat(" ", rightPad)
}
