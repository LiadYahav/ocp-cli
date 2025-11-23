// Portions of this file are based on code from Kubernetes kubectl
// under the Apache License 2.0.
// Source: https://github.com/kubernetes/kubectl
// See LICENSE and NOTICE files in the project root for full license information.

package cli

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

type podUsage struct {
	namespace    string
	name         string
	cpuUsage     int64
	memoryUsage  int64
	containerCPU map[string]int64
	containerMem map[string]int64
	labels       map[string]string
}

func newTopCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "top",
		Short: "Display resource (CPU/memory) usage",
		Long: `Display resource (CPU/memory) usage for nodes or pods.

The top command allows you to see the resource consumption of nodes or pods in your cluster.
This command requires the metrics-server to be installed in your cluster.

Available Commands:
  top nodes    Display resource usage of nodes
  top pods     Display resource usage of pods`,
		Example: `  # Show metrics for all nodes
  ocp top nodes

  # Show metrics for all pods in the current namespace
  ocp top pods

  # Show metrics for all pods in all namespaces
  ocp top pods --all-namespaces

  # Show metrics for pods with label selector
  ocp top pods -l app=myapp

  # Show metrics and sort by CPU
  ocp top pods --sort-by=cpu

  # Show metrics and sort by memory
  ocp top pods --sort-by=memory`,
	}

	cmd.AddCommand(
		newTopNodesCommand(),
		newTopPodsCommand(),
	)

	return cmd
}

func newTopNodesCommand() *cobra.Command {
	var sortBy string
	var showLabels bool

	cmd := &cobra.Command{
		Use:   "nodes [NODE_NAME]",
		Short: "Display resource (CPU/memory) usage of nodes",
		Long: `Display resource (CPU/memory) usage of nodes.

The top nodes command shows the CPU and memory usage of each node in the cluster.
This requires the metrics-server to be installed in your cluster.`,
		Aliases: []string{"node", "no"},
		Example: `  # Show metrics for all nodes
  ocp top nodes

  # Show metrics for a specific node
  ocp top nodes worker-1

  # Sort by CPU usage
  ocp top nodes --sort-by=cpu

  # Sort by memory usage
  ocp top nodes --sort-by=memory

  # Show labels
  ocp top nodes --show-labels`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			nodeName := ""
			if len(args) > 0 {
				nodeName = args[0]
			}

			return topNodes(ctx, nodeName, sortBy, showLabels, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVar(&sortBy, "sort-by", "", "Sort by: cpu, memory")
	cmd.Flags().BoolVar(&showLabels, "show-labels", false, "Show labels in output")

	return cmd
}

func newTopPodsCommand() *cobra.Command {
	var namespace string
	var allNamespaces bool
	var selector string
	var sortBy string
	var showLabels bool
	var containers bool

	cmd := &cobra.Command{
		Use:   "pods [POD_NAME]",
		Short: "Display resource (CPU/memory) usage of pods",
		Long: `Display resource (CPU/memory) usage of pods.

The top pods command shows the CPU and memory usage of pods in a namespace.
This requires the metrics-server to be installed in your cluster.`,
		Aliases: []string{"pod", "po"},
		Example: `  # Show metrics for all pods in the current namespace
  ocp top pods

  # Show metrics for all pods in all namespaces
  ocp top pods --all-namespaces

  # Show metrics for pods in specific namespace
  ocp top pods -n kube-system

  # Show metrics for a specific pod
  ocp top pods my-pod

  # Show metrics with container breakdown
  ocp top pods --containers

  # Show metrics with label selector
  ocp top pods -l app=myapp

  # Sort by CPU usage
  ocp top pods --sort-by=cpu

  # Sort by memory usage
  ocp top pods --sort-by=memory

  # Show labels
  ocp top pods --show-labels`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			podName := ""
			if len(args) > 0 {
				podName = args[0]
			}

			// Resolve namespace
			ns := resolveNamespace(ctx, namespace, allNamespaces)

			return topPods(ctx, ns, podName, selector, sortBy, allNamespaces, showLabels, containers, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "List pods in all namespaces")
	cmd.Flags().StringVarP(&selector, "selector", "l", "", "Selector (label query) to filter on")
	cmd.Flags().StringVar(&sortBy, "sort-by", "", "Sort by: cpu, memory")
	cmd.Flags().BoolVar(&showLabels, "show-labels", false, "Show labels in output")
	cmd.Flags().BoolVar(&containers, "containers", false, "Show container resource usage")

	return cmd
}

func topNodes(ctx context.Context, nodeName string, sortBy string, showLabels bool, out io.Writer) error {
	// Create metrics client
	metricsClient, err := createMetricsClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create metrics client: %w\n\nNote: The 'top' command requires metrics-server to be installed in your cluster.\nTo install metrics-server, visit: https://github.com/kubernetes-sigs/metrics-server", err)
	}

	// Fetch node metrics
	var nodeMetricsList *metricsv1beta1.NodeMetricsList
	if nodeName != "" {
		nodeMetrics, err := metricsClient.MetricsV1beta1().NodeMetricses().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get metrics for node %q: %w", nodeName, err)
		}
		nodeMetricsList = &metricsv1beta1.NodeMetricsList{
			Items: []metricsv1beta1.NodeMetrics{*nodeMetrics},
		}
	} else {
		nodeMetricsList, err = metricsClient.MetricsV1beta1().NodeMetricses().List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("failed to list node metrics: %w", err)
		}
	}

	if len(nodeMetricsList.Items) == 0 {
		fmt.Fprintf(out, "No metrics found.\n")
		return nil
	}

	// Get node details for capacity information
	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return fmt.Errorf("failed to create clientset: %w", err)
	}

	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	nodeMap := make(map[string]*corev1.Node)
	for i := range nodes.Items {
		nodeMap[nodes.Items[i].Name] = &nodes.Items[i]
	}

	// Build node usage data
	type nodeUsage struct {
		name        string
		cpuUsage    int64
		memoryUsage int64
		cpuPercent  float64
		memPercent  float64
		labels      map[string]string
	}

	var usageList []nodeUsage
	for _, metric := range nodeMetricsList.Items {
		node, exists := nodeMap[metric.Name]
		if !exists {
			continue
		}

		cpuUsage := metric.Usage.Cpu().MilliValue()
		memUsage := metric.Usage.Memory().Value()

		cpuCapacity := node.Status.Capacity.Cpu().MilliValue()
		memCapacity := node.Status.Capacity.Memory().Value()

		cpuPercent := float64(0)
		if cpuCapacity > 0 {
			cpuPercent = float64(cpuUsage) / float64(cpuCapacity) * 100
		}

		memPercent := float64(0)
		if memCapacity > 0 {
			memPercent = float64(memUsage) / float64(memCapacity) * 100
		}

		usage := nodeUsage{
			name:        metric.Name,
			cpuUsage:    cpuUsage,
			memoryUsage: memUsage,
			cpuPercent:  cpuPercent,
			memPercent:  memPercent,
			labels:      node.Labels,
		}
		usageList = append(usageList, usage)
	}

	// Sort if requested
	switch strings.ToLower(sortBy) {
	case "cpu":
		sort.Slice(usageList, func(i, j int) bool {
			return usageList[i].cpuUsage > usageList[j].cpuUsage
		})
	case "memory":
		sort.Slice(usageList, func(i, j int) bool {
			return usageList[i].memoryUsage > usageList[j].memoryUsage
		})
	default:
		sort.Slice(usageList, func(i, j int) bool {
			return usageList[i].name < usageList[j].name
		})
	}

	// Calculate column widths
	widths := map[string]int{
		"name":   len("NAME"),
		"cpu":    len("CPU(cores)"),
		"cpuper": len("CPU%"),
		"mem":    len("MEMORY(bytes)"),
		"memper": len("MEMORY%"),
	}

	if showLabels {
		widths["labels"] = len("LABELS")
	}

	for _, usage := range usageList {
		if len(usage.name) > widths["name"] {
			widths["name"] = len(usage.name)
		}

		cpuStr := formatCPU(usage.cpuUsage)
		if len(cpuStr) > widths["cpu"] {
			widths["cpu"] = len(cpuStr)
		}

		memStr := formatMemory(usage.memoryUsage)
		if len(memStr) > widths["mem"] {
			widths["mem"] = len(memStr)
		}

		if showLabels {
			labelStr := formatLabels(usage.labels)
			if len(labelStr) > widths["labels"] {
				widths["labels"] = len(labelStr)
			}
		}
	}

	// Build format string
	var formatParts []string
	var headerParts []string

	formatParts = append(formatParts,
		fmt.Sprintf("%%-%ds", widths["name"]),
		fmt.Sprintf("%%-%ds", widths["cpu"]),
		fmt.Sprintf("%%-%ds", widths["cpuper"]),
		fmt.Sprintf("%%-%ds", widths["mem"]),
		fmt.Sprintf("%%-%ds", widths["memper"]))
	headerParts = append(headerParts, "NAME", "CPU(cores)", "CPU%", "MEMORY(bytes)", "MEMORY%")

	if showLabels {
		formatParts = append(formatParts, fmt.Sprintf("%%-%ds", widths["labels"]))
		headerParts = append(headerParts, "LABELS")
	}

	dataFormat := strings.Join(formatParts, "   ") + "\n"

	// Print header
	fmt.Fprintf(out, dataFormat, toInterfaceSlice(headerParts)...)

	// Print each node
	for _, usage := range usageList {
		var rowParts []interface{}
		rowParts = append(rowParts,
			usage.name,
			formatCPU(usage.cpuUsage),
			fmt.Sprintf("%.0f%%", usage.cpuPercent),
			formatMemory(usage.memoryUsage),
			fmt.Sprintf("%.0f%%", usage.memPercent))

		if showLabels {
			rowParts = append(rowParts, formatLabels(usage.labels))
		}

		fmt.Fprintf(out, dataFormat, rowParts...)
	}

	return nil
}

func topPods(ctx context.Context, namespace string, podName string, selector string, sortBy string, allNamespaces bool, showLabels bool, containers bool, out io.Writer) error {
	// Create metrics client
	metricsClient, err := createMetricsClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create metrics client: %w\n\nNote: The 'top' command requires metrics-server to be installed in your cluster.\nTo install metrics-server, visit: https://github.com/kubernetes-sigs/metrics-server", err)
	}

	// Fetch pod metrics
	var podMetricsList *metricsv1beta1.PodMetricsList
	if podName != "" {
		podMetrics, err := metricsClient.MetricsV1beta1().PodMetricses(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get metrics for pod %q: %w", podName, err)
		}
		podMetricsList = &metricsv1beta1.PodMetricsList{
			Items: []metricsv1beta1.PodMetrics{*podMetrics},
		}
	} else {
		listOptions := metav1.ListOptions{}
		if selector != "" {
			listOptions.LabelSelector = selector
		}

		if allNamespaces {
			podMetricsList, err = metricsClient.MetricsV1beta1().PodMetricses(metav1.NamespaceAll).List(ctx, listOptions)
		} else {
			podMetricsList, err = metricsClient.MetricsV1beta1().PodMetricses(namespace).List(ctx, listOptions)
		}
		if err != nil {
			return fmt.Errorf("failed to list pod metrics: %w", err)
		}
	}

	if len(podMetricsList.Items) == 0 {
		fmt.Fprintf(out, "No metrics found.\n")
		return nil
	}

	// Get pod details for labels
	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return fmt.Errorf("failed to create clientset: %w", err)
	}

	var pods *corev1.PodList
	listOptions := metav1.ListOptions{}
	if selector != "" {
		listOptions.LabelSelector = selector
	}

	if allNamespaces {
		pods, err = clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, listOptions)
	} else {
		pods, err = clientset.CoreV1().Pods(namespace).List(ctx, listOptions)
	}
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	podMap := make(map[string]*corev1.Pod)
	for i := range pods.Items {
		key := pods.Items[i].Namespace + "/" + pods.Items[i].Name
		podMap[key] = &pods.Items[i]
	}

	var usageList []podUsage
	for _, metric := range podMetricsList.Items {
		key := metric.Namespace + "/" + metric.Name
		pod, exists := podMap[key]
		if !exists {
			continue
		}

		var totalCPU int64
		var totalMem int64
		containerCPU := make(map[string]int64)
		containerMem := make(map[string]int64)

		for _, container := range metric.Containers {
			cpu := container.Usage.Cpu().MilliValue()
			mem := container.Usage.Memory().Value()
			totalCPU += cpu
			totalMem += mem
			containerCPU[container.Name] = cpu
			containerMem[container.Name] = mem
		}

		usage := podUsage{
			namespace:    metric.Namespace,
			name:         metric.Name,
			cpuUsage:     totalCPU,
			memoryUsage:  totalMem,
			containerCPU: containerCPU,
			containerMem: containerMem,
			labels:       pod.Labels,
		}
		usageList = append(usageList, usage)
	}

	// Sort if requested
	switch strings.ToLower(sortBy) {
	case "cpu":
		sort.Slice(usageList, func(i, j int) bool {
			return usageList[i].cpuUsage > usageList[j].cpuUsage
		})
	case "memory":
		sort.Slice(usageList, func(i, j int) bool {
			return usageList[i].memoryUsage > usageList[j].memoryUsage
		})
	default:
		sort.Slice(usageList, func(i, j int) bool {
			if usageList[i].namespace != usageList[j].namespace {
				return usageList[i].namespace < usageList[j].namespace
			}
			return usageList[i].name < usageList[j].name
		})
	}

	if containers {
		return printPodContainerMetrics(usageList, allNamespaces, showLabels, out)
	}

	// Calculate column widths
	widths := map[string]int{
		"namespace": len("NAMESPACE"),
		"name":      len("NAME"),
		"cpu":       len("CPU(cores)"),
		"mem":       len("MEMORY(bytes)"),
	}

	if showLabels {
		widths["labels"] = len("LABELS")
	}

	for _, usage := range usageList {
		if len(usage.namespace) > widths["namespace"] {
			widths["namespace"] = len(usage.namespace)
		}
		if len(usage.name) > widths["name"] {
			widths["name"] = len(usage.name)
		}

		cpuStr := formatCPU(usage.cpuUsage)
		if len(cpuStr) > widths["cpu"] {
			widths["cpu"] = len(cpuStr)
		}

		memStr := formatMemory(usage.memoryUsage)
		if len(memStr) > widths["mem"] {
			widths["mem"] = len(memStr)
		}

		if showLabels {
			labelStr := formatLabels(usage.labels)
			if len(labelStr) > widths["labels"] {
				widths["labels"] = len(labelStr)
			}
		}
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
		fmt.Sprintf("%%-%ds", widths["cpu"]),
		fmt.Sprintf("%%-%ds", widths["mem"]))
	headerParts = append(headerParts, "NAME", "CPU(cores)", "MEMORY(bytes)")

	if showLabels {
		formatParts = append(formatParts, fmt.Sprintf("%%-%ds", widths["labels"]))
		headerParts = append(headerParts, "LABELS")
	}

	dataFormat := strings.Join(formatParts, "   ") + "\n"

	// Print header
	fmt.Fprintf(out, dataFormat, toInterfaceSlice(headerParts)...)

	// Print each pod
	for _, usage := range usageList {
		var rowParts []interface{}
		if allNamespaces {
			rowParts = append(rowParts, usage.namespace)
		}
		rowParts = append(rowParts,
			usage.name,
			formatCPU(usage.cpuUsage),
			formatMemory(usage.memoryUsage))

		if showLabels {
			rowParts = append(rowParts, formatLabels(usage.labels))
		}

		fmt.Fprintf(out, dataFormat, rowParts...)
	}

	return nil
}

func printPodContainerMetrics(usageList []podUsage, allNamespaces bool, showLabels bool, out io.Writer) error {
	// Calculate column widths
	widths := map[string]int{
		"pod":       len("POD"),
		"namespace": len("NAMESPACE"),
		"container": len("CONTAINER"),
		"cpu":       len("CPU(cores)"),
		"mem":       len("MEMORY(bytes)"),
	}

	if showLabels {
		widths["labels"] = len("LABELS")
	}

	for _, usage := range usageList {
		if len(usage.name) > widths["pod"] {
			widths["pod"] = len(usage.name)
		}
		if len(usage.namespace) > widths["namespace"] {
			widths["namespace"] = len(usage.namespace)
		}
		for containerName, cpu := range usage.containerCPU {
			if len(containerName) > widths["container"] {
				widths["container"] = len(containerName)
			}
			cpuStr := formatCPU(cpu)
			if len(cpuStr) > widths["cpu"] {
				widths["cpu"] = len(cpuStr)
			}
		}
		for _, mem := range usage.containerMem {
			memStr := formatMemory(mem)
			if len(memStr) > widths["mem"] {
				widths["mem"] = len(memStr)
			}
		}
		if showLabels {
			labelStr := formatLabels(usage.labels)
			if len(labelStr) > widths["labels"] {
				widths["labels"] = len(labelStr)
			}
		}
	}

	// Build format string
	var formatParts []string
	var headerParts []string

	if allNamespaces {
		formatParts = append(formatParts, fmt.Sprintf("%%-%ds", widths["namespace"]))
		headerParts = append(headerParts, "NAMESPACE")
	}

	formatParts = append(formatParts,
		fmt.Sprintf("%%-%ds", widths["pod"]),
		fmt.Sprintf("%%-%ds", widths["container"]),
		fmt.Sprintf("%%-%ds", widths["cpu"]),
		fmt.Sprintf("%%-%ds", widths["mem"]))
	headerParts = append(headerParts, "POD", "CONTAINER", "CPU(cores)", "MEMORY(bytes)")

	if showLabels {
		formatParts = append(formatParts, fmt.Sprintf("%%-%ds", widths["labels"]))
		headerParts = append(headerParts, "LABELS")
	}

	dataFormat := strings.Join(formatParts, "   ") + "\n"

	// Print header
	fmt.Fprintf(out, dataFormat, toInterfaceSlice(headerParts)...)

	// Print each pod's containers
	for _, usage := range usageList {
		// Sort container names for consistent output
		var containerNames []string
		for name := range usage.containerCPU {
			containerNames = append(containerNames, name)
		}
		sort.Strings(containerNames)

		for _, containerName := range containerNames {
			var rowParts []interface{}
			if allNamespaces {
				rowParts = append(rowParts, usage.namespace)
			}
			rowParts = append(rowParts,
				usage.name,
				containerName,
				formatCPU(usage.containerCPU[containerName]),
				formatMemory(usage.containerMem[containerName]))

			if showLabels {
				rowParts = append(rowParts, formatLabels(usage.labels))
			}

			fmt.Fprintf(out, dataFormat, rowParts...)
		}
	}

	return nil
}

func createMetricsClient(ctx context.Context) (*metricsclientset.Clientset, error) {
	config, err := kube.GetConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig: %w", err)
	}

	return metricsclientset.NewForConfig(config)
}

func formatCPU(milliCores int64) string {
	if milliCores < 1000 {
		return fmt.Sprintf("%dm", milliCores)
	}
	return fmt.Sprintf("%.2f", float64(milliCores)/1000)
}

func formatMemory(bytes int64) string {
	mem := resource.NewQuantity(bytes, resource.BinarySI)
	return mem.String()
}

func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return "<none>"
	}

	var pairs []string
	for k, v := range labels {
		pairs = append(pairs, fmt.Sprintf("%s=%s", k, v))
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

