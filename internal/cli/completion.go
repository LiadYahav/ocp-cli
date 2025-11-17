package cli

import (
	"context"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

// completeNodeNames returns node names matching the prefix
func completeNodeNames(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	// Pre-allocate slice with estimated capacity
	completions := make([]string, 0, len(nodes.Items))
	for _, node := range nodes.Items {
		if strings.HasPrefix(node.Name, toComplete) {
			completions = append(completions, node.Name)
		}
	}

	return completions, cobra.ShellCompDirectiveNoFileComp
}

// completeMCPNames returns MCP names matching the prefix
func completeMCPNames(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	dynamicClient, err := kube.NewDynamicClient(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	gvr := schema.GroupVersionResource{
		Group:    "machineconfiguration.openshift.io",
		Version:  "v1",
		Resource: "machineconfigpools",
	}

	mcps, err := dynamicClient.Resource(gvr).List(ctx, metav1.ListOptions{})
	if meta.IsNoMatchError(err) {
		// API not available, return empty
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	// Pre-allocate slice with estimated capacity
	completions := make([]string, 0, len(mcps.Items))
	for _, mcp := range mcps.Items {
		name, found, _ := unstructured.NestedString(mcp.Object, "metadata", "name")
		if found && name != "" && strings.HasPrefix(name, toComplete) {
			completions = append(completions, name)
		}
	}

	return completions, cobra.ShellCompDirectiveNoFileComp
}

// completeMCPActions returns the available actions for MCP command
func completeMCPActions(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	actions := []string{"resume", "pause"}
	completions := make([]string, 0, len(actions))
	for _, action := range actions {
		if strings.HasPrefix(action, toComplete) {
			completions = append(completions, action)
		}
	}
	return completions, cobra.ShellCompDirectiveNoFileComp
}

// completeSchedulableNodes returns only schedulable nodes (not cordoned)
func completeSchedulableNodes(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	completions := make([]string, 0)
	for _, node := range nodes.Items {
		// Only include schedulable nodes (not cordoned)
		if !node.Spec.Unschedulable && strings.HasPrefix(node.Name, toComplete) {
			completions = append(completions, node.Name)
		}
	}

	return completions, cobra.ShellCompDirectiveNoFileComp
}

// completeUnschedulableNodes returns only unschedulable nodes (cordoned)
func completeUnschedulableNodes(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	completions := make([]string, 0)
	for _, node := range nodes.Items {
		// Only include unschedulable nodes (cordoned)
		if node.Spec.Unschedulable && strings.HasPrefix(node.Name, toComplete) {
			completions = append(completions, node.Name)
		}
	}

	return completions, cobra.ShellCompDirectiveNoFileComp
}

// completeOutputFormats returns available output formats
func completeOutputFormats(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	formats := []string{"json", "yaml", "wide", "name"}
	completions := make([]string, 0, len(formats))
	for _, format := range formats {
		if strings.HasPrefix(format, toComplete) {
			completions = append(completions, format)
		}
	}
	return completions, cobra.ShellCompDirectiveNoFileComp
}

// completeNamespaces returns namespace names matching the prefix
func completeNamespaces(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	namespaces, err := clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	completions := make([]string, 0, len(namespaces.Items))
	for _, ns := range namespaces.Items {
		if strings.HasPrefix(ns.Name, toComplete) {
			completions = append(completions, ns.Name)
		}
	}

	return completions, cobra.ShellCompDirectiveNoFileComp
}

// completePatchTypes returns available patch types
func completePatchTypes(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	types := []string{"strategic", "merge", "json"}
	completions := make([]string, 0, len(types))
	for _, t := range types {
		if strings.HasPrefix(t, toComplete) {
			completions = append(completions, t)
		}
	}
	return completions, cobra.ShellCompDirectiveNoFileComp
}

// filterCompletions filters a slice of strings by prefix
func filterCompletions(items []string, prefix string) []string {
	completions := make([]string, 0)
	for _, item := range items {
		if strings.HasPrefix(item, prefix) {
			completions = append(completions, item)
		}
	}
	return completions
}
