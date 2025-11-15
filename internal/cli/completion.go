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
	actions := []string{"resume", "stop"}
	completions := make([]string, 0, len(actions))
	for _, action := range actions {
		if strings.HasPrefix(action, toComplete) {
			completions = append(completions, action)
		}
	}
	return completions, cobra.ShellCompDirectiveNoFileComp
}
