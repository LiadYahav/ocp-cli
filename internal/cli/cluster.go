package cli

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	unstructuredhelpers "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Display cluster information",
		Long: `Display information about the current Kubernetes cluster.

Examples:
  ocp cluster info
  ocp cluster version`,
	}

	cmd.AddCommand(
		newClusterInfoCommand(),
		newClusterVersionCommand(),
		newClusterWatchCommand(),
	)

	return cmd
}

func newClusterInfoCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "info",
		Short: "Display cluster information",
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

			// Get nodes
			nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if err != nil {
				return fmt.Errorf("failed to list nodes: %w", err)
			}

			// Get namespaces
			namespaces, err := clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
			if err != nil {
				return fmt.Errorf("failed to list namespaces: %w", err)
			}

			// Get all pods
			pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
			if err != nil {
				return fmt.Errorf("failed to list pods: %w", err)
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
		Short: "Watch cluster operators, clusterversion, and machineconfigpools",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			watchCmd := exec.CommandContext(ctx, "watch", "-n", "1", "-d", "sh", "-c", "oc get co,clusterversion,mcp")
			watchCmd.Stdin = cmd.InOrStdin()
			watchCmd.Stdout = cmd.OutOrStdout()
			watchCmd.Stderr = cmd.ErrOrStderr()

			return watchCmd.Run()
		},
	}
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
