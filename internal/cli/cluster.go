package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
