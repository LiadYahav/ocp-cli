package cli

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"

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
		Short: "Watch cluster operators, clusterversion, and machineconfigpools",
		Example: `  # Continuously watch operators, clusterversion, and MCPs
  ocp cluster watch`,
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

func newClusterConfigureDNSCommand() *cobra.Command {
	var user string
	var identityFile string

	cmd := &cobra.Command{
		Use:   "configure-dns <nameservers>",
		Short: "Configure node DNS settings",
		Long: `Configure DNS servers on every node via NetworkManager.

**RISK**: This command modifies live networking on all nodes. Ensure you have
         console access before proceeding.`,
		Example: `  # Prepend a single nameserver while keeping existing entries
  ocp cluster configure-dns 1.1.1.1

  # Override DNS on all nodes with two nameservers
  ocp cluster configure-dns 8.8.8.8,8.8.4.4`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			nameservers, err := parseNameservers(args[0])
			if err != nil {
				return err
			}

			if err := validateNameservers(ctx, nameservers); err != nil {
				return err
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
				return errors.New("no nodes found in the cluster")
			}

			mode := dnsModeOverride
			if len(nameservers) == 1 {
				mode = dnsModeAppend
			}

			for _, node := range nodes.Items {
				fmt.Fprintf(cmd.OutOrStdout(), "Configuring DNS on %s...\n", node.Name)
				script := buildDNSConfigureScript(nameservers, mode)
				if err := runSSHCommand(ctx, user, identityFile, node.Name, []string{script}, cmd); err != nil {
					return fmt.Errorf("failed to configure DNS on node %s: %w", node.Name, err)
				}
			}

			fmt.Fprintln(cmd.OutOrStdout(), "DNS configuration updated on all nodes.")
			return nil
		},
	}

	cmd.Flags().StringVarP(&user, "user", "u", "core", "Username for SSH connection")
	cmd.Flags().StringVarP(&identityFile, "identity", "i", "", "Path to private key file for SSH authentication (default: ~/.ssh/id_rsa_ocp if exists)")

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
		result = append(result, part)
	}

	if len(result) == 0 {
		return nil, errors.New("no nameservers provided")
	}

	return result, nil
}

func validateNameservers(ctx context.Context, nameservers []string) error {
	for _, ns := range nameservers {
		if ip := net.ParseIP(ns); ip == nil {
			return fmt.Errorf("invalid nameserver IP: %s", ns)
		}

		if err := queryNameserver(ctx, ns); err != nil {
			return fmt.Errorf("nameserver %s failed validation: %w", ns, err)
		}
	}

	return nil
}

func queryNameserver(ctx context.Context, nameserver string) error {
	lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(lookupCtx, "nslookup", "example.com", nameserver)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("nslookup failed: %w", err)
	}

	return nil
}

func buildDNSConfigureScript(nameservers []string, mode dnsMode) string {
	joined := strings.Join(nameservers, ",")
	body := fmt.Sprintf(`set -euo pipefail
conn=$(nmcli -t -f NAME connection show --active | head -n1)
if [ -z "$conn" ]; then
  echo "No active NetworkManager connection found" >&2
  exit 1
fi
current=$(nmcli -g ipv4.dns connection show "$conn" | tr -d "\r")
newdns="%s"
if [ "%s" = "append" ] && [ -n "$current" ]; then
  newdns="${newdns},${current}"
fi
nmcli connection modify "$conn" ipv4.dns "$newdns"
nmcli connection modify "$conn" ipv4.ignore-auto-dns yes
nmcli connection up "$conn" >/dev/null 2>&1 || true
systemctl restart NetworkManager
`, joined, mode)

	return fmt.Sprintf("sudo bash -c %q", body)
}
