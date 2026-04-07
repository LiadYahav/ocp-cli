package cli

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/liadyahav/ocp-cli/internal/kube"
	"github.com/liadyahav/ocp-cli/internal/pkg/sshutil"
)

func newEtcdHealthCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "etcd",
		Short: "etcd cluster diagnostics",
	}

	cmd.AddCommand(newEtcdHealthCheckCommand())
	return cmd
}

func newEtcdHealthCheckCommand() *cobra.Command {
	var user string
	var identityFile string

	cmd := &cobra.Command{
		Use:   "health",
		Short: "Check etcd cluster health",
		Long: `Connects to a master node via SSH and runs etcdctl commands to check:
- Endpoint health
- Member list
- Endpoint status (leader, DB size, raft index)
- Alarm list

Automatically detects the etcd container and tries both OpenShift and vanilla K8s cert paths.`,
		Example: `  # Check etcd health
  ocp etcd health

  # With custom SSH user
  ocp etcd health --user=core`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := ensureContext(cmd.Context())
			out := cmd.OutOrStdout()

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			// Find a master node
			nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if err != nil {
				return fmt.Errorf("failed to list nodes: %w", err)
			}

			var masterName string
			for _, node := range nodes.Items {
				if _, ok := node.Labels["node-role.kubernetes.io/master"]; ok {
					masterName = node.Name
					break
				}
				if _, ok := node.Labels["node-role.kubernetes.io/control-plane"]; ok {
					masterName = node.Name
					break
				}
			}

			if masterName == "" {
				return fmt.Errorf("no master/control-plane node found")
			}

			fmt.Fprintf(out, "Using master node: %s\n\n", masterName)

			ip, err := resolveNodeIP(ctx, masterName)
			if err != nil {
				return fmt.Errorf("failed to resolve node IP: %w", err)
			}

			resolvedKey, err := resolveIdentityFile(identityFile)
			if err != nil {
				return err
			}

			runner := sshutil.NewRunner(user, resolvedKey)

			// Try to find etcd container ID
			findEtcd := `sudo crictl ps --name etcd -q 2>/dev/null | head -1`
			etcdID, _, err := runner.RunWithOutput(ctx, ip, findEtcd)
			etcdID = strings.TrimSpace(etcdID)

			if err != nil || etcdID == "" {
				fmt.Fprintln(out, "Could not find etcd container via crictl. Trying direct etcdctl...")
				return runDirectEtcdctl(runner, ctx, ip, out)
			}

			fmt.Fprintf(out, "Found etcd container: %s\n\n", etcdID)

			// Try OpenShift cert paths first, then vanilla K8s
			certPaths := []struct {
				ca   string
				cert string
				key  string
			}{
				{
					ca:   "/etc/kubernetes/static-pod-certs/configmaps/etcd-serving-ca/ca-bundle.crt",
					cert: "/etc/kubernetes/static-pod-certs/secrets/etcd-all-certs/etcd-serving-master-0.crt",
					key:  "/etc/kubernetes/static-pod-certs/secrets/etcd-all-certs/etcd-serving-master-0.key",
				},
				{
					ca:   "/etc/kubernetes/pki/etcd/ca.crt",
					cert: "/etc/kubernetes/pki/etcd/server.crt",
					key:  "/etc/kubernetes/pki/etcd/server.key",
				},
			}

			etcdCommands := []struct {
				label string
				args  string
			}{
				{"Endpoint Health", "endpoint health --cluster -w table"},
				{"Member List", "member list -w table"},
				{"Endpoint Status", "endpoint status --cluster -w table"},
				{"Alarm List", "alarm list"},
			}

			for _, cp := range certPaths {
				baseCmd := fmt.Sprintf(
					`sudo crictl exec %s etcdctl --cacert=%s --cert=%s --key=%s`,
					etcdID, cp.ca, cp.cert, cp.key)

				testCmd := baseCmd + " endpoint health 2>&1"
				testOut, _, testErr := runner.RunWithOutput(ctx, ip, testCmd)
				if testErr != nil || strings.Contains(testOut, "Error") || strings.Contains(testOut, "error") {
					continue
				}

				for _, ec := range etcdCommands {
					fmt.Fprintf(out, "=== %s ===\n", ec.label)
					fullCmd := baseCmd + " " + ec.args
					runner.Stdout = out
					runner.Stderr = cmd.ErrOrStderr()
					_ = runner.Run(ctx, ip, fullCmd)
					fmt.Fprintln(out)
				}

				return nil
			}

			fmt.Fprintln(out, "Could not find valid etcd cert paths. Attempting without TLS...")
			return runDirectEtcdctl(runner, ctx, ip, out)
		},
	}

	cmd.Flags().StringVarP(&user, "user", "u", "core", "SSH username")
	cmd.Flags().StringVarP(&identityFile, "identity", "i", "", "SSH identity file")

	return cmd
}

func runDirectEtcdctl(runner *sshutil.Runner, ctx context.Context, ip string, out io.Writer) error {
	commands := []struct {
		label string
		cmd   string
	}{
		{"Endpoint Health", "sudo etcdctl endpoint health --cluster -w table 2>&1 || true"},
		{"Member List", "sudo etcdctl member list -w table 2>&1 || true"},
		{"Endpoint Status", "sudo etcdctl endpoint status --cluster -w table 2>&1 || true"},
		{"Alarm List", "sudo etcdctl alarm list 2>&1 || true"},
	}

	for _, c := range commands {
		fmt.Fprintf(out, "=== %s ===\n", c.label)
		runner.Stdout = out
		_ = runner.Run(ctx, ip, c.cmd)
		fmt.Fprintln(out)
	}

	return nil
}
