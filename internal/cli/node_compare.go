package cli

import (
	"fmt"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/liadyahav/ocp-cli/internal/kube"
	"github.com/liadyahav/ocp-cli/internal/pkg/sshutil"
)

func newNodeCompareCommand() *cobra.Command {
	var user string
	var identityFile string

	cmd := &cobra.Command{
		Use:               "compare <node1> <node2>",
		Short:             "Side-by-side comparison of two nodes",
		Long:              `Compares two nodes by SSH-gathered system info and Kubernetes-level data. Mismatched rows are highlighted.`,
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completeNodeNames,
		Example: `  # Compare two worker nodes
  ocp node compare worker-0 worker-1

  # Compare with custom SSH user
  ocp node compare master-0 master-1 --user=core`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := ensureContext(cmd.Context())
			out := cmd.OutOrStdout()
			node1Name, node2Name := args[0], args[1]

			resolvedKey, err := resolveIdentityFile(identityFile)
			if err != nil {
				return err
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			// Gather SSH data concurrently
			sshScript := `echo "OS:$(cat /etc/os-release 2>/dev/null | grep PRETTY_NAME | cut -d= -f2 | tr -d '"')"; ` +
				`echo "KERNEL:$(uname -r)"; ` +
				`echo "CPU:$(nproc)"; ` +
				`echo "MEMORY:$(free -h | grep Mem | awk '{print $2}')"; ` +
				`echo "DISK:$(df -h / | tail -1 | awk '{print $5}')"; ` +
				`echo "UPTIME:$(uptime -p 2>/dev/null || uptime)"`

			type sshResult struct {
				data map[string]string
				err  error
			}

			var wg sync.WaitGroup
			ssh1 := make(chan sshResult, 1)
			ssh2 := make(chan sshResult, 1)

			collectSSH := func(nodeName string, ch chan<- sshResult) {
				defer wg.Done()
				ip, err := resolveNodeIP(ctx, nodeName)
				if err != nil {
					ch <- sshResult{err: fmt.Errorf("resolve IP for %s: %w", nodeName, err)}
					return
				}
				runner := sshutil.NewRunner(user, resolvedKey)
				stdout, _, err := runner.RunWithOutput(ctx, ip, sshScript)
				if err != nil {
					ch <- sshResult{err: fmt.Errorf("SSH to %s: %w", nodeName, err)}
					return
				}
				data := parseSSHOutput(stdout)
				ch <- sshResult{data: data}
			}

			wg.Add(2)
			go collectSSH(node1Name, ssh1)
			go collectSSH(node2Name, ssh2)
			wg.Wait()

			r1 := <-ssh1
			r2 := <-ssh2

			if r1.err != nil {
				return r1.err
			}
			if r2.err != nil {
				return r2.err
			}

			// Gather K8s data
			n1, err := clientset.CoreV1().Nodes().Get(ctx, node1Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get node %s: %w", node1Name, err)
			}
			n2, err := clientset.CoreV1().Nodes().Get(ctx, node2Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get node %s: %w", node2Name, err)
			}

			// Get pod counts
			pods1, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
				FieldSelector: fmt.Sprintf("spec.nodeName=%s", node1Name),
			})
			if err != nil {
				return fmt.Errorf("failed to list pods on %s: %w", node1Name, err)
			}
			pods2, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
				FieldSelector: fmt.Sprintf("spec.nodeName=%s", node2Name),
			})
			if err != nil {
				return fmt.Errorf("failed to list pods on %s: %w", node2Name, err)
			}

			// Build comparison rows
			type row struct {
				field string
				val1  string
				val2  string
			}

			var rows []row

			// SSH data
			sshFields := []string{"OS", "KERNEL", "CPU", "MEMORY", "DISK", "UPTIME"}
			for _, f := range sshFields {
				rows = append(rows, row{f, r1.data[f], r2.data[f]})
			}

			// K8s data
			rows = append(rows,
				row{"Kubelet Version", n1.Status.NodeInfo.KubeletVersion, n2.Status.NodeInfo.KubeletVersion},
				row{"Container Runtime", n1.Status.NodeInfo.ContainerRuntimeVersion, n2.Status.NodeInfo.ContainerRuntimeVersion},
				row{"CPU Allocatable", n1.Status.Allocatable.Cpu().String(), n2.Status.Allocatable.Cpu().String()},
				row{"Mem Allocatable", n1.Status.Allocatable.Memory().String(), n2.Status.Allocatable.Memory().String()},
				row{"Pods", fmt.Sprintf("%d", len(pods1.Items)), fmt.Sprintf("%d", len(pods2.Items))},
			)

			// Labels diff
			rows = append(rows, row{"Roles", nodeRoles(n1.Labels), nodeRoles(n2.Labels)})

			// Taints
			rows = append(rows, row{"Taints", fmt.Sprintf("%d", len(n1.Spec.Taints)), fmt.Sprintf("%d", len(n2.Spec.Taints))})

			// Print
			w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
			fmt.Fprintf(w, "FIELD\t%s\t%s\tMATCH\n", node1Name, node2Name)
			for _, r := range rows {
				match := "ok"
				if r.val1 != r.val2 {
					match = "\033[1;33m*DIFF*\033[0m"
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", r.field, r.val1, r.val2, match)
			}
			w.Flush()

			return nil
		},
	}

	cmd.Flags().StringVarP(&user, "user", "u", "core", "SSH username")
	cmd.Flags().StringVarP(&identityFile, "identity", "i", "", "SSH identity file")

	return cmd
}

func parseSSHOutput(output string) map[string]string {
	data := make(map[string]string)
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if idx := strings.Index(line, ":"); idx > 0 {
			key := line[:idx]
			val := strings.TrimSpace(line[idx+1:])
			data[key] = val
		}
	}
	return data
}

func nodeRoles(labels map[string]string) string {
	var roles []string
	for key := range labels {
		if strings.HasPrefix(key, "node-role.kubernetes.io/") {
			roles = append(roles, strings.TrimPrefix(key, "node-role.kubernetes.io/"))
		}
	}
	if len(roles) == 0 {
		return "<none>"
	}
	return strings.Join(roles, ",")
}
