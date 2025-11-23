package cli

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/liadyahav/ocp-cli/internal/kube"
	"github.com/liadyahav/ocp-cli/internal/pkg/diagnose"
	"github.com/liadyahav/ocp-cli/internal/pkg/sshutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
)

func newDiagnoseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diagnose",
		Short: "Run diagnostics on cluster components",
		Long:  `Run diagnostic checks on cluster nodes and components via SSH and API.`,
	}

	cmd.AddCommand(newDiagnoseNodeCommand())
	cmd.AddCommand(newDiagnosePodCommand())
	return cmd
}

func newDiagnoseNodeCommand() *cobra.Command {
	var user string
	var identityFile string

	cmd := &cobra.Command{
		Use:   "node [node-name]",
		Short: "Check node health metrics (CPU, Disk, Services)",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			
			// Resolve nodes
			var nodes []string
			if len(args) > 0 {
				nodes = []string{args[0]}
			} else {
				// Get all nodes
				clientset, err := kube.NewClientset(ctx)
				if err != nil {
					return err
				}
				nodeList, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				if err != nil {
					return err
				}
				for _, n := range nodeList.Items {
					nodes = append(nodes, n.Name)
				}
			}

			runner := sshutil.NewRunner(user, identityFile)
			// Don't print SSH output to stdout during diagnostics, we parse it
			runner.Stdout = nil 
			
			// Try to create metrics client (optional)
			var metricsClient *metrics.Clientset
			cfg, err := kube.GetConfig(ctx)
			if err == nil {
				metricsClient, _ = metrics.NewForConfig(cfg)
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "NODE\tSTATUS\tLOAD\tMEM\tDISK\tCPU_REQ\tMEM_REQ\tISSUES")

			for _, node := range nodes {
				diag, err := diagnose.CheckNodeHealth(ctx, runner, metricsClient, node)
				if err != nil {
					fmt.Fprintf(w, "%s\tERROR\t-\t-\t-\t-\t-\t%v\n", node, err)
					continue
				}

				status := "OK"
				issues := ""
				if len(diag.Errors) > 0 {
					status = "WARN"
					issues = fmt.Sprintf("%v", diag.Errors)
				}

				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", 
					node, status, diag.LoadAverage, diag.MemoryUsed, diag.DiskUsage, diag.CPURequest, diag.MemoryRequest, issues)
			}
			w.Flush()
			return nil
		},
	}

	cmd.Flags().StringVarP(&user, "user", "u", "core", "SSH User")
	cmd.Flags().StringVarP(&identityFile, "identity", "i", "", "SSH Identity file")

	return cmd
}

func newDiagnosePodCommand() *cobra.Command {
	var namespace string
	
	cmd := &cobra.Command{
		Use:   "pod <pod-name>",
		Short: "Check pod health (status, restarts, events, logs)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			podName := args[0]
			
			// Resolve namespace
			if namespace == "" {
				namespace = "default"
				if ns, err := kube.GetCurrentNamespace(ctx); err == nil {
					namespace = ns
				}
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return err
			}

			diag, err := diagnose.CheckPodHealth(ctx, clientset, podName, namespace)
			if err != nil {
				return err
			}

			fmt.Printf("Pod: %s/%s\n", diag.Namespace, diag.Name)
			fmt.Printf("Status: %s\n", diag.Status)
			fmt.Printf("Age: %s\n", diag.Age)
			fmt.Printf("Restarts: %d\n", diag.Restarts)
			
			if len(diag.Issues) > 0 {
				fmt.Println("\nIssues:")
				for _, issue := range diag.Issues {
					fmt.Printf("- %s\n", issue)
				}
			} else {
				fmt.Println("\nNo issues found.")
			}

			if len(diag.Events) > 0 {
				fmt.Println("\nWarning Events:")
				for _, event := range diag.Events {
					fmt.Printf("- %s\n", event)
				}
			}

			if diag.Logs != "" {
				fmt.Println("\nRecent Logs (Error/Tail):")
				fmt.Println(diag.Logs)
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")

	return cmd
}

