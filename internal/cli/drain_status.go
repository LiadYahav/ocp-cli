package cli

import (
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newDrainStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "drain-status",
		Short: "Show cordoned nodes and their drain progress",
		Long: `Lists all nodes marked as unschedulable (cordoned) with details:
- Maintenance reason (from annotation)
- Remaining non-DaemonSet pods
- DaemonSet pod count
- Node age`,
		Example: `  # Show cordoned nodes
  ocp drain-status`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := ensureContext(cmd.Context())
			out := cmd.OutOrStdout()

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if err != nil {
				return fmt.Errorf("failed to list nodes: %w", err)
			}

			type cordonedNode struct {
				name         string
				reason       string
				podsLeft     int
				daemonSetPod int
				age          string
			}

			var cordoned []cordonedNode
			for _, node := range nodes.Items {
				if !node.Spec.Unschedulable {
					continue
				}

				reason := "<none>"
				if node.Annotations != nil {
					if r, ok := node.Annotations[maintenanceAnnotationKey]; ok && r != "" {
						reason = r
					}
				}

				pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
					FieldSelector: fmt.Sprintf("spec.nodeName=%s", node.Name),
				})
				if err != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "warning: failed to list pods on %s: %v\n", node.Name, err)
					continue
				}

				var nonDS, ds int
				for _, pod := range pods.Items {
					isDaemonSet := false
					for _, ref := range pod.OwnerReferences {
						if ref.Kind == "DaemonSet" {
							isDaemonSet = true
							break
						}
					}
					if isDaemonSet {
						ds++
					} else {
						nonDS++
					}
				}

				cordoned = append(cordoned, cordonedNode{
					name:         node.Name,
					reason:       reason,
					podsLeft:     nonDS,
					daemonSetPod: ds,
					age:          formatAge(node.CreationTimestamp.Time),
				})
			}

			if len(cordoned) == 0 {
				fmt.Fprintln(out, "No cordoned nodes found.")
				return nil
			}

			w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "NODE\tREASON\tPODS_REMAINING\tDAEMONSET_PODS\tAGE")
			for _, cn := range cordoned {
				fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\n",
					cn.name, cn.reason, cn.podsLeft, cn.daemonSetPod, cn.age)
			}
			w.Flush()

			fullyDrained := 0
			for _, cn := range cordoned {
				if cn.podsLeft == 0 {
					fullyDrained++
				}
			}

			fmt.Fprintf(out, "\nTotal cordoned: %d  Fully drained: %d  Still draining: %d\n",
				len(cordoned), fullyDrained, len(cordoned)-fullyDrained)

			return nil
		},
	}
}
