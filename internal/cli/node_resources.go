package cli

import (
	"fmt"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newNodeResourcesCommand() *cobra.Command {
	var sortBy string

	cmd := &cobra.Command{
		Use:               "resources <node>",
		Short:             "Show per-pod resource usage on a node",
		Long:              `Displays all pods running on a node with their CPU and memory requests/limits, plus node totals and percentages.`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeNodeNames,
		Example: `  # Show resource usage on a node
  ocp node resources worker-0

  # Sort by memory requests
  ocp node resources worker-0 --sort-by=memory`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := ensureContext(cmd.Context())
			out := cmd.OutOrStdout()
			nodeName := args[0]

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			node, err := findNodeByName(ctx, nodeName)
			if err != nil {
				return err
			}

			pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
				FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
			})
			if err != nil {
				return fmt.Errorf("failed to list pods: %w", err)
			}

			type podRow struct {
				namespace string
				name      string
				cpuReq    int64
				cpuLim    int64
				memReq    int64
				memLim    int64
			}

			var rows []podRow
			for _, pod := range pods.Items {
				if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
					continue
				}
				r := podRow{namespace: pod.Namespace, name: pod.Name}
				for _, c := range pod.Spec.Containers {
					if req, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
						r.cpuReq += req.MilliValue()
					}
					if lim, ok := c.Resources.Limits[corev1.ResourceCPU]; ok {
						r.cpuLim += lim.MilliValue()
					}
					if req, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
						r.memReq += req.Value()
					}
					if lim, ok := c.Resources.Limits[corev1.ResourceMemory]; ok {
						r.memLim += lim.Value()
					}
				}
				rows = append(rows, r)
			}

			switch sortBy {
			case "memory":
				sort.Slice(rows, func(i, j int) bool { return rows[i].memReq > rows[j].memReq })
			default:
				sort.Slice(rows, func(i, j int) bool { return rows[i].cpuReq > rows[j].cpuReq })
			}

			w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "NAMESPACE\tNAME\tCPU_REQ\tCPU_LIM\tMEM_REQ\tMEM_LIM")

			var totalCPUReq, totalCPULim, totalMemReq, totalMemLim int64
			for _, r := range rows {
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
					r.namespace, r.name,
					formatMilliCPU(r.cpuReq), formatMilliCPU(r.cpuLim),
					formatMemory(r.memReq), formatMemory(r.memLim))
				totalCPUReq += r.cpuReq
				totalCPULim += r.cpuLim
				totalMemReq += r.memReq
				totalMemLim += r.memLim
			}
			w.Flush()

			alloc := node.Status.Allocatable
			cpuAlloc := alloc.Cpu().MilliValue()
			memAlloc := alloc.Memory().Value()

			fmt.Fprintf(out, "\n=== Node %s Totals ===\n", nodeName)
			fmt.Fprintf(out, "CPU:    %s / %s (%d%%)\n",
				formatMilliCPU(totalCPUReq), formatMilliCPU(cpuAlloc), pctSafe(totalCPUReq, cpuAlloc))
			fmt.Fprintf(out, "Memory: %s / %s (%d%%)\n",
				formatMemory(totalMemReq), formatMemory(memAlloc), pctSafe(totalMemReq, memAlloc))
			fmt.Fprintf(out, "Pods:   %d / %s\n", len(rows), alloc.Pods().String())

			return nil
		},
	}

	cmd.Flags().StringVar(&sortBy, "sort-by", "cpu", "Sort by: cpu, memory")

	return cmd
}
