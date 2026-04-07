package cli

import (
	"fmt"
	"sort"
	"sync"
	"text/tabwriter"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newCapacityCommand() *cobra.Command {
	var sortBy string

	cmd := &cobra.Command{
		Use:   "capacity",
		Short: "Show cluster resource utilization per node",
		Long: `Displays per-node resource utilization including CPU, memory, and pod counts.
Nodes with >90% request utilization are marked with *.
Cluster totals are shown at the bottom.`,
		Example: `  # Show capacity overview
  ocp capacity

  # Sort by memory utilization
  ocp capacity --sort-by=memory

  # Sort by pod count
  ocp capacity --sort-by=pods`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := ensureContext(cmd.Context())
			out := cmd.OutOrStdout()

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			var (
				nodes    *corev1.NodeList
				pods     *corev1.PodList
				nodesErr error
				podsErr  error
			)

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()
				nodes, nodesErr = clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			}()

			go func() {
				defer wg.Done()
				opts := metav1.ListOptions{Limit: 500}
				var allPods []corev1.Pod
				for {
					list, err := clientset.CoreV1().Pods("").List(ctx, opts)
					if err != nil {
						podsErr = err
						return
					}
					allPods = append(allPods, list.Items...)
					if list.Continue == "" {
						break
					}
					opts.Continue = list.Continue
				}
				pods = &corev1.PodList{Items: allPods}
			}()

			wg.Wait()

			if nodesErr != nil {
				return fmt.Errorf("failed to list nodes: %w", nodesErr)
			}
			if podsErr != nil {
				return fmt.Errorf("failed to list pods: %w", podsErr)
			}

			// Group pods by node
			podsByNode := make(map[string][]corev1.Pod)
			for _, pod := range pods.Items {
				if pod.Spec.NodeName != "" && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
					podsByNode[pod.Spec.NodeName] = append(podsByNode[pod.Spec.NodeName], pod)
				}
			}

			type nodeCapacity struct {
				name     string
				cpuAlloc int64 // millicores
				cpuReq   int64
				cpuLim   int64
				memAlloc int64 // bytes
				memReq   int64
				memLim   int64
				pods     int
				podAlloc int64
			}

			var rows []nodeCapacity
			for _, node := range nodes.Items {
				alloc := node.Status.Allocatable
				nc := nodeCapacity{
					name:     node.Name,
					cpuAlloc: alloc.Cpu().MilliValue(),
					memAlloc: alloc.Memory().Value(),
					podAlloc: alloc.Pods().Value(),
				}

				for _, pod := range podsByNode[node.Name] {
					for _, c := range pod.Spec.Containers {
						if req, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
							nc.cpuReq += req.MilliValue()
						}
						if lim, ok := c.Resources.Limits[corev1.ResourceCPU]; ok {
							nc.cpuLim += lim.MilliValue()
						}
						if req, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
							nc.memReq += req.Value()
						}
						if lim, ok := c.Resources.Limits[corev1.ResourceMemory]; ok {
							nc.memLim += lim.Value()
						}
					}
					nc.pods++
				}

				rows = append(rows, nc)
			}

			// Sort
			switch sortBy {
			case "memory":
				sort.Slice(rows, func(i, j int) bool {
					return pctSafe(rows[i].memReq, rows[i].memAlloc) > pctSafe(rows[j].memReq, rows[j].memAlloc)
				})
			case "pods":
				sort.Slice(rows, func(i, j int) bool {
					return rows[i].pods > rows[j].pods
				})
			default: // cpu
				sort.Slice(rows, func(i, j int) bool {
					return pctSafe(rows[i].cpuReq, rows[i].cpuAlloc) > pctSafe(rows[j].cpuReq, rows[j].cpuAlloc)
				})
			}

			w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "NODE\tCPU_ALLOC\tCPU_REQ\tCPU_LIM\tCPU_REQ%\tMEM_ALLOC\tMEM_REQ\tMEM_LIM\tMEM_REQ%\tPODS")

			var totalCPUAlloc, totalCPUReq, totalCPULim int64
			var totalMemAlloc, totalMemReq, totalMemLim int64
			var totalPods int

			for _, r := range rows {
				cpuPct := pctSafe(r.cpuReq, r.cpuAlloc)
				memPct := pctSafe(r.memReq, r.memAlloc)
				marker := ""
				if cpuPct > 90 || memPct > 90 {
					marker = " *"
				}

				fmt.Fprintf(w, "%s%s\t%s\t%s\t%s\t%d%%\t%s\t%s\t%s\t%d%%\t%d/%d\n",
					r.name, marker,
					formatMilliCPU(r.cpuAlloc), formatMilliCPU(r.cpuReq), formatMilliCPU(r.cpuLim), cpuPct,
					formatMemory(r.memAlloc), formatMemory(r.memReq), formatMemory(r.memLim), memPct,
					r.pods, r.podAlloc)

				totalCPUAlloc += r.cpuAlloc
				totalCPUReq += r.cpuReq
				totalCPULim += r.cpuLim
				totalMemAlloc += r.memAlloc
				totalMemReq += r.memReq
				totalMemLim += r.memLim
				totalPods += r.pods
			}

			fmt.Fprintf(w, "---\t---\t---\t---\t---\t---\t---\t---\t---\t---\n")
			fmt.Fprintf(w, "TOTAL\t%s\t%s\t%s\t%d%%\t%s\t%s\t%s\t%d%%\t%d\n",
				formatMilliCPU(totalCPUAlloc), formatMilliCPU(totalCPUReq), formatMilliCPU(totalCPULim),
				pctSafe(totalCPUReq, totalCPUAlloc),
				formatMemory(totalMemAlloc), formatMemory(totalMemReq), formatMemory(totalMemLim),
				pctSafe(totalMemReq, totalMemAlloc),
				totalPods)

			w.Flush()
			return nil
		},
	}

	cmd.Flags().StringVar(&sortBy, "sort-by", "cpu", "Sort nodes by: cpu, memory, pods")

	return cmd
}

func pctSafe(used, total int64) int64 {
	if total == 0 {
		return 0
	}
	return (used * 100) / total
}

func formatMilliCPU(milli int64) string {
	if milli >= 1000 {
		cores := float64(milli) / 1000
		return fmt.Sprintf("%.1f", cores)
	}
	return fmt.Sprintf("%dm", milli)
}

