package cli

import (
	"fmt"
	"sort"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newRestartReportCommand() *cobra.Command {
	var namespace string
	var allNamespaces bool
	var minRestarts int

	cmd := &cobra.Command{
		Use:   "restart-report",
		Short: "List pods with high restart counts",
		Long: `Lists pods sorted by total restart count descending.
Shows namespace, name, restarts, status, last restart time, and age.`,
		Example: `  # Show pods with 5+ restarts across all namespaces
  ocp restart-report

  # Show pods with 10+ restarts in a specific namespace
  ocp restart-report --min-restarts=10 --namespace=my-app`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := ensureContext(cmd.Context())
			out := cmd.OutOrStdout()

			ns := resolveNamespace(ctx, namespace, allNamespaces)

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			opts := metav1.ListOptions{Limit: 500}
			var allPods []corev1.Pod
			for {
				list, err := clientset.CoreV1().Pods(ns).List(ctx, opts)
				if err != nil {
					return fmt.Errorf("failed to list pods: %w", err)
				}
				allPods = append(allPods, list.Items...)
				if list.Continue == "" {
					break
				}
				opts.Continue = list.Continue
			}

			type podRestart struct {
				namespace   string
				name        string
				restarts    int32
				status      string
				lastRestart string
				age         string
			}

			var results []podRestart
			for _, pod := range allPods {
				var totalRestarts int32
				var lastTerminated time.Time
				for _, cs := range pod.Status.ContainerStatuses {
					totalRestarts += cs.RestartCount
					if cs.LastTerminationState.Terminated != nil {
						t := cs.LastTerminationState.Terminated.FinishedAt.Time
						if t.After(lastTerminated) {
							lastTerminated = t
						}
					}
				}

				if int(totalRestarts) < minRestarts {
					continue
				}

				lastRestartStr := "<none>"
				if !lastTerminated.IsZero() {
					lastRestartStr = formatAge(lastTerminated) + " ago"
				}

				results = append(results, podRestart{
					namespace:   pod.Namespace,
					name:        pod.Name,
					restarts:    totalRestarts,
					status:      string(pod.Status.Phase),
					lastRestart: lastRestartStr,
					age:         formatAge(pod.CreationTimestamp.Time),
				})
			}

			sort.Slice(results, func(i, j int) bool {
				return results[i].restarts > results[j].restarts
			})

			if len(results) == 0 {
				fmt.Fprintf(out, "No pods found with %d+ restarts.\n", minRestarts)
				return nil
			}

			w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "NAMESPACE\tNAME\tRESTARTS\tSTATUS\tLAST RESTART\tAGE")
			for _, r := range results {
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
					r.namespace, r.name, strconv.Itoa(int(r.restarts)),
					r.status, r.lastRestart, r.age)
			}
			w.Flush()

			fmt.Fprintf(out, "\nTotal: %d pod(s) with %d+ restarts\n", len(results), minRestarts)
			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace to check")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", true, "Check all namespaces")
	cmd.Flags().IntVar(&minRestarts, "min-restarts", 5, "Minimum restart count to include")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}
