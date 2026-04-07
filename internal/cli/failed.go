package cli

import (
	"fmt"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/spf13/cobra"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	unstructuredhelpers "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newFailedCommand() *cobra.Command {
	var namespace string
	var allNamespaces bool

	cmd := &cobra.Command{
		Use:   "failed",
		Short: "Show broken resources across the cluster",
		Long: `Displays a summary of unhealthy resources:
- Crashlooping pods (CrashLoopBackOff, ImagePullBackOff, ErrImagePull)
- Pending pods stuck in Pending phase
- Failed jobs
- Unhealthy nodes (NotReady, MemoryPressure, DiskPressure, PIDPressure)
- Degraded ClusterOperators (OpenShift only)`,
		Example: `  # Show all broken resources cluster-wide
  ocp failed

  # Show broken resources in a specific namespace
  ocp failed --namespace=my-app

  # Show broken resources in current namespace only
  ocp failed --namespace=""`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := ensureContext(cmd.Context())
			out := cmd.OutOrStdout()

			ns := resolveNamespace(ctx, namespace, allNamespaces)

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			var (
				pods     *corev1.PodList
				nodes    *corev1.NodeList
				jobs     *batchv1.JobList
				podsErr  error
				nodesErr error
				jobsErr  error
			)

			var wg sync.WaitGroup
			wg.Add(3)

			go func() {
				defer wg.Done()
				opts := metav1.ListOptions{Limit: 500}
				var allPods []corev1.Pod
				for {
					list, err := clientset.CoreV1().Pods(ns).List(ctx, opts)
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

			go func() {
				defer wg.Done()
				nodes, nodesErr = clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			}()

			go func() {
				defer wg.Done()
				opts := metav1.ListOptions{Limit: 500}
				var allJobs []batchv1.Job
				for {
					list, err := clientset.BatchV1().Jobs(ns).List(ctx, opts)
					if err != nil {
						jobsErr = err
						return
					}
					allJobs = append(allJobs, list.Items...)
					if list.Continue == "" {
						break
					}
					opts.Continue = list.Continue
				}
				jobs = &batchv1.JobList{Items: allJobs}
			}()

			wg.Wait()

			if podsErr != nil {
				return fmt.Errorf("failed to list pods: %w", podsErr)
			}
			if nodesErr != nil {
				return fmt.Errorf("failed to list nodes: %w", nodesErr)
			}
			if jobsErr != nil {
				return fmt.Errorf("failed to list jobs: %w", jobsErr)
			}

			totalIssues := 0

			// Crashlooping pods
			var crashlooping []corev1.Pod
			for _, pod := range pods.Items {
				for _, cs := range pod.Status.ContainerStatuses {
					if cs.State.Waiting != nil {
						reason := cs.State.Waiting.Reason
						if reason == "CrashLoopBackOff" || reason == "ImagePullBackOff" || reason == "ErrImagePull" {
							crashlooping = append(crashlooping, pod)
							break
						}
					}
				}
			}
			if len(crashlooping) > 0 {
				totalIssues += len(crashlooping)
				fmt.Fprintf(out, "=== Crashlooping Pods (%d) ===\n", len(crashlooping))
				w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
				fmt.Fprintln(w, "NAMESPACE\tNAME\tSTATUS\tRESTARTS\tAGE")
				for _, pod := range crashlooping {
					reason := ""
					for _, cs := range pod.Status.ContainerStatuses {
						if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
							reason = cs.State.Waiting.Reason
							break
						}
					}
					fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
						pod.Namespace, pod.Name, reason,
						getPodRestarts(&pod), formatAge(pod.CreationTimestamp.Time))
				}
				w.Flush()
				fmt.Fprintln(out)
			}

			// Pending pods
			var pending []corev1.Pod
			for _, pod := range pods.Items {
				if pod.Status.Phase == corev1.PodPending {
					pending = append(pending, pod)
				}
			}
			if len(pending) > 0 {
				totalIssues += len(pending)
				fmt.Fprintf(out, "=== Pending Pods (%d) ===\n", len(pending))
				w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
				fmt.Fprintln(w, "NAMESPACE\tNAME\tAGE")
				for _, pod := range pending {
					fmt.Fprintf(w, "%s\t%s\t%s\n",
						pod.Namespace, pod.Name, formatAge(pod.CreationTimestamp.Time))
				}
				w.Flush()
				fmt.Fprintln(out)
			}

			// Failed jobs
			var failedJobs []batchv1.Job
			for _, job := range jobs.Items {
				if job.Status.Failed > 0 && !isJobComplete(&job) {
					failedJobs = append(failedJobs, job)
				}
			}
			if len(failedJobs) > 0 {
				totalIssues += len(failedJobs)
				fmt.Fprintf(out, "=== Failed Jobs (%d) ===\n", len(failedJobs))
				w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
				fmt.Fprintln(w, "NAMESPACE\tNAME\tFAILED\tAGE")
				for _, job := range failedJobs {
					fmt.Fprintf(w, "%s\t%s\t%d\t%s\n",
						job.Namespace, job.Name, job.Status.Failed,
						formatAge(job.CreationTimestamp.Time))
				}
				w.Flush()
				fmt.Fprintln(out)
			}

			// Unhealthy nodes
			var unhealthyNodes []struct {
				node   corev1.Node
				issues []string
			}
			for _, node := range nodes.Items {
				var issues []string
				for _, cond := range node.Status.Conditions {
					switch cond.Type {
					case corev1.NodeReady:
						if cond.Status != corev1.ConditionTrue {
							issues = append(issues, "NotReady")
						}
					case corev1.NodeMemoryPressure:
						if cond.Status == corev1.ConditionTrue {
							issues = append(issues, "MemoryPressure")
						}
					case corev1.NodeDiskPressure:
						if cond.Status == corev1.ConditionTrue {
							issues = append(issues, "DiskPressure")
						}
					case corev1.NodePIDPressure:
						if cond.Status == corev1.ConditionTrue {
							issues = append(issues, "PIDPressure")
						}
					}
				}
				if len(issues) > 0 {
					unhealthyNodes = append(unhealthyNodes, struct {
						node   corev1.Node
						issues []string
					}{node, issues})
				}
			}
			if len(unhealthyNodes) > 0 {
				totalIssues += len(unhealthyNodes)
				fmt.Fprintf(out, "=== Unhealthy Nodes (%d) ===\n", len(unhealthyNodes))
				w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
				fmt.Fprintln(w, "NODE\tISSUES\tAGE")
				for _, un := range unhealthyNodes {
					fmt.Fprintf(w, "%s\t%s\t%s\n",
						un.node.Name, strings.Join(un.issues, ","),
						formatAge(un.node.CreationTimestamp.Time))
				}
				w.Flush()
				fmt.Fprintln(out)
			}

			// Degraded ClusterOperators (OpenShift only)
			distro, _ := detectClusterDistribution(ctx)
			if distro == "openshift" {
				dynamicClient, err := kube.NewDynamicClient(ctx)
				if err == nil {
					gvr := schema.GroupVersionResource{
						Group: "config.openshift.io", Version: "v1", Resource: "clusteroperators",
					}
					coList, err := dynamicClient.Resource(gvr).List(ctx, metav1.ListOptions{})
					if err == nil {
						var degraded []struct {
							name    string
							message string
						}
						for _, item := range coList.Items {
							name, _, _ := unstructuredhelpers.NestedString(item.Object, "metadata", "name")
							conditions, _, _ := unstructuredhelpers.NestedSlice(item.Object, "status", "conditions")
							for _, cond := range conditions {
								condMap, ok := cond.(map[string]interface{})
								if !ok {
									continue
								}
								condType, _, _ := unstructuredhelpers.NestedString(condMap, "type")
								condStatus, _, _ := unstructuredhelpers.NestedString(condMap, "status")
								if condType == "Degraded" && condStatus == "True" {
									msg, _, _ := unstructuredhelpers.NestedString(condMap, "message")
									degraded = append(degraded, struct {
										name    string
										message string
									}{name, msg})
									break
								}
							}
						}
						if len(degraded) > 0 {
							totalIssues += len(degraded)
							fmt.Fprintf(out, "=== Degraded ClusterOperators (%d) ===\n", len(degraded))
							w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
							fmt.Fprintln(w, "NAME\tMESSAGE")
							for _, d := range degraded {
								msg := d.message
								if len(msg) > 80 {
									msg = msg[:80] + "..."
								}
								fmt.Fprintf(w, "%s\t%s\n", d.name, msg)
							}
							w.Flush()
							fmt.Fprintln(out)
						}
					} else if !meta.IsNoMatchError(err) {
						fmt.Fprintf(cmd.ErrOrStderr(), "warning: failed to list ClusterOperators: %v\n", err)
					}
				}
			}

			if totalIssues == 0 {
				fmt.Fprintln(out, "No issues found.")
			} else {
				fmt.Fprintf(out, "Total issues: %d\n", totalIssues)
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace to check")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", true, "Check all namespaces")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func isJobComplete(job *batchv1.Job) bool {
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
