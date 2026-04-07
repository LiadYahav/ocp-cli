package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newStatusCommand() *cobra.Command {
	var namespace string
	var allNamespaces bool

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show a high-level overview of the current project",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			ns := resolveNamespace(ctx, namespace, allNamespaces)
			if ns == "" {
				ns = "default"
			}
			out := cmd.OutOrStdout()

			config, err := kube.GetConfig(ctx)
			if err != nil {
				return fmt.Errorf("failed to get config: %w", err)
			}

			fmt.Fprintf(out, "In project %q on server %q\n\n", ns, config.Host)

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			// Deployments
			deployments, err := clientset.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{})
			if err == nil && len(deployments.Items) > 0 {
				for _, dep := range deployments.Items {
					replicas := int32(1)
					if dep.Spec.Replicas != nil {
						replicas = *dep.Spec.Replicas
					}
					fmt.Fprintf(out, "deployment/%s deploys %s\n",
						dep.Name, getDeploymentImages(dep.Spec.Template.Spec.Containers))
					fmt.Fprintf(out, "  %d/%d replicas ready, %d available\n",
						dep.Status.ReadyReplicas, replicas, dep.Status.AvailableReplicas)
				}
				fmt.Fprintln(out)
			}

			// StatefulSets
			statefulsets, err := clientset.AppsV1().StatefulSets(ns).List(ctx, metav1.ListOptions{})
			if err == nil && len(statefulsets.Items) > 0 {
				for _, sts := range statefulsets.Items {
					replicas := int32(1)
					if sts.Spec.Replicas != nil {
						replicas = *sts.Spec.Replicas
					}
					fmt.Fprintf(out, "statefulset/%s deploys %s\n",
						sts.Name, getDeploymentImages(sts.Spec.Template.Spec.Containers))
					fmt.Fprintf(out, "  %d/%d replicas ready\n",
						sts.Status.ReadyReplicas, replicas)
				}
				fmt.Fprintln(out)
			}

			// DaemonSets
			daemonsets, err := clientset.AppsV1().DaemonSets(ns).List(ctx, metav1.ListOptions{})
			if err == nil && len(daemonsets.Items) > 0 {
				for _, ds := range daemonsets.Items {
					fmt.Fprintf(out, "daemonset/%s deploys %s\n",
						ds.Name, getDeploymentImages(ds.Spec.Template.Spec.Containers))
					fmt.Fprintf(out, "  %d desired, %d ready\n",
						ds.Status.DesiredNumberScheduled, ds.Status.NumberReady)
				}
				fmt.Fprintln(out)
			}

			// Services
			services, err := clientset.CoreV1().Services(ns).List(ctx, metav1.ListOptions{})
			if err == nil && len(services.Items) > 0 {
				for _, svc := range services.Items {
					ports := make([]string, 0, len(svc.Spec.Ports))
					for _, p := range svc.Spec.Ports {
						ports = append(ports, fmt.Sprintf("%d/%s", p.Port, p.Protocol))
					}
					fmt.Fprintf(out, "svc/%s - %s %s\n",
						svc.Name, svc.Spec.Type, strings.Join(ports, ", "))
				}
				fmt.Fprintln(out)
			}

			// Routes (graceful skip)
			routeGVR := schema.GroupVersionResource{
				Group:    "route.openshift.io",
				Version:  "v1",
				Resource: "routes",
			}
			routes, err := dynamicClient.Resource(routeGVR).Namespace(ns).List(ctx, metav1.ListOptions{})
			if err == nil && len(routes.Items) > 0 {
				for _, route := range routes.Items {
					host, _, _ := unstructured.NestedString(route.Object, "spec", "host")
					fmt.Fprintf(out, "route/%s -> %s\n", route.GetName(), host)
				}
				fmt.Fprintln(out)
			}

			// Pods summary
			pods, err := clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
			if err == nil {
				running, pending, failed, succeeded := 0, 0, 0, 0
				for _, pod := range pods.Items {
					switch pod.Status.Phase {
					case corev1.PodRunning:
						running++
					case corev1.PodPending:
						pending++
					case corev1.PodFailed:
						failed++
					case corev1.PodSucceeded:
						succeeded++
					}
				}
				fmt.Fprintf(out, "%d pods: %d running, %d pending, %d succeeded, %d failed\n",
					len(pods.Items), running, pending, succeeded, failed)
			}

			// Jobs
			jobs, err := clientset.BatchV1().Jobs(ns).List(ctx, metav1.ListOptions{})
			if err == nil && len(jobs.Items) > 0 {
				active, complete, jobFailed := 0, 0, 0
				for _, job := range jobs.Items {
					active += int(job.Status.Active)
					complete += int(job.Status.Succeeded)
					jobFailed += int(job.Status.Failed)
				}
				fmt.Fprintf(out, "%d jobs: %d active, %d complete, %d failed\n",
					len(jobs.Items), active, complete, jobFailed)
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "All namespaces")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func getDeploymentImages(containers []corev1.Container) string {
	images := make([]string, 0, len(containers))
	for _, c := range containers {
		images = append(images, c.Image)
	}
	return strings.Join(images, ", ")
}
