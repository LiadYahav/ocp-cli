package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newAutoscaleCommand() *cobra.Command {
	var namespace string
	var min int32
	var max int32
	var cpuPercent int32

	cmd := &cobra.Command{
		Use:   "autoscale <resource-type>/<name>",
		Short: "Create a Horizontal Pod Autoscaler",
		Long: `Create an HPA targeting a deployment, statefulset, or other scalable resource.

Examples:
  # Autoscale a deployment between 2 and 5 replicas at 80% CPU
  ocp autoscale deployment/myapp --min=2 --max=5

  # Autoscale with custom CPU target
  ocp autoscale deploy/myapp --max=10 --cpu-percent=50`,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return completeResourceTypes(cmd, toComplete)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			if !cmd.Flags().Changed("max") {
				return fmt.Errorf("--max is required")
			}

			ns := resolveNamespace(ctx, namespace, false)
			out := cmd.OutOrStdout()

			// Parse resource type/name
			target := args[0]
			parts := strings.SplitN(target, "/", 2)
			if len(parts) != 2 {
				return fmt.Errorf("expected TYPE/NAME format (e.g. deployment/myapp)")
			}

			resourceType := parts[0]
			resourceName := parts[1]

			// Determine API group for the target
			apiVersion, kind := resolveScaleTarget(resourceType)

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			hpa := &autoscalingv2.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: ns,
				},
				Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
						APIVersion: apiVersion,
						Kind:       kind,
						Name:       resourceName,
					},
					MinReplicas: &min,
					MaxReplicas: max,
					Metrics: []autoscalingv2.MetricSpec{
						{
							Type: autoscalingv2.ResourceMetricSourceType,
							Resource: &autoscalingv2.ResourceMetricSource{
								Name: "cpu",
								Target: autoscalingv2.MetricTarget{
									Type:               autoscalingv2.UtilizationMetricType,
									AverageUtilization: &cpuPercent,
								},
							},
						},
					},
				},
			}

			_, err = clientset.AutoscalingV2().HorizontalPodAutoscalers(ns).Create(ctx, hpa, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("failed to create HPA: %w", err)
			}

			fmt.Fprintf(out, "horizontalpodautoscaler/%s autoscaled (min=%d, max=%d, cpu=%d%%)\n",
				resourceName, min, max, cpuPercent)

			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	cmd.Flags().Int32Var(&min, "min", 1, "Minimum number of replicas")
	cmd.Flags().Int32Var(&max, "max", 0, "Maximum number of replicas (required)")
	cmd.Flags().Int32Var(&cpuPercent, "cpu-percent", 80, "Target CPU utilization percentage")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func resolveScaleTarget(resourceType string) (apiVersion, kind string) {
	switch strings.ToLower(resourceType) {
	case "deployment", "deploy", "deployments":
		return "apps/v1", "Deployment"
	case "statefulset", "sts", "statefulsets":
		return "apps/v1", "StatefulSet"
	case "replicaset", "rs", "replicasets":
		return "apps/v1", "ReplicaSet"
	case "replicationcontroller", "rc":
		return "v1", "ReplicationController"
	default:
		return "apps/v1", "Deployment"
	}
}
