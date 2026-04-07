package cli

import (
	"context"
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newExposeCommand() *cobra.Command {
	var namespace string
	var port int32
	var targetPort string
	var svcType string
	var name string

	cmd := &cobra.Command{
		Use:   "expose <resource-type> <resource-name>",
		Short: "Expose a resource as a new Kubernetes service",
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return completeResourceTypes(cmd, toComplete)
			}
			if len(args) == 1 {
				return completeResourceNames(cmd, args[0], toComplete, namespace, false)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			resourceType := args[0]
			resourceName := args[1]
			ns := resolveNamespace(ctx, namespace, false)

			if port == 0 {
				return fmt.Errorf("--port is required")
			}

			svcName := name
			if svcName == "" {
				svcName = resourceName
			}

			// Determine target port
			var tp intstr.IntOrString
			if targetPort != "" {
				if p, err := strconv.Atoi(targetPort); err == nil {
					tp = intstr.FromInt32(int32(p))
				} else {
					tp = intstr.FromString(targetPort)
				}
			} else {
				tp = intstr.FromInt32(port)
			}

			// Get labels from the source resource to use as selector
			resolver, err := getResourceResolver(ctx)
			if err != nil {
				return err
			}
			gvr, _, err := resolver.resolveResource(resourceType)
			if err != nil {
				return fmt.Errorf("resource type %q not found: %w", resourceType, err)
			}

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return err
			}

			obj, err := dynamicClient.Resource(gvr).Namespace(ns).Get(ctx, resourceName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get %s/%s: %w", resourceType, resourceName, err)
			}

			selector := obj.GetLabels()
			if len(selector) == 0 {
				selector = map[string]string{"app": resourceName}
			}

			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      svcName,
					Namespace: ns,
				},
				Spec: corev1.ServiceSpec{
					Selector: selector,
					Type:     corev1.ServiceType(svcType),
					Ports: []corev1.ServicePort{
						{
							Port:       port,
							TargetPort: tp,
							Protocol:   corev1.ProtocolTCP,
						},
					},
				},
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return err
			}

			created, err := clientset.CoreV1().Services(ns).Create(ctx, svc, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("failed to create service: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "service/%s exposed\n", created.Name)
			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	cmd.Flags().Int32Var(&port, "port", 0, "The port that the service should serve on")
	cmd.Flags().StringVar(&targetPort, "target-port", "", "The container port to expose")
	cmd.Flags().StringVar(&svcType, "type", "ClusterIP", "Type of service (ClusterIP, NodePort, LoadBalancer)")
	cmd.Flags().StringVar(&name, "name", "", "The name of the service to create")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	_ = cmd.RegisterFlagCompletionFunc("type", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		types := []string{"ClusterIP", "NodePort", "LoadBalancer"}
		return filterCompletions(types, toComplete), cobra.ShellCompDirectiveNoFileComp
	})

	return cmd
}
