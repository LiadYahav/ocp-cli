package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newRunCommand() *cobra.Command {
	var namespace string
	var image string
	var envVars []string
	var rm bool
	var command []string

	cmd := &cobra.Command{
		Use:   "run <name> --image=<image> [-- <command> [args...]]",
		Short: "Create and run a pod with the specified image",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			if image == "" {
				return fmt.Errorf("--image is required")
			}

			name := args[0]
			ns := resolveNamespace(ctx, namespace, false)

			// Parse command after --
			dashIdx := cmd.ArgsLenAtDash()
			if dashIdx >= 0 && dashIdx < len(args) {
				command = args[dashIdx:]
			}

			// Build env vars
			var envList []corev1.EnvVar
			for _, e := range envVars {
				parts := strings.SplitN(e, "=", 2)
				if len(parts) == 2 {
					envList = append(envList, corev1.EnvVar{Name: parts[0], Value: parts[1]})
				}
			}

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: ns,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    name,
							Image:   image,
							Env:     envList,
							Command: command,
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			created, err := clientset.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("failed to create pod: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "pod/%s created\n", created.Name)

			if rm {
				fmt.Fprintf(cmd.OutOrStdout(), "(Note: --rm is set, but automatic cleanup requires watching the pod. Use 'ocp delete pod %s' when done.)\n", name)
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	cmd.Flags().StringVar(&image, "image", "", "The image to run")
	cmd.Flags().StringSliceVar(&envVars, "env", nil, "Environment variables (KEY=VALUE)")
	cmd.Flags().BoolVar(&rm, "rm", false, "Delete the pod after it exits")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}
