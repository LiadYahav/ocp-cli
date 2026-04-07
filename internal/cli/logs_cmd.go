package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newLogsCommand() *cobra.Command {
	var namespace string
	var container string
	var follow bool
	var previous bool
	var tailLines int
	var since time.Duration

	cmd := &cobra.Command{
		Use:   "logs <pod-name>",
		Short: "Print the logs for a container in a pod",
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return completePodNames(cmd, toComplete, namespace)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			podName := args[0]
			ns := resolveNamespace(ctx, namespace, false)

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			return getPodLogs(ctx, clientset, podName, ns, container, follow, previous, tailLines, since, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().StringVarP(&container, "container", "c", "", "Print logs from this container")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Follow log output")
	cmd.Flags().BoolVar(&previous, "previous", false, "Print logs for the previous instance of the container")
	cmd.Flags().IntVar(&tailLines, "tail", -1, "Lines of recent log file to display")
	cmd.Flags().DurationVar(&since, "since", 0, "Only return logs newer than a relative duration like 5s, 2m, or 3h")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func getPodLogs(ctx context.Context, clientset *kubernetes.Clientset, podName string, namespace string, container string, follow bool, previous bool, tailLines int, since time.Duration, out io.Writer) error {
	opts := &corev1.PodLogOptions{
		Follow:    follow,
		Previous:  previous,
		Container: container,
	}

	if tailLines > 0 {
		tail := int64(tailLines)
		opts.TailLines = &tail
	}

	if since > 0 {
		sinceTime := metav1.NewTime(time.Now().Add(-since))
		opts.SinceTime = &sinceTime
	}

	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		return fmt.Errorf("failed to get logs: %w", err)
	}
	defer stream.Close()

	_, err = io.Copy(out, stream)
	return err
}
