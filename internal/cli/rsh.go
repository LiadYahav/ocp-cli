package cli

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	"golang.org/x/term"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newRshCommand() *cobra.Command {
	var namespace string
	var container string
	var shell string

	cmd := &cobra.Command{
		Use:   "rsh <pod-name>",
		Short: "Start a shell session in a pod",
		Long: `Start a remote shell session in a pod's container.

Supports TYPE/NAME syntax to resolve pods from higher-level resources:
  ocp rsh deployment/myapp
  ocp rsh dc/myapp`,
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

			ns := resolveNamespace(ctx, namespace, false)
			target := args[0]

			podName, err := resolvePodName(ctx, ns, target)
			if err != nil {
				return err
			}

			return execInPod(ctx, ns, podName, container, []string{shell}, true, true)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	cmd.Flags().StringVarP(&container, "container", "c", "", "Container name")
	cmd.Flags().StringVar(&shell, "shell", "/bin/sh", "Shell to use")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	_ = cmd.RegisterFlagCompletionFunc("container", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) > 0 {
			return completeContainerNames(cmd, namespace, args[0], toComplete)
		}
		return nil, cobra.ShellCompDirectiveNoFileComp
	})

	return cmd
}

// execInPod executes a command in a pod container with optional stdin/tty support.
// This is shared by rsh, cp, attach, and debug commands.
func execInPod(ctx context.Context, namespace, podName, container string, command []string, stdin, tty bool) error {
	config, err := kube.GetConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to get config: %w", err)
	}

	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdin:     stdin,
			Stdout:    true,
			Stderr:    true,
			TTY:       tty,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("failed to create executor: %w", err)
	}

	if tty && stdin {
		oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err != nil {
			return fmt.Errorf("failed to set terminal raw mode: %w", err)
		}
		defer term.Restore(int(os.Stdin.Fd()), oldState)
	}

	streamOpts := remotecommand.StreamOptions{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
	if stdin {
		streamOpts.Stdin = os.Stdin
	}
	if tty {
		streamOpts.Tty = true
	}

	return executor.StreamWithContext(ctx, streamOpts)
}

// execInPodWithStreams executes a command in a pod with custom stream options.
func execInPodWithStreams(ctx context.Context, namespace, podName, container string, command []string, streamOpts remotecommand.StreamOptions) error {
	config, err := kube.GetConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to get config: %w", err)
	}

	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdin:     streamOpts.Stdin != nil,
			Stdout:    streamOpts.Stdout != nil,
			Stderr:    streamOpts.Stderr != nil,
			TTY:       streamOpts.Tty,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("failed to create executor: %w", err)
	}

	return executor.StreamWithContext(ctx, streamOpts)
}

// resolvePodName resolves TYPE/NAME to a pod name.
// For deployments, statefulsets, etc., it finds a running pod.
func resolvePodName(ctx context.Context, namespace, target string) (string, error) {
	// If no slash, it's a direct pod name
	if !strings.Contains(target, "/") {
		return target, nil
	}

	parts := strings.SplitN(target, "/", 2)
	resourceType := strings.ToLower(parts[0])
	resourceName := parts[1]

	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to create client: %w", err)
	}

	// Build label selector based on resource type
	var labelSelector string
	switch resourceType {
	case "deployment", "deploy":
		dep, err := clientset.AppsV1().Deployments(namespace).Get(ctx, resourceName, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("deployment %q not found: %w", resourceName, err)
		}
		selectors := make([]string, 0)
		for k, v := range dep.Spec.Selector.MatchLabels {
			selectors = append(selectors, fmt.Sprintf("%s=%s", k, v))
		}
		labelSelector = strings.Join(selectors, ",")

	case "statefulset", "sts":
		sts, err := clientset.AppsV1().StatefulSets(namespace).Get(ctx, resourceName, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("statefulset %q not found: %w", resourceName, err)
		}
		selectors := make([]string, 0)
		for k, v := range sts.Spec.Selector.MatchLabels {
			selectors = append(selectors, fmt.Sprintf("%s=%s", k, v))
		}
		labelSelector = strings.Join(selectors, ",")

	case "daemonset", "ds":
		ds, err := clientset.AppsV1().DaemonSets(namespace).Get(ctx, resourceName, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("daemonset %q not found: %w", resourceName, err)
		}
		selectors := make([]string, 0)
		for k, v := range ds.Spec.Selector.MatchLabels {
			selectors = append(selectors, fmt.Sprintf("%s=%s", k, v))
		}
		labelSelector = strings.Join(selectors, ",")

	case "dc", "deploymentconfig":
		labelSelector = "deploymentconfig=" + resourceName

	case "pod", "po":
		return resourceName, nil

	default:
		return "", fmt.Errorf("unsupported resource type %q for rsh", resourceType)
	}

	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return "", fmt.Errorf("failed to list pods: %w", err)
	}

	// Find first running pod
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodRunning {
			return pod.Name, nil
		}
	}

	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no pods found for %s", target)
	}

	return "", fmt.Errorf("no running pods found for %s", target)
}
