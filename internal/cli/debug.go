package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newDebugCommand() *cobra.Command {
	var namespace string
	var container string
	var image string
	var asRoot bool
	var preservePod bool

	cmd := &cobra.Command{
		Use:   "debug <pod-name>",
		Short: "Launch a debug pod based on a resource template",
		Long: `Create a debug copy of a pod with modifications for debugging.

Probes are stripped, the command is overridden to a shell, and labels
are removed so the pod doesn't receive traffic.

Examples:
  # Debug a running pod
  ocp debug mypod

  # Debug with a specific image
  ocp debug mypod --image=busybox

  # Debug as root
  ocp debug mypod --as-root`,
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

			target := args[0]
			ns := resolveNamespace(ctx, namespace, false)
			out := cmd.OutOrStdout()

			// Resolve to pod name
			podName, err := resolvePodName(ctx, ns, target)
			if err != nil {
				return err
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			// Get the source pod
			sourcePod, err := clientset.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get pod %q: %w", podName, err)
			}

			// Build debug pod
			debugPod := buildDebugPod(sourcePod, container, image, asRoot)

			// Create the debug pod
			created, err := clientset.CoreV1().Pods(ns).Create(ctx, debugPod, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("failed to create debug pod: %w", err)
			}

			debugPodName := created.Name
			fmt.Fprintf(out, "Starting pod/%s ...\n", debugPodName)

			// Cleanup on exit (unless --preserve-pod)
			if !preservePod {
				defer func() {
					fmt.Fprintf(out, "\nRemoving debug pod/%s ...\n", debugPodName)
					clientset.CoreV1().Pods(ns).Delete(context.Background(), debugPodName, metav1.DeleteOptions{})
				}()
			}

			// Handle Ctrl+C
			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, os.Interrupt)
			go func() {
				<-sigChan
				if !preservePod {
					clientset.CoreV1().Pods(ns).Delete(context.Background(), debugPodName, metav1.DeleteOptions{})
				}
				os.Exit(0)
			}()

			// Wait for pod to be running
			err = wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
				pod, err := clientset.CoreV1().Pods(ns).Get(ctx, debugPodName, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				return pod.Status.Phase == corev1.PodRunning, nil
			})
			if err != nil {
				return fmt.Errorf("timed out waiting for debug pod to start: %w", err)
			}

			// Exec into debug pod
			targetContainer := ""
			if container != "" {
				targetContainer = container
			}

			return execInPod(ctx, ns, debugPodName, targetContainer, []string{"/bin/sh"}, true, true)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	cmd.Flags().StringVarP(&container, "container", "c", "", "Container name to debug")
	cmd.Flags().StringVar(&image, "image", "", "Override container image (default: use source image)")
	cmd.Flags().BoolVar(&asRoot, "as-root", false, "Run as root user")
	cmd.Flags().BoolVar(&preservePod, "preserve-pod", false, "Do not delete debug pod on exit")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	_ = cmd.RegisterFlagCompletionFunc("container", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) > 0 {
			return completeContainerNames(cmd, namespace, args[0], toComplete)
		}
		return nil, cobra.ShellCompDirectiveNoFileComp
	})

	return cmd
}

func buildDebugPod(source *corev1.Pod, targetContainer, overrideImage string, asRoot bool) *corev1.Pod {
	debugPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-debug-", source.Name),
			Namespace:    source.Namespace,
			Annotations: map[string]string{
				"debug.ocp-cli/source-pod": source.Name,
			},
		},
		Spec: *source.Spec.DeepCopy(),
	}

	// Clear labels so the pod doesn't receive traffic
	debugPod.Labels = nil

	// Clear node name to allow scheduling
	debugPod.Spec.NodeName = ""

	// Modify containers
	for i := range debugPod.Spec.Containers {
		c := &debugPod.Spec.Containers[i]

		// Skip containers that don't match target
		if targetContainer != "" && c.Name != targetContainer {
			continue
		}

		// Override image if specified
		if overrideImage != "" {
			c.Image = overrideImage
		}

		// Override command to shell
		c.Command = []string{"/bin/sh"}
		c.Args = nil

		// Strip probes
		c.LivenessProbe = nil
		c.ReadinessProbe = nil
		c.StartupProbe = nil

		// Enable stdin/tty
		c.Stdin = true
		c.TTY = true

		// Run as root if requested
		if asRoot {
			var zero int64
			if c.SecurityContext == nil {
				c.SecurityContext = &corev1.SecurityContext{}
			}
			c.SecurityContext.RunAsUser = &zero
			c.SecurityContext.RunAsNonRoot = nil
		}
	}

	// Clear init containers
	debugPod.Spec.InitContainers = nil

	// Set restart policy to Never
	debugPod.Spec.RestartPolicy = corev1.RestartPolicyNever

	// Clear service account token auto-mount if not needed
	debugPod.Spec.AutomountServiceAccountToken = nil

	return debugPod
}
