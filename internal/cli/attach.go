package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	"golang.org/x/term"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newAttachCommand() *cobra.Command {
	var namespace string
	var container string
	var stdinFlag bool
	var ttyFlag bool

	cmd := &cobra.Command{
		Use:   "attach <pod-name>",
		Short: "Attach to a running container",
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
				Namespace(ns).
				SubResource("attach").
				VersionedParams(&corev1.PodAttachOptions{
					Container: container,
					Stdin:     stdinFlag,
					Stdout:    true,
					Stderr:    true,
					TTY:       ttyFlag,
				}, scheme.ParameterCodec)

			executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
			if err != nil {
				return fmt.Errorf("failed to create executor: %w", err)
			}

			if ttyFlag && stdinFlag {
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
			if stdinFlag {
				streamOpts.Stdin = os.Stdin
			}
			if ttyFlag {
				streamOpts.Tty = true
			}

			return executor.StreamWithContext(ctx, streamOpts)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	cmd.Flags().StringVarP(&container, "container", "c", "", "Container name")
	cmd.Flags().BoolVarP(&stdinFlag, "stdin", "i", false, "Pass stdin to the container")
	cmd.Flags().BoolVarP(&ttyFlag, "tty", "t", false, "Stdin is a TTY")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	_ = cmd.RegisterFlagCompletionFunc("container", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) > 0 {
			return completeContainerNames(cmd, namespace, args[0], toComplete)
		}
		return nil, cobra.ShellCompDirectiveNoFileComp
	})

	return cmd
}
