package cli

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newPortForwardCommand() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "port-forward <pod-name> <local-port>:<remote-port> [...]",
		Short: "Forward one or more local ports to a pod",
		Args:  cobra.MinimumNArgs(2),
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return completePodNames(cmd, toComplete, namespace)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			podName := args[0]
			ports := args[1:]
			ns := resolveNamespace(ctx, namespace, false)

			config, err := kube.GetConfig(ctx)
			if err != nil {
				return fmt.Errorf("failed to get config: %w", err)
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			req := clientset.CoreV1().RESTClient().Post().
				Resource("pods").
				Name(podName).
				Namespace(ns).
				SubResource("portforward")

			transport, upgrader, err := spdy.RoundTripperFor(config)
			if err != nil {
				return fmt.Errorf("failed to create round tripper: %w", err)
			}

			dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", req.URL())

			stopChan := make(chan struct{}, 1)
			readyChan := make(chan struct{})

			signals := make(chan os.Signal, 1)
			signal.Notify(signals, os.Interrupt)
			go func() {
				<-signals
				close(stopChan)
			}()

			fw, err := portforward.New(dialer, ports, stopChan, readyChan, os.Stdout, os.Stderr)
			if err != nil {
				return fmt.Errorf("failed to create port forwarder: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Forwarding from %s ...\n", strings.Join(ports, ", "))
			return fw.ForwardPorts()
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}
