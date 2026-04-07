package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newStartBuildCommand() *cobra.Command {
	var namespace string
	var follow bool
	var waitFlag bool

	cmd := &cobra.Command{
		Use:   "start-build <buildconfig-name>",
		Short: "Start a new build from a BuildConfig",
		Long: `Trigger a new build from an OpenShift BuildConfig.

This command requires the build.openshift.io API to be available.

Examples:
  # Start a build
  ocp start-build myapp

  # Start and follow logs
  ocp start-build myapp --follow

  # Start and wait for completion
  ocp start-build myapp --wait`,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return completeBuildConfigNames(cmd, toComplete, namespace)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			bcName := args[0]
			ns := resolveNamespace(ctx, namespace, false)
			out := cmd.OutOrStdout()

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			// Instantiate build from BuildConfig
			buildRequest := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "build.openshift.io/v1",
					"kind":       "BuildRequest",
					"metadata": map[string]interface{}{
						"name": bcName,
					},
				},
			}

			bcGVR := schema.GroupVersionResource{
				Group:    "build.openshift.io",
				Version:  "v1",
				Resource: "buildconfigs",
			}

			// Use the instantiate subresource
			result, err := dynamicClient.Resource(bcGVR).Namespace(ns).Create(ctx, buildRequest, metav1.CreateOptions{}, "instantiate")
			if meta.IsNoMatchError(err) {
				return fmt.Errorf("build API not available on this cluster (requires OpenShift)")
			}
			if err != nil {
				return fmt.Errorf("failed to start build: %w", err)
			}

			buildName, _, _ := unstructured.NestedString(result.Object, "metadata", "name")
			fmt.Fprintf(out, "build.build.openshift.io/%s started\n", buildName)

			if !follow && !waitFlag {
				return nil
			}

			buildGVR := schema.GroupVersionResource{
				Group:    "build.openshift.io",
				Version:  "v1",
				Resource: "builds",
			}

			if waitFlag || follow {
				// Wait for build pod to appear and complete
				fmt.Fprintf(out, "Waiting for build %s to complete...\n", buildName)

				err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 30*time.Minute, true, func(ctx context.Context) (bool, error) {
					build, err := dynamicClient.Resource(buildGVR).Namespace(ns).Get(ctx, buildName, metav1.GetOptions{})
					if err != nil {
						return false, nil
					}

					phase, _, _ := unstructured.NestedString(build.Object, "status", "phase")
					switch phase {
					case "Complete":
						fmt.Fprintf(out, "Build %s completed successfully\n", buildName)
						return true, nil
					case "Failed", "Error", "Cancelled":
						return true, fmt.Errorf("build %s %s", buildName, phase)
					default:
						return false, nil
					}
				})

				if err != nil {
					return err
				}
			}

			if follow {
				// Follow build logs by tailing the build pod
				podName := buildName + "-build"
				clientset, err := kube.NewClientset(ctx)
				if err != nil {
					return nil
				}

				req := clientset.CoreV1().Pods(ns).GetLogs(podName, &corev1.PodLogOptions{
					Follow: true,
				})

				stream, err := req.Stream(ctx)
				if err != nil {
					return nil // Best-effort log following
				}
				defer stream.Close()
				io.Copy(os.Stdout, stream)
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	cmd.Flags().BoolVar(&follow, "follow", false, "Follow build logs")
	cmd.Flags().BoolVar(&waitFlag, "wait", false, "Wait for build to complete")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

// completeBuildConfigNames returns buildconfig names for completion
func completeBuildConfigNames(cmd *cobra.Command, toComplete string, namespace string) ([]string, cobra.ShellCompDirective) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	dynamicClient, err := kube.NewDynamicClient(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	ns := resolveNamespace(ctx, namespace, false)

	gvr := schema.GroupVersionResource{
		Group:    "build.openshift.io",
		Version:  "v1",
		Resource: "buildconfigs",
	}

	list, err := dynamicClient.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
	if meta.IsNoMatchError(err) {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	names := make([]string, 0, len(list.Items))
	for _, item := range list.Items {
		name := item.GetName()
		if len(toComplete) == 0 || len(name) >= len(toComplete) && name[:len(toComplete)] == toComplete {
			names = append(names, name)
		}
	}

	return names, cobra.ShellCompDirectiveNoFileComp
}
