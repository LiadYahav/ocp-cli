package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newMCPCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mcp <mcp-name> <resume|stop>",
		Short: "Manage Machine Config Pools",
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			// Complete MCP names for the first argument
			if len(args) == 0 {
				return completeMCPNames(cmd, args, toComplete)
			}
			// Complete actions for the second argument
			if len(args) == 1 {
				return completeMCPActions(cmd, args, toComplete)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Long: `Manage Machine Config Pools (MCP) in OpenShift.
Use 'resume' to unpause an MCP (sets paused: false) or 'stop' to pause an MCP (sets paused: true).

Examples:
  ocp mcp worker resume
  ocp mcp master stop`,
		Args: cobra.ExactArgs(2),
		Example: `  # Resume (unpause) the worker Machine Config Pool
  ocp mcp worker resume

  # Stop (pause) the master Machine Config Pool
  ocp mcp master stop`,
		RunE: func(cmd *cobra.Command, args []string) error {
			mcpName := args[0]
			action := args[1]

			var paused bool
			switch action {
			case "resume":
				paused = false
			case "stop":
				paused = true
			default:
				return fmt.Errorf("invalid action %q: must be 'resume' or 'stop'", action)
			}

			return setMCPPaused(cmd.Context(), mcpName, paused, cmd.OutOrStdout())
		},
	}
	return cmd
}

func setMCPPaused(ctx context.Context, mcpName string, paused bool, out io.Writer) error {
	if ctx == nil {
		ctx = context.Background()
	}

	dynamicClient, err := kube.NewDynamicClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	gvr := schema.GroupVersionResource{
		Group:    "machineconfiguration.openshift.io",
		Version:  "v1",
		Resource: "machineconfigpools",
	}

	// Verify the MCP exists
	_, err = dynamicClient.Resource(gvr).Get(ctx, mcpName, metav1.GetOptions{})
	if meta.IsNoMatchError(err) {
		return fmt.Errorf("machine config pool api not available - ensure you're connected to an OpenShift cluster")
	}
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("machine config pool %q not found", mcpName)
	}
	if err != nil {
		return fmt.Errorf("failed to get machine config pool %q: %w", mcpName, err)
	}

	// Build the patch
	patch := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"spec": map[string]interface{}{
				"paused": paused,
			},
		},
	}

	patchBytes, err := patch.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal patch: %w", err)
	}

	// Apply the patch
	_, err = dynamicClient.Resource(gvr).Patch(ctx, mcpName, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch machine config pool %q: %w", mcpName, err)
	}

	action := "resumed"
	if paused {
		action = "stopped (paused)"
	}

	fmt.Fprintf(out, "Successfully %s Machine Config Pool %q\n", action, mcpName)
	return nil
}
