package cli

import (
	"fmt"
	"sync"
	"text/tabwriter"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	unstructuredhelpers "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newUpgradeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade management commands",
	}

	cmd.AddCommand(newUpgradeStatusCommand())
	return cmd
}

func newUpgradeStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show OpenShift upgrade progress",
		Long: `Displays the current upgrade status including:
- Current and target cluster version
- ClusterOperator status (progressing/degraded/available)
- MachineConfigPool update progress
- Nodes not yet at the target kubelet version`,
		Example: `  # Show upgrade status
  ocp upgrade status`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := ensureContext(cmd.Context())
			out := cmd.OutOrStdout()

			distro, err := detectClusterDistribution(ctx)
			if err != nil {
				return fmt.Errorf("failed to detect cluster distribution: %w", err)
			}
			if distro != "openshift" {
				fmt.Fprintln(out, "This command is only available on OpenShift clusters.")
				return nil
			}

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			gvrCV := schema.GroupVersionResource{Group: "config.openshift.io", Version: "v1", Resource: "clusterversions"}
			gvrCO := schema.GroupVersionResource{Group: "config.openshift.io", Version: "v1", Resource: "clusteroperators"}
			gvrMCP := schema.GroupVersionResource{Group: "machineconfiguration.openshift.io", Version: "v1", Resource: "machineconfigpools"}

			var (
				cvList   *unstructured.UnstructuredList
				coList   *unstructured.UnstructuredList
				mcpList  *unstructured.UnstructuredList
				nodes    *corev1.NodeList
				cvErr    error
				coErr    error
				mcpErr   error
				nodesErr error
			)

			var wg sync.WaitGroup
			wg.Add(4)

			go func() {
				defer wg.Done()
				cvList, cvErr = dynamicClient.Resource(gvrCV).List(ctx, metav1.ListOptions{})
			}()
			go func() {
				defer wg.Done()
				coList, coErr = dynamicClient.Resource(gvrCO).List(ctx, metav1.ListOptions{})
			}()
			go func() {
				defer wg.Done()
				mcpList, mcpErr = dynamicClient.Resource(gvrMCP).List(ctx, metav1.ListOptions{})
			}()
			go func() {
				defer wg.Done()
				nodes, nodesErr = clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			}()

			wg.Wait()

			if cvErr != nil {
				return fmt.Errorf("failed to get ClusterVersions: %w", cvErr)
			}
			if coErr != nil {
				return fmt.Errorf("failed to get ClusterOperators: %w", coErr)
			}
			if nodesErr != nil {
				return fmt.Errorf("failed to get nodes: %w", nodesErr)
			}

			// ClusterVersion
			if len(cvList.Items) > 0 {
				cv := cvList.Items[0]
				desiredVersion, _, _ := unstructuredhelpers.NestedString(cv.Object, "status", "desired", "version")

				currentVersion := "<unknown>"
				if history, found, _ := unstructuredhelpers.NestedSlice(cv.Object, "status", "history"); found && len(history) > 0 {
					for _, h := range history {
						if hMap, ok := h.(map[string]interface{}); ok {
							state, _, _ := unstructuredhelpers.NestedString(hMap, "state")
							ver, _, _ := unstructuredhelpers.NestedString(hMap, "version")
							if state == "Completed" {
								currentVersion = ver
								break
							}
							if currentVersion == "<unknown>" {
								currentVersion = ver
							}
						}
					}
				}

				progressStatus := "Unknown"
				conditions, _, _ := unstructuredhelpers.NestedSlice(cv.Object, "status", "conditions")
				for _, cond := range conditions {
					condMap, ok := cond.(map[string]interface{})
					if !ok {
						continue
					}
					condType, _, _ := unstructuredhelpers.NestedString(condMap, "type")
					condStatus, _, _ := unstructuredhelpers.NestedString(condMap, "status")
					msg, _, _ := unstructuredhelpers.NestedString(condMap, "message")
					if condType == "Progressing" {
						if condStatus == "True" {
							progressStatus = "Upgrading"
							if msg != "" {
								progressStatus += ": " + truncate(msg, 60)
							}
						} else {
							progressStatus = "Idle"
						}
					}
				}

				fmt.Fprintf(out, "=== Cluster Version ===\n")
				if currentVersion != desiredVersion {
					fmt.Fprintf(out, "Version: %s -> %s\n", currentVersion, desiredVersion)
				} else {
					fmt.Fprintf(out, "Version: %s\n", currentVersion)
				}
				fmt.Fprintf(out, "Status:  %s\n\n", progressStatus)
			}

			// ClusterOperators summary
			if coList != nil {
				var available, progressing, degraded int
				var degradedOps []string
				for _, item := range coList.Items {
					name, _, _ := unstructuredhelpers.NestedString(item.Object, "metadata", "name")
					conditions, _, _ := unstructuredhelpers.NestedSlice(item.Object, "status", "conditions")
					for _, cond := range conditions {
						condMap, ok := cond.(map[string]interface{})
						if !ok {
							continue
						}
						condType, _, _ := unstructuredhelpers.NestedString(condMap, "type")
						condStatus, _, _ := unstructuredhelpers.NestedString(condMap, "status")
						switch condType {
						case "Available":
							if condStatus == "True" {
								available++
							}
						case "Progressing":
							if condStatus == "True" {
								progressing++
							}
						case "Degraded":
							if condStatus == "True" {
								degraded++
								degradedOps = append(degradedOps, name)
							}
						}
					}
				}

				fmt.Fprintf(out, "=== ClusterOperators (%d total) ===\n", len(coList.Items))
				fmt.Fprintf(out, "Available: %d  Progressing: %d  Degraded: %d\n", available, progressing, degraded)
				if len(degradedOps) > 0 {
					fmt.Fprintf(out, "Degraded operators: %s\n", joinTruncated(degradedOps, 5))
				}
				fmt.Fprintln(out)
			}

			// MachineConfigPools
			if mcpErr == nil && mcpList != nil {
				fmt.Fprintf(out, "=== MachineConfigPools ===\n")
				w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
				fmt.Fprintln(w, "POOL\tUPDATED\tUPDATING\tDEGRADED\tTOTAL")
				for _, item := range mcpList.Items {
					name, _, _ := unstructuredhelpers.NestedString(item.Object, "metadata", "name")
					machineCount, _, _ := unstructuredhelpers.NestedInt64(item.Object, "status", "machineCount")
					updatedCount, _, _ := unstructuredhelpers.NestedInt64(item.Object, "status", "updatedMachineCount")
					degradedCount, _, _ := unstructuredhelpers.NestedInt64(item.Object, "status", "degradedMachineCount")

					conditions, _, _ := unstructuredhelpers.NestedSlice(item.Object, "status", "conditions")
					updating := "False"
					for _, cond := range conditions {
						condMap, ok := cond.(map[string]interface{})
						if !ok {
							continue
						}
						condType, _, _ := unstructuredhelpers.NestedString(condMap, "type")
						condStatus, _, _ := unstructuredhelpers.NestedString(condMap, "status")
						if condType == "Updating" && condStatus == "True" {
							updating = "True"
						}
					}

					fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%d\n", name, updatedCount, updating, degradedCount, machineCount)
				}
				w.Flush()
				fmt.Fprintln(out)
			} else if mcpErr != nil && !meta.IsNoMatchError(mcpErr) {
				fmt.Fprintf(cmd.ErrOrStderr(), "warning: failed to list MachineConfigPools: %v\n", mcpErr)
			}

			// Nodes not at target version
			if len(cvList.Items) > 0 {
				desiredVersion, _, _ := unstructuredhelpers.NestedString(cvList.Items[0].Object, "status", "desired", "version")
				if desiredVersion != "" {
					var outdated []string
					for _, node := range nodes.Items {
						kv := node.Status.NodeInfo.KubeletVersion
						if kv != "" && kv != desiredVersion {
							outdated = append(outdated, fmt.Sprintf("%s (%s)", node.Name, kv))
						}
					}
					if len(outdated) > 0 {
						fmt.Fprintf(out, "=== Nodes not at target kubelet version (%d) ===\n", len(outdated))
						for _, s := range outdated {
							fmt.Fprintf(out, "  %s\n", s)
						}
						fmt.Fprintln(out)
					}
				}
			}

			return nil
		},
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func joinTruncated(items []string, max int) string {
	if len(items) <= max {
		return joinStrings(items)
	}
	return fmt.Sprintf("%s (+%d more)", joinStrings(items[:max]), len(items)-max)
}

func joinStrings(items []string) string {
	result := ""
	for i, item := range items {
		if i > 0 {
			result += ", "
		}
		result += item
	}
	return result
}

