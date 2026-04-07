// Portions of this file are based on code from Kubernetes kubectl
// under the Apache License 2.0.
// Source: https://github.com/kubernetes/kubectl
// See LICENSE and NOTICE files in the project root for full license information.

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

// staticResourceTypes is a hardcoded list of common resource types (with short names)
// used as a fallback when dynamic discovery fails or is unavailable.
var staticResourceTypes = []string{
	// Core resources
	"pods", "pod", "po",
	"services", "service", "svc",
	"nodes", "node", "no",
	"namespaces", "namespace", "ns",
	"configmaps", "configmap", "cm",
	"secrets", "secret",
	"serviceaccounts", "serviceaccount", "sa",
	"persistentvolumes", "persistentvolume", "pv",
	"persistentvolumeclaims", "persistentvolumeclaim", "pvc",
	"endpoints", "endpoint", "ep",
	"events", "event", "ev",
	"resourcequotas", "resourcequota",
	"limitranges", "limitrange",
	// Apps
	"deployments", "deployment", "deploy",
	"replicasets", "replicaset", "rs",
	"statefulsets", "statefulset", "sts",
	"daemonsets", "daemonset", "ds",
	"replicationcontrollers", "replicationcontroller", "rc",
	// Batch
	"jobs", "job",
	"cronjobs", "cronjob", "cj",
	// Networking
	"ingresses", "ingress", "ing",
	"networkpolicies", "networkpolicy", "np",
	// RBAC
	"roles", "role",
	"rolebindings", "rolebinding",
	"clusterroles", "clusterrole",
	"clusterrolebindings", "clusterrolebinding",
	// Storage
	"storageclasses", "storageclass", "sc",
	// Policy
	"poddisruptionbudgets", "poddisruptionbudget", "pdb",
	// Autoscaling
	"horizontalpodautoscalers", "horizontalpodautoscaler", "hpa",
	// OpenShift
	"routes", "route",
	"buildconfigs", "buildconfig", "bc",
	"builds", "build",
	"deploymentconfigs", "deploymentconfig", "dc",
	"imagestreams", "imagestream", "is",
	"projects", "project",
	"machineconfigpools", "machineconfigpool", "mcp",
	"clusteroperators", "clusteroperator", "co",
	"clusterversions", "clusterversion", "cv",
	"securitycontextconstraints", "securitycontextconstraint", "scc",
	// CRDs
	"customresourcedefinitions", "customresourcedefinition", "crd",
}

// completeResourceTypes returns discovered resource types for completion
func completeResourceTypes(cmd *cobra.Command, toComplete string) ([]string, cobra.ShellCompDirective) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	// Try dynamic discovery first
	resolver, err := getResourceResolver(ctx)
	if err == nil {
		resources, discoverErr := resolver.discoverResources()
		if discoverErr == nil && len(resources) > 0 {
			return filterCompletions(resources, toComplete), cobra.ShellCompDirectiveNoFileComp
		}
	}

	// Fall back to static list
	return filterCompletions(staticResourceTypes, toComplete), cobra.ShellCompDirectiveNoFileComp
}

// completeResourceNames returns resource names matching the prefix for completion
func completeResourceNames(cmd *cobra.Command, resourceType string, toComplete string, namespace string, allNamespaces bool) ([]string, cobra.ShellCompDirective) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	ns := resolveNamespace(ctx, namespace, allNamespaces)

	names, err := listResourceNames(ctx, resourceType, ns, allNamespaces)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	return filterCompletions(names, toComplete), cobra.ShellCompDirectiveNoFileComp
}

// completePodNames returns pod names matching the prefix for completion
func completePodNames(cmd *cobra.Command, toComplete string, namespace string) ([]string, cobra.ShellCompDirective) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	ns := resolveNamespace(ctx, namespace, false)

	pods, err := clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	names := make([]string, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if strings.HasPrefix(pod.Name, toComplete) {
			names = append(names, pod.Name)
		}
	}

	return names, cobra.ShellCompDirectiveNoFileComp
}

// getPodRestarts returns the total restart count for a pod as a string.
func getPodRestarts(pod *corev1.Pod) string {
	totalRestarts := int32(0)
	if pod != nil && pod.Status.ContainerStatuses != nil {
		for _, containerStatus := range pod.Status.ContainerStatuses {
			totalRestarts += containerStatus.RestartCount
		}
	}
	return fmt.Sprintf("%d", totalRestarts)
}

// toInterfaceSlice converts a string slice to an interface slice (for tabwriter formatting).
func toInterfaceSlice(strs []string) []interface{} {
	result := make([]interface{}, len(strs))
	for i, s := range strs {
		result[i] = s
	}
	return result
}
