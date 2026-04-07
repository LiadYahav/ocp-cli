package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newProjectsCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "projects",
		Aliases: []string{"get-projects"},
		Short:   "List all projects/namespaces you have access to",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			out := cmd.OutOrStdout()
			currentNS := getCurrentNamespace(ctx)

			// Try OpenShift projects API first
			names, err := listOpenShiftProjects(ctx)
			if err != nil {
				// Fall back to namespaces
				names, err = listNamespaces(ctx)
				if err != nil {
					return fmt.Errorf("failed to list namespaces: %w", err)
				}
			}

			if len(names) == 0 {
				fmt.Fprintln(out, "No projects found.")
				return nil
			}

			fmt.Fprintf(out, "You have access to the following projects and can switch between them with 'ocp project <projectname>':\n\n")
			for _, name := range names {
				if name == currentNS {
					fmt.Fprintf(out, "  * %s (current)\n", name)
				} else {
					fmt.Fprintf(out, "    %s\n", name)
				}
			}
			fmt.Fprintf(out, "\nUsing project %q on server.\n", currentNS)

			return nil
		},
	}
}

func listOpenShiftProjects(ctx context.Context) ([]string, error) {
	dynamicClient, err := kube.NewDynamicClient(ctx)
	if err != nil {
		return nil, err
	}

	gvr := schema.GroupVersionResource{
		Group:    "project.openshift.io",
		Version:  "v1",
		Resource: "projects",
	}

	list, err := dynamicClient.Resource(gvr).List(ctx, metav1.ListOptions{})
	if meta.IsNoMatchError(err) {
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(list.Items))
	for _, item := range list.Items {
		name, _, _ := unstructured.NestedString(item.Object, "metadata", "name")
		if name != "" {
			names = append(names, name)
		}
	}
	return names, nil
}

func listNamespaces(ctx context.Context) ([]string, error) {
	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return nil, err
	}

	nsList, err := clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(nsList.Items))
	for _, ns := range nsList.Items {
		names = append(names, ns.Name)
	}
	return names, nil
}
