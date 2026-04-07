package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newNewProjectCommand() *cobra.Command {
	var displayName string
	var description string

	cmd := &cobra.Command{
		Use:   "new-project <project-name>",
		Short: "Create a new project and switch to it",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			projectName := args[0]
			out := cmd.OutOrStdout()

			// Try OpenShift projectrequests API first
			created, err := createOpenShiftProject(ctx, projectName, displayName, description)
			if err == nil && created {
				fmt.Fprintf(out, "Now using project %q\n", projectName)
				return setProjectNamespace(projectName)
			}

			// Fall back to namespace creation
			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: projectName,
					Annotations: map[string]string{},
				},
			}

			if displayName != "" {
				ns.Annotations["openshift.io/display-name"] = displayName
			}
			if description != "" {
				ns.Annotations["openshift.io/description"] = description
			}
			if len(ns.Annotations) == 0 {
				ns.Annotations = nil
			}

			_, err = clientset.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("failed to create project %q: %w", projectName, err)
			}

			fmt.Fprintf(out, "Now using project %q\n", projectName)
			return setProjectNamespace(projectName)
		},
	}

	cmd.Flags().StringVar(&displayName, "display-name", "", "Display name for the project")
	cmd.Flags().StringVar(&description, "description", "", "Description for the project")

	return cmd
}

func createOpenShiftProject(ctx context.Context, name, displayName, description string) (bool, error) {
	dynamicClient, err := kube.NewDynamicClient(ctx)
	if err != nil {
		return false, err
	}

	gvr := schema.GroupVersionResource{
		Group:    "project.openshift.io",
		Version:  "v1",
		Resource: "projectrequests",
	}

	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "project.openshift.io/v1",
			"kind":       "ProjectRequest",
			"metadata": map[string]interface{}{
				"name": name,
			},
		},
	}

	if displayName != "" {
		obj.Object["displayName"] = displayName
	}
	if description != "" {
		obj.Object["description"] = description
	}

	_, err = dynamicClient.Resource(gvr).Create(ctx, obj, metav1.CreateOptions{})
	if meta.IsNoMatchError(err) {
		return false, err
	}
	if err != nil {
		return false, err
	}

	return true, nil
}
