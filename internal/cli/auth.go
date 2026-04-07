package cli

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newAuthCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Authorization management commands",
	}

	cmd.AddCommand(newAuthCanICommand())
	return cmd
}

func newAuthCanICommand() *cobra.Command {
	var namespace string
	var allNamespaces bool
	var list bool

	cmd := &cobra.Command{
		Use:   "can-i <verb> <resource> [name]",
		Short: "Check if an action is allowed",
		Long: `Check whether an action is allowed against the cluster.

Examples:
  # Check if you can create pods
  ocp auth can-i create pods

  # Check if you can get a specific deployment
  ocp auth can-i get deployments my-deploy

  # List all allowed actions
  ocp auth can-i --list`,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				verbs := []string{"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"}
				return filterCompletions(verbs, toComplete), cobra.ShellCompDirectiveNoFileComp
			}
			if len(args) == 1 {
				return completeResourceTypes(cmd, toComplete)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			out := cmd.OutOrStdout()
			ns := resolveNamespace(ctx, namespace, allNamespaces)

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			if list {
				return listAllowedActions(ctx, clientset, ns, out)
			}

			if len(args) < 2 {
				return fmt.Errorf("verb and resource are required (or use --list)")
			}

			verb := args[0]
			resource := args[1]
			var resourceName string
			if len(args) > 2 {
				resourceName = args[2]
			}

			// Parse resource group if specified (e.g. "deployments.apps")
			var group string
			if idx := strings.Index(resource, "."); idx > 0 {
				group = resource[idx+1:]
				resource = resource[:idx]
			}

			review := &authorizationv1.SelfSubjectAccessReview{
				Spec: authorizationv1.SelfSubjectAccessReviewSpec{
					ResourceAttributes: &authorizationv1.ResourceAttributes{
						Namespace: ns,
						Verb:      verb,
						Group:     group,
						Resource:  resource,
						Name:      resourceName,
					},
				},
			}

			result, err := clientset.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, review, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("access review failed: %w", err)
			}

			if result.Status.Allowed {
				fmt.Fprintln(out, "yes")
			} else {
				reason := result.Status.Reason
				if reason != "" {
					fmt.Fprintf(out, "no - %s\n", reason)
				} else {
					fmt.Fprintln(out, "no")
				}
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "Check cluster-wide")
	cmd.Flags().BoolVar(&list, "list", false, "List all allowed actions")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func listAllowedActions(ctx context.Context, clientset *kubernetes.Clientset, namespace string, out io.Writer) error {
	review := &authorizationv1.SelfSubjectRulesReview{
		Spec: authorizationv1.SelfSubjectRulesReviewSpec{
			Namespace: namespace,
		},
	}

	result, err := clientset.AuthorizationV1().SelfSubjectRulesReviews().Create(ctx, review, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("rules review failed: %w", err)
	}

	fmt.Fprintf(out, "%-40s %-20s %s\n", "RESOURCES", "VERBS", "RESOURCE NAMES")
	for _, rule := range result.Status.ResourceRules {
		resources := strings.Join(rule.Resources, ", ")
		if len(rule.APIGroups) > 0 {
			for _, group := range rule.APIGroups {
				if group != "" {
					resources += "." + group
					break
				}
			}
		}
		verbs := strings.Join(rule.Verbs, ", ")
		names := strings.Join(rule.ResourceNames, ", ")
		if names == "" {
			names = "[all]"
		}
		fmt.Fprintf(out, "%-40s %-20s %s\n", resources, verbs, names)
	}

	return nil
}
