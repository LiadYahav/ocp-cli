package cli

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/spf13/cobra"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newBindingCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "binding",
		Short: "Manage RBAC role bindings and cluster role bindings",
		Long: `Manage Kubernetes RBAC RoleBindings and ClusterRoleBindings.

By default, commands work with namespace-scoped RoleBindings. Use --cluster flag
to work with ClusterRoleBindings.

Examples:
  # List role bindings in a namespace
  ocp binding list --namespace=default
  
  # List cluster role bindings
  ocp binding list --cluster
  
  # Add a user to a role binding
  ocp binding add admin --user=john --namespace=default
  
  # Add a user to a cluster role binding
  ocp binding add cluster-admin --user=john --cluster
  
  # See who has a specific role
  ocp binding who-has admin --namespace=default`,
	}

	cmd.AddCommand(
		newBindingListCommand(),
		newBindingGetCommand(),
		newBindingDescribeCommand(),
		newBindingAddCommand(),
		newBindingRemoveCommand(),
		newBindingWhoHasCommand(),
		newBindingWhatCanCommand(),
	)

	return cmd
}

func newBindingListCommand() *cobra.Command {
	var cluster bool
	var namespace string
	var roleName string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List role bindings or cluster role bindings",
		Long: `List RoleBindings or ClusterRoleBindings.

By default, lists namespace-scoped RoleBindings. Use --cluster to list ClusterRoleBindings.
Use --role to filter by role name.`,
		Example: `  # List role bindings in a namespace
  ocp binding list --namespace=default
  
  # List cluster role bindings
  ocp binding list --cluster
  
  # List bindings for a specific role
  ocp binding list --role=admin --namespace=default`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			if cluster {
				return listClusterRoleBindings(ctx, clientset, roleName, cmd.OutOrStdout())
			} else {
				if namespace == "" {
					namespace = getCurrentNamespace(ctx)
					if namespace == "" {
						return fmt.Errorf("namespace is required for role bindings. Use --namespace or set current project with 'ocp project <namespace>'")
					}
				}
				return listRoleBindings(ctx, clientset, namespace, roleName, cmd.OutOrStdout())
			}
		},
	}

	cmd.Flags().BoolVar(&cluster, "cluster", false, "List cluster role bindings instead of role bindings")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace for role bindings (ignored with --cluster, overrides current project)")
	cmd.Flags().StringVar(&roleName, "role", "", "Filter by role name")

	return cmd
}

func newBindingGetCommand() *cobra.Command {
	var cluster bool
	var namespace string

	cmd := &cobra.Command{
		Use:   "get <binding-name>",
		Short: "Get a role binding or cluster role binding",
		Args:  cobra.ExactArgs(1),
		Example: `  # Get a role binding
  ocp binding get admin --namespace=default
  
  # Get a cluster role binding
  ocp binding get cluster-admin --cluster`,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				cluster, _ := cmd.Flags().GetBool("cluster")
				namespace, _ := cmd.Flags().GetString("namespace")
				return completeBindingNames(cmd, cluster, namespace, toComplete)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			bindingName := args[0]
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			if cluster {
				binding, err := clientset.RbacV1().ClusterRoleBindings().Get(ctx, bindingName, metav1.GetOptions{})
				if err != nil {
					return fmt.Errorf("failed to get cluster role binding %q: %w", bindingName, err)
				}
				return displayBinding(cmd.OutOrStdout(), binding, true)
			} else {
				if namespace == "" {
					namespace = getCurrentNamespace(ctx)
					if namespace == "" {
						return fmt.Errorf("namespace is required for role bindings. Use --namespace or set current project with 'ocp project <namespace>'")
					}
				}
				binding, err := clientset.RbacV1().RoleBindings(namespace).Get(ctx, bindingName, metav1.GetOptions{})
				if err != nil {
					return fmt.Errorf("failed to get role binding %q: %w", bindingName, err)
				}
				return displayBinding(cmd.OutOrStdout(), binding, false)
			}
		},
	}

	cmd.Flags().BoolVar(&cluster, "cluster", false, "Get cluster role binding instead of role binding")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace for role binding (ignored with --cluster, overrides current project)")

	return cmd
}

func newBindingDescribeCommand() *cobra.Command {
	var cluster bool
	var namespace string

	cmd := &cobra.Command{
		Use:   "describe <binding-name>",
		Short: "Describe a role binding or cluster role binding",
		Args:  cobra.ExactArgs(1),
		Example: `  # Describe a role binding
  ocp binding describe admin --namespace=default
  
  # Describe a cluster role binding
  ocp binding describe cluster-admin --cluster`,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				cluster, _ := cmd.Flags().GetBool("cluster")
				namespace, _ := cmd.Flags().GetString("namespace")
				return completeBindingNames(cmd, cluster, namespace, toComplete)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			bindingName := args[0]
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			if cluster {
				binding, err := clientset.RbacV1().ClusterRoleBindings().Get(ctx, bindingName, metav1.GetOptions{})
				if err != nil {
					return fmt.Errorf("failed to get cluster role binding %q: %w", bindingName, err)
				}
				return describeBinding(cmd.OutOrStdout(), binding, true)
			} else {
				if namespace == "" {
					namespace = getCurrentNamespace(ctx)
					if namespace == "" {
						return fmt.Errorf("namespace is required for role bindings. Use --namespace or set current project with 'ocp project <namespace>'")
					}
				}
				binding, err := clientset.RbacV1().RoleBindings(namespace).Get(ctx, bindingName, metav1.GetOptions{})
				if err != nil {
					return fmt.Errorf("failed to get role binding %q: %w", bindingName, err)
				}
				return describeBinding(cmd.OutOrStdout(), binding, false)
			}
		},
	}

	cmd.Flags().BoolVar(&cluster, "cluster", false, "Describe cluster role binding instead of role binding")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace for role binding (ignored with --cluster, overrides current project)")

	return cmd
}

func newBindingAddCommand() *cobra.Command {
	var cluster bool
	var namespace string
	var user string
	var group string
	var serviceAccount string

	cmd := &cobra.Command{
		Use:   "add <role-name>",
		Short: "Add a subject to a role binding or cluster role binding",
		Long: `Add a user, group, or service account to a role binding.

The binding will be created if it doesn't exist. If it exists, the subject
will be added to the existing binding.

For regular RoleBindings (namespace-scoped), --namespace is REQUIRED.
For ClusterRoleBindings (cluster-scoped), use --cluster flag (no namespace needed).`,
		Args: cobra.ExactArgs(1),
		Example: `  # Add user to role binding
  ocp binding add admin --user=john --namespace=default
  
  # Add service account to role binding
  ocp binding add edit --serviceaccount=default/my-sa --namespace=default
  
  # Add user to cluster role binding
  ocp binding add cluster-admin --user=admin --cluster`,
		RunE: func(cmd *cobra.Command, args []string) error {
			roleName := args[0]
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			// Validate exactly one subject type is specified
			subjectCount := 0
			if user != "" {
				subjectCount++
			}
			if group != "" {
				subjectCount++
			}
			if serviceAccount != "" {
				subjectCount++
			}
			if subjectCount != 1 {
				return fmt.Errorf("exactly one of --user, --group, or --serviceaccount must be specified")
			}

			var subject rbacv1.Subject

			if user != "" {
				subject = rbacv1.Subject{
					Kind:     "User",
					Name:     user,
					APIGroup: "rbac.authorization.k8s.io",
				}
			} else if group != "" {
				subject = rbacv1.Subject{
					Kind:     "Group",
					Name:     group,
					APIGroup: "rbac.authorization.k8s.io",
				}
			} else if serviceAccount != "" {
				parts := strings.Split(serviceAccount, "/")
				if len(parts) != 2 {
					return fmt.Errorf("serviceaccount must be in format 'namespace/name'")
				}
				subject = rbacv1.Subject{
					Kind:      "ServiceAccount",
					Name:      parts[1],
					Namespace: parts[0],
				}
			}

			// Determine if this is a ClusterRole or Role
			isClusterRole, isRole, err := checkRoleType(ctx, clientset, roleName, namespace)
			if err != nil {
				return err
			}

			// Validate namespace requirements
			if cluster {
				// User explicitly requested cluster role binding - cluster-scoped, no namespace
				if namespace != "" {
					fmt.Fprintf(cmd.ErrOrStderr(), "warning: --namespace is ignored for cluster role bindings (cluster-scoped)\n")
				}
				if isRole && !isClusterRole {
					return fmt.Errorf("role %q exists as a Role (namespace-scoped), not a ClusterRole. Use 'ocp binding add %s --user=%s --namespace=<namespace>' instead", roleName, roleName, user)
				}
				return addSubjectToClusterRoleBinding(ctx, clientset, roleName, subject, cmd.OutOrStdout())
			} else {
				// User wants regular role binding - namespace is REQUIRED
				if namespace == "" {
					namespace = getCurrentNamespace(ctx)
					if namespace == "" {
						return fmt.Errorf("namespace is required for role bindings (RoleBinding is namespace-scoped).\n\nUse --namespace flag:\n  ocp binding add %s --user=%s --namespace=<namespace>\n\nOr set current project:\n  ocp project <namespace>\n  ocp binding add %s --user=%s", roleName, user, roleName, user)
					}
				}

				// Warn if it's a ClusterRole being used in namespace scope (this is valid - ClusterRoles can be bound in namespaces)
				if isClusterRole && !isRole {
					fmt.Fprintf(cmd.ErrOrStderr(), "info: %q is a ClusterRole. It will be bound in namespace %q (this is valid - ClusterRoles can be referenced by RoleBindings).\n", roleName, namespace)
					fmt.Fprintf(cmd.ErrOrStderr(), "      To bind it cluster-wide instead, use: ocp binding add %s --user=%s --cluster\n", roleName, user)
				}

				return addSubjectToRoleBinding(ctx, clientset, namespace, roleName, subject, cmd.OutOrStdout())
			}
		},
	}

	cmd.Flags().BoolVar(&cluster, "cluster", false, "Add to cluster role binding (cluster-scoped, no namespace required)")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace for role binding (required for regular RoleBinding, ignored with --cluster, overrides current project)")
	cmd.Flags().StringVar(&user, "user", "", "User name to add")
	cmd.Flags().StringVar(&group, "group", "", "Group name to add")
	cmd.Flags().StringVar(&serviceAccount, "serviceaccount", "", "Service account to add (format: namespace/name)")

	return cmd
}

func newBindingRemoveCommand() *cobra.Command {
	var cluster bool
	var namespace string
	var user string
	var group string
	var serviceAccount string

	cmd := &cobra.Command{
		Use:   "remove <role-name>",
		Short: "Remove a subject from a role binding or cluster role binding",
		Long: `Remove a user, group, or service account from a role binding.

If the binding becomes empty after removal, it will be deleted.`,
		Args: cobra.ExactArgs(1),
		Example: `  # Remove user from role binding
  ocp binding remove admin --user=john --namespace=default
  
  # Remove service account from role binding
  ocp binding remove edit --serviceaccount=default/my-sa --namespace=default
  
  # Remove user from cluster role binding
  ocp binding remove cluster-admin --user=admin --cluster`,
		RunE: func(cmd *cobra.Command, args []string) error {
			roleName := args[0]
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			// Validate exactly one subject type is specified
			subjectCount := 0
			if user != "" {
				subjectCount++
			}
			if group != "" {
				subjectCount++
			}
			if serviceAccount != "" {
				subjectCount++
			}
			if subjectCount != 1 {
				return fmt.Errorf("exactly one of --user, --group, or --serviceaccount must be specified")
			}

			var subject rbacv1.Subject

			if user != "" {
				subject = rbacv1.Subject{
					Kind:     "User",
					Name:     user,
					APIGroup: "rbac.authorization.k8s.io",
				}
			} else if group != "" {
				subject = rbacv1.Subject{
					Kind:     "Group",
					Name:     group,
					APIGroup: "rbac.authorization.k8s.io",
				}
			} else if serviceAccount != "" {
				parts := strings.Split(serviceAccount, "/")
				if len(parts) != 2 {
					return fmt.Errorf("serviceaccount must be in format 'namespace/name'")
				}
				subject = rbacv1.Subject{
					Kind:      "ServiceAccount",
					Name:      parts[1],
					Namespace: parts[0],
				}
			}

			// Determine if this is a ClusterRole or Role
			isClusterRole, isRole, err := checkRoleType(ctx, clientset, roleName, namespace)
			if err != nil {
				return err
			}

			// Validate namespace requirements
			if cluster {
				// User explicitly requested cluster role binding
				if namespace != "" {
					fmt.Fprintf(cmd.ErrOrStderr(), "warning: --namespace is ignored for cluster role bindings (cluster-scoped)\n")
				}
				if isRole && !isClusterRole {
					return fmt.Errorf("role %q exists as a Role (namespace-scoped), not a ClusterRole. Use 'ocp binding remove %s --user=%s --namespace=<namespace>' instead", roleName, roleName, user)
				}
				return removeSubjectFromClusterRoleBinding(ctx, clientset, roleName, subject, cmd.OutOrStdout())
			} else {
				// User wants regular role binding - namespace is required
				if namespace == "" {
					namespace = getCurrentNamespace(ctx)
					if namespace == "" {
						return fmt.Errorf("namespace is required for role bindings. Use --namespace or set current project with 'ocp project <namespace>'\n\nExample: ocp binding remove %s --user=%s --namespace=default", roleName, user)
					}
				}

				// Warn if it's a ClusterRole being used in namespace scope
				if isClusterRole && !isRole {
					fmt.Fprintf(cmd.ErrOrStderr(), "warning: %q is a ClusterRole. To remove from cluster-wide binding, use 'ocp binding remove %s --user=%s --cluster'\n", roleName, roleName, user)
				}

				return removeSubjectFromRoleBinding(ctx, clientset, namespace, roleName, subject, cmd.OutOrStdout())
			}
		},
	}

	cmd.Flags().BoolVar(&cluster, "cluster", false, "Remove from cluster role binding instead of role binding")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace for role binding (ignored with --cluster, overrides current project)")
	cmd.Flags().StringVar(&user, "user", "", "User name to remove")
	cmd.Flags().StringVar(&group, "group", "", "Group name to remove")
	cmd.Flags().StringVar(&serviceAccount, "serviceaccount", "", "Service account to remove (format: namespace/name)")

	return cmd
}

func newBindingWhoHasCommand() *cobra.Command {
	var cluster bool
	var namespace string

	cmd := &cobra.Command{
		Use:   "who-has <role-name>",
		Short: "List all subjects bound to a role",
		Long: `List all subjects (users, groups, service accounts) that are bound to a specific role.

Searches through RoleBindings or ClusterRoleBindings to find all subjects
that have the specified role.`,
		Args: cobra.ExactArgs(1),
		Example: `  # See who has a role in a namespace
  ocp binding who-has admin --namespace=default
  
  # See who has a cluster role
  ocp binding who-has cluster-admin --cluster`,
		RunE: func(cmd *cobra.Command, args []string) error {
			roleName := args[0]
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			if cluster {
				return whoHasClusterRole(ctx, clientset, roleName, cmd.OutOrStdout())
			} else {
				if namespace == "" {
					namespace = getCurrentNamespace(ctx)
					if namespace == "" {
						return fmt.Errorf("namespace is required for role bindings. Use --namespace or set current project with 'ocp project <namespace>'")
					}
				}
				return whoHasRole(ctx, clientset, namespace, roleName, cmd.OutOrStdout())
			}
		},
	}

	cmd.Flags().BoolVar(&cluster, "cluster", false, "Check cluster role bindings instead of role bindings")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace for role bindings (ignored with --cluster, overrides current project)")

	return cmd
}

func newBindingWhatCanCommand() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "what-can <user>",
		Short: "List all roles bound to a user",
		Long: `List all roles (both RoleBindings and ClusterRoleBindings) that are bound to a specific user.

Searches through all bindings to find roles assigned to the user.`,
		Args: cobra.ExactArgs(1),
		Example: `  # See what roles a user has
  ocp binding what-can john
  
  # See what roles a user has in a specific namespace
  ocp binding what-can john --namespace=default`,
		RunE: func(cmd *cobra.Command, args []string) error {
			userName := args[0]
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			clientset, err := kube.NewClientset(ctx)
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %w", err)
			}

			return whatCanUser(ctx, clientset, userName, namespace, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Filter to specific namespace (empty for all namespaces, overrides current project)")

	return cmd
}

// Helper functions

// checkRoleType checks if a role exists as a ClusterRole, Role, or both
// Returns: (isClusterRole, isRole, error)
// This function is optimized to check ClusterRole first (single API call) and only
// checks Roles if needed, avoiding unnecessary namespace queries when possible.
func checkRoleType(ctx context.Context, clientset *kubernetes.Clientset, roleName string, namespace string) (bool, bool, error) {
	// Check if it's a ClusterRole (single API call, cluster-scoped)
	_, err := clientset.RbacV1().ClusterRoles().Get(ctx, roleName, metav1.GetOptions{})
	isClusterRole := err == nil

	// Check if it's a Role (namespace-scoped)
	// First try the provided namespace if available
	isRole := false
	if namespace != "" {
		_, err := clientset.RbacV1().Roles(namespace).Get(ctx, roleName, metav1.GetOptions{})
		isRole = err == nil
	}

	// If not found in specified namespace and we don't have a namespace hint,
	// we could check all namespaces, but that's expensive. Instead, we'll let
	// the caller handle the ambiguity. For now, only check if namespace was provided.
	// If namespace is empty and we need to check, we'll do a limited search.
	if !isRole && namespace == "" {
		// Only check a few common namespaces to avoid expensive full namespace scan
		// This is a performance optimization - if role isn't found in common namespaces,
		// we'll assume it might exist elsewhere and let the binding operation handle it
		commonNamespaces := []string{"default", "kube-system", "kube-public"}
		for _, ns := range commonNamespaces {
			_, err := clientset.RbacV1().Roles(ns).Get(ctx, roleName, metav1.GetOptions{})
			if err == nil {
				isRole = true
				break
			}
		}
	}

	return isClusterRole, isRole, nil
}

func listRoleBindings(ctx context.Context, clientset *kubernetes.Clientset, namespace, roleName string, out io.Writer) error {
	bindings, err := clientset.RbacV1().RoleBindings(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list role bindings in namespace %q: %w", namespace, err)
	}

	if len(bindings.Items) == 0 {
		fmt.Fprintf(out, "No role bindings found in namespace %q\n", namespace)
		return nil
	}

	fmt.Fprintf(out, "ROLE BINDINGS in namespace %q:\n\n", namespace)
	fmt.Fprintf(out, "%-30s %-20s %s\n", "NAME", "ROLE", "SUBJECTS")
	fmt.Fprintf(out, "%-30s %-20s %s\n", strings.Repeat("-", 30), strings.Repeat("-", 20), strings.Repeat("-", 40))

	for _, binding := range bindings.Items {
		if roleName != "" && binding.RoleRef.Name != roleName {
			continue
		}

		subjectsStr := formatSubjects(binding.Subjects)
		fmt.Fprintf(out, "%-30s %-20s %s\n", binding.Name, binding.RoleRef.Name, subjectsStr)
	}

	return nil
}

func listClusterRoleBindings(ctx context.Context, clientset *kubernetes.Clientset, roleName string, out io.Writer) error {
	bindings, err := clientset.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list cluster role bindings: %w", err)
	}

	if len(bindings.Items) == 0 {
		fmt.Fprintf(out, "No cluster role bindings found\n")
		return nil
	}

	fmt.Fprintf(out, "CLUSTER ROLE BINDINGS:\n\n")
	fmt.Fprintf(out, "%-30s %-20s %s\n", "NAME", "ROLE", "SUBJECTS")
	fmt.Fprintf(out, "%-30s %-20s %s\n", strings.Repeat("-", 30), strings.Repeat("-", 20), strings.Repeat("-", 40))

	for _, binding := range bindings.Items {
		if roleName != "" && binding.RoleRef.Name != roleName {
			continue
		}

		subjectsStr := formatSubjects(binding.Subjects)
		fmt.Fprintf(out, "%-30s %-20s %s\n", binding.Name, binding.RoleRef.Name, subjectsStr)
	}

	return nil
}

func displayBinding(out io.Writer, binding interface{}, isCluster bool) error {
	if isCluster {
		crb := binding.(*rbacv1.ClusterRoleBinding)
		fmt.Fprintf(out, "Name:         %s\n", crb.Name)
		fmt.Fprintf(out, "Role:         %s\n", crb.RoleRef.Name)
		fmt.Fprintf(out, "Subjects:\n")
		for _, subject := range crb.Subjects {
			fmt.Fprintf(out, "  - %s: %s", subject.Kind, subject.Name)
			if subject.Namespace != "" {
				fmt.Fprintf(out, " (namespace: %s)", subject.Namespace)
			}
			fmt.Fprintf(out, "\n")
		}
	} else {
		rb := binding.(*rbacv1.RoleBinding)
		fmt.Fprintf(out, "Name:         %s\n", rb.Name)
		fmt.Fprintf(out, "Namespace:    %s\n", rb.Namespace)
		fmt.Fprintf(out, "Role:         %s\n", rb.RoleRef.Name)
		fmt.Fprintf(out, "Subjects:\n")
		for _, subject := range rb.Subjects {
			fmt.Fprintf(out, "  - %s: %s", subject.Kind, subject.Name)
			if subject.Namespace != "" {
				fmt.Fprintf(out, " (namespace: %s)", subject.Namespace)
			}
			fmt.Fprintf(out, "\n")
		}
	}
	return nil
}

func describeBinding(out io.Writer, binding interface{}, isCluster bool) error {
	return displayBinding(out, binding, isCluster)
}

func addSubjectToRoleBinding(ctx context.Context, clientset *kubernetes.Clientset, namespace, roleName string, subject rbacv1.Subject, out io.Writer) error {
	// Common pattern: binding name = role name
	bindingName := roleName

	// Try to get existing binding
	binding, err := clientset.RbacV1().RoleBindings(namespace).Get(ctx, bindingName, metav1.GetOptions{})

	if apierrors.IsNotFound(err) {
		// Create new binding
		binding = &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      bindingName,
				Namespace: namespace,
			},
			RoleRef: rbacv1.RoleRef{
				Kind:     "Role",
				Name:     roleName,
				APIGroup: "rbac.authorization.k8s.io",
			},
			Subjects: []rbacv1.Subject{subject},
		}
		_, err = clientset.RbacV1().RoleBindings(namespace).Create(ctx, binding, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create role binding: %w", err)
		}
		fmt.Fprintf(out, "✓ Created role binding %q in namespace %q and added %s %q\n", bindingName, namespace, subject.Kind, subject.Name)
		fmt.Fprintf(out, "\n=== Summary ===\n")
		fmt.Fprintf(out, "Operation: Created new role binding\n")
		fmt.Fprintf(out, "Binding: %s (namespace: %s)\n", bindingName, namespace)
		fmt.Fprintf(out, "Role: %s (namespace-scoped)\n", roleName)
		fmt.Fprintf(out, "Subject added: %s %q\n", subject.Kind, subject.Name)
		fmt.Fprintf(out, "\n✓ Binding operation completed successfully\n")
		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to get role binding: %w", err)
	}

	// Verify role ref matches
	if binding.RoleRef.Name != roleName {
		return fmt.Errorf("role binding %q references role %q, not %q", bindingName, binding.RoleRef.Name, roleName)
	}

	// Check if subject already exists
	for _, s := range binding.Subjects {
		if s.Kind == subject.Kind && s.Name == subject.Name && s.Namespace == subject.Namespace {
			fmt.Fprintf(out, "⊘ %s %q already has role %q in namespace %q\n", subject.Kind, subject.Name, roleName, namespace)
			fmt.Fprintf(out, "\n=== Summary ===\n")
			fmt.Fprintf(out, "Operation: No change needed\n")
			fmt.Fprintf(out, "Binding: %s (namespace: %s)\n", bindingName, namespace)
			fmt.Fprintf(out, "Role: %s\n", roleName)
			fmt.Fprintf(out, "Subject: %s %q (already exists)\n", subject.Kind, subject.Name)
			return nil
		}
	}

	// Add subject
	binding.Subjects = append(binding.Subjects, subject)
	_, err = clientset.RbacV1().RoleBindings(namespace).Update(ctx, binding, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update role binding: %w", err)
	}

	fmt.Fprintf(out, "✓ Added %s %q to role binding %q\n", subject.Kind, subject.Name, bindingName)
	fmt.Fprintf(out, "\n=== Summary ===\n")
	fmt.Fprintf(out, "Operation: Updated existing role binding\n")
	fmt.Fprintf(out, "Binding: %s (namespace: %s)\n", bindingName, namespace)
	fmt.Fprintf(out, "Role: %s\n", roleName)
	fmt.Fprintf(out, "Subject added: %s %q\n", subject.Kind, subject.Name)
	fmt.Fprintf(out, "Total subjects in binding: %d\n", len(binding.Subjects))
	fmt.Fprintf(out, "\n✓ Binding operation completed successfully\n")
	return nil
}

func addSubjectToClusterRoleBinding(ctx context.Context, clientset *kubernetes.Clientset, roleName string, subject rbacv1.Subject, out io.Writer) error {
	// Common pattern: binding name = role name
	bindingName := roleName

	// Try to get existing binding
	binding, err := clientset.RbacV1().ClusterRoleBindings().Get(ctx, bindingName, metav1.GetOptions{})

	if apierrors.IsNotFound(err) {
		// Create new binding
		binding = &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: bindingName,
			},
			RoleRef: rbacv1.RoleRef{
				Kind:     "ClusterRole",
				Name:     roleName,
				APIGroup: "rbac.authorization.k8s.io",
			},
			Subjects: []rbacv1.Subject{subject},
		}
		_, err = clientset.RbacV1().ClusterRoleBindings().Create(ctx, binding, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create cluster role binding: %w", err)
		}
		fmt.Fprintf(out, "✓ Created cluster role binding %q (cluster-scoped) and added %s %q\n", bindingName, subject.Kind, subject.Name)
		fmt.Fprintf(out, "\n=== Summary ===\n")
		fmt.Fprintf(out, "Operation: Created new cluster role binding\n")
		fmt.Fprintf(out, "Binding: %s (cluster-scoped, no namespace)\n", bindingName)
		fmt.Fprintf(out, "Role: %s (ClusterRole)\n", roleName)
		fmt.Fprintf(out, "Subject added: %s %q\n", subject.Kind, subject.Name)
		fmt.Fprintf(out, "\n✓ Binding operation completed successfully\n")
		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to get cluster role binding: %w", err)
	}

	// Verify role ref matches
	if binding.RoleRef.Name != roleName {
		return fmt.Errorf("cluster role binding %q references role %q, not %q", bindingName, binding.RoleRef.Name, roleName)
	}

	// Check if subject already exists
	for _, s := range binding.Subjects {
		if s.Kind == subject.Kind && s.Name == subject.Name && s.Namespace == subject.Namespace {
			fmt.Fprintf(out, "⊘ %s %q already has cluster role %q\n", subject.Kind, subject.Name, roleName)
			fmt.Fprintf(out, "\n=== Summary ===\n")
			fmt.Fprintf(out, "Operation: No change needed\n")
			fmt.Fprintf(out, "Binding: %s (cluster-scoped, no namespace)\n", bindingName)
			fmt.Fprintf(out, "Role: %s (ClusterRole)\n", roleName)
			fmt.Fprintf(out, "Subject: %s %q (already exists)\n", subject.Kind, subject.Name)
			return nil
		}
	}

	// Add subject
	binding.Subjects = append(binding.Subjects, subject)
	_, err = clientset.RbacV1().ClusterRoleBindings().Update(ctx, binding, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update cluster role binding: %w", err)
	}

	fmt.Fprintf(out, "✓ Added %s %q to cluster role binding %q\n", subject.Kind, subject.Name, bindingName)
	fmt.Fprintf(out, "\n=== Summary ===\n")
	fmt.Fprintf(out, "Operation: Updated existing cluster role binding\n")
	fmt.Fprintf(out, "Binding: %s (cluster-scoped, no namespace)\n", bindingName)
	fmt.Fprintf(out, "Role: %s (ClusterRole)\n", roleName)
	fmt.Fprintf(out, "Subject added: %s %q\n", subject.Kind, subject.Name)
	fmt.Fprintf(out, "Total subjects in binding: %d\n", len(binding.Subjects))
	fmt.Fprintf(out, "\n✓ Binding operation completed successfully\n")
	return nil
}

func removeSubjectFromRoleBinding(ctx context.Context, clientset *kubernetes.Clientset, namespace, roleName string, subject rbacv1.Subject, out io.Writer) error {
	bindingName := roleName

	binding, err := clientset.RbacV1().RoleBindings(namespace).Get(ctx, bindingName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("role binding %q not found in namespace %q", bindingName, namespace)
	}
	if err != nil {
		return fmt.Errorf("failed to get role binding: %w", err)
	}

	// Verify role ref matches
	if binding.RoleRef.Name != roleName {
		return fmt.Errorf("role binding %q references role %q, not %q", bindingName, binding.RoleRef.Name, roleName)
	}

	// Find and remove subject
	found := false
	newSubjects := make([]rbacv1.Subject, 0, len(binding.Subjects))
	for _, s := range binding.Subjects {
		if s.Kind == subject.Kind && s.Name == subject.Name && s.Namespace == subject.Namespace {
			found = true
			continue
		}
		newSubjects = append(newSubjects, s)
	}

	if !found {
		return fmt.Errorf("%s %q not found in role binding %q", subject.Kind, subject.Name, bindingName)
	}

	// If no subjects left, delete the binding
	if len(newSubjects) == 0 {
		err = clientset.RbacV1().RoleBindings(namespace).Delete(ctx, bindingName, metav1.DeleteOptions{})
		if err != nil {
			return fmt.Errorf("failed to delete empty role binding: %w", err)
		}
		fmt.Fprintf(out, "✓ Removed %s %q from role binding %q (binding deleted as it became empty)\n", subject.Kind, subject.Name, bindingName)
		fmt.Fprintf(out, "\n=== Summary ===\n")
		fmt.Fprintf(out, "Operation: Removed subject and deleted empty binding\n")
		fmt.Fprintf(out, "Binding: %s (namespace: %s)\n", bindingName, namespace)
		fmt.Fprintf(out, "Role: %s\n", roleName)
		fmt.Fprintf(out, "Subject removed: %s %q\n", subject.Kind, subject.Name)
		fmt.Fprintf(out, "\n✓ Binding operation completed successfully\n")
		return nil
	}

	// Update binding
	binding.Subjects = newSubjects
	_, err = clientset.RbacV1().RoleBindings(namespace).Update(ctx, binding, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update role binding: %w", err)
	}

	fmt.Fprintf(out, "✓ Removed %s %q from role binding %q\n", subject.Kind, subject.Name, bindingName)
	fmt.Fprintf(out, "\n=== Summary ===\n")
	fmt.Fprintf(out, "Operation: Removed subject from role binding\n")
	fmt.Fprintf(out, "Binding: %s (namespace: %s)\n", bindingName, namespace)
	fmt.Fprintf(out, "Role: %s\n", roleName)
	fmt.Fprintf(out, "Subject removed: %s %q\n", subject.Kind, subject.Name)
	fmt.Fprintf(out, "Remaining subjects in binding: %d\n", len(newSubjects))
	fmt.Fprintf(out, "\n✓ Binding operation completed successfully\n")
	return nil
}

func removeSubjectFromClusterRoleBinding(ctx context.Context, clientset *kubernetes.Clientset, roleName string, subject rbacv1.Subject, out io.Writer) error {
	bindingName := roleName

	binding, err := clientset.RbacV1().ClusterRoleBindings().Get(ctx, bindingName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("cluster role binding %q not found", bindingName)
	}
	if err != nil {
		return fmt.Errorf("failed to get cluster role binding: %w", err)
	}

	// Verify role ref matches
	if binding.RoleRef.Name != roleName {
		return fmt.Errorf("cluster role binding %q references role %q, not %q", bindingName, binding.RoleRef.Name, roleName)
	}

	// Find and remove subject
	found := false
	newSubjects := make([]rbacv1.Subject, 0, len(binding.Subjects))
	for _, s := range binding.Subjects {
		if s.Kind == subject.Kind && s.Name == subject.Name && s.Namespace == subject.Namespace {
			found = true
			continue
		}
		newSubjects = append(newSubjects, s)
	}

	if !found {
		return fmt.Errorf("%s %q not found in cluster role binding %q", subject.Kind, subject.Name, bindingName)
	}

	// If no subjects left, delete the binding
	if len(newSubjects) == 0 {
		err = clientset.RbacV1().ClusterRoleBindings().Delete(ctx, bindingName, metav1.DeleteOptions{})
		if err != nil {
			return fmt.Errorf("failed to delete empty cluster role binding: %w", err)
		}
		fmt.Fprintf(out, "✓ Removed %s %q from cluster role binding %q (binding deleted as it became empty)\n", subject.Kind, subject.Name, bindingName)
		fmt.Fprintf(out, "\n=== Summary ===\n")
		fmt.Fprintf(out, "Operation: Removed subject and deleted empty binding\n")
		fmt.Fprintf(out, "Binding: %s (cluster-scoped, no namespace)\n", bindingName)
		fmt.Fprintf(out, "Role: %s (ClusterRole)\n", roleName)
		fmt.Fprintf(out, "Subject removed: %s %q\n", subject.Kind, subject.Name)
		fmt.Fprintf(out, "\n✓ Binding operation completed successfully\n")
		return nil
	}

	// Update binding
	binding.Subjects = newSubjects
	_, err = clientset.RbacV1().ClusterRoleBindings().Update(ctx, binding, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update cluster role binding: %w", err)
	}

	fmt.Fprintf(out, "✓ Removed %s %q from cluster role binding %q\n", subject.Kind, subject.Name, bindingName)
	fmt.Fprintf(out, "\n=== Summary ===\n")
	fmt.Fprintf(out, "Operation: Removed subject from cluster role binding\n")
	fmt.Fprintf(out, "Binding: %s (cluster-scoped, no namespace)\n", bindingName)
	fmt.Fprintf(out, "Role: %s (ClusterRole)\n", roleName)
	fmt.Fprintf(out, "Subject removed: %s %q\n", subject.Kind, subject.Name)
	fmt.Fprintf(out, "Remaining subjects in binding: %d\n", len(newSubjects))
	fmt.Fprintf(out, "\n✓ Binding operation completed successfully\n")
	return nil
}

func whoHasRole(ctx context.Context, clientset *kubernetes.Clientset, namespace, roleName string, out io.Writer) error {
	bindings, err := clientset.RbacV1().RoleBindings(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list role bindings: %w", err)
	}

	var subjects []rbacv1.Subject
	for _, binding := range bindings.Items {
		if binding.RoleRef.Name == roleName {
			subjects = append(subjects, binding.Subjects...)
		}
	}

	if len(subjects) == 0 {
		fmt.Fprintf(out, "No subjects found with role %q in namespace %q\n", roleName, namespace)
		return nil
	}

	fmt.Fprintf(out, "Subjects with role %q in namespace %q:\n\n", roleName, namespace)
	for _, subject := range subjects {
		fmt.Fprintf(out, "  - %s: %s", subject.Kind, subject.Name)
		if subject.Namespace != "" {
			fmt.Fprintf(out, " (namespace: %s)", subject.Namespace)
		}
		fmt.Fprintf(out, "\n")
	}

	return nil
}

func whoHasClusterRole(ctx context.Context, clientset *kubernetes.Clientset, roleName string, out io.Writer) error {
	bindings, err := clientset.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list cluster role bindings: %w", err)
	}

	var subjects []rbacv1.Subject
	for _, binding := range bindings.Items {
		if binding.RoleRef.Name == roleName {
			subjects = append(subjects, binding.Subjects...)
		}
	}

	if len(subjects) == 0 {
		fmt.Fprintf(out, "No subjects found with cluster role %q\n", roleName)
		return nil
	}

	fmt.Fprintf(out, "Subjects with cluster role %q:\n\n", roleName)
	for _, subject := range subjects {
		fmt.Fprintf(out, "  - %s: %s", subject.Kind, subject.Name)
		if subject.Namespace != "" {
			fmt.Fprintf(out, " (namespace: %s)", subject.Namespace)
		}
		fmt.Fprintf(out, "\n")
	}

	return nil
}

func whatCanUser(ctx context.Context, clientset *kubernetes.Clientset, userName, namespace string, out io.Writer) error {
	var roles []string
	var clusterRoles []string
	var rolesMu sync.Mutex
	var clusterRolesMu sync.Mutex
	var wg sync.WaitGroup

	// Search RoleBindings
	if namespace != "" {
		// Single namespace - no need for concurrency on RoleBindings
		bindings, err := clientset.RbacV1().RoleBindings(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("failed to list role bindings: %w", err)
		}
		for _, binding := range bindings.Items {
			for _, subject := range binding.Subjects {
				if subject.Kind == "User" && subject.Name == userName {
					roles = append(roles, fmt.Sprintf("%s/%s", namespace, binding.RoleRef.Name))
					break
				}
			}
		}
		// Still need to search ClusterRoleBindings in parallel
	} else {
		// Search all namespaces - parallelize for performance
		nsList, err := clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("failed to list namespaces: %w", err)
		}

		// Use worker pool pattern for concurrent namespace queries
		const maxConcurrency = 20
		nsChan := make(chan string, len(nsList.Items))
		for _, ns := range nsList.Items {
			nsChan <- ns.Name
		}
		close(nsChan)

		// Launch workers
		for i := 0; i < maxConcurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for nsName := range nsChan {
					bindings, err := clientset.RbacV1().RoleBindings(nsName).List(ctx, metav1.ListOptions{})
					if err != nil {
						continue // Skip namespaces we can't access
					}
					var foundRoles []string
					for _, binding := range bindings.Items {
						for _, subject := range binding.Subjects {
							if subject.Kind == "User" && subject.Name == userName {
								foundRoles = append(foundRoles, fmt.Sprintf("%s/%s", nsName, binding.RoleRef.Name))
								break
							}
						}
					}
					if len(foundRoles) > 0 {
						rolesMu.Lock()
						roles = append(roles, foundRoles...)
						rolesMu.Unlock()
					}
				}
			}()
		}
	}

	// Search ClusterRoleBindings - can be done in parallel with RoleBindings
	wg.Add(1)
	go func() {
		defer wg.Done()
		clusterBindings, err := clientset.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
		if err != nil {
			return // Will be handled after wait
		}
		var foundClusterRoles []string
		for _, binding := range clusterBindings.Items {
			for _, subject := range binding.Subjects {
				if subject.Kind == "User" && subject.Name == userName {
					foundClusterRoles = append(foundClusterRoles, binding.RoleRef.Name)
					break
				}
			}
		}
		if len(foundClusterRoles) > 0 {
			clusterRolesMu.Lock()
			clusterRoles = append(clusterRoles, foundClusterRoles...)
			clusterRolesMu.Unlock()
		}
	}()

	// Wait for all goroutines to complete
	wg.Wait()

	if len(roles) == 0 && len(clusterRoles) == 0 {
		fmt.Fprintf(out, "User %q has no roles", userName)
		if namespace != "" {
			fmt.Fprintf(out, " in namespace %q", namespace)
		}
		fmt.Fprintf(out, "\n")
		return nil
	}

	fmt.Fprintf(out, "Roles for user %q", userName)
	if namespace != "" {
		fmt.Fprintf(out, " in namespace %q", namespace)
	}
	fmt.Fprintf(out, ":\n\n")

	if len(roles) > 0 {
		fmt.Fprintf(out, "RoleBindings:\n")
		for _, role := range roles {
			fmt.Fprintf(out, "  - %s\n", role)
		}
		fmt.Fprintf(out, "\n")
	}

	if len(clusterRoles) > 0 {
		fmt.Fprintf(out, "ClusterRoleBindings:\n")
		for _, role := range clusterRoles {
			fmt.Fprintf(out, "  - %s\n", role)
		}
	}

	return nil
}

func formatSubjects(subjects []rbacv1.Subject) string {
	if len(subjects) == 0 {
		return "<none>"
	}

	var parts []string
	for _, s := range subjects {
		part := fmt.Sprintf("%s:%s", s.Kind, s.Name)
		if s.Namespace != "" {
			part += fmt.Sprintf("(%s)", s.Namespace)
		}
		parts = append(parts, part)
	}

	return strings.Join(parts, ", ")
}

func getCurrentNamespace(ctx context.Context) string {
	// First check if project namespace is set in context
	if projectNS := kube.ProjectNamespaceFromContext(ctx); projectNS != "" {
		return projectNS
	}

	// Then try to get from stored project namespace file
	if projectNS := getProjectNamespace(); projectNS != "" {
		return projectNS
	}

	// Finally, try to get from kubeconfig context
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	clientConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)

	ns, _, err := clientConfig.Namespace()
	if err != nil || ns == "" {
		return ""
	}
	return ns
}

func completeBindingNames(cmd *cobra.Command, cluster bool, namespace string, toComplete string) ([]string, cobra.ShellCompDirective) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	var completions []string

	if cluster {
		bindings, err := clientset.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, cobra.ShellCompDirectiveError
		}
		completions = make([]string, 0, len(bindings.Items))
		for _, binding := range bindings.Items {
			if strings.HasPrefix(binding.Name, toComplete) {
				completions = append(completions, binding.Name)
			}
		}
	} else {
		if namespace == "" {
			namespace = getCurrentNamespace(ctx)
			if namespace == "" {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
		}
		bindings, err := clientset.RbacV1().RoleBindings(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, cobra.ShellCompDirectiveError
		}
		completions = make([]string, 0, len(bindings.Items))
		for _, binding := range bindings.Items {
			if strings.HasPrefix(binding.Name, toComplete) {
				completions = append(completions, binding.Name)
			}
		}
	}

	return completions, cobra.ShellCompDirectiveNoFileComp
}
