package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/liadyahav/ocp-cli/internal/kube"
	"github.com/liadyahav/ocp-cli/internal/pkg/cleaner"
)

func newClearYMLCommand() *cobra.Command {
	var namespace string
	var removeOwnerRefs bool
	var removeFinalizers bool
	var keepStatus bool
	var stripAnnotationsPrefix []string

	cmd := &cobra.Command{
		Use:   "clearyml <resource-type> [resource-name]",
		Short: "Get cleaned YAML output for resources",
		Long: `Get cleaned YAML output for Kubernetes resources with unnecessary fields removed.

Removes status, managedFields, uid, resourceVersion, creationTimestamp, and other noise.
Also removes resource-specific defaults (sessionAffinity, clusterIP, terminationMessagePath, etc.).
Output is clean YAML suitable for applying or version control.

Flags:
  --remove-owner-refs: Also remove ownerReferences (not removed by default)
  --remove-finalizers: Also remove finalizers (not removed by default)
  --keep-status: Keep the status section
  --strip-annotations-prefix: Additional annotation prefixes to strip`,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return completeResourceTypes(cmd, toComplete)
			}
			if len(args) == 1 {
				ns, _ := cmd.Flags().GetString("namespace")
				return completeResourceNames(cmd, args[0], toComplete, ns, false)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Args: cobra.RangeArgs(1, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := ensureContext(cmd.Context())

			resourceType := args[0]
			var resourceName string
			var ns string

			resolver, err := getResourceResolver(ctx)
			if err != nil {
				return formatErrorWithContext(err, "getResourceResolver", resourceType, "", "")
			}

			_, namespaced, err := resolver.resolveResource(resourceType)
			if err != nil {
				return formatErrorWithContext(err, "resolveResource", resourceType, "", "")
			}

			if namespace != "" {
				if len(args) >= 2 {
					resourceName = args[1]
				}
				ns = namespace
			} else {
				if namespaced {
					if len(args) == 1 {
						ns = resolveNamespace(ctx, "", false)
						if ns == "" {
							return fmt.Errorf("namespace is required for namespaced resource %q", resourceType)
						}
					} else if len(args) == 2 {
						ns = args[1]
					} else if len(args) == 3 {
						ns = args[1]
						resourceName = args[2]
					}
				} else {
					if len(args) == 2 {
						resourceName = args[1]
					}
				}
			}

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			opts := cleaner.DefaultOptions()
			opts.RemoveOwnerReferences = removeOwnerRefs
			opts.RemoveFinalizers = removeFinalizers
			opts.KeepStatus = keepStatus
			opts.AnnotationPrefixes = stripAnnotationsPrefix

			return clearYMLResource(ctx, dynamicClient, resourceType, resourceName, ns, opts, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().BoolVar(&removeOwnerRefs, "remove-owner-refs", false, "Also remove ownerReferences")
	cmd.Flags().BoolVar(&removeFinalizers, "remove-finalizers", false, "Also remove finalizers")
	cmd.Flags().BoolVar(&keepStatus, "keep-status", false, "Keep the status section")
	cmd.Flags().StringSliceVar(&stripAnnotationsPrefix, "strip-annotations-prefix", nil, "Additional annotation prefixes to strip")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func clearYMLResource(ctx context.Context, dynamicClient dynamic.Interface, resourceType, resourceName, namespace string, opts cleaner.CleanOptions, out io.Writer) error {
	resolver, err := getResourceResolver(ctx)
	if err != nil {
		return formatErrorWithContext(err, "getResourceResolver", resourceType, "", namespace)
	}

	gvr, namespaced, err := resolver.resolveResource(resourceType)
	if err != nil {
		return formatErrorWithContext(err, "resolveResource", resourceType, "", namespace)
	}

	if resourceName != "" {
		obj, err := getSingleResource(ctx, dynamicClient, gvr, resourceName, namespace, namespaced)
		if err != nil {
			return formatErrorWithContext(err, "getSingleResource", resourceType, resourceName, namespace)
		}

		cleaned := obj.DeepCopy()
		cleaner.CleanUnstructuredObjectWithOptions(cleaned, opts)
		return printCleanedYAML(cleaned, out)
	}

	// List resources
	var list *unstructured.UnstructuredList
	if !namespaced {
		list, err = dynamicClient.Resource(gvr).List(ctx, metav1.ListOptions{})
	} else {
		if namespace == "" {
			return fmt.Errorf("namespace is required for namespaced resource %q", resourceType)
		}
		list, err = dynamicClient.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{})
	}
	if err != nil {
		return formatErrorWithContext(err, "listResources", resourceType, "", namespace)
	}

	if len(list.Items) == 0 {
		fmt.Fprintf(out, "# No %s found", resourceType)
		if namespace != "" {
			fmt.Fprintf(out, " in namespace %q", namespace)
		}
		fmt.Fprintf(out, "\n")
		return nil
	}

	cleanedItems := make([]*unstructured.Unstructured, 0, len(list.Items))
	for i := range list.Items {
		cleaned := list.Items[i].DeepCopy()
		cleaner.CleanUnstructuredObjectWithOptions(cleaned, opts)
		cleanedItems = append(cleanedItems, cleaned)
	}
	return printCleanedYAMLList(cleanedItems, out)
}

func printCleanedYAML(obj *unstructured.Unstructured, out io.Writer) error {
	data, err := sigsyaml.Marshal(obj)
	if err != nil {
		return fmt.Errorf("failed to marshal YAML: %w", err)
	}
	_, err = out.Write(data)
	return err
}

func printCleanedYAMLList(items []*unstructured.Unstructured, out io.Writer) error {
	for i, item := range items {
		if i > 0 {
			fmt.Fprintf(out, "---\n")
		}
		if err := printCleanedYAML(item, out); err != nil {
			return err
		}
	}
	return nil
}
