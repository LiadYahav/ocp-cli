package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newReplaceCommand() *cobra.Command {
	var namespace string
	var force bool

	cmd := &cobra.Command{
		Use:   "replace -f <file>",
		Short: "Replace a resource from a file",
		Long: `Replace a resource by file. The resource must already exist.

With --force, the resource is deleted and recreated.

Examples:
  # Replace a deployment from file
  ocp replace -f deployment.yaml

  # Force replace (delete + create)
  ocp replace -f deployment.yaml --force`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			fileFlag, err := cmd.Flags().GetStringSlice("filename")
			if err != nil {
				return err
			}

			if len(fileFlag) == 0 {
				return fmt.Errorf("missing required flag: -f or --filename")
			}

			ns := resolveNamespace(ctx, namespace, false)
			out := cmd.OutOrStdout()

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			var failedCount int
			for _, file := range fileFlag {
				func() {
					var reader io.Reader
					if file == "-" {
						reader = os.Stdin
					} else {
						f, err := os.Open(file)
						if err != nil {
							fmt.Fprintf(out, "Error opening file %s: %v\n", file, err)
							failedCount++
							return
						}
						defer f.Close()
						reader = f
					}

					decoder := yaml.NewYAMLOrJSONDecoder(reader, 4096)
					for {
						var obj unstructured.Unstructured
						if err := decoder.Decode(&obj); err != nil {
							if err == io.EOF {
								break
							}
							fmt.Fprintf(out, "Error decoding from %s: %v\n", file, err)
							failedCount++
							continue
						}

						if obj.Object == nil {
							continue
						}

						if ns != "" {
							obj.SetNamespace(ns)
						}

						gvr := schema.GroupVersionResource{
							Group:    obj.GroupVersionKind().Group,
							Version:  obj.GroupVersionKind().Version,
							Resource: strings.ToLower(obj.GetKind()) + "s",
						}

						objNS := obj.GetNamespace()
						if objNS == "" {
							objNS = "default"
						}

						resourceClient := dynamicClient.Resource(gvr).Namespace(objNS)

						if force {
							// Delete then create
							err := resourceClient.Delete(ctx, obj.GetName(), metav1.DeleteOptions{})
							if err != nil && !apierrors.IsNotFound(err) {
								fmt.Fprintf(out, "Error deleting %s/%s: %v\n", obj.GetKind(), obj.GetName(), err)
								failedCount++
								continue
							}

							_, err = resourceClient.Create(ctx, &obj, metav1.CreateOptions{})
							if err != nil {
								fmt.Fprintf(out, "Error creating %s/%s: %v\n", obj.GetKind(), obj.GetName(), err)
								failedCount++
								continue
							}
						} else {
							// Get existing to get resourceVersion
							existing, err := resourceClient.Get(ctx, obj.GetName(), metav1.GetOptions{})
							if err != nil {
								fmt.Fprintf(out, "Error getting %s/%s: %v\n", obj.GetKind(), obj.GetName(), err)
								failedCount++
								continue
							}
							obj.SetResourceVersion(existing.GetResourceVersion())

							_, err = resourceClient.Update(ctx, &obj, metav1.UpdateOptions{})
							if err != nil {
								fmt.Fprintf(out, "Error replacing %s/%s: %v\n", obj.GetKind(), obj.GetName(), err)
								failedCount++
								continue
							}
						}

						fmt.Fprintf(out, "%s/%s replaced\n", strings.ToLower(obj.GetKind()), obj.GetName())
					}
				}()
			}

			if failedCount > 0 {
				return fmt.Errorf("failed to replace %d resource(s)", failedCount)
			}
			return nil
		},
	}

	cmd.Flags().StringSliceP("filename", "f", []string{}, "Filename(s) to replace")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	cmd.Flags().BoolVar(&force, "force", false, "Delete and recreate the resource")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	_ = cmd.RegisterFlagCompletionFunc("filename", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"yaml", "yml", "json"}, cobra.ShellCompDirectiveFilterFileExt
	})

	return cmd
}
