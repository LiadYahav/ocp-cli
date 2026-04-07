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
	"k8s.io/client-go/dynamic"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newApplyCommand() *cobra.Command {
	var namespace string
	var force bool

	cmd := &cobra.Command{
		Use:   "apply -f <file>",
		Short: "Apply a configuration to a resource",
		Args:  cobra.NoArgs,
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

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			return applyResources(ctx, dynamicClient, fileFlag, ns, force, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringSliceP("filename", "f", []string{}, "Filename(s) to apply")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().BoolVar(&force, "force", false, "Force apply, recreate resources if necessary")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func applyResources(ctx context.Context, dynamicClient dynamic.Interface, files []string, namespace string, force bool, out io.Writer) error {
	var appliedCount, failedCount int

	for _, file := range files {
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
					fmt.Fprintf(out, "Error decoding resource from %s: %v\n", file, err)
					failedCount++
					continue
				}

				if obj.Object == nil {
					continue
				}

				if namespace != "" {
					obj.SetNamespace(namespace)
				}

				gvr := schema.GroupVersionResource{
					Group:    obj.GroupVersionKind().Group,
					Version:  obj.GroupVersionKind().Version,
					Resource: strings.ToLower(obj.GetKind()) + "s",
				}

				ns := obj.GetNamespace()
				if ns == "" {
					ns = "default"
				}

				resourceClient := dynamicClient.Resource(gvr).Namespace(ns)
				_, err := resourceClient.Get(ctx, obj.GetName(), metav1.GetOptions{})
				if err != nil {
					if apierrors.IsNotFound(err) {
						_, err = resourceClient.Create(ctx, &obj, metav1.CreateOptions{})
						if err != nil {
							fmt.Fprintf(out, "Error creating %s/%s: %v\n", obj.GetKind(), obj.GetName(), err)
							failedCount++
							continue
						}
						fmt.Fprintf(out, "%s/%s created\n", strings.ToLower(obj.GetKind()), obj.GetName())
					} else {
						fmt.Fprintf(out, "Error getting %s/%s: %v\n", obj.GetKind(), obj.GetName(), err)
						failedCount++
						continue
					}
				} else {
					_, err = resourceClient.Update(ctx, &obj, metav1.UpdateOptions{})
					if err != nil {
						fmt.Fprintf(out, "Error updating %s/%s: %v\n", obj.GetKind(), obj.GetName(), err)
						failedCount++
						continue
					}
					fmt.Fprintf(out, "%s/%s configured\n", strings.ToLower(obj.GetKind()), obj.GetName())
				}
				appliedCount++
			}
		}()
	}

	if failedCount > 0 {
		return fmt.Errorf("failed to apply %d resource(s)", failedCount)
	}
	return nil
}
