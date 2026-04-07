package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/dynamic"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newCreateCommand() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "create -f <file>",
		Short: "Create a resource from a file",
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

			return createResources(ctx, dynamicClient, fileFlag, ns, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringSliceP("filename", "f", []string{}, "Filename(s) to use to create the resource")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func createResources(ctx context.Context, dynamicClient dynamic.Interface, files []string, namespace string, out io.Writer) error {
	var createdCount int
	var failedCount int

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

				_, err := dynamicClient.Resource(gvr).Namespace(ns).Create(ctx, &obj, metav1.CreateOptions{})
				if err != nil {
					fmt.Fprintf(out, "Error creating %s/%s: %v\n", obj.GetKind(), obj.GetName(), err)
					failedCount++
					continue
				}

				fmt.Fprintf(out, "%s/%s created\n", strings.ToLower(obj.GetKind()), obj.GetName())
				createdCount++
			}
		}()
	}

	if failedCount > 0 {
		return fmt.Errorf("failed to create %d resource(s)", failedCount)
	}
	return nil
}
