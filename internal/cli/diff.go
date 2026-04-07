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
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newDiffCommand() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "diff -f <file>",
		Short: "Diff a local file against the live resource on the server",
		Long: `Compare a local YAML/JSON file against the live version of the resource on the server.

Examples:
  # Diff a deployment file
  ocp diff -f deployment.yaml

  # Diff with specific namespace
  ocp diff -f service.yaml -n my-namespace`,
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

			hasDiffs := false
			for _, file := range fileFlag {
				diff, err := diffFile(ctx, dynamicClient, file, ns)
				if err != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "Error processing %s: %v\n", file, err)
					continue
				}
				if diff != "" {
					hasDiffs = true
					fmt.Fprint(out, diff)
				}
			}

			if !hasDiffs {
				fmt.Fprintln(out, "No differences found.")
			}

			return nil
		},
	}

	cmd.Flags().StringSliceP("filename", "f", []string{}, "Filename(s) to diff")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)
	_ = cmd.RegisterFlagCompletionFunc("filename", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"yaml", "yml", "json"}, cobra.ShellCompDirectiveFilterFileExt
	})

	return cmd
}

func diffFile(ctx context.Context, dynamicClient dynamic.Interface, file, namespace string) (string, error) {
	var reader io.Reader
	if file == "-" {
		reader = os.Stdin
	} else {
		f, err := os.Open(file)
		if err != nil {
			return "", fmt.Errorf("failed to open file: %w", err)
		}
		defer f.Close()
		reader = f
	}

	decoder := yaml.NewYAMLOrJSONDecoder(reader, 4096)

	var result strings.Builder
	for {
		var obj unstructured.Unstructured
		if err := decoder.Decode(&obj); err != nil {
			if err == io.EOF {
				break
			}
			return "", fmt.Errorf("failed to decode: %w", err)
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

		// Get live version
		live, err := dynamicClient.Resource(gvr).Namespace(ns).Get(ctx, obj.GetName(), metav1.GetOptions{})
		if err != nil {
			result.WriteString(fmt.Sprintf("--- %s/%s (not found on server)\n", strings.ToLower(obj.GetKind()), obj.GetName()))
			result.WriteString("+++ " + file + "\n")
			localYAML, _ := sigsyaml.Marshal(obj.Object)
			for _, line := range strings.Split(string(localYAML), "\n") {
				if line != "" {
					result.WriteString("+ " + line + "\n")
				}
			}
			continue
		}

		// Clean both for comparison
		cleanForDiff(live)
		cleanForDiff(&obj)

		liveYAML, err := sigsyaml.Marshal(live.Object)
		if err != nil {
			return "", fmt.Errorf("failed to marshal live resource: %w", err)
		}

		localYAML, err := sigsyaml.Marshal(obj.Object)
		if err != nil {
			return "", fmt.Errorf("failed to marshal local resource: %w", err)
		}

		if string(liveYAML) == string(localYAML) {
			continue
		}

		diff := unifiedDiff(
			fmt.Sprintf("a/%s/%s (live)", strings.ToLower(obj.GetKind()), obj.GetName()),
			fmt.Sprintf("b/%s/%s (local)", strings.ToLower(obj.GetKind()), obj.GetName()),
			string(liveYAML),
			string(localYAML),
		)
		result.WriteString(diff)
	}

	return result.String(), nil
}

func cleanForDiff(obj *unstructured.Unstructured) {
	unstructured.RemoveNestedField(obj.Object, "metadata", "managedFields")
	unstructured.RemoveNestedField(obj.Object, "metadata", "resourceVersion")
	unstructured.RemoveNestedField(obj.Object, "metadata", "uid")
	unstructured.RemoveNestedField(obj.Object, "metadata", "creationTimestamp")
	unstructured.RemoveNestedField(obj.Object, "metadata", "generation")
	unstructured.RemoveNestedField(obj.Object, "metadata", "selfLink")
	unstructured.RemoveNestedField(obj.Object, "status")

	annotations, _, _ := unstructured.NestedMap(obj.Object, "metadata", "annotations")
	if len(annotations) == 0 {
		unstructured.RemoveNestedField(obj.Object, "metadata", "annotations")
	}
}

func unifiedDiff(fromLabel, toLabel, from, to string) string {
	fromLines := strings.Split(from, "\n")
	toLines := strings.Split(to, "\n")

	var result strings.Builder
	result.WriteString(fmt.Sprintf("--- %s\n", fromLabel))
	result.WriteString(fmt.Sprintf("+++ %s\n", toLabel))
	result.WriteString(fmt.Sprintf("@@ -%d,%d +%d,%d @@\n", 1, len(fromLines), 1, len(toLines)))

	i, j := 0, 0
	for i < len(fromLines) || j < len(toLines) {
		if i < len(fromLines) && j < len(toLines) && fromLines[i] == toLines[j] {
			result.WriteString("  " + fromLines[i] + "\n")
			i++
			j++
		} else {
			if i < len(fromLines) {
				result.WriteString("- " + fromLines[i] + "\n")
				i++
			}
			if j < len(toLines) {
				result.WriteString("+ " + toLines[j] + "\n")
				j++
			}
		}
	}

	return result.String()
}
