package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newLabelCommand() *cobra.Command {
	var namespace string
	var allNamespaces bool
	var maxConcurrency int

	cmd := &cobra.Command{
		Use:   "label <resource-type> <resource-name> <label> [flags]",
		Short: "Update the labels on one or more resources",
		Long: `Update labels on one or more resources.
Labels format: "key=value" (add/update) or "key-" (remove).`,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return completeResourceTypes(cmd, toComplete)
			}
			if len(args) == 1 {
				return completeResourceNames(cmd, args[0], toComplete, namespace, allNamespaces)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Args: cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			resourceType := args[0]
			resourceNames := args[1 : len(args)-1]
			labelsStr := args[len(args)-1]

			if len(resourceNames) == 0 {
				return fmt.Errorf("must specify at least one resource name")
			}

			ns := resolveNamespace(ctx, namespace, allNamespaces)

			dynamicClient, err := kube.NewDynamicClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create dynamic client: %w", err)
			}

			if maxConcurrency <= 0 {
				maxConcurrency = 10
			}

			return labelResources(ctx, dynamicClient, resourceType, resourceNames, ns, labelsStr, maxConcurrency, cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace (overrides current project)")
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "Label resources in all namespaces")
	cmd.Flags().IntVar(&maxConcurrency, "max-concurrency", 10, "Maximum number of concurrent operations")
	_ = cmd.RegisterFlagCompletionFunc("namespace", completeNamespaces)

	return cmd
}

func labelResources(ctx context.Context, dynamicClient dynamic.Interface, resourceType string, resourceNames []string, namespace string, labelsStr string, maxConcurrency int, out io.Writer, errOut io.Writer) error {
	resolver, err := getResourceResolver(ctx)
	if err != nil {
		return fmt.Errorf("failed to get resource resolver: %w", err)
	}

	gvr, namespaced, err := resolver.resolveResource(resourceType)
	if err != nil {
		return fmt.Errorf("resource type %q not found: %w", resourceType, err)
	}

	labelParts := strings.Split(labelsStr, ",")
	var patchOps []string
	var needsInit bool

	for _, label := range labelParts {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}

		if strings.HasSuffix(label, "-") {
			key := strings.TrimSuffix(label, "-")
			escapedKey := strings.ReplaceAll(key, "/", "~1")
			patchOps = append(patchOps, fmt.Sprintf(`{"op":"remove","path":"/metadata/labels/%s"}`, escapedKey))
		} else {
			parts := strings.SplitN(label, "=", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid label format: expected key=value or key-, got %q", label)
			}
			escapedKey := strings.ReplaceAll(parts[0], "/", "~1")
			jsonValue, _ := json.Marshal(parts[1])
			jsonValueStr := string(jsonValue[1 : len(jsonValue)-1])
			patchOps = append(patchOps, fmt.Sprintf(`{"op":"add","path":"/metadata/labels/%s","value":"%s"}`, escapedKey, jsonValueStr))
			needsInit = true
		}
	}

	if len(patchOps) == 0 {
		return fmt.Errorf("no valid labels provided")
	}

	var patch string
	if needsInit {
		patch = fmt.Sprintf(`[{"op":"add","path":"/metadata/labels","value":{}},%s]`, strings.Join(patchOps, ","))
	} else {
		patch = fmt.Sprintf(`[%s]`, strings.Join(patchOps, ","))
	}

	type result struct {
		name string
		err  error
	}

	resChan := make(chan string, len(resourceNames))
	resultChan := make(chan result, len(resourceNames))

	for _, name := range resourceNames {
		resChan <- name
	}
	close(resChan)

	var wg sync.WaitGroup
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name := range resChan {
				ns := namespace
				if !namespaced {
					ns = ""
				}
				var patchErr error
				if ns == "" {
					_, patchErr = dynamicClient.Resource(gvr).Patch(ctx, name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
				} else {
					_, patchErr = dynamicClient.Resource(gvr).Namespace(ns).Patch(ctx, name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
				}
				resultChan <- result{name: name, err: patchErr}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var failedCount int
	for r := range resultChan {
		if r.err != nil {
			fmt.Fprintf(errOut, "Error labeling %s: %v\n", r.name, r.err)
			failedCount++
		} else {
			fmt.Fprintf(out, "%s/%s labeled\n", resourceType, r.name)
		}
	}

	if failedCount > 0 {
		return fmt.Errorf("failed to label %d resource(s)", failedCount)
	}
	return nil
}
