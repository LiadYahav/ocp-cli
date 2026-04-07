package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func newExplainCommand() *cobra.Command {
	var recursive bool
	var apiVersion string

	cmd := &cobra.Command{
		Use:   "explain <resource[.field...]>",
		Short: "Get documentation for a resource or field",
		Long: `Get documentation for a resource or its fields from the cluster's OpenAPI schema.

Examples:
  # Explain pods
  ocp explain pod

  # Explain a specific field path
  ocp explain pod.spec.containers

  # Recursively show all fields
  ocp explain pod.spec --recursive`,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return completeResourceTypes(cmd, toComplete)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			out := cmd.OutOrStdout()
			resourcePath := args[0]

			parts := strings.SplitN(resourcePath, ".", 2)
			resourceType := parts[0]
			fieldPath := ""
			if len(parts) > 1 {
				fieldPath = parts[1]
			}

			discoveryClient, err := kube.NewDiscoveryClient(ctx)
			if err != nil {
				return fmt.Errorf("failed to create discovery client: %w", err)
			}

			// Resolve resource type to GVR
			resolver, err := getResourceResolver(ctx)
			if err != nil {
				return fmt.Errorf("failed to get resource resolver: %w", err)
			}

			gvr, _, err := resolver.resolveResource(resourceType)
			if err != nil {
				return fmt.Errorf("resource %q not found: %w", resourceType, err)
			}

			if apiVersion != "" {
				avParts := strings.SplitN(apiVersion, "/", 2)
				if len(avParts) == 2 {
					gvr.Group = avParts[0]
					gvr.Version = avParts[1]
				} else {
					gvr.Version = avParts[0]
				}
			}

			// Get kind from discovery
			kind, err := resolveKind(discoveryClient, gvr)
			if err != nil {
				return err
			}

			// Get OpenAPI schema as raw JSON
			return explainFromOpenAPI(discoveryClient, gvr, kind, fieldPath, recursive, out)
		},
	}

	cmd.Flags().BoolVar(&recursive, "recursive", false, "Print fields recursively")
	cmd.Flags().StringVar(&apiVersion, "api-version", "", "API version (e.g. apps/v1)")
	_ = cmd.RegisterFlagCompletionFunc("api-version", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		ctx := cmd.Context()
		if ctx == nil {
			ctx = context.Background()
		}
		dc, err := kube.NewDiscoveryClient(ctx)
		if err != nil {
			return nil, cobra.ShellCompDirectiveError
		}
		groups, err := dc.ServerGroups()
		if err != nil {
			return nil, cobra.ShellCompDirectiveError
		}
		versions := make([]string, 0)
		for _, g := range groups.Groups {
			for _, v := range g.Versions {
				if strings.HasPrefix(v.GroupVersion, toComplete) {
					versions = append(versions, v.GroupVersion)
				}
			}
		}
		return versions, cobra.ShellCompDirectiveNoFileComp
	})

	return cmd
}

func resolveKind(dc discovery.DiscoveryInterface, gvr schema.GroupVersionResource) (string, error) {
	resourceList, err := dc.ServerResourcesForGroupVersion(gvr.GroupVersion().String())
	if err != nil {
		// Best guess
		return strings.ToUpper(gvr.Resource[:1]) + gvr.Resource[1:], nil
	}

	for _, r := range resourceList.APIResources {
		if r.Name == gvr.Resource {
			return r.Kind, nil
		}
	}

	return strings.ToUpper(gvr.Resource[:1]) + gvr.Resource[1:], nil
}

func explainFromOpenAPI(dc discovery.DiscoveryInterface, gvr schema.GroupVersionResource, kind, fieldPath string, recursive bool, out io.Writer) error {
	// Fetch OpenAPI v2 spec
	doc, err := dc.OpenAPISchema()
	if err != nil {
		return fmt.Errorf("failed to get OpenAPI schema: %w", err)
	}

	// Marshal to JSON for easier navigation
	docJSON, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal OpenAPI schema: %w", err)
	}

	var spec map[string]interface{}
	if err := json.Unmarshal(docJSON, &spec); err != nil {
		return fmt.Errorf("failed to parse OpenAPI schema: %w", err)
	}

	definitions, ok := spec["definitions"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("no definitions found in OpenAPI schema")
	}

	// Find the definition key
	defKey := findDefKey(definitions, gvr, kind)
	if defKey == "" {
		return fmt.Errorf("no schema found for %s/%s %s", gvr.Group, gvr.Version, kind)
	}

	def, ok := definitions[defKey].(map[string]interface{})
	if !ok {
		return fmt.Errorf("definition %q is not a valid schema", defKey)
	}

	// Navigate to field path
	if fieldPath != "" {
		var err error
		def, err = navigateFieldPath(definitions, def, fieldPath)
		if err != nil {
			return err
		}
	}

	gv := gvr.Version
	if gvr.Group != "" {
		gv = gvr.Group + "/" + gvr.Version
	}

	if fieldPath == "" {
		fmt.Fprintf(out, "KIND:     %s\n", kind)
		fmt.Fprintf(out, "VERSION:  %s\n\n", gv)
	}

	desc, _ := def["description"].(string)
	if desc != "" {
		fmt.Fprintf(out, "DESCRIPTION:\n")
		for _, line := range wrapText(desc, 76) {
			fmt.Fprintf(out, "  %s\n", line)
		}
		fmt.Fprintln(out)
	}

	printFields(out, definitions, def, recursive, 0)

	return nil
}

func findDefKey(definitions map[string]interface{}, gvr schema.GroupVersionResource, kind string) string {
	group := gvr.Group
	if group == "" {
		group = "core"
	}

	// Common patterns
	patterns := []string{
		fmt.Sprintf("io.k8s.api.%s.%s.%s", group, gvr.Version, kind),
		fmt.Sprintf("io.k8s.api.%s.%s.%s", strings.ReplaceAll(group, ".", "-"), gvr.Version, kind),
	}

	for _, p := range patterns {
		if _, ok := definitions[p]; ok {
			return p
		}
	}

	// Search by suffix match
	suffix := "." + kind
	for key := range definitions {
		if strings.HasSuffix(key, suffix) && strings.Contains(key, gvr.Version) {
			return key
		}
	}

	return ""
}

func navigateFieldPath(definitions map[string]interface{}, def map[string]interface{}, path string) (map[string]interface{}, error) {
	fields := strings.Split(path, ".")

	current := def
	for _, field := range fields {
		props, ok := current["properties"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("field %q has no sub-fields", field)
		}

		prop, ok := props[field].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("field %q not found", field)
		}

		// Follow $ref if present
		if ref, ok := prop["$ref"].(string); ok {
			refKey := strings.TrimPrefix(ref, "#/definitions/")
			if refDef, ok := definitions[refKey].(map[string]interface{}); ok {
				current = refDef
				continue
			}
		}

		// Follow items.$ref for arrays
		if items, ok := prop["items"].(map[string]interface{}); ok {
			if ref, ok := items["$ref"].(string); ok {
				refKey := strings.TrimPrefix(ref, "#/definitions/")
				if refDef, ok := definitions[refKey].(map[string]interface{}); ok {
					current = refDef
					continue
				}
			}
		}

		current = prop
	}

	return current, nil
}

func printFields(out io.Writer, definitions map[string]interface{}, def map[string]interface{}, recursive bool, indent int) {
	props, ok := def["properties"].(map[string]interface{})
	if !ok {
		return
	}

	// Get required fields
	requiredSet := make(map[string]bool)
	if req, ok := def["required"].([]interface{}); ok {
		for _, r := range req {
			if s, ok := r.(string); ok {
				requiredSet[s] = true
			}
		}
	}

	// Sort field names
	names := make([]string, 0, len(props))
	for name := range props {
		names = append(names, name)
	}
	sort.Strings(names)

	prefix := strings.Repeat("  ", indent)

	fmt.Fprintf(out, "%sFIELDS:\n", prefix)
	for _, name := range names {
		prop, ok := props[name].(map[string]interface{})
		if !ok {
			continue
		}

		typeName := getTypeName(prop, definitions)
		required := ""
		if requiredSet[name] {
			required = " -required-"
		}

		fmt.Fprintf(out, "%s  %s\t<%s>%s\n", prefix, name, typeName, required)

		if !recursive {
			desc, _ := prop["description"].(string)
			if desc != "" {
				lines := wrapText(desc, 72-indent*2)
				for _, line := range lines {
					fmt.Fprintf(out, "%s    %s\n", prefix, line)
				}
				fmt.Fprintln(out)
			}
		} else {
			// Recurse into sub-fields
			resolvedDef := resolveRef(prop, definitions)
			if resolvedDef != nil {
				if _, hasProps := resolvedDef["properties"]; hasProps {
					printFields(out, definitions, resolvedDef, recursive, indent+1)
				}
			}
		}
	}
}

func getTypeName(prop map[string]interface{}, definitions map[string]interface{}) string {
	if ref, ok := prop["$ref"].(string); ok {
		refKey := strings.TrimPrefix(ref, "#/definitions/")
		parts := strings.Split(refKey, ".")
		return parts[len(parts)-1]
	}

	t, _ := prop["type"].(string)

	if t == "array" {
		if items, ok := prop["items"].(map[string]interface{}); ok {
			if ref, ok := items["$ref"].(string); ok {
				refKey := strings.TrimPrefix(ref, "#/definitions/")
				parts := strings.Split(refKey, ".")
				return "[]" + parts[len(parts)-1]
			}
			itemType, _ := items["type"].(string)
			return "[]" + itemType
		}
		return "[]object"
	}

	if t == "" {
		return "object"
	}

	return t
}

func resolveRef(prop map[string]interface{}, definitions map[string]interface{}) map[string]interface{} {
	if ref, ok := prop["$ref"].(string); ok {
		refKey := strings.TrimPrefix(ref, "#/definitions/")
		if refDef, ok := definitions[refKey].(map[string]interface{}); ok {
			return refDef
		}
	}

	if items, ok := prop["items"].(map[string]interface{}); ok {
		if ref, ok := items["$ref"].(string); ok {
			refKey := strings.TrimPrefix(ref, "#/definitions/")
			if refDef, ok := definitions[refKey].(map[string]interface{}); ok {
				return refDef
			}
		}
	}

	return prop
}

func wrapText(text string, maxWidth int) []string {
	if maxWidth <= 0 {
		maxWidth = 76
	}

	var lines []string
	for _, paragraph := range strings.Split(text, "\n") {
		if paragraph == "" {
			lines = append(lines, "")
			continue
		}

		words := strings.Fields(paragraph)
		if len(words) == 0 {
			lines = append(lines, "")
			continue
		}

		current := words[0]
		for _, word := range words[1:] {
			if len(current)+1+len(word) > maxWidth {
				lines = append(lines, current)
				current = word
			} else {
				current += " " + word
			}
		}
		lines = append(lines, current)
	}

	return lines
}
