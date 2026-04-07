package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/template"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/jsonpath"
	sigsyaml "sigs.k8s.io/yaml"
)

// outputFormat represents a parsed -o flag value.
type outputFormat struct {
	kind string // "json", "yaml", "name", "wide", "table", "jsonpath", "go-template", "custom-columns"
	arg  string // argument after '=' for jsonpath, go-template, custom-columns
}

// parseOutputFormat parses the -o flag string into an outputFormat.
func parseOutputFormat(output string) outputFormat {
	if output == "" {
		return outputFormat{kind: "table"}
	}

	// Handle formats with arguments: jsonpath=..., go-template=..., custom-columns=...
	for _, prefix := range []string{"jsonpath=", "go-template=", "custom-columns="} {
		if strings.HasPrefix(output, prefix) {
			return outputFormat{
				kind: strings.TrimSuffix(prefix, "="),
				arg:  strings.TrimPrefix(output, prefix),
			}
		}
	}

	// Handle jsonpath-as-json, jsonpath-file, template variants
	if strings.HasPrefix(output, "jsonpath-as-json=") {
		return outputFormat{kind: "jsonpath", arg: strings.TrimPrefix(output, "jsonpath-as-json=")}
	}

	return outputFormat{kind: output}
}

// isTableFormat returns true if the output format requires server-side table rendering.
func (f outputFormat) isTableFormat() bool {
	return f.kind == "table" || f.kind == "wide"
}

// printFormattedObject prints a single runtime.Object in the requested output format.
func printFormattedObject(obj runtime.Object, format outputFormat, out io.Writer) error {
	switch format.kind {
	case "json":
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(obj)
	case "yaml":
		data, err := sigsyaml.Marshal(obj)
		if err != nil {
			return err
		}
		_, err = out.Write(data)
		return err
	case "name":
		metaObj, err := meta.Accessor(obj)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "%s\n", metaObj.GetName())
		return nil
	case "jsonpath":
		return printJSONPath(obj, format.arg, out)
	case "go-template":
		return printGoTemplate(obj, format.arg, out)
	case "custom-columns":
		return printCustomColumns(obj, format.arg, out)
	default:
		// Fallback to YAML for unknown formats
		data, err := sigsyaml.Marshal(obj)
		if err != nil {
			return err
		}
		_, err = out.Write(data)
		return err
	}
}

// printFormattedList prints a list of objects in the requested output format.
func printFormattedList(list runtime.Object, format outputFormat, out io.Writer, allNamespaces bool) error {
	switch format.kind {
	case "json":
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(list)
	case "yaml":
		data, err := sigsyaml.Marshal(list)
		if err != nil {
			return err
		}
		_, err = out.Write(data)
		return err
	case "name":
		items, err := meta.ExtractList(list)
		if err != nil {
			return err
		}
		for _, item := range items {
			accessor, err := meta.Accessor(item)
			if err != nil {
				continue
			}
			if allNamespaces {
				fmt.Fprintf(out, "%s/%s\n", accessor.GetNamespace(), accessor.GetName())
			} else {
				fmt.Fprintf(out, "%s\n", accessor.GetName())
			}
		}
		return nil
	case "jsonpath":
		return printJSONPath(list, format.arg, out)
	case "go-template":
		return printGoTemplate(list, format.arg, out)
	case "custom-columns":
		return printCustomColumnsList(list, format.arg, out)
	default:
		data, err := sigsyaml.Marshal(list)
		if err != nil {
			return err
		}
		_, err = out.Write(data)
		return err
	}
}

// printJSONPath evaluates a jsonpath expression against an object and prints the result.
func printJSONPath(obj runtime.Object, expression string, out io.Writer) error {
	// Strip wrapping quotes if present (users often write -o jsonpath='{...}')
	expression = strings.TrimPrefix(expression, "'")
	expression = strings.TrimSuffix(expression, "'")

	jp := jsonpath.New("output")
	if err := jp.Parse(expression); err != nil {
		return fmt.Errorf("error parsing jsonpath %q: %w", expression, err)
	}
	jp.AllowMissingKeys(true)

	// Convert to unstructured for jsonpath evaluation
	data, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("failed to marshal object: %w", err)
	}

	var generic interface{}
	if err := json.Unmarshal(data, &generic); err != nil {
		return fmt.Errorf("failed to unmarshal object: %w", err)
	}

	var buf bytes.Buffer
	if err := jp.Execute(&buf, generic); err != nil {
		return fmt.Errorf("error executing jsonpath: %w", err)
	}

	fmt.Fprint(out, buf.String())
	if buf.Len() > 0 {
		fmt.Fprintln(out)
	}
	return nil
}

// printGoTemplate evaluates a Go template against an object and prints the result.
func printGoTemplate(obj runtime.Object, tmplStr string, out io.Writer) error {
	// Strip wrapping quotes if present
	tmplStr = strings.TrimPrefix(tmplStr, "'")
	tmplStr = strings.TrimSuffix(tmplStr, "'")

	tmpl, err := template.New("output").Parse(tmplStr)
	if err != nil {
		return fmt.Errorf("error parsing template %q: %w", tmplStr, err)
	}

	// Convert to map for template evaluation
	data, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("failed to marshal object: %w", err)
	}

	var generic interface{}
	if err := json.Unmarshal(data, &generic); err != nil {
		return fmt.Errorf("failed to unmarshal object: %w", err)
	}

	return tmpl.Execute(out, generic)
}

// printCustomColumns prints an object using custom column definitions.
// Format: NAME:.metadata.name,STATUS:.status.phase
func printCustomColumns(obj runtime.Object, spec string, out io.Writer) error {
	columns, err := parseCustomColumns(spec)
	if err != nil {
		return err
	}

	data, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("failed to marshal object: %w", err)
	}

	var generic interface{}
	if err := json.Unmarshal(data, &generic); err != nil {
		return fmt.Errorf("failed to unmarshal object: %w", err)
	}

	// Print header
	headers := make([]string, len(columns))
	for i, col := range columns {
		headers[i] = col.name
	}
	fmt.Fprintln(out, strings.Join(headers, "\t"))

	// Print values
	values := make([]string, len(columns))
	for i, col := range columns {
		values[i] = evaluateJSONPath(generic, col.path)
	}
	fmt.Fprintln(out, strings.Join(values, "\t"))

	return nil
}

// printCustomColumnsList prints a list using custom column definitions.
func printCustomColumnsList(list runtime.Object, spec string, out io.Writer) error {
	columns, err := parseCustomColumns(spec)
	if err != nil {
		return err
	}

	items, err := meta.ExtractList(list)
	if err != nil {
		return fmt.Errorf("failed to extract list items: %w", err)
	}

	// Print header
	headers := make([]string, len(columns))
	for i, col := range columns {
		headers[i] = col.name
	}
	fmt.Fprintln(out, strings.Join(headers, "\t"))

	// Print each item
	for _, item := range items {
		data, err := json.Marshal(item)
		if err != nil {
			continue
		}
		var generic interface{}
		if err := json.Unmarshal(data, &generic); err != nil {
			continue
		}

		values := make([]string, len(columns))
		for i, col := range columns {
			values[i] = evaluateJSONPath(generic, col.path)
		}
		fmt.Fprintln(out, strings.Join(values, "\t"))
	}

	return nil
}

type customColumn struct {
	name string
	path string
}

func parseCustomColumns(spec string) ([]customColumn, error) {
	parts := strings.Split(spec, ",")
	columns := make([]customColumn, 0, len(parts))
	for _, part := range parts {
		kv := strings.SplitN(part, ":", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid custom column spec %q: expected NAME:JSONPATH", part)
		}
		columns = append(columns, customColumn{
			name: strings.TrimSpace(kv[0]),
			path: strings.TrimSpace(kv[1]),
		})
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns specified")
	}
	return columns, nil
}

// evaluateJSONPath evaluates a simple dot-notation jsonpath against a value.
func evaluateJSONPath(obj interface{}, path string) string {
	// Try using the full jsonpath parser for complex expressions
	jp := jsonpath.New("col")
	// Wrap in {  } if not already
	expr := path
	if !strings.HasPrefix(expr, "{") {
		expr = "{" + expr + "}"
	}
	if err := jp.Parse(expr); err != nil {
		return "<error>"
	}
	jp.AllowMissingKeys(true)

	var buf bytes.Buffer
	if err := jp.Execute(&buf, obj); err != nil {
		return "<none>"
	}
	result := buf.String()
	if result == "" {
		return "<none>"
	}
	return result
}
