// Portions of this file are based on code from Kubernetes kubectl
// under the Apache License 2.0.
// Source: https://github.com/kubernetes/kubectl
// See LICENSE and NOTICE files in the project root for full license information.

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

const tableAcceptHeader = "application/json;as=Table;g=meta.k8s.io;v=v1"

// tablePrinter fetches and renders server-side table data from the Kubernetes API.
// This replaces all hand-written per-resource table formatters with a single
// generic implementation that works for any resource type.
type tablePrinter struct {
	restConfig *rest.Config
	transport  http.RoundTripper
}

func newTablePrinter(ctx context.Context) (*tablePrinter, error) {
	cfg, err := kube.GetConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get config: %w", err)
	}

	transport, err := rest.TransportFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	return &tablePrinter{
		restConfig: cfg,
		transport:  transport,
	}, nil
}

// resourcePath constructs the REST API path for a resource.
func resourcePath(gvr schema.GroupVersionResource, namespace, name string) string {
	var sb strings.Builder
	if gvr.Group == "" {
		sb.WriteString("/api/")
		sb.WriteString(gvr.Version)
	} else {
		sb.WriteString("/apis/")
		sb.WriteString(gvr.Group)
		sb.WriteString("/")
		sb.WriteString(gvr.Version)
	}
	if namespace != "" {
		sb.WriteString("/namespaces/")
		sb.WriteString(url.PathEscape(namespace))
	}
	sb.WriteString("/")
	sb.WriteString(gvr.Resource)
	if name != "" {
		sb.WriteString("/")
		sb.WriteString(url.PathEscape(name))
	}
	return sb.String()
}

// fetchTable sends a request with the Table accept header and returns the parsed table.
func (p *tablePrinter) fetchTable(ctx context.Context, gvr schema.GroupVersionResource, namespace, name, labelSelector, fieldSelector string) (*metav1.Table, error) {
	u, err := url.Parse(p.restConfig.Host)
	if err != nil {
		return nil, fmt.Errorf("failed to parse host URL: %w", err)
	}
	u.Path = resourcePath(gvr, namespace, name)

	q := u.Query()
	q.Set("includeObject", "Metadata")
	if labelSelector != "" {
		q.Set("labelSelector", labelSelector)
	}
	if fieldSelector != "" {
		q.Set("fieldSelector", fieldSelector)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", tableAcceptHeader)

	client := &http.Client{Transport: p.transport}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		if name != "" {
			return nil, fmt.Errorf("%q not found", name)
		}
		// Try to parse as Status for a better error message
		var status metav1.Status
		if err := json.NewDecoder(resp.Body).Decode(&status); err == nil && status.Message != "" {
			return nil, fmt.Errorf("%s", status.Message)
		}
		return nil, fmt.Errorf("resource not found")
	}

	if resp.StatusCode != http.StatusOK {
		var status metav1.Status
		if err := json.NewDecoder(resp.Body).Decode(&status); err == nil && status.Message != "" {
			return nil, fmt.Errorf("%s", status.Message)
		}
		return nil, fmt.Errorf("API request failed with status %d", resp.StatusCode)
	}

	var table metav1.Table
	if err := json.NewDecoder(resp.Body).Decode(&table); err != nil {
		return nil, fmt.Errorf("failed to decode table response: %w", err)
	}

	return &table, nil
}

type tableRenderOptions struct {
	wide          bool
	allNamespaces bool
	showLabels    bool
	sortBy        string // jsonpath expression for sorting
	noHeaders     bool
}

// renderTable prints a metav1.Table to the writer.
// Default mode shows priority-0 columns; wide shows all columns.
func renderTable(table *metav1.Table, out io.Writer, opts tableRenderOptions) error {
	if len(table.Rows) == 0 {
		fmt.Fprintf(out, "No resources found.\n")
		return nil
	}

	// Sort rows if --sort-by is specified
	if opts.sortBy != "" {
		sortTableRows(table, opts.sortBy)
	}

	// Determine which columns to show based on priority
	var visibleCols []int
	for i, col := range table.ColumnDefinitions {
		if opts.wide || col.Priority == 0 {
			visibleCols = append(visibleCols, i)
		}
	}

	w := tabwriter.NewWriter(out, 1, 8, 3, ' ', 0)

	// Print header
	if !opts.noHeaders {
		var headers []string
		if opts.allNamespaces {
			headers = append(headers, "NAMESPACE")
		}
		for _, idx := range visibleCols {
			headers = append(headers, strings.ToUpper(table.ColumnDefinitions[idx].Name))
		}
		if opts.showLabels {
			headers = append(headers, "LABELS")
		}
		fmt.Fprintln(w, strings.Join(headers, "\t"))
	}

	// Print rows
	for _, row := range table.Rows {
		var cells []string

		if opts.allNamespaces {
			ns := extractNamespaceFromRow(row)
			cells = append(cells, ns)
		}

		for _, idx := range visibleCols {
			if idx < len(row.Cells) {
				cells = append(cells, fmt.Sprintf("%v", row.Cells[idx]))
			} else {
				cells = append(cells, "")
			}
		}

		if opts.showLabels {
			labels := extractLabelsFromRow(row)
			cells = append(cells, labels)
		}

		fmt.Fprintln(w, strings.Join(cells, "\t"))
	}

	return w.Flush()
}

// extractNamespaceFromRow extracts the namespace from a TableRow's embedded partial object metadata.
func extractNamespaceFromRow(row metav1.TableRow) string {
	if row.Object.Raw != nil {
		var obj map[string]interface{}
		if err := json.Unmarshal(row.Object.Raw, &obj); err == nil {
			if metadata, ok := obj["metadata"].(map[string]interface{}); ok {
				if ns, ok := metadata["namespace"].(string); ok {
					return ns
				}
			}
		}
	}
	return ""
}

// extractLabelsFromRow extracts labels from a TableRow's embedded partial object metadata.
func extractLabelsFromRow(row metav1.TableRow) string {
	if row.Object.Raw != nil {
		var obj map[string]interface{}
		if err := json.Unmarshal(row.Object.Raw, &obj); err == nil {
			if metadata, ok := obj["metadata"].(map[string]interface{}); ok {
				if labels, ok := metadata["labels"].(map[string]interface{}); ok {
					pairs := make([]string, 0, len(labels))
					for k, v := range labels {
						pairs = append(pairs, fmt.Sprintf("%s=%v", k, v))
					}
					sort.Strings(pairs)
					if len(pairs) > 0 {
						return strings.Join(pairs, ",")
					}
				}
			}
		}
	}
	return "<none>"
}

// sortTableRows sorts table rows by a jsonpath expression applied to the embedded object.
func sortTableRows(table *metav1.Table, sortBy string) {
	// Strip leading dot if present
	path := strings.TrimPrefix(sortBy, ".")

	sort.SliceStable(table.Rows, func(i, j int) bool {
		vi := extractFieldFromRow(table.Rows[i], path)
		vj := extractFieldFromRow(table.Rows[j], path)
		return vi < vj
	})
}

// extractFieldFromRow extracts a field value from a TableRow's embedded object using a dot-path.
func extractFieldFromRow(row metav1.TableRow, path string) string {
	if row.Object.Raw == nil {
		return ""
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(row.Object.Raw, &obj); err != nil {
		return ""
	}

	parts := strings.Split(path, ".")
	var current interface{} = obj
	for _, part := range parts {
		if part == "" {
			continue
		}
		m, ok := current.(map[string]interface{})
		if !ok {
			return ""
		}
		current = m[part]
	}

	if current == nil {
		return ""
	}
	return fmt.Sprintf("%v", current)
}

// buildFallbackTable constructs a metav1.Table from an UnstructuredList when
// server-side table printing is unavailable (e.g., some CRDs/OpenShift resources).
func buildFallbackTable(items []map[string]interface{}, namespaced bool) *metav1.Table {
	table := &metav1.Table{}

	// Define columns
	table.ColumnDefinitions = []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string", Priority: 0},
	}
	if namespaced {
		table.ColumnDefinitions = append(table.ColumnDefinitions,
			metav1.TableColumnDefinition{Name: "Namespace", Type: "string", Priority: 1},
		)
	}

	// Check if any item has status.phase or status.conditions
	hasPhase := false
	hasConditions := false
	for _, item := range items {
		if status, ok := item["status"].(map[string]interface{}); ok {
			if _, ok := status["phase"].(string); ok {
				hasPhase = true
			}
			if conds, ok := status["conditions"].([]interface{}); ok && len(conds) > 0 {
				hasConditions = true
			}
		}
	}

	if hasPhase {
		table.ColumnDefinitions = append(table.ColumnDefinitions,
			metav1.TableColumnDefinition{Name: "Status", Type: "string", Priority: 0},
		)
	} else if hasConditions {
		table.ColumnDefinitions = append(table.ColumnDefinitions,
			metav1.TableColumnDefinition{Name: "Status", Type: "string", Priority: 0},
		)
	}

	table.ColumnDefinitions = append(table.ColumnDefinitions,
		metav1.TableColumnDefinition{Name: "Age", Type: "string", Priority: 0},
	)

	for _, item := range items {
		metadata, _ := item["metadata"].(map[string]interface{})
		if metadata == nil {
			continue
		}

		name, _ := metadata["name"].(string)
		namespace, _ := metadata["namespace"].(string)

		// Compute age
		age := "<unknown>"
		if ts, ok := metadata["creationTimestamp"].(string); ok {
			if t, err := time.Parse(time.RFC3339, ts); err == nil {
				age = formatAge(t)
			}
		}

		// Extract status
		statusStr := ""
		if hasPhase || hasConditions {
			if status, ok := item["status"].(map[string]interface{}); ok {
				if phase, ok := status["phase"].(string); ok && phase != "" {
					statusStr = phase
				} else if conds, ok := status["conditions"].([]interface{}); ok && len(conds) > 0 {
					if cond, ok := conds[0].(map[string]interface{}); ok {
						condType, _ := cond["type"].(string)
						condStatus, _ := cond["status"].(string)
						if condStatus == "True" {
							statusStr = condType
						} else {
							statusStr = "Not" + condType
						}
					}
				}
			}
		}

		cells := []interface{}{name}
		if namespaced {
			cells = append(cells, namespace)
		}
		if hasPhase || hasConditions {
			cells = append(cells, statusStr)
		}
		cells = append(cells, age)

		row := metav1.TableRow{Cells: cells}
		table.Rows = append(table.Rows, row)
	}

	return table
}

