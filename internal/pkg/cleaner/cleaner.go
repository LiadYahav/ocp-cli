package cleaner

import (
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// CleanUnstructuredObject removes unnecessary fields from a Kubernetes resource
func CleanUnstructuredObject(obj *unstructured.Unstructured) {
	// Remove status
	unstructured.RemoveNestedField(obj.Object, "status")

	// Remove metadata fields
	metadata, found, _ := unstructured.NestedMap(obj.Object, "metadata")
	if found {
		delete(metadata, "uid")
		delete(metadata, "resourceVersion")
		delete(metadata, "generation")
		delete(metadata, "creationTimestamp")
		delete(metadata, "managedFields")
		delete(metadata, "selfLink")
		delete(metadata, "ownerReferences")
		delete(metadata, "finalizers") // Often we want to remove finalizers for clean YAMLs
		
		// Clean annotations
		if annotations, ok := metadata["annotations"].(map[string]interface{}); ok {
			delete(annotations, "kubectl.kubernetes.io/last-applied-configuration")
			delete(annotations, "deployment.kubernetes.io/revision")
			
			// Remove OpenShift specific annotations that are often noise
			for k := range annotations {
				if strings.Contains(k, "openshift.io") && strings.Contains(k, "generated") {
					delete(annotations, k)
				}
			}
			
			if len(annotations) == 0 {
				delete(metadata, "annotations")
			}
		}

		obj.Object["metadata"] = metadata
	}

	// Remove specific specs that are often auto-generated
	// Service ClusterIPs
	if obj.GetKind() == "Service" {
		unstructured.RemoveNestedField(obj.Object, "spec", "clusterIP")
		unstructured.RemoveNestedField(obj.Object, "spec", "clusterIPs")
	}

	// Recursively remove empty maps and slices
	removeEmptyFields(obj.Object)
}

func removeEmptyFields(obj interface{}) {
	switch v := obj.(type) {
	case map[string]interface{}:
		for key, val := range v {
			removeEmptyFields(val)
			if valMap, ok := val.(map[string]interface{}); ok && len(valMap) == 0 {
				delete(v, key)
			} else if valSlice, ok := val.([]interface{}); ok && len(valSlice) == 0 {
				delete(v, key)
			} else if valStr, ok := val.(string); ok && valStr == "" {
				// Optional: preserve empty strings or remove them?
				// For generic cleaning, often we keep them if they are significant,
				// but for "clearyml" usually we want to remove nulls/empty things.
				// Keeping empty strings for now as they might be important (e.g. env vars).
			} else if val == nil {
				delete(v, key)
			}
		}
	case []interface{}:
		for _, item := range v {
			removeEmptyFields(item)
		}
	}
}

