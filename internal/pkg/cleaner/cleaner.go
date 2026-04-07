package cleaner

import (
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// CleanOptions configures which fields to remove during cleaning.
type CleanOptions struct {
	RemoveStatus          bool
	RemoveOwnerReferences bool
	RemoveFinalizers      bool
	RemoveManagedFields   bool
	RemoveDefaultValues   bool
	KeepStatus            bool
	AnnotationPrefixes    []string // additional annotation prefixes to strip
}

// DefaultOptions returns the default clean options.
func DefaultOptions() CleanOptions {
	return CleanOptions{
		RemoveStatus:          true,
		RemoveOwnerReferences: false, // intentional by default
		RemoveFinalizers:      false, // intentional by default
		RemoveManagedFields:   true,
		RemoveDefaultValues:   true,
	}
}

// noiseAnnotationPrefixes are annotation prefixes that are typically auto-generated noise.
var noiseAnnotationPrefixes = []string{
	"kubectl.kubernetes.io/",
	"deployment.kubernetes.io/",
	"field.cattle.io/",
	"meta.helm.sh/",
	"autopilot.gke.io/",
	"cloud.google.com/",
}

// noiseAnnotationKeys are specific annotation keys to remove.
var noiseAnnotationKeys = []string{
	"kubectl.kubernetes.io/last-applied-configuration",
	"deployment.kubernetes.io/revision",
}

// CleanUnstructuredObject removes unnecessary fields from a Kubernetes resource using default options.
func CleanUnstructuredObject(obj *unstructured.Unstructured) {
	CleanUnstructuredObjectWithOptions(obj, DefaultOptions())
}

// CleanUnstructuredObjectWithOptions removes unnecessary fields using the given options.
func CleanUnstructuredObjectWithOptions(obj *unstructured.Unstructured, opts CleanOptions) {
	// Remove status
	if opts.RemoveStatus && !opts.KeepStatus {
		unstructured.RemoveNestedField(obj.Object, "status")
	}

	// Remove metadata fields
	metadata, found, _ := unstructured.NestedMap(obj.Object, "metadata")
	if found {
		delete(metadata, "uid")
		delete(metadata, "resourceVersion")
		delete(metadata, "generation")
		delete(metadata, "creationTimestamp")
		delete(metadata, "selfLink")

		if opts.RemoveManagedFields {
			delete(metadata, "managedFields")
		}
		if opts.RemoveOwnerReferences {
			delete(metadata, "ownerReferences")
		}
		if opts.RemoveFinalizers {
			delete(metadata, "finalizers")
		}

		// Clean annotations
		if annotations, ok := metadata["annotations"].(map[string]interface{}); ok {
			// Remove specific noise keys
			for _, key := range noiseAnnotationKeys {
				delete(annotations, key)
			}

			// Remove by noise prefixes
			allPrefixes := append(noiseAnnotationPrefixes, opts.AnnotationPrefixes...)
			for k := range annotations {
				for _, prefix := range allPrefixes {
					if strings.HasPrefix(k, prefix) {
						delete(annotations, k)
						break
					}
				}
				// Remove OpenShift generated annotations
				if strings.Contains(k, "openshift.io") && strings.Contains(k, "generated") {
					delete(annotations, k)
				}
			}

			if len(annotations) == 0 {
				delete(metadata, "annotations")
			}
		}

		// Clean empty labels map
		if labels, ok := metadata["labels"].(map[string]interface{}); ok {
			if len(labels) == 0 {
				delete(metadata, "labels")
			}
		}

		obj.Object["metadata"] = metadata
	}

	// Resource-specific cleanups
	if opts.RemoveDefaultValues {
		cleanResourceDefaults(obj)
	}

	// Recursively remove empty maps and slices
	removeEmptyFields(obj.Object)
}

// cleanResourceDefaults removes auto-assigned default values for specific resource types.
func cleanResourceDefaults(obj *unstructured.Unstructured) {
	kind := obj.GetKind()

	switch kind {
	case "Service":
		unstructured.RemoveNestedField(obj.Object, "spec", "clusterIP")
		unstructured.RemoveNestedField(obj.Object, "spec", "clusterIPs")
		// Remove default values
		removeIfValue(obj.Object, "None", "spec", "sessionAffinity")
		removeIfValue(obj.Object, "SingleStack", "spec", "ipFamilyPolicy")
		removeIfValue(obj.Object, "Cluster", "spec", "internalTrafficPolicy")
		unstructured.RemoveNestedField(obj.Object, "spec", "ipFamilies")
		// Remove auto-assigned nodePorts
		removeNodePorts(obj.Object)

	case "Deployment":
		unstructured.RemoveNestedField(obj.Object, "spec", "template", "metadata", "creationTimestamp")
		removeIfValue(obj.Object, "RollingUpdate", "spec", "strategy", "type")
		unstructured.RemoveNestedField(obj.Object, "spec", "revisionHistoryLimit")
		unstructured.RemoveNestedField(obj.Object, "spec", "progressDeadlineSeconds")
		cleanPodSpecDefaults(obj.Object, "spec", "template", "spec")

	case "StatefulSet":
		unstructured.RemoveNestedField(obj.Object, "spec", "template", "metadata", "creationTimestamp")
		unstructured.RemoveNestedField(obj.Object, "spec", "revisionHistoryLimit")
		cleanPodSpecDefaults(obj.Object, "spec", "template", "spec")

	case "DaemonSet":
		unstructured.RemoveNestedField(obj.Object, "spec", "template", "metadata", "creationTimestamp")
		unstructured.RemoveNestedField(obj.Object, "spec", "revisionHistoryLimit")
		cleanPodSpecDefaults(obj.Object, "spec", "template", "spec")

	case "Job":
		unstructured.RemoveNestedField(obj.Object, "spec", "template", "metadata", "creationTimestamp")
		cleanPodSpecDefaults(obj.Object, "spec", "template", "spec")

	case "Pod":
		cleanPodSpecDefaults(obj.Object, "spec")
	}

}

// cleanPodSpecDefaults removes default values from a pod spec at the given path.
func cleanPodSpecDefaults(obj map[string]interface{}, path ...string) {
	podSpec, found, _ := unstructured.NestedMap(obj, path...)
	if !found {
		return
	}

	if v, ok := podSpec["dnsPolicy"].(string); ok && v == "ClusterFirst" {
		delete(podSpec, "dnsPolicy")
	}
	if v, ok := podSpec["restartPolicy"].(string); ok && v == "Always" {
		delete(podSpec, "restartPolicy")
	}
	if v, ok := podSpec["schedulerName"].(string); ok && v == "default-scheduler" {
		delete(podSpec, "schedulerName")
	}
	delete(podSpec, "terminationGracePeriodSeconds")

	// Clean container defaults in-place
	cleanContainersInMap(podSpec, "containers")
	cleanContainersInMap(podSpec, "initContainers")

	// Write back
	_ = unstructured.SetNestedField(obj, podSpec, path...)
}

// cleanContainersInMap removes default values from containers directly in a pod spec map.
func cleanContainersInMap(podSpec map[string]interface{}, key string) {
	containers, ok := podSpec[key].([]interface{})
	if !ok {
		return
	}
	for _, c := range containers {
		container, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if v, ok := container["terminationMessagePath"].(string); ok && v == "/dev/termination-log" {
			delete(container, "terminationMessagePath")
		}
		if v, ok := container["terminationMessagePolicy"].(string); ok && v == "File" {
			delete(container, "terminationMessagePolicy")
		}
		if v, ok := container["imagePullPolicy"].(string); ok {
			image, _ := container["image"].(string)
			if (strings.HasSuffix(image, ":latest") && v == "Always") ||
				(!strings.HasSuffix(image, ":latest") && v == "IfNotPresent") {
				delete(container, "imagePullPolicy")
			}
		}
	}
}

// removeIfValue removes a nested field only if it matches the given value.
func removeIfValue(obj map[string]interface{}, value string, fields ...string) {
	v, found, _ := unstructured.NestedString(obj, fields...)
	if found && v == value {
		unstructured.RemoveNestedField(obj, fields...)
	}
}

// removeNodePorts removes auto-assigned nodePort values from Service ports.
func removeNodePorts(obj map[string]interface{}) {
	ports, found, _ := unstructured.NestedSlice(obj, "spec", "ports")
	if !found {
		return
	}
	for _, p := range ports {
		port, ok := p.(map[string]interface{})
		if !ok {
			continue
		}
		delete(port, "nodePort")
	}
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
