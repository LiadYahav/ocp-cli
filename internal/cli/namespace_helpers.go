package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

func resolveNamespace(ctx context.Context, explicitNS string, allNamespaces bool) string {
	if allNamespaces {
		return ""
	}
	if explicitNS != "" {
		return explicitNS
	}
	return getCurrentNamespace(ctx)
}

// normalizeResourceType resolves a resource type alias to its canonical plural form.
func normalizeResourceType(ctx context.Context, resourceType string) string {
	resolver, err := getResourceResolver(ctx)
	if err != nil {
		return resourceType
	}

	_, _, err = resolver.resolveResource(resourceType)
	if err != nil {
		return resourceType
	}

	resolver.mu.RLock()
	defer resolver.mu.RUnlock()

	for _, info := range resolver.resourceCache {
		for _, name := range info.resourceNames {
			if strings.EqualFold(name, resourceType) {
				if len(info.resourceNames) > 0 {
					return info.resourceNames[0]
				}
			}
		}
	}

	return resourceType
}

// getOpenShiftGVR returns the GroupVersionResource for an OpenShift resource type.
func getOpenShiftGVR(resourceType string) *schema.GroupVersionResource {
	gvrMap := map[string]schema.GroupVersionResource{
		"routes":                       {Group: "route.openshift.io", Version: "v1", Resource: "routes"},
		"buildconfigs":                 {Group: "build.openshift.io", Version: "v1", Resource: "buildconfigs"},
		"builds":                       {Group: "build.openshift.io", Version: "v1", Resource: "builds"},
		"deploymentconfigs":            {Group: "apps.openshift.io", Version: "v1", Resource: "deploymentconfigs"},
		"imagestreams":                 {Group: "image.openshift.io", Version: "v1", Resource: "imagestreams"},
		"imagestreamtags":              {Group: "image.openshift.io", Version: "v1", Resource: "imagestreamtags"},
		"imagestreamimages":            {Group: "image.openshift.io", Version: "v1", Resource: "imagestreamimages"},
		"templates":                    {Group: "template.openshift.io", Version: "v1", Resource: "templates"},
		"projects":                     {Group: "project.openshift.io", Version: "v1", Resource: "projects"},
		"clusterresourcequotas":        {Group: "quota.openshift.io", Version: "v1", Resource: "clusterresourcequotas"},
		"securitycontextconstraints":   {Group: "security.openshift.io", Version: "v1", Resource: "securitycontextconstraints"},
		"networkattachmentdefinitions": {Group: "k8s.cni.cncf.io", Version: "v1", Resource: "networkattachmentdefinitions"},
		"clusteroperators":             {Group: "config.openshift.io", Version: "v1", Resource: "clusteroperators"},
		"clusterversions":              {Group: "config.openshift.io", Version: "v1", Resource: "clusterversions"},
		"machineconfigpools":           {Group: "machineconfiguration.openshift.io", Version: "v1", Resource: "machineconfigpools"},
	}

	if gvr, ok := gvrMap[resourceType]; ok {
		return &gvr
	}
	return nil
}

// isClusterScopedResource returns true if the resource is cluster-scoped.
func isClusterScopedResource(resourceType string) bool {
	clusterScoped := map[string]bool{
		"nodes": true, "namespaces": true, "persistentvolumes": true,
		"clusterroles": true, "clusterrolebindings": true,
		"machineconfigpools": true, "clusteroperators": true,
		"clusterversions": true, "clusterresourcequotas": true,
		"securitycontextconstraints": true, "networkattachmentdefinitions": true,
	}
	return clusterScoped[resourceType]
}

// formatAge formats a time.Time as a human-readable age string.
func formatAge(t time.Time) string {
	duration := time.Since(t)
	if duration < 0 {
		return "<unknown>"
	}

	days := int(duration.Hours() / 24)
	hours := int(duration.Hours()) % 24
	minutes := int(duration.Minutes()) % 60
	seconds := int(duration.Seconds()) % 60

	if days > 365 {
		years := days / 365
		remainingDays := days % 365
		return fmt.Sprintf("%dy%dd", years, remainingDays)
	}
	if days > 0 {
		return fmt.Sprintf("%dd%dh", days, hours)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh%dm", hours, minutes)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
}

// formatDuration formats a time.Duration as a human-readable string.
func formatDuration(d time.Duration) string {
	if d < 0 {
		return "<unknown>"
	}

	days := int(d.Hours() / 24)
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if days > 0 {
		return fmt.Sprintf("%dd%dh", days, hours)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh%dm", hours, minutes)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
}

// getCurrentNamespaceFromKube returns the current namespace from kubeconfig.
func getCurrentNamespaceFromKube(ctx context.Context) string {
	ns, err := kube.GetCurrentNamespace(ctx)
	if err != nil || ns == "" {
		return "default"
	}
	return ns
}
