// Portions of this file are based on code from Kubernetes kubectl
// under the Apache License 2.0.
// Source: https://github.com/kubernetes/kubectl
// See LICENSE and NOTICE files in the project root for full license information.

package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	discoverycached "k8s.io/client-go/discovery/cached"
	"k8s.io/client-go/restmapper"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

// resourceResolver provides dynamic resource discovery and resolution
type resourceResolver struct {
	discoveryClient discovery.DiscoveryInterface
	mapper          meta.RESTMapper
	resourceCache   map[string]*resourceInfo
	mu              sync.RWMutex
	discoverOnce    sync.Once
	discoverErr     error
	cacheKey        string
	createdAt       time.Time
}

type resourceInfo struct {
	gvr           schema.GroupVersionResource
	namespaced    bool
	kind          string
	resourceName  string   // singular form
	resourceNames []string // plural forms and aliases
}

const (
	cacheTTL             = 5 * time.Minute
	cacheCleanupInterval = 1 * time.Minute
)

var (
	resolverCache = make(map[string]*resourceResolver)
	resolverMu    sync.RWMutex
	lastCleanup   time.Time
	cleanupMu     sync.Mutex
)

// getCacheKey generates a stable cache key from the context (kubeconfig path + cluster endpoint)
func getCacheKey(ctx context.Context) (string, error) {
	cfg, err := kube.GetConfig(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get config: %w", err)
	}

	keyParts := []string{}
	if configPath := kube.ConfigPathFromContext(ctx); configPath != "" {
		keyParts = append(keyParts, configPath)
	}
	if cfg.Host != "" {
		keyParts = append(keyParts, cfg.Host)
	}
	if len(keyParts) == 0 {
		keyParts = append(keyParts, "in-cluster")
	}

	key := strings.Join(keyParts, "|")
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:]), nil
}

func cleanupExpiredResolvers() {
	cleanupMu.Lock()
	defer cleanupMu.Unlock()

	now := time.Now()
	if now.Sub(lastCleanup) < cacheCleanupInterval {
		return
	}
	lastCleanup = now

	resolverMu.Lock()
	defer resolverMu.Unlock()

	for key, resolver := range resolverCache {
		if now.Sub(resolver.createdAt) > cacheTTL {
			delete(resolverCache, key)
		}
	}
}

func getResourceResolver(ctx context.Context) (*resourceResolver, error) {
	cacheKey, err := getCacheKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to generate cache key: %w", err)
	}

	cleanupExpiredResolvers()

	resolverMu.RLock()
	if resolver, ok := resolverCache[cacheKey]; ok {
		if time.Since(resolver.createdAt) < cacheTTL {
			resolverMu.RUnlock()
			return resolver, nil
		}
		resolverMu.RUnlock()
		resolverMu.Lock()
		delete(resolverCache, cacheKey)
		resolverMu.Unlock()
	} else {
		resolverMu.RUnlock()
	}

	discoveryClient, err := kube.NewDiscoveryClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create discovery client: %w", err)
	}

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(discoverycached.NewMemCacheClient(discoveryClient))

	resolver := &resourceResolver{
		discoveryClient: discoveryClient,
		mapper:          mapper,
		resourceCache:   make(map[string]*resourceInfo),
		cacheKey:        cacheKey,
		createdAt:       time.Now(),
	}

	resolverMu.Lock()
	resolverCache[cacheKey] = resolver
	resolverMu.Unlock()

	return resolver, nil
}

func (r *resourceResolver) discoverResources() ([]string, error) {
	r.discoverOnce.Do(func() {
		r.discoverErr = r.doDiscoverResources()
	})

	if r.discoverErr != nil {
		return nil, r.discoverErr
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	resources := make([]string, 0, len(r.resourceCache)*2)
	for _, info := range r.resourceCache {
		resources = append(resources, info.resourceNames...)
	}
	return resources, nil
}

func (r *resourceResolver) doDiscoverResources() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.resourceCache) > 0 {
		return nil
	}

	_, apiResourceLists, err := r.discoveryClient.ServerGroupsAndResources()
	if err != nil {
		return err
	}

	resourceMap := make(map[string]*resourceInfo)

	for _, apiResourceList := range apiResourceLists {
		gv, err := schema.ParseGroupVersion(apiResourceList.GroupVersion)
		if err != nil {
			continue
		}

		for _, apiResource := range apiResourceList.APIResources {
			if strings.Contains(apiResource.Name, "/") {
				continue
			}
			if !containsString(apiResource.Verbs, "get") && !containsString(apiResource.Verbs, "list") {
				continue
			}

			gvr := schema.GroupVersionResource{
				Group:    gv.Group,
				Version:  gv.Version,
				Resource: apiResource.Name,
			}

			pluralName := apiResource.Name
			resourceNames := []string{pluralName}

			if apiResource.SingularName != "" && apiResource.SingularName != pluralName {
				resourceNames = append(resourceNames, apiResource.SingularName)
			}
			resourceNames = append(resourceNames, apiResource.ShortNames...)

			if existing, exists := resourceMap[pluralName]; exists {
				existing.resourceNames = append(existing.resourceNames, resourceNames...)
			} else {
				resourceMap[pluralName] = &resourceInfo{
					gvr:           gvr,
					namespaced:    apiResource.Namespaced,
					kind:          apiResource.Kind,
					resourceName:  apiResource.SingularName,
					resourceNames: resourceNames,
				}
			}
		}
	}

	// Common aliases
	aliases := map[string]string{
		"mcp": "machineconfigpools", "machineconfigpool": "machineconfigpools",
		"crd": "customresourcedefinitions", "customresourcedefinition": "customresourcedefinitions",
		"cs": "componentstatuses", "componentstatus": "componentstatuses",
		"csr": "certificatesigningrequests", "certificatesigningrequest": "certificatesigningrequests",
		"pv": "persistentvolumes", "persistentvolume": "persistentvolumes",
		"pvc": "persistentvolumeclaims", "persistentvolumeclaim": "persistentvolumeclaims",
		"sc": "storageclasses", "storageclass": "storageclasses",
		"sa": "serviceaccounts", "serviceaccount": "serviceaccounts",
		"cm": "configmaps", "configmap": "configmaps",
		"secret": "secrets",
		"ing": "ingresses", "ingress": "ingresses",
		"ingressclass": "ingressclasses",
		"np": "networkpolicies", "networkpolicy": "networkpolicies",
		"pdb": "poddisruptionbudgets", "poddisruptionbudget": "poddisruptionbudgets",
		"hpa": "horizontalpodautoscalers", "horizontalpodautoscaler": "horizontalpodautoscalers",
		"vpa": "verticalpodautoscalers", "verticalpodautoscaler": "verticalpodautoscalers",
		"cronjob": "cronjobs", "cj": "cronjobs",
		"sts": "statefulsets", "statefulset": "statefulsets",
		"ds": "daemonsets", "daemonset": "daemonsets",
		"rs": "replicasets", "replicaset": "replicasets",
		"rc": "replicationcontrollers", "replicationcontroller": "replicationcontrollers",
		"ep": "endpoints", "endpoint": "endpoints",
		"epslice": "endpointslices", "endpointslice": "endpointslices",
		"co": "clusteroperators", "clusteroperator": "clusteroperators",
		"cv": "clusterversions", "clusterversion": "clusterversions",
		"route": "routes",
		"bc": "buildconfigs", "buildconfig": "buildconfigs",
		"build": "builds",
		"dc": "deploymentconfigs", "deploymentconfig": "deploymentconfigs",
		"is": "imagestreams", "imagestream": "imagestreams",
		"istag": "imagestreamtags", "imagestreamtag": "imagestreamtags",
		"isimage": "imagestreamimages", "imagestreamimage": "imagestreamimages",
		"template": "templates",
		"project": "projects",
		"crq": "clusterresourcequotas", "clusterresourcequota": "clusterresourcequotas",
		"scc": "securitycontextconstraints", "securitycontextconstraint": "securitycontextconstraints",
		"nad": "networkattachmentdefinitions", "networkattachmentdefinition": "networkattachmentdefinitions",
	}

	for alias, target := range aliases {
		if existing, exists := resourceMap[target]; exists {
			found := false
			for _, name := range existing.resourceNames {
				if strings.EqualFold(name, alias) {
					found = true
					break
				}
			}
			if !found {
				existing.resourceNames = append(existing.resourceNames, alias)
			}
		}
	}

	r.resourceCache = resourceMap
	return nil
}

func (r *resourceResolver) resolveResource(resourceType string) (schema.GroupVersionResource, bool, error) {
	if strings.EqualFold(resourceType, "all") {
		return schema.GroupVersionResource{}, false, fmt.Errorf("resource type %q is a special keyword - use 'ocp get all' to list common resources", resourceType)
	}

	r.mu.RLock()
	cacheLen := len(r.resourceCache)
	r.mu.RUnlock()

	if cacheLen == 0 {
		_, err := r.discoverResources()
		if err != nil {
			return schema.GroupVersionResource{}, false, fmt.Errorf("failed to discover resources: %w", err)
		}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	normalizedType := strings.ToLower(strings.TrimSpace(resourceType))

	for _, info := range r.resourceCache {
		for _, name := range info.resourceNames {
			if strings.EqualFold(name, normalizedType) {
				return info.gvr, info.namespaced, nil
			}
		}
	}

	// Fallback to REST mapper
	gvr, err := r.mapper.ResourceFor(schema.GroupVersionResource{Resource: resourceType})
	if err == nil {
		mapping, err := r.mapper.RESTMapping(schema.GroupKind{Group: gvr.Group, Kind: ""}, gvr.Version)
		if err == nil {
			return gvr, mapping.Scope.Name() == meta.RESTScopeNameNamespace, nil
		}
		return gvr, true, nil
	}

	return schema.GroupVersionResource{}, false, fmt.Errorf("resource type %q not found in cluster", resourceType)
}

func containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
