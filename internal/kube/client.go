package kube

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	versionCache     = make(map[string]*version.Info)
	versionCacheMu   sync.RWMutex
)

type configPathKey struct{}
type projectNamespaceKey struct{}

// WithConfigPath returns a context that carries an explicit kubeconfig path.
// When path is empty, the context is returned unchanged.
func WithConfigPath(ctx context.Context, path string) context.Context {
	if path == "" {
		return ctx
	}

	if ctx == nil {
		ctx = context.Background()
	}

	return context.WithValue(ctx, configPathKey{}, path)
}

func configPathFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	if v, ok := ctx.Value(configPathKey{}).(string); ok {
		return v
	}

	return ""
}

// ConfigPathFromContext returns the kubeconfig path from context, if set.
// This is exported for use by other packages.
func ConfigPathFromContext(ctx context.Context) string {
	return configPathFromContext(ctx)
}

// WithProjectNamespace returns a context that carries an explicit project namespace.
// When namespace is empty, the context is returned unchanged.
func WithProjectNamespace(ctx context.Context, namespace string) context.Context {
	if namespace == "" {
		return ctx
	}

	if ctx == nil {
		ctx = context.Background()
	}

	return context.WithValue(ctx, projectNamespaceKey{}, namespace)
}

// ProjectNamespaceFromContext returns the project namespace from context, if set.
func ProjectNamespaceFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	if v, ok := ctx.Value(projectNamespaceKey{}).(string); ok {
		return v
	}

	return ""
}

// NewClientset returns a client-go Clientset either using in-cluster configuration
// or the user's local kubeconfig.
func NewClientset(ctx context.Context) (*kubernetes.Clientset, error) {
	cfg, err := restConfig(ctx)
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(cfg)
}

// NewDynamicClient returns a dynamic Kubernetes client using the same auth resolution as NewClientset.
func NewDynamicClient(ctx context.Context) (dynamic.Interface, error) {
	cfg, err := restConfig(ctx)
	if err != nil {
		return nil, err
	}

	return dynamic.NewForConfig(cfg)
}

// NewDiscoveryClient returns a discovery client for API server resource discovery.
func NewDiscoveryClient(ctx context.Context) (discovery.DiscoveryInterface, error) {
	cfg, err := restConfig(ctx)
	if err != nil {
		return nil, err
	}

	return discovery.NewDiscoveryClientForConfig(cfg)
}

// GetConfig returns the REST config for reuse by other clients.
func GetConfig(ctx context.Context) (*rest.Config, error) {
	return restConfig(ctx)
}

func loadKubeConfig(ctx context.Context) (*rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if explicit := configPathFromContext(ctx); explicit != "" {
		loadingRules.ExplicitPath = explicit
	}

	configOverrides := &clientcmd.ConfigOverrides{}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides).ClientConfig()
}

func restConfig(ctx context.Context) (*rest.Config, error) {
	cfg, err := rest.InClusterConfig()
	if err == nil {
		return cfg, nil
	}

	cfg, err = loadKubeConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to create Kubernetes config: %w", err)
	}

	return cfg, nil
}

// GetCurrentContext returns the current context name and cluster name from kubeconfig
func GetCurrentContext(ctx context.Context) (contextName, clusterName string, err error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if explicit := configPathFromContext(ctx); explicit != "" {
		loadingRules.ExplicitPath = explicit
	}

	configOverrides := &clientcmd.ConfigOverrides{}
	clientConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)

	rawConfig, err := clientConfig.RawConfig()
	if err != nil {
		return "", "", fmt.Errorf("failed to load kubeconfig: %w", err)
	}

	contextName = rawConfig.CurrentContext
	if contextName == "" {
		return "", "", fmt.Errorf("no current context set in kubeconfig")
	}

	currentContext, exists := rawConfig.Contexts[contextName]
	if !exists {
		return "", "", fmt.Errorf("context %q not found in kubeconfig", contextName)
	}

	clusterName = currentContext.Cluster
	return contextName, clusterName, nil
}

// GetCurrentNamespace returns the namespace from the current context in kubeconfig
func GetCurrentNamespace(ctx context.Context) (string, error) {
	// Check context override first
	if ns := ProjectNamespaceFromContext(ctx); ns != "" {
		return ns, nil
	}

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if explicit := configPathFromContext(ctx); explicit != "" {
		loadingRules.ExplicitPath = explicit
	}

	configOverrides := &clientcmd.ConfigOverrides{}
	ns, _, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides).Namespace()
	return ns, err
}

// GetServerVersion returns the Kubernetes server version, with caching
func GetServerVersion(ctx context.Context) (*version.Info, error) {
	// Generate cache key from context
	cacheKey := ""
	if configPath := configPathFromContext(ctx); configPath != "" {
		cacheKey = configPath
	} else {
		cfg, err := GetConfig(ctx)
		if err == nil && cfg.Host != "" {
			cacheKey = cfg.Host
		}
	}
	if cacheKey == "" {
		cacheKey = "default"
	}

	// Check cache
	versionCacheMu.RLock()
	if cached, ok := versionCache[cacheKey]; ok {
		versionCacheMu.RUnlock()
		return cached, nil
	}
	versionCacheMu.RUnlock()

	// Fetch version
	clientset, err := NewClientset(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create clientset: %w", err)
	}

	serverVersion, err := clientset.Discovery().ServerVersion()
	if err != nil {
		return nil, fmt.Errorf("failed to get server version: %w", err)
	}

	// Cache it
	versionCacheMu.Lock()
	versionCache[cacheKey] = serverVersion
	versionCacheMu.Unlock()

	return serverVersion, nil
}

// GetKubernetesVersion returns the major.minor version string (e.g., "1.28")
func GetKubernetesVersion(ctx context.Context) (string, error) {
	version, err := GetServerVersion(ctx)
	if err != nil {
		return "", err
	}

	// Extract major.minor from GitVersion (e.g., "v1.28.0" -> "1.28")
	gitVersion := version.GitVersion
	if len(gitVersion) > 0 && gitVersion[0] == 'v' {
		gitVersion = gitVersion[1:]
	}

	parts := strings.Split(gitVersion, ".")
	if len(parts) >= 2 {
		return fmt.Sprintf("%s.%s", parts[0], parts[1]), nil
	}

	return "", fmt.Errorf("unable to parse version: %s", version.GitVersion)
}
