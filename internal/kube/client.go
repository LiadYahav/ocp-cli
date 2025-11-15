package kube

import (
	"context"
	"fmt"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
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
