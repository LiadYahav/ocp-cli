# OCP CLI Architecture

This document describes the architecture of the OCP CLI and provides guidance for converting it to a web application.

## Architecture Overview

The OCP CLI follows a layered architecture designed to separate concerns and enable easy conversion to a web application:

```
┌─────────────────────────────────────┐
│   CLI Layer (cmd/ocp, internal/cli) │
│   - Cobra commands                   │
│   - Flag parsing                     │
│   - Output formatting                │
└────────────────┬────────────────────┘
                 │
┌────────────────▼────────────────────┐
│   Business Logic Layer               │
│   - Resource operations              │
│   - RBAC management                  │
│   - Node management                  │
│   - Cluster operations               │
└────────────────┬────────────────────┘
                 │
┌────────────────▼────────────────────┐
│   Kubernetes Client Layer (kube/)   │
│   - Client creation                  │
│   - Context management               │
│   - API server communication         │
└────────────────┬────────────────────┘
                 │
┌────────────────▼────────────────────┐
│   Kubernetes API Server              │
└──────────────────────────────────────┘
```

## Directory Structure

```
ocp-cli/
├── cmd/ocp/                    # Main entry point
│   └── main.go                 # CLI bootstrap
├── internal/
│   ├── cli/                    # CLI commands and presentation
│   │   ├── cli.go              # Command registration
│   │   ├── kubectl.go          # Generic resource commands
│   │   ├── binding.go          # RBAC management
│   │   ├── node.go             # Node operations
│   │   ├── cluster.go          # Cluster operations
│   │   ├── mcp.go              # MCP management
│   │   ├── ssh.go              # SSH operations
│   │   ├── project.go          # Namespace management
│   │   └── completion.go       # Shell completion
│   └── kube/                   # Kubernetes client abstraction
│       └── client.go           # Client creation and config
└── go.mod                      # Dependencies
```

## Key Design Patterns

### 1. Context-Based Configuration

All operations use `context.Context` to pass configuration:

```go
// Kubeconfig path is stored in context
ctx = kube.WithConfigPath(ctx, configPath)

// Project namespace is stored in context
ctx = kube.WithProjectNamespace(ctx, namespace)

// Retrieve values from context
configPath := kube.ConfigPathFromContext(ctx)
namespace := kube.ProjectNamespaceFromContext(ctx)
```

**Web App Benefit**: Context can carry HTTP request metadata, user authentication, and session information.

### 2. Client Factory Pattern

Kubernetes clients are created through factory functions:

```go
// Create typed clientset
clientset, err := kube.NewClientset(ctx)

// Create dynamic client for generic resources
dynamicClient, err := kube.NewDynamicClient(ctx)

// Create discovery client for resource discovery
discoveryClient, err := kube.NewDiscoveryClient(ctx)
```

**Web App Benefit**: Easy to mock for testing, and can be enhanced with connection pooling and caching.

### 3. Resource Discovery and Resolution

Resources are dynamically discovered from the API server:

```go
// Get resource resolver (cached per cluster)
resolver, err := getResourceResolver(ctx)

// Discover all available resources
resources, err := resolver.discoverResources()

// Resolve resource type to GVR (GroupVersionResource)
info, err := resolver.resolveResource(ctx, resourceType)
```

**Web App Benefit**: No hardcoded resource types, automatically supports new CRDs and API versions.

### 4. Separation of Concerns

Each command file focuses on a specific domain:
- `kubectl.go`: Generic Kubernetes resource operations
- `binding.go`: RBAC-specific operations
- `node.go`: Node-specific operations
- `cluster.go`: Cluster-wide operations

**Web App Benefit**: Each file can become a separate HTTP handler or API endpoint.

## Converting to Web Application

### Step 1: Extract Business Logic

Create a service layer that separates business logic from CLI presentation:

```go
// Example: Extract pod listing logic
package services

type PodService struct {
    ctx context.Context
}

func (s *PodService) ListPods(namespace string, selector string) ([]corev1.Pod, error) {
    clientset, err := kube.NewClientset(s.ctx)
    if err != nil {
        return nil, err
    }
    
    listOptions := metav1.ListOptions{}
    if selector != "" {
        listOptions.LabelSelector = selector
    }
    
    podList, err := clientset.CoreV1().Pods(namespace).List(s.ctx, listOptions)
    if err != nil {
        return nil, err
    }
    
    return podList.Items, nil
}
```

### Step 2: Create HTTP Handlers

Wrap service calls in HTTP handlers:

```go
package handlers

func (h *Handler) ListPods(w http.ResponseWriter, r *http.Request) {
    namespace := r.URL.Query().Get("namespace")
    selector := r.URL.Query().Get("selector")
    
    service := services.PodService{ctx: r.Context()}
    pods, err := service.ListPods(namespace, selector)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    
    json.NewEncoder(w).Encode(pods)
}
```

### Step 3: Add Authentication and Authorization

Use middleware for auth:

```go
func AuthMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // Extract JWT token or session
        token := r.Header.Get("Authorization")
        
        // Validate token
        user, err := validateToken(token)
        if err != nil {
            http.Error(w, "Unauthorized", http.StatusUnauthorized)
            return
        }
        
        // Add user to context
        ctx := context.WithValue(r.Context(), "user", user)
        next.ServeHTTP(w, r.WithContext(ctx))
    })
}
```

### Step 4: Handle Multiple Clusters

Support multiple kubeconfig contexts:

```go
// Store cluster configurations in database
type ClusterConfig struct {
    ID         string
    Name       string
    Kubeconfig []byte
    Context    string
}

// Create context-specific client
func NewClientForCluster(ctx context.Context, clusterID string) (*kubernetes.Clientset, error) {
    config, err := loadClusterConfig(clusterID)
    if err != nil {
        return nil, err
    }
    
    // Parse kubeconfig and create client
    clientConfig, err := clientcmd.NewClientConfigFromBytes(config.Kubeconfig)
    if err != nil {
        return nil, err
    }
    
    restConfig, err := clientConfig.ClientConfig()
    if err != nil {
        return nil, err
    }
    
    return kubernetes.NewForConfig(restConfig)
}
```

## Web App Architecture Example

```
┌─────────────────────────────────────┐
│   Frontend (React/Vue/Angular)      │
│   - Dashboard                        │
│   - Resource lists                   │
│   - Forms                            │
└────────────────┬────────────────────┘
                 │ HTTP/WebSocket
┌────────────────▼────────────────────┐
│   API Layer (Go HTTP Server)        │
│   - REST endpoints                   │
│   - WebSocket for real-time updates │
│   - Authentication middleware        │
└────────────────┬────────────────────┘
                 │
┌────────────────▼────────────────────┐
│   Service Layer (Business Logic)    │
│   - Extracted from CLI commands      │
│   - Returns structured data          │
└────────────────┬────────────────────┘
                 │
┌────────────────▼────────────────────┐
│   Kubernetes Client Layer            │
│   - Same as CLI (reused!)            │
│   - Multi-cluster support            │
└──────────────────────────────────────┘
```

## Current Web-Ready Features

### ✅ Already Web-Ready

1. **Context-based configuration**: All operations use context for configuration
2. **Client factory pattern**: Easy to create clients for different clusters
3. **Error handling**: Consistent error types that can be serialized to JSON
4. **Resource discovery**: Dynamic resource discovery works in any environment
5. **Concurrent operations**: Already uses goroutines and channels effectively
6. **Structured data**: Most operations return structured Go types (not just strings)

### ⚠️ Needs Refactoring for Web

1. **Output formatting**: Currently writes directly to `io.Writer`
   - **Solution**: Extract formatting functions to return structured data
   - **Example**: Instead of `printPodsTable(pods, os.Stdout)`, return `[]PodTableRow`

2. **Interactive prompts**: Commands like `node drain` prompt for confirmation
   - **Solution**: Add `force` flags or return confirmation requirements to caller

3. **SSH operations**: Direct SSH connections from CLI
   - **Solution**: Move to background jobs with status tracking

4. **File operations**: Config files stored in user home directory
   - **Solution**: Use database or cloud storage for multi-user web app

## Security Considerations for Web App

1. **Multi-tenancy**: Isolate cluster access per user/organization
2. **RBAC**: Respect Kubernetes RBAC - don't bypass it
3. **Audit logging**: Log all operations with user attribution
4. **Secret handling**: Never expose kubeconfig files directly to frontend
5. **Rate limiting**: Prevent API abuse
6. **Session management**: Secure token storage and validation

## Recommended Tech Stack for Web App

### Backend
- **Framework**: Go with `net/http` or `gin-gonic/gin`
- **Authentication**: JWT tokens or OAuth2
- **Database**: PostgreSQL (for storing cluster configs, audit logs)
- **Real-time**: WebSocket for live updates (pod logs, events)
- **Background jobs**: For long-running operations (node drain, cluster backup)

### Frontend
- **Framework**: React, Vue, or Angular
- **State management**: Redux, Vuex, or similar
- **Real-time**: WebSocket client for live data
- **UI Components**: Ant Design, Material-UI, or Kubernetes Dashboard components

### Deployment
- **Container**: Docker image with Go binary
- **Orchestration**: Kubernetes Deployment
- **Ingress**: NGINX Ingress or Traefik
- **TLS**: Cert-manager for automatic certificates

## Migration Path

### Phase 1: Service Layer (1-2 weeks)
- Extract business logic from CLI commands into service package
- Add unit tests for services
- Ensure services return structured data, not formatted output

### Phase 2: API Layer (1-2 weeks)
- Create HTTP handlers wrapping services
- Add authentication middleware
- Implement REST endpoints for common operations

### Phase 3: Frontend (3-4 weeks)
- Build dashboard UI
- Implement resource list views
- Add forms for create/edit operations
- Integrate WebSocket for real-time updates

### Phase 4: Advanced Features (2-3 weeks)
- Multi-cluster support
- User management and RBAC
- Audit logging
- Metrics and monitoring

## Testing Strategy

### CLI Testing
- Unit tests for business logic
- Integration tests with test clusters
- Table-driven tests for parsers and formatters

### Web App Testing
- API endpoint tests (integration)
- Frontend unit tests (Jest, Vue Test Utils)
- E2E tests (Cypress, Playwright)
- Load testing for concurrent operations

## Performance Considerations

1. **Caching**: Cache resource discoveries and API server metadata
2. **Connection pooling**: Reuse Kubernetes client connections
3. **Pagination**: Support pagination for large resource lists
4. **Streaming**: Stream logs and events instead of buffering
5. **Rate limiting**: Respect API server rate limits

## Monitoring and Observability

1. **Metrics**: Expose Prometheus metrics (request count, latency, errors)
2. **Logging**: Structured logging with correlation IDs
3. **Tracing**: OpenTelemetry for distributed tracing
4. **Health checks**: Readiness and liveness endpoints

## Example API Endpoints

```
# Resource operations
GET    /api/v1/clusters/:cluster/resources/:kind
GET    /api/v1/clusters/:cluster/resources/:kind/:name
POST   /api/v1/clusters/:cluster/resources/:kind
PUT    /api/v1/clusters/:cluster/resources/:kind/:name
DELETE /api/v1/clusters/:cluster/resources/:kind/:name

# Node operations
GET    /api/v1/clusters/:cluster/nodes
POST   /api/v1/clusters/:cluster/nodes/:name/cordon
POST   /api/v1/clusters/:cluster/nodes/:name/drain
POST   /api/v1/clusters/:cluster/nodes/:name/uncordon

# RBAC operations
GET    /api/v1/clusters/:cluster/bindings
POST   /api/v1/clusters/:cluster/bindings/:name/subjects
DELETE /api/v1/clusters/:cluster/bindings/:name/subjects

# Real-time endpoints
WS     /api/v1/clusters/:cluster/pods/:name/logs
WS     /api/v1/clusters/:cluster/events
```

## Conclusion

The OCP CLI is already well-structured for conversion to a web application. The main work involves:
1. Extracting business logic into a service layer
2. Wrapping services with HTTP handlers
3. Building a frontend UI
4. Adding authentication and multi-tenancy support

The current architecture's use of context, interfaces, and separation of concerns makes this transition straightforward.

