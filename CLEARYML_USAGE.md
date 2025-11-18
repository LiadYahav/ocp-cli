# OCP clearyml Command - Complete Usage Guide

## Overview

The `clearyml` command fetches Kubernetes resources and outputs clean YAML with unnecessary fields removed. This is perfect for:
- Version control (Git)
- Reapplying resources
- Creating templates from existing resources
- Comparing resources across environments

## What Gets Removed

The command automatically removes:
- **`status`** field - Runtime data that changes constantly
- **`metadata.uid`** - Unique identifier, auto-generated
- **`metadata.resourceVersion`** - Version for optimistic concurrency, auto-generated
- **`metadata.generation`** - Generation counter, auto-generated
- **`metadata.creationTimestamp`** - Creation time, auto-generated
- **`metadata.managedFields`** - Server-side apply tracking, auto-generated
- **`metadata.selfLink`** - Deprecated API endpoint, auto-generated
- **Empty maps** - Empty nested objects
- **Empty slices** - Empty arrays
- **Null values** - Null fields

## What Gets Preserved

The command preserves all important fields:
- **`spec`** - Complete specification (the important part!)
- **`metadata.name`** - Resource name
- **`metadata.namespace`** - Resource namespace
- **`metadata.labels`** - All labels
- **`metadata.annotations`** - All annotations
- **`metadata.finalizers`** - Finalizers (important for lifecycle)
- **`metadata.ownerReferences`** - Owner references (important for GC)
- **Empty strings** - Preserved as they might be valid values

## Usage Patterns

### Pattern 1: Get ALL resources of a type in a namespace (Recommended)

```bash
# Get ALL deployments in a namespace
ocp clearyml deployment --namespace test-ns

# Get ALL pods in a namespace
ocp clearyml pods --namespace default

# Get ALL services in a namespace
ocp clearyml services --namespace production

# Using short form -n flag
ocp clearyml deployment -n test-ns
ocp clearyml pods -n default
```

**Output**: Multiple YAML documents separated by `---`

### Pattern 2: Get a specific resource

```bash
# Get a specific deployment
ocp clearyml deployment my-app --namespace test-ns

# Get a specific pod
ocp clearyml pod my-pod --namespace default

# Get a specific service
ocp clearyml service my-svc --namespace production

# Using short form
ocp clearyml deployment my-app -n test-ns
```

**Output**: Single YAML document

### Pattern 3: Cluster-scoped resources (no namespace needed)

```bash
# Get ALL nodes
ocp clearyml nodes

# Get a specific node
ocp clearyml node worker-0

# Get ALL namespaces
ocp clearyml namespaces

# Get a specific namespace
ocp clearyml namespace default
```

**Output**: Single or multiple YAML documents depending on whether resource name is specified

### Pattern 4: Using current project namespace

```bash
# Set current project
ocp project test-ns

# Get ALL deployments (uses current project namespace)
ocp clearyml deployment

# Get specific deployment (uses current project namespace)
ocp clearyml deployment my-app
```

**Note**: This only works if you've set a current project with `ocp project <namespace>`

## Smart Argument Parsing

The command is smart about parsing arguments:

1. **With `--namespace` or `-n` flag (Recommended)**:
   - `ocp clearyml <resource-type> --namespace <ns>` → Lists all resources
   - `ocp clearyml <resource-type> <resource-name> --namespace <ns>` → Gets specific resource
   - `ocp clearyml <resource-type> -n <ns>` → Short form (lists all)
   - `ocp clearyml <resource-type> <resource-name> -n <ns>` → Short form (specific resource)

2. **Without `--namespace` flag**:
   - For **namespaced resources**:
     - `ocp clearyml <resource-type>` → Uses current project namespace (if set)
     - `ocp clearyml <resource-type> <namespace>` → Lists all in that namespace
     - `ocp clearyml <resource-type> <namespace> <resource-name>` → Gets specific resource
   - For **cluster-scoped resources**:
     - `ocp clearyml <resource-type>` → Lists all
     - `ocp clearyml <resource-type> <resource-name>` → Gets specific resource

## Examples

### Standard Kubernetes Resources

```bash
# All deployments in namespace
ocp clearyml deployment --namespace default

# Specific deployment
ocp clearyml deployment nginx-deployment --namespace default

# All pods
ocp clearyml pods --namespace kube-system

# All services
ocp clearyml services --namespace production

# Using short form
ocp clearyml deployment -n default
ocp clearyml pods -n kube-system
```

### OpenShift Resources

```bash
# All routes in namespace
ocp clearyml routes --namespace default

# Specific route
ocp clearyml route my-route --namespace default

# All build configs
ocp clearyml buildconfigs --namespace default

# All deployment configs
ocp clearyml deploymentconfigs --namespace default

# Machine Config Pools (cluster-scoped)
ocp clearyml machineconfigpools
ocp clearyml mcp  # Short alias works too
```

### Custom Resources (CRDs)

```bash
# All instances of a custom resource
ocp clearyml mycustomresources --namespace default

# Specific custom resource instance
ocp clearyml mycustomresource my-instance --namespace default
```

## Saving to Files

You can redirect output to files:

```bash
# Save all deployments to a file
ocp clearyml deployment --namespace=test-ns > deployments.yaml

# Save a specific deployment
ocp clearyml deployment my-app --namespace=test-ns > my-app.yaml

# Save multiple resource types
ocp clearyml deployment --namespace=test-ns > deployments.yaml
ocp clearyml service --namespace=test-ns >> services.yaml
```

## Use Cases

### 1. Backup Resources

```bash
# Backup all deployments in a namespace
ocp clearyml deployment --namespace production > backup/deployments.yaml

# Backup all resources of multiple types
for resource in deployment service configmap secret; do
  ocp clearyml $resource --namespace production > backup/${resource}s.yaml
done
```

### 2. Create Templates

```bash
# Get a deployment and modify it
ocp clearyml deployment my-app --namespace default > template.yaml
# Edit template.yaml, then apply
ocp apply -f template.yaml
```

### 3. Compare Resources

```bash
# Get resources from two namespaces and compare
ocp clearyml deployment --namespace staging > staging.yaml
ocp clearyml deployment --namespace production > production.yaml
diff staging.yaml production.yaml
```

### 4. Version Control

```bash
# Export all resources for Git
ocp clearyml deployment --namespace my-app > k8s/deployment.yaml
ocp clearyml service --namespace my-app > k8s/service.yaml
git add k8s/
git commit -m "Add Kubernetes manifests"
```

## Tips

1. **Always use `--namespace` or `-n` flag** for clarity and to avoid confusion
2. **Use space-separated format** - `--namespace <ns>` or `-n <ns>` (no equals sign needed)
3. **Use shell completion** - Tab completion works for resource types and names
4. **Redirect to files** - Use `>` to save output to files
5. **Multiple resources** - When listing all, output uses `---` separators (standard YAML multi-document format)
6. **Works with any resource** - Standard K8s, OpenShift, and CRDs all work

## Troubleshooting

### "namespace is required" error
- Use `--namespace` or `-n` flag: `ocp clearyml deployment --namespace default`
- Or use short form: `ocp clearyml deployment -n default`
- Or set current project: `ocp project default`

### "resource type not found" error
- Check if the resource type exists: `ocp get <resource-type>`
- For CRDs, ensure the CustomResourceDefinition is installed
- Use correct singular/plural form (both work)

### Empty output
- Check if resources exist: `ocp get <resource-type> --namespace <ns>`
- Verify namespace name is correct
- Check permissions to access the namespace

