# OCP CLI

A powerful command-line tool for managing OpenShift and Kubernetes clusters, designed for the OCP team. This CLI provides an intuitive interface for cluster operations, node management, RBAC binding management, and more.

## Features

- **Generic Resource Support**: Automatically discovers and works with all Kubernetes resources, OpenShift resources, and Custom Resource Definitions (CRDs) in your cluster
- **Comprehensive Table Output**: kubectl/oc-compatible table formatting for all resource types with proper status, age, and resource-specific columns
- **Node Management**: Cordon, drain, uncordon, reboot, and manage nodes with automatic annotation handling
- **RBAC Management**: Manage RoleBindings and ClusterRoleBindings with intuitive commands
- **SSH Integration**: Execute commands on cluster nodes via SSH with automatic retry logic
- **Cluster Operations**: Configure DNS, monitor cluster health, and view cluster information
- **Machine Config Pool Management**: Pause and resume Machine Config Pools
- **Project Namespace Management**: Set default namespace for all namespace-scoped commands
- **Kubectl-like Commands**: Full support for `get`, `create`, `edit`, `delete`, `describe`, `logs`, `apply`, `patch`, `annotate`, `label`, and `clearyml` (no external dependencies - fully self-contained)
- **Shell Completion**: Full bash/zsh/fish completion support for all commands and resources
- **Concurrent Operations**: Parallel execution for multi-resource operations with configurable concurrency
- **Error Handling**: Robust error handling with retry logic and detailed error messages
- **Web-Ready Architecture**: Business logic separated from CLI presentation for easy conversion to web applications

## Installation

### Prerequisites

- Go 1.25.3 or later (for building from source)
- Access to a Kubernetes/OpenShift cluster (via kubeconfig)
- SSH access to cluster nodes (only required for SSH-related features: `ocp ssh`, `ocp node reboot`, `ocp cluster configure-dns`)

**Note**: The OCP CLI is completely standalone and does **NOT** require `kubectl` or `oc` to be installed. All functionality is implemented natively using the Kubernetes client-go library. Table output formats match kubectl/oc output exactly.

### Build from Source

```bash
# Clone the repository
git clone <repository-url>
cd ocp-cli

# Build the binary
go build ./cmd/ocp

# The binary will be created as ./ocp
# Move it to your PATH
sudo mv ./ocp /usr/local/bin/
```

### Using Go Install

```bash
go install github.com/liadyahav/ocp-cli/cmd/ocp@latest
```

## Configuration

### Kubeconfig

The CLI automatically detects your kubeconfig in the following order:
1. `KUBECONFIG` environment variable
2. `~/.kube/config`

You can override the kubeconfig path using the `--config` flag:

```bash
ocp --config /path/to/kubeconfig get pods
```

### SSH Private Key

By default, the CLI uses `~/.ssh/id_rsa_ocp` for SSH connections. You can specify a different key:

```bash
ocp ssh -i ~/.ssh/my_key node-1 "uptime"
```

### Project Namespace

Set a default namespace for all namespace-scoped commands:

```bash
# Set default namespace
ocp project my-namespace

# View current project
ocp project

# The -n/--namespace flag always overrides the project namespace
ocp get pods -n other-namespace
```

## Usage

### SSH Commands

Execute commands on cluster nodes via SSH:

```bash
# Single command
ocp ssh node-24 "echo hello"

# Multiple commands (semicolon-separated)
ocp ssh node-24 "echo hello; cat /etc/os-release; uptime"

# Multiple command arguments (auto-joined)
ocp ssh node-24 "echo hello" "cat /etc/os-release" "uptime"

# With custom identity file and retries
ocp ssh -i ~/.ssh/id_rsa node-1 "whoami" --max-retries 5
```

### Diagnostics

Run health checks on cluster nodes (CPU, Memory, Disk, Services):

```bash
# Diagnose specific node
ocp diagnose node worker-0

# Diagnose all nodes
ocp diagnose node
```

### Get Cleaned YAML

Get resource YAMLs cleaned of runtime metadata (status, uids, managedFields), ready for gitops/apply:

```bash
# Get cleaned deployment
ocp clearyml deployment my-app -n my-ns

# Get all deployments in namespace
ocp clearyml deployments -n my-ns
```

### Cluster Commands

#### Cluster Information

```bash
# Display cluster information
ocp cluster info

# Show cluster version
ocp cluster version

# Watch important cluster resources (auto-detects distribution)
# For OpenShift: shows ClusterOperators, ClusterVersions, and MachineConfigPools
# For Kubernetes: shows Nodes, Namespaces summary, and Pods summary
# Refreshes every 1 second with change highlighting
ocp cluster watch
```

#### Configure DNS

**⚠️ WARNING: This command modifies DNS settings on all nodes. Use with caution!**

```bash
# Add a single nameserver (prepended to existing)
ocp cluster configure-dns 8.8.8.8

# Replace all nameservers
ocp cluster configure-dns 8.8.8.8,8.8.4.4

# With concurrency control
ocp cluster configure-dns 8.8.8.8 --max-concurrency 20
```

### Node Management

#### List Nodes

```bash
# List all nodes
ocp node list

# List only schedulable nodes
ocp node list --schedulable

# List only unschedulable (cordoned) nodes
ocp node list --scheduling-disabled
```

#### Node Operations

```bash
# Cordon a node (marks as unschedulable)
ocp node cordon node-1

# Drain a node (cordon + evict pods)
ocp node drain node-1

# Uncordon a node (marks as schedulable)
ocp node uncordon node-1

# Reboot a node
ocp node reboot node-1

# View all pods on a node
ocp node getpods node-1
```

**Note**: The `cordon` and `drain` commands automatically add the `node.dana.io/reason: Maintenance` annotation. The `uncordon` command automatically removes it.

### Machine Config Pool (MCP) Management

```bash
# Pause an MCP
ocp mcp worker pause

# Resume an MCP
ocp mcp worker resume
```

### RBAC Binding Management

#### List Bindings

```bash
# List RoleBindings in a namespace
ocp binding list --namespace=default

# List ClusterRoleBindings
ocp binding list --cluster

# List bindings for a specific role
ocp binding list --role=admin --namespace=default
```

#### Get Binding Details

```bash
# Get a RoleBinding
ocp binding get admin --namespace=default

# Get a ClusterRoleBinding
ocp binding get cluster-admin --cluster

# Describe a binding
ocp binding describe admin --namespace=default
```

#### Add Subjects to Bindings

```bash
# Add a user to a RoleBinding
ocp binding add admin --user=john --namespace=default

# Add a group to a ClusterRoleBinding
ocp binding add cluster-admin --group=admins --cluster

# Add a service account
ocp binding add admin --serviceaccount=default:my-sa --namespace=default

# Add multiple subjects at once
ocp binding add admin --user=john --user=jane --namespace=default
```

#### Remove Subjects from Bindings

```bash
# Remove a user from a RoleBinding
ocp binding remove admin --user=john --namespace=default

# Remove a user from a ClusterRoleBinding
ocp binding remove cluster-admin --user=john --cluster
```

#### Query Bindings

```bash
# See who has a specific role
ocp binding who-has admin --namespace=default

# See who has a cluster role
ocp binding who-has cluster-admin --cluster

# See what roles a user has
ocp binding what-can john
```

### Project Namespace

```bash
# Set default namespace
ocp project my-namespace

# View current project
ocp project
```

### Kubectl-like Commands

All standard kubectl-like commands are supported with generic resource discovery. The CLI is fully self-contained and does not require `kubectl` or `oc` to be installed:

#### Get Resources

```bash
# List all pods
ocp get pods

# Get a specific pod
ocp get pod my-pod

# List all resources in all namespaces
ocp get pods --all-namespaces

# List with label selector
ocp get pods -l app=myapp

# Wide output (more columns)
ocp get pods -owide

# Show labels
ocp get pods --show-labels

# JSON output
ocp get pods -o json

# YAML output
ocp get pods -o yaml

# Just names
ocp get pods -o name

# List any custom resource (CRD)
ocp get mycustomresources

# List OpenShift resources
ocp get routes
ocp get buildconfigs
ocp get deploymentconfigs

# Use shortcuts/aliases (works for all resources)
ocp get mcp                    # Machine Config Pools
ocp get crd                    # Custom Resource Definitions
ocp get co                     # Cluster Operators (OpenShift)
ocp get cv                     # Cluster Versions (OpenShift)
ocp get po                     # Pods
ocp get svc                    # Services
ocp get deploy                 # Deployments
ocp get no                     # Nodes
ocp get ns                     # Namespaces
ocp get cm                     # ConfigMaps
ocp get secret                 # Secrets
ocp get pvc                    # PersistentVolumeClaims
ocp get pv                     # PersistentVolumes
ocp get sa                     # ServiceAccounts
ocp get ing                    # Ingresses
ocp get sts                    # StatefulSets
ocp get ds                     # DaemonSets
ocp get rs                     # ReplicaSets
ocp get cj                     # CronJobs
ocp get job                    # Jobs
ocp get hpa                    # HorizontalPodAutoscalers
ocp get pdb                    # PodDisruptionBudgets
ocp get np                     # NetworkPolicies
ocp get csr                    # CertificateSigningRequests
ocp get sc                     # StorageClasses
```

#### Resource Table Formats

The CLI provides kubectl/oc-compatible table output for all resource types. Here are the columns displayed for each resource:

**Pods** (`ocp get pods`):
- Default: `NAME`, `READY`, `STATUS`, `RESTARTS`, `AGE`
- Wide (`-owide`): Adds `NODE`, `IP`, `NODE IP`, `READINESS GATES`

**Deployments** (`ocp get deployments`):
- Default: `NAME`, `READY`, `UP-TO-DATE`, `AVAILABLE`, `AGE`
- Wide (`-owide`): Adds `CONTAINERS`, `IMAGES`, `SELECTOR`

**ReplicaSets** (`ocp get replicasets`):
- Default: `NAME`, `DESIRED`, `CURRENT`, `READY`, `AGE`
- Wide (`-owide`): Adds `CONTAINERS`, `IMAGES`, `SELECTOR`

**StatefulSets** (`ocp get statefulsets`):
- Default: `NAME`, `READY`, `AGE`
- Wide (`-owide`): Adds `CONTAINERS`, `IMAGES`, `SELECTOR`

**DaemonSets** (`ocp get daemonsets`):
- Default: `NAME`, `DESIRED`, `CURRENT`, `READY`, `UP-TO-DATE`, `AVAILABLE`, `AGE`
- Wide (`-owide`): Adds `CONTAINERS`, `IMAGES`, `SELECTOR`

**Jobs** (`ocp get jobs`):
- Default: `NAME`, `COMPLETIONS`, `DURATION`, `AGE`
- Wide (`-owide`): Adds `CONTAINERS`, `IMAGES`

**CronJobs** (`ocp get cronjobs`):
- Default: `NAME`, `SCHEDULE`, `SUSPEND`, `ACTIVE`, `LAST SCHEDULE`, `AGE`
- Wide (`-owide`): Adds `CONTAINERS`, `IMAGES`

**Services** (`ocp get services`):
- Default: `NAME`, `TYPE`, `CLUSTER-IP`, `EXTERNAL-IP`, `PORT(S)`, `AGE`
- Wide (`-owide`): Adds `SELECTOR`, `ENDPOINTS`, `SESSION AFFINITY`

**Secrets** (`ocp get secrets`):
- Default: `NAME`, `TYPE`, `DATA`, `AGE`

**ConfigMaps** (`ocp get configmaps`):
- Default: `NAME`, `DATA`, `AGE`

**ServiceAccounts** (`ocp get serviceaccounts`):
- Default: `NAME`, `SECRETS`, `AGE`

**Nodes** (`ocp get nodes`):
- Default: `NAME`, `STATUS`, `ROLES`, `AGE`, `VERSION`
- Wide (`-owide`): Adds `INTERNAL-IP`, `EXTERNAL-IP`, `OS-IMAGE`, `KERNEL-VERSION`, `CONTAINER-RUNTIME`

**Namespaces** (`ocp get namespaces`):
- Default: `NAME`, `STATUS`, `AGE`

**Machine Config Pools** (`ocp get mcp`):
- Default: `NAME`, `CONFIG`, `UPDATED`, `UPDATING`, `DEGRADED`, `MACHINECOUNT`, `READYMACHINECOUNT`, `UPDATEDMACHINECOUNT`, `DEGRADEDMACHINECOUNT`, `AGE`

**Routes** (`ocp get routes` - OpenShift):
- Default: `NAME`, `HOST/PORT`, `PATH`, `SERVICES`, `PORT`, `TERMINATION`, `WILDCARD`, `AGE`

All table outputs support:
- `--show-labels`: Display labels in the last column
- `-owide` or `-o wide`: Extended output with additional columns
- `-A` or `--all-namespaces`: Include namespace column and show resources from all namespaces
```

#### Create Resources

```bash
# Create from file
ocp create -f deployment.yaml

# Create from multiple files
ocp create -f deployment.yaml -f service.yaml

# Create from stdin
cat deployment.yaml | ocp create -f -

# Create in specific namespace
ocp create -f deployment.yaml -n my-namespace
```

#### Edit Resources

```bash
# Edit a pod
ocp edit pod my-pod

# Edit in specific namespace
ocp edit pod my-pod -n my-namespace

# Edit any resource
ocp edit deployment my-deployment
```

#### Delete Resources

```bash
# Delete a pod
ocp delete pod my-pod

# Delete multiple resources
ocp delete pod pod1 pod2 pod3

# Delete by label selector
ocp delete pods -l app=myapp

# Force delete
ocp delete pod my-pod --force

# Delete with concurrency
ocp delete pods -l app=myapp --max-concurrency 10
```

#### Describe Resources

```bash
# Describe a pod
ocp describe pod my-pod

# Describe in all namespaces
ocp describe pods --all-namespaces

# Describe any resource
ocp describe deployment my-deployment
```

#### View Logs

```bash
# View pod logs
ocp logs my-pod

# Follow logs
ocp logs my-pod -f

# Logs from specific container
ocp logs my-pod -c my-container

# Last 100 lines
ocp logs my-pod --tail=100

# Logs since 1 hour ago
ocp logs my-pod --since=1h

# Previous container logs
ocp logs my-pod --previous
```

#### Apply Resources

```bash
# Apply from file
ocp apply -f deployment.yaml

# Apply from multiple files
ocp apply -f deployment.yaml -f service.yaml

# Apply from stdin
cat deployment.yaml | ocp apply -f -
```

#### Patch Resources

```bash
# Patch with JSON
ocp patch pod my-pod -p '{"metadata":{"labels":{"new":"label"}}}'

# Patch from file
ocp patch pod my-pod --patch-file patch.json

# Strategic merge patch (default)
ocp patch pod my-pod -p '{"metadata":{"labels":{"new":"label"}}}'

# JSON patch
ocp patch pod my-pod --type json -p '[{"op":"add","path":"/metadata/labels/new","value":"label"}]'

# Merge patch
ocp patch pod my-pod --type merge -p '{"metadata":{"labels":{"new":"label"}}}'
```

#### Annotate Resources

```bash
# Add annotation
ocp annotate pod my-pod key=value

# Add multiple annotations
ocp annotate pod my-pod key1=value1,key2=value2

# Remove annotation
ocp annotate pod my-pod key-

# Annotate multiple resources
ocp annotate pods -l app=myapp key=value

# Annotate with concurrency
ocp annotate pods -l app=myapp key=value --max-concurrency 10
```

#### Label Resources

```bash
# Add label
ocp label pod my-pod key=value

# Add multiple labels
ocp label pod my-pod key1=value1,key2=value2

# Remove label
ocp label pod my-pod key-

# Label multiple resources
ocp label pods -l app=myapp key=value

# Label with concurrency
ocp label pods -l app=myapp key=value --max-concurrency 10
```

#### Get Cleaned YAML

Get cleaned YAML output for resources with unnecessary fields removed. Perfect for version control or applying resources.

```bash
# Get cleaned YAML for a specific pod
ocp clearyml pod my-pod --namespace default

# Get cleaned YAML for ALL pods in a namespace (no resource name)
ocp clearyml pods --namespace default

# Get cleaned YAML for ALL deployments in a namespace
ocp clearyml deployment --namespace test-ns

# Get cleaned YAML for a specific deployment
ocp clearyml deployment my-app --namespace default

# Get cleaned YAML for cluster-scoped resource (no namespace needed)
ocp clearyml node worker-0

# Get cleaned YAML for ALL nodes
ocp clearyml nodes

# Get cleaned YAML for OpenShift resources
ocp clearyml routes --namespace default
ocp clearyml route my-route --namespace default

# Get cleaned YAML for custom resources (CRDs)
ocp clearyml mycustomresource my-instance --namespace default

# Using short form -n flag
ocp clearyml deployment -n test-ns
ocp clearyml pods -n default
```

The `clearyml` command automatically removes:
- `status` fields (runtime data)
- `metadata.uid`, `metadata.resourceVersion`, `metadata.generation`
- `metadata.creationTimestamp`, `metadata.managedFields`
- Empty and null fields

It preserves all important fields like `spec`, `metadata.name`, `metadata.namespace`, `metadata.labels`, and `metadata.annotations`.

## Shell Completion

Generate shell completion scripts:

```bash
# Bash
ocp completion bash > /etc/bash_completion.d/ocp
# or
ocp completion bash > ~/.bash_completion.d/ocp

# Zsh
ocp completion zsh > "${fpath[1]}/_ocp"
# or
ocp completion zsh > ~/.zsh/completion/_ocp

# Fish
ocp completion fish > ~/.config/fish/completions/ocp.fish

# PowerShell
ocp completion powershell > ocp.ps1
```

After generating, reload your shell or source the completion file.

## Generic Resource Discovery

The CLI automatically discovers all available resources in your cluster, including:

- **Standard Kubernetes resources**: pods, services, deployments, statefulsets, daemonsets, jobs, cronjobs, configmaps, secrets, persistentvolumes, persistentvolumeclaims, nodes, namespaces, ingresses, networkpolicies, serviceaccounts, roles, rolebindings, clusterroles, clusterrolebindings, poddisruptionbudgets, etc.

- **OpenShift resources**: routes, buildconfigs, builds, deploymentconfigs, imagestreams, imagestreamtags, imagestreamimages, templates, projects, clusterresourcequotas, securitycontextconstraints, networkattachmentdefinitions, clusteroperators, clusterversions, machineconfigpools, etc.

- **Custom Resource Definitions (CRDs)**: Any CRD installed in your cluster is automatically available for use with all commands.

If you try to use a resource type that doesn't exist in your cluster, you'll get a clear error message indicating that the CRD is not available.

## Error Handling and Retry Logic

The CLI includes robust error handling:

- **SSH Commands**: Automatic retry with exponential backoff (default: 3 retries)
- **Connection Timeouts**: 3-second connection timeout for SSH to quickly identify unreachable hosts
- **Server Alive**: Automatic detection of dead connections during long-running commands
- **Concurrent Operations**: Parallel execution with configurable concurrency limits
- **Detailed Summaries**: Clear success/failure counts and lists for multi-resource operations

## Examples

### Complete Workflow Example

```bash
# Set default namespace
ocp project production

# List all pods
ocp get pods

# Get detailed info about a pod
ocp describe pod my-pod

# View logs
ocp logs my-pod -f

# Cordon a node for maintenance
ocp node cordon worker-1

# Drain the node
ocp node drain worker-1

# SSH into the node to perform maintenance
ocp ssh worker-1 "sudo systemctl restart some-service"

# Uncordon the node
ocp node uncordon worker-1

# Check cluster health
ocp cluster watch
```

### RBAC Management Example

```bash
# See what roles a user has
ocp binding what-can john

# Add user to admin role
ocp binding add admin --user=john --namespace=default

# Verify the binding
ocp binding get admin --namespace=default

# Remove user from role
ocp binding remove admin --user=john --namespace=default
```

### Multi-Resource Operations

```bash
# Delete all pods with a label
ocp delete pods -l app=myapp --max-concurrency 10

# Label all deployments
ocp label deployments -l app=myapp environment=production

# Annotate all services
ocp annotate services -l app=myapp managed-by=ocp-cli

# Get cleaned YAML for all deployments in a namespace
ocp clearyml deployment --namespace test-ns

# Get cleaned YAML for all pods in a namespace
ocp clearyml pods --namespace default

# Using short form
ocp clearyml deployment -n test-ns
ocp clearyml pods -n default
```

## Command Reference

### Global Flags

- `--config`: Path to kubeconfig file (overrides KUBECONFIG env var and ~/.kube/config)

### Common Flags

- `-n, --namespace`: Namespace (overrides project namespace)
- `-A, --all-namespaces`: All namespaces
- `-l, --selector`: Label selector
- `-o, --output`: Output format (json, yaml, name, wide)
- `--show-labels`: Show labels in output
- `--max-concurrency`: Maximum concurrent operations (default varies by command)
- `--max-retries`: Maximum retry attempts for SSH commands (default: 3)

## Troubleshooting

### SSH Connection Issues

If SSH connections fail:

1. Check that the node is reachable: `ping <node-ip>`
2. Verify SSH key permissions: `chmod 600 ~/.ssh/id_rsa_ocp`
3. Test SSH manually: `ssh -i ~/.ssh/id_rsa_ocp core@<node-ip>`
4. Increase retry count: `ocp ssh --max-retries 5 node-1 "uptime"`

### Resource Not Found

If a resource type is not recognized:

1. Verify the resource exists in your cluster: `ocp get <resource-type>` (if it fails, the CRD may not be installed)
2. Check if it's a CRD: `ocp get crd`
3. Ensure you're using the correct resource name (singular vs plural)

### Permission Errors

If you get permission errors:

1. Check your kubeconfig context: `ocp cluster info` (shows current context)
2. Verify your RBAC permissions: Try the command and check the error message
3. Check if you need to switch contexts: Use `kubectl config use-context <context-name>` or update your kubeconfig file

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please ensure that:

1. Code follows Go best practices
2. All commands have proper error handling
3. Shell completion is implemented for new commands
4. Help text and examples are included
5. Tests are added for new functionality

## Support

For issues, questions, or contributions, please open an issue in the repository.

