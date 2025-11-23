package diagnose

import (
	"context"
	"fmt"
	"strings"

	"github.com/liadyahav/ocp-cli/internal/pkg/sshutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
)

// NodeDiagnostics contains health information about a node
type NodeDiagnostics struct {
	Name            string
	Uptime          string
	LoadAverage     string
	MemoryUsed      string
	MemoryTotal     string
	DiskUsage       string // Root partition
	KubeletStatus   string
	CrioStatus      string
	ContainerdStatus string
	CPURequest      string // From metrics API
	MemoryRequest   string // From metrics API
	Errors          []string
}

// CheckNodeHealth performs diagnostics on a node via SSH and optional Metrics API
func CheckNodeHealth(ctx context.Context, runner *sshutil.Runner, metricsClient metrics.Interface, nodeHost string) (*NodeDiagnostics, error) {
	diag := &NodeDiagnostics{
		Name: nodeHost,
	}

	// 1. Metrics API Check (if client provided)
	if metricsClient != nil {
		nm, err := metricsClient.MetricsV1beta1().NodeMetricses().Get(ctx, nodeHost, metav1.GetOptions{})
		if err == nil {
			cpu := nm.Usage.Cpu().MilliValue()
			mem := nm.Usage.Memory().Value()
			diag.CPURequest = fmt.Sprintf("%dm", cpu)
			diag.MemoryRequest = fmt.Sprintf("%dMi", mem/1024/1024)
		} else {
			diag.Errors = append(diag.Errors, fmt.Sprintf("Metrics API failed: %v", err))
		}
	}

	// 2. SSH Checks
	if runner != nil {
		// Run checks script
		script := `
echo "UPTIME:$(uptime -p)"
echo "LOAD:$(cat /proc/loadavg | awk '{print $1,$2,$3}')"
echo "MEM:$(free -m | grep Mem | awk '{print $3 "/" $2 " MB"}')"
echo "DISK:$(df -h / | tail -1 | awk '{print $5}')"
echo "KUBELET:$(systemctl is-active kubelet || echo inactive)"
echo "CRIO:$(systemctl is-active crio || echo inactive)"
echo "CONTAINERD:$(systemctl is-active containerd || echo inactive)"
`
		
		stdout, stderr, err := runner.RunWithOutput(ctx, nodeHost, script)
		if err != nil {
			diag.Errors = append(diag.Errors, fmt.Sprintf("SSH diagnostics failed: %v (stderr: %s)", err, stderr))
		} else {
			lines := strings.Split(stdout, "\n")
			for _, line := range lines {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) != 2 {
					continue
				}
				key, val := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])

				switch key {
				case "UPTIME":
					diag.Uptime = val
				case "LOAD":
					diag.LoadAverage = val
				case "MEM":
					diag.MemoryUsed = val // actually "Used/Total MB"
				case "DISK":
					diag.DiskUsage = val
				case "KUBELET":
					diag.KubeletStatus = val
				case "CRIO":
					diag.CrioStatus = val
				case "CONTAINERD":
					diag.ContainerdStatus = val
				}
			}

			// Validate health
			if diag.KubeletStatus != "active" {
				diag.Errors = append(diag.Errors, fmt.Sprintf("Kubelet is %s", diag.KubeletStatus))
			}
			
			// Check runtime (either crio or containerd should be active)
			if diag.CrioStatus != "active" && diag.ContainerdStatus != "active" {
				diag.Errors = append(diag.Errors, "No active container runtime found (crio/containerd)")
			}

			// Parse disk usage
			diskStr := strings.TrimSuffix(diag.DiskUsage, "%")
			var diskPercent int
			fmt.Sscanf(diskStr, "%d", &diskPercent)
			if diskPercent > 85 {
				diag.Errors = append(diag.Errors, fmt.Sprintf("High disk usage: %s", diag.DiskUsage))
			}
		}
	}

	return diag, nil
}

