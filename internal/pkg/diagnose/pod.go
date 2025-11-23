package diagnose

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// PodDiagnostics contains health information about a pod
type PodDiagnostics struct {
	Name      string
	Namespace string
	Status    string
	Restarts  int32
	Age       string
	Issues    []string
	Events    []string
	Logs      string // Last few lines of logs if error
}

// CheckPodHealth performs diagnostics on a pod
func CheckPodHealth(ctx context.Context, clientset *kubernetes.Clientset, podName, namespace string) (*PodDiagnostics, error) {
	pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get pod %s: %w", podName, err)
	}

	diag := &PodDiagnostics{
		Name:      pod.Name,
		Namespace: pod.Namespace,
		Status:    string(pod.Status.Phase),
		Age:       formatAge(pod.CreationTimestamp.Time),
	}

	// Check status
	if pod.Status.Phase != corev1.PodRunning && pod.Status.Phase != corev1.PodSucceeded {
		diag.Issues = append(diag.Issues, fmt.Sprintf("Pod is in %s phase", pod.Status.Phase))
	}

	// Check containers
	for _, cs := range pod.Status.ContainerStatuses {
		diag.Restarts += cs.RestartCount
		if !cs.Ready {
			diag.Issues = append(diag.Issues, fmt.Sprintf("Container %s is not ready", cs.Name))
		}
		if cs.State.Waiting != nil {
			diag.Issues = append(diag.Issues, fmt.Sprintf("Container %s waiting: %s - %s", cs.Name, cs.State.Waiting.Reason, cs.State.Waiting.Message))
		}
		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
			diag.Issues = append(diag.Issues, fmt.Sprintf("Container %s terminated with exit code %d: %s", cs.Name, cs.State.Terminated.ExitCode, cs.State.Terminated.Message))
		}
	}

	if diag.Restarts > 0 {
		diag.Issues = append(diag.Issues, fmt.Sprintf("Pod has %d restarts", diag.Restarts))
	}

	// Get events
	events, err := clientset.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("involvedObject.name=%s,involvedObject.uid=%s", pod.Name, pod.UID),
	})
	if err == nil {
		for _, e := range events.Items {
			if e.Type == corev1.EventTypeWarning {
				diag.Events = append(diag.Events, fmt.Sprintf("%s: %s", e.Reason, e.Message))
			}
		}
	}

	// Get logs if issues found
	if len(diag.Issues) > 0 {
		req := clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
			TailLines: func() *int64 { i := int64(10); return &i }(),
		})
		logs, err := req.DoRaw(ctx)
		if err == nil {
			diag.Logs = string(logs)
		}
	}

	return diag, nil
}

func formatAge(t time.Time) string {
	return time.Since(t).Round(time.Second).String()
}

