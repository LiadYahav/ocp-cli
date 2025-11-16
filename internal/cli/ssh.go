package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

const defaultSSHKeyPath = "~/.ssh/id_rsa_ocp"

func newSSHCommand() *cobra.Command {
	var user string
	var identityFile string
	var maxRetries int

	cmd := &cobra.Command{
		Use:   "ssh <node-name> [remote-command...]",
		Short: "Establish an SSH session to a cluster node",
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			// Complete node names for the first argument
			if len(args) == 0 {
				return completeNodeNames(cmd, args, toComplete)
			}
			// No completion for remote commands
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		Long: `Establish an SSH connection to a node by name.

When additional arguments are supplied after the node name they are treated
as remote commands to execute in sequence on the node (like a script).

By default, the CLI will use ~/.ssh/id_rsa_ocp as the SSH private key if it exists.
You can override this with the --identity flag.

The command will automatically retry failed SSH connections (default: 3 retries).
Use --max-retries to customize the number of retry attempts.

Multiple commands can be provided in two ways:
  1. As a single string with semicolons: "cmd1; cmd2; cmd3"
  2. As multiple arguments (auto-joined with "; "): "cmd1" "cmd2" "cmd3"

Examples:
  # Single command
  ocp ssh node-24 "echo hello"
  
  # Multiple commands in one string
  ocp ssh node-24 "echo hello; cat /etc/os-release; uptime"
  
  # Multiple command arguments (auto-joined)
  ocp ssh node-24 "echo hello" "cat /etc/os-release" "uptime"
  
  # With identity file and custom retries
  ocp ssh -i ~/.ssh/id_rsa master-1 "whoami" "df -h" "free -m" --max-retries 5`,
		Args: cobra.MinimumNArgs(1),
		Example: `  # Open an interactive session
  ocp ssh worker-1

  # Run multiple commands sequentially
  ocp ssh master-0 "whoami" "cat /etc/os-release"

  # With custom retry count
  ocp ssh worker-2 "uptime" --max-retries 5`,
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeName := args[0]

			// Use retry logic for SSH commands
			if maxRetries <= 0 {
				maxRetries = 3
			}

			err := runSSHCommandWithRetry(cmd.Context(), user, identityFile, nodeName, args[1:], cmd, maxRetries, time.Second)
			if err != nil {
				return fmt.Errorf("failed to connect to node %s: %w", nodeName, err)
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&user, "user", "u", "core", "Username for SSH connection")
	cmd.Flags().StringVarP(&identityFile, "identity", "i", "", "Path to private key file for SSH authentication (default: ~/.ssh/id_rsa_ocp if exists)")
	cmd.Flags().IntVar(&maxRetries, "max-retries", 3, "Maximum number of retry attempts for SSH connection")

	return cmd
}

// expandPath expands ~ to the user's home directory
func expandPath(path string) (string, error) {
	if !strings.HasPrefix(path, "~") {
		return path, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	if path == "~" {
		return home, nil
	}

	return filepath.Join(home, strings.TrimPrefix(path, "~/")), nil
}

func resolveNodeIP(ctx context.Context, nodeName string) (string, error) {
	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return "", err
	}

	node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get node %q: %w", nodeName, err)
	}

	ip := findNodeIP(node.Status.Addresses)
	if ip == "" {
		return "", fmt.Errorf("node %q has no IP address", nodeName)
	}

	return ip, nil
}

func findNodeIP(addresses []corev1.NodeAddress) string {
	var external string

	for _, addr := range addresses {
		switch addr.Type {
		case corev1.NodeInternalIP:
			return addr.Address
		case corev1.NodeExternalIP:
			if external == "" {
				external = addr.Address
			}
		}
	}

	return external
}

func runSSHCommand(ctx context.Context, user, identityFile, nodeName string, commands []string, cobraCmd *cobra.Command) error {
	if ctx == nil {
		ctx = context.Background()
	}

	ip, err := resolveNodeIP(ctx, nodeName)
	if err != nil {
		return err
	}

	resolvedIdentityFile, err := resolveIdentityFile(identityFile)
	if err != nil {
		return err
	}

	target := fmt.Sprintf("%s@%s", user, ip)
	sshArgs := []string{}

	// Disable strict host key checking to avoid prompts for new hosts
	sshArgs = append(sshArgs, "-o", "StrictHostKeyChecking=no")

	if resolvedIdentityFile != "" {
		sshArgs = append(sshArgs, "-i", resolvedIdentityFile)
	}

	sshArgs = append(sshArgs, target)
	if len(commands) > 0 {
		combinedCmd := strings.Join(commands, "; ")
		sshArgs = append(sshArgs, combinedCmd)
	}

	sshCmd := exec.CommandContext(ctx, "ssh", sshArgs...)
	if cobraCmd != nil {
		sshCmd.Stdin = cobraCmd.InOrStdin()
		sshCmd.Stdout = cobraCmd.OutOrStdout()
		sshCmd.Stderr = cobraCmd.ErrOrStderr()
	}

	return sshCmd.Run()
}

// runSSHCommandWithRetry executes an SSH command with retry logic
// maxRetries: maximum number of retry attempts (default: 3)
// initialDelay: initial delay between retries (default: 1 second)
func runSSHCommandWithRetry(ctx context.Context, user, identityFile, nodeName string, commands []string, cobraCmd *cobra.Command, maxRetries int, initialDelay time.Duration) error {
	if maxRetries <= 0 {
		maxRetries = 3
	}
	if initialDelay <= 0 {
		initialDelay = time.Second
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: delay = initialDelay * 2^(attempt-1)
			delay := initialDelay * time.Duration(1<<uint(attempt-1))
			if cobraCmd != nil {
				fmt.Fprintf(cobraCmd.ErrOrStderr(), "Retrying SSH to %s (attempt %d/%d) after %v...\n", nodeName, attempt+1, maxRetries+1, delay)
			}
			time.Sleep(delay)
		}

		err := runSSHCommand(ctx, user, identityFile, nodeName, commands, cobraCmd)
		if err == nil {
			return nil
		}

		lastErr = err
		if cobraCmd != nil {
			fmt.Fprintf(cobraCmd.ErrOrStderr(), "SSH attempt %d/%d failed for %s: %v\n", attempt+1, maxRetries+1, nodeName, err)
		}
	}

	return fmt.Errorf("SSH to %s failed after %d attempts: %w", nodeName, maxRetries+1, lastErr)
}

func resolveIdentityFile(identityFile string) (string, error) {
	resolvedIdentityFile := identityFile
	if resolvedIdentityFile == "" {
		defaultKey, err := expandPath(defaultSSHKeyPath)
		if err == nil {
			if _, err := os.Stat(defaultKey); err == nil {
				resolvedIdentityFile = defaultKey
			}
		}
	}

	if resolvedIdentityFile != "" {
		if _, err := os.Stat(resolvedIdentityFile); err != nil {
			if os.IsNotExist(err) {
				return "", fmt.Errorf("identity file %q does not exist", resolvedIdentityFile)
			}
			return "", fmt.Errorf("failed to access identity file %q: %w", resolvedIdentityFile, err)
		}
	}

	return resolvedIdentityFile, nil
}
