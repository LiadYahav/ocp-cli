package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

const defaultSSHKeyPath = "~/.ssh/id_rsa_ocp"

func newSSHCommand() *cobra.Command {
	var user string
	var identityFile string

	cmd := &cobra.Command{
		Use:   "ssh <node-pattern> [remote-command...]",
		Short: "Establish an SSH session to a cluster node",
		Long: `Establish an SSH connection to a node matched by the provided pattern.

When additional arguments are supplied after the node pattern they are treated
as remote commands to execute in sequence on the node (like a script).

By default, the CLI will use ~/.ssh/id_rsa_ocp as the SSH private key if it exists.
You can override this with the --identity flag.

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
  
  # With identity file
  ocp ssh -i ~/.ssh/id_rsa master-1 "whoami" "df -h" "free -m"`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pattern := args[0]

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			ip, err := resolveNodeIP(ctx, pattern)
			if err != nil {
				return err
			}

			// Resolve identity file: use provided flag, or default if it exists
			resolvedIdentityFile := identityFile
			if resolvedIdentityFile == "" {
				defaultKey, err := expandPath(defaultSSHKeyPath)
				if err == nil {
					if _, err := os.Stat(defaultKey); err == nil {
						resolvedIdentityFile = defaultKey
					}
				}
			}

			// Validate identity file if set
			if resolvedIdentityFile != "" {
				if _, err := os.Stat(resolvedIdentityFile); os.IsNotExist(err) {
					return fmt.Errorf("identity file %q does not exist", resolvedIdentityFile)
				}
			}

			target := fmt.Sprintf("%s@%s", user, ip)
			sshArgs := []string{}

			if resolvedIdentityFile != "" {
				sshArgs = append(sshArgs, "-i", resolvedIdentityFile)
			}

			sshArgs = append(sshArgs, target)
			if len(args) > 1 {
				// Join multiple commands with "; "
				combinedCmd := strings.Join(args[1:], "; ")
				sshArgs = append(sshArgs, combinedCmd)
			}

			sshCmd := exec.CommandContext(ctx, "ssh", sshArgs...)
			sshCmd.Stdin = cmd.InOrStdin()
			sshCmd.Stdout = cmd.OutOrStdout()
			sshCmd.Stderr = cmd.ErrOrStderr()

			return sshCmd.Run()
		},
	}

	cmd.Flags().StringVarP(&user, "user", "u", "core", "Username for SSH connection")
	cmd.Flags().StringVarP(&identityFile, "identity", "i", "", "Path to private key file for SSH authentication (default: ~/.ssh/id_rsa_ocp if exists)")

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

func resolveNodeIP(ctx context.Context, pattern string) (string, error) {
	clientset, err := kube.NewClientset(ctx)
	if err != nil {
		return "", err
	}

	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to list nodes: %w", err)
	}

	ip, err := selectNodeIP(nodes.Items, pattern)
	if err != nil {
		return "", err
	}

	return ip, nil
}

func selectNodeIP(nodes []corev1.Node, pattern string) (string, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return "", fmt.Errorf("invalid node pattern %q: %w", pattern, err)
	}

	type nodeEntry struct {
		name string
		ip   string
	}

	var matches []nodeEntry

	for _, node := range nodes {
		if !re.MatchString(node.Name) {
			continue
		}

		ip := findNodeIP(node.Status.Addresses)
		if ip == "" {
			continue
		}

		matches = append(matches, nodeEntry{name: node.Name, ip: ip})
	}

	switch len(matches) {
	case 0:
		return "", fmt.Errorf("no node matched pattern %q", pattern)
	case 1:
		return matches[0].ip, nil
	default:
		var names []string
		for _, match := range matches {
			names = append(names, match.name)
		}

		return "", fmt.Errorf("pattern matched multiple nodes: %s", strings.Join(names, ", "))
	}
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
