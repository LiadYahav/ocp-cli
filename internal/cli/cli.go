package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/liadyahav/ocp-cli/internal/kube"
)

type CLI struct {
	root *cobra.Command
}

func New() *CLI {
	root := &cobra.Command{
		Use:           "ocp",
		Short:         "OCP team CLI",
		SilenceUsage:  false,
		SilenceErrors: true,
	}

	c := &CLI{
		root: root,
	}

	c.configure()

	return c
}

func (c *CLI) configure() {
	c.root.PersistentFlags().StringP("config", "c", "", "Path to kubeconfig file")

	c.root.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		configPath, err := cmd.Flags().GetString("config")
		if err != nil {
			return err
		}

		resolvedConfig, err := resolveConfigPath(configPath)
		if err != nil {
			return err
		}

		ctx := cmd.Context()
		if ctx == nil {
			ctx = context.Background()
		}

		cmd.SetContext(kube.WithConfigPath(ctx, resolvedConfig))
		return nil
	}

	c.root.AddCommand(
		newSSHCommand(),
		newClusterCommand(),
		newNodeCommand(),
		newVersionCommand(),
	)
}

func (c *CLI) Execute() error {
	return c.root.Execute()
}

func resolveConfigPath(path string) (string, error) {
	if path == "" {
		return "", nil
	}

	if strings.HasPrefix(path, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve kubeconfig path %q: %w", path, err)
		}

		path = filepath.Join(home, strings.TrimPrefix(path, "~"))
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve kubeconfig path %q: %w", path, err)
	}

	info, err := os.Stat(absPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("kubeconfig %q does not exist", absPath)
		}

		return "", fmt.Errorf("check kubeconfig %q: %w", absPath, err)
	}

	if info.IsDir() {
		return "", fmt.Errorf("kubeconfig %q is a directory", absPath)
	}

	return absPath, nil
}
