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
		SilenceUsage:  false, // Show usage on error to help users
		SilenceErrors: false, // Show errors properly
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			// Complete subcommand names
			var completions []string
			for _, subCmd := range cmd.Commands() {
				if strings.HasPrefix(subCmd.Name(), toComplete) {
					completions = append(completions, subCmd.Name())
				}
			}
			return completions, cobra.ShellCompDirectiveNoFileComp
		},
	}

	c := &CLI{
		root: root,
	}

	c.configure()

	return c
}

func (c *CLI) configure() {
	c.root.PersistentFlags().String("config", "", "Path to kubeconfig file")

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

		ctx = kube.WithConfigPath(ctx, resolvedConfig)

		// Load project namespace from file and add to context
		if projectNS := getProjectNamespace(); projectNS != "" {
			ctx = kube.WithProjectNamespace(ctx, projectNS)
		}

		cmd.SetContext(ctx)
		return nil
	}

	c.root.AddCommand(
		newSSHCommand(),
		newClusterCommand(),
		newNodeCommand(),
		newMCPCommand(),
		newBindingCommand(),
		newProjectCommand(),
		newGetCommand(),
		newCreateCommand(),
		newEditCommand(),
		newDeleteCommand(),
		newDescribeCommand(),
		newLogsCommand(),
		newApplyCommand(),
		newPatchCommand(),
		newAnnotateCommand(),
		newLabelCommand(),
		newClearYMLCommand(),
		newDiagnoseCommand(),
		newTopCommand(),
		newVersionCommand(),
		newCompletionCommand(c.root),
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

		if path == "~" {
			path = home
		} else {
			path = filepath.Join(home, strings.TrimPrefix(path, "~/"))
		}
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

func newCompletionCommand(root *cobra.Command) *cobra.Command {
	return &cobra.Command{
		Use:   "completion [bash|zsh|fish|powershell]",
		Short: "Generate shell completion scripts",
		Long: `Generate shell completion scripts for ocp CLI.

To load completions in your current shell session:
  source <(ocp completion bash)  # for bash
  source <(ocp completion zsh)   # for zsh

To load completions for all new shells, add the completion script to your shell's completion directory.
For bash, this is typically ~/.bashrc or ~/.bash_profile.
For zsh, this is typically ~/.zshrc.`,
		Example: `  # Generate bash completion
  ocp completion bash > /etc/bash_completion.d/ocp

  # Generate zsh completion
  ocp completion zsh > "${fpath[1]}/_ocp"`,
		ValidArgs: []string{"bash", "zsh", "fish", "powershell"},
		Args:      cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		RunE: func(cmd *cobra.Command, args []string) error {
			shell := args[0]
			switch shell {
			case "bash":
				return root.GenBashCompletion(cmd.OutOrStdout())
			case "zsh":
				return root.GenZshCompletion(cmd.OutOrStdout())
			case "fish":
				return root.GenFishCompletion(cmd.OutOrStdout(), true)
			case "powershell":
				return root.GenPowerShellCompletion(cmd.OutOrStdout())
			default:
				return fmt.Errorf("unsupported shell: %s", shell)
			}
		},
	}
}
