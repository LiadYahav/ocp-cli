package sshutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"
)

// Runner handles SSH command execution
type Runner struct {
	User         string
	IdentityFile string
	MaxRetries   int
	RetryDelay   time.Duration
	Stdout       io.Writer
	Stderr       io.Writer
}

// NewRunner creates a new SSH runner
func NewRunner(user, identityFile string) *Runner {
	return &Runner{
		User:         user,
		IdentityFile: identityFile,
		MaxRetries:   3,
		RetryDelay:   1 * time.Second,
		Stdout:       os.Stdout,
		Stderr:       os.Stderr,
	}
}

// Run executes a command on a remote host via SSH
func (r *Runner) Run(ctx context.Context, host string, command string) error {
	var lastErr error

	for i := 0; i <= r.MaxRetries; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.RetryDelay):
				// Continue
			}
		}

		err := r.runOnce(ctx, host, command)
		if err == nil {
			return nil
		}
		lastErr = err
		
		// Don't retry if context is cancelled
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	return fmt.Errorf("ssh command failed after %d retries: %w", r.MaxRetries, lastErr)
}

// RunWithOutput executes a command and captures stdout/stderr
func (r *Runner) RunWithOutput(ctx context.Context, host string, command string) (string, string, error) {
	// Temporarily swap writers to capture output
	var stdoutBuf, stderrBuf bytes.Buffer
	oldStdout, oldStderr := r.Stdout, r.Stderr
	r.Stdout = &stdoutBuf
	r.Stderr = &stderrBuf
	defer func() {
		r.Stdout = oldStdout
		r.Stderr = oldStderr
	}()

	err := r.Run(ctx, host, command)
	return stdoutBuf.String(), stderrBuf.String(), err
}

func (r *Runner) runOnce(ctx context.Context, host string, command string) error {
	args := []string{
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "LogLevel=ERROR",
		"-o", "ConnectTimeout=10",
		"-o", "ServerAliveInterval=5",
		"-o", "ServerAliveCountMax=2",
	}

	if r.IdentityFile != "" {
		args = append(args, "-i", r.IdentityFile)
	}

	target := host
	if r.User != "" {
		target = fmt.Sprintf("%s@%s", r.User, host)
	}

	args = append(args, target)
	if command != "" {
		args = append(args, command)
	}

	cmd := exec.CommandContext(ctx, "ssh", args...)
	cmd.Stdout = r.Stdout
	cmd.Stderr = r.Stderr
	cmd.Stdin = os.Stdin // Connect stdin for interactive sessions

	return cmd.Run()
}

