package cli

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	// DefaultTimeout is the default timeout for operations
	DefaultTimeout = 30 * time.Second
	// LongTimeout is for operations that may take longer
	LongTimeout = 5 * time.Minute
	// ShortTimeout is for quick operations
	ShortTimeout = 10 * time.Second
)

// withTimeout creates a context with timeout, respecting parent context cancellation
func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, timeout)
}

// formatErrorWithContext adds context information to error messages
func formatErrorWithContext(err error, operation, resourceType, resourceName, namespace string) error {
	if err == nil {
		return nil
	}

	var ctxParts []string
	if operation != "" {
		ctxParts = append(ctxParts, fmt.Sprintf("operation=%q", operation))
	}
	if resourceType != "" {
		ctxParts = append(ctxParts, fmt.Sprintf("resourceType=%q", resourceType))
	}
	if resourceName != "" {
		ctxParts = append(ctxParts, fmt.Sprintf("resourceName=%q", resourceName))
	}
	if namespace != "" {
		ctxParts = append(ctxParts, fmt.Sprintf("namespace=%q", namespace))
	}

	if len(ctxParts) > 0 {
		ctx := strings.Join(ctxParts, " ")
		return fmt.Errorf("%s (%s): %w", err.Error(), ctx, err)
	}
	return err
}

// ensureContext ensures a context is not nil
func ensureContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

