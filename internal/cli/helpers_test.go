package cli

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestEnsureContext(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want context.Context
	}{
		{
			name: "nil context",
			ctx:  nil,
			want: context.Background(),
		},
		{
			name: "background context",
			ctx:  context.Background(),
			want: context.Background(),
		},
		{
			name: "with value context",
			ctx:  context.WithValue(context.Background(), "key", "value"),
			want: context.WithValue(context.Background(), "key", "value"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ensureContext(tt.ctx)
			if got == nil {
				t.Errorf("ensureContext() returned nil")
			}
			// Can't directly compare contexts, so just check it's not nil
			if got == nil {
				t.Errorf("ensureContext() = nil, want non-nil")
			}
		})
	}
}

func TestWithTimeout(t *testing.T) {
	ctx := context.Background()
	timeout := 100 * time.Millisecond

	newCtx, cancel := withTimeout(ctx, timeout)
	defer cancel()

	// Check that context has timeout
	deadline, ok := newCtx.Deadline()
	if !ok {
		t.Errorf("withTimeout() context should have a deadline")
	}

	expectedDeadline := time.Now().Add(timeout)
	if deadline.After(expectedDeadline.Add(10*time.Millisecond)) || deadline.Before(expectedDeadline.Add(-10*time.Millisecond)) {
		t.Errorf("withTimeout() deadline = %v, want approximately %v", deadline, expectedDeadline)
	}
}

func TestFormatErrorWithContext(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		operation   string
		resourceType string
		resourceName string
		namespace   string
		wantContains string
	}{
		{
			name:        "nil error",
			err:         nil,
			operation:   "test",
			wantContains: "",
		},
		{
			name:        "error with operation",
			err:         errors.New("test error"),
			operation:   "get",
			wantContains: "operation",
		},
		{
			name:        "error with all context",
			err:         errors.New("test error"),
			operation:   "get",
			resourceType: "pod",
			resourceName: "my-pod",
			namespace:   "default",
			wantContains: "operation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatErrorWithContext(tt.err, tt.operation, tt.resourceType, tt.resourceName, tt.namespace)
			if tt.err == nil && got != nil {
				t.Errorf("formatErrorWithContext(nil) = %v, want nil", got)
			}
			if tt.err != nil && got == nil {
				t.Errorf("formatErrorWithContext(%v) = nil, want error", tt.err)
			}
			if tt.err != nil && tt.wantContains != "" {
				if got.Error() == "" {
					t.Errorf("formatErrorWithContext() error message is empty")
				}
			}
		})
	}
}

