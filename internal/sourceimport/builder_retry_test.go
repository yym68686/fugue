package sourceimport

import (
	"context"
	"errors"
	"testing"
)

func TestRunBuilderJobWithRetryRetriesTransientFailures(t *testing.T) {
	t.Parallel()

	attempts := 0
	err := runBuilderJobWithRetry(context.Background(), "dockerfile", "build-demo", "registry.example/demo:upload-abc123", nil, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return errors.New("kaniko job build-demo: pod build-demo-abc container kaniko failed: Error: exit_code=1\npod build-demo-abc container kaniko log tail: error building image: failed to get filesystem from image: unexpected EOF")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected transient builder error to recover, got %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected two attempts, got %d", attempts)
	}
}

func TestRunBuilderJobWithRetryDoesNotRetryPermanentFailures(t *testing.T) {
	t.Parallel()

	attempts := 0
	wantErr := errors.New("kaniko job build-demo: invalid Dockerfile: unknown instruction FROOOM")
	err := runBuilderJobWithRetry(context.Background(), "dockerfile", "build-demo", "registry.example/demo:upload-abc123", nil, func(context.Context) error {
		attempts++
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected permanent error to be returned unchanged, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected permanent error to avoid retry, got %d attempts", attempts)
	}
}

func TestShouldRetryBuilderJobFailureSignals(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		message   string
		retriable bool
	}{
		{
			name:      "unexpected eof log tail",
			message:   "kaniko job build-demo: pod build-demo-abc container kaniko log tail: error building image: failed to get filesystem from image: unexpected EOF",
			retriable: true,
		},
		{
			name:      "evicted pod",
			message:   "pod build-demo-abc on node node-a failed: Evicted: The node was low on resource: ephemeral-storage.",
			retriable: true,
		},
		{
			name:      "kubectl signal killed",
			message:   "kaniko job build-demo: kubectl -n fugue-system get job build-demo -o json: signal: killed",
			retriable: true,
		},
		{
			name:      "dockerfile syntax",
			message:   "kaniko job build-demo: invalid Dockerfile: unknown instruction FROOOM",
			retriable: false,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			retriable, _ := shouldRetryBuilderJobFailure(errors.New(tc.message))
			if retriable != tc.retriable {
				t.Fatalf("unexpected retry decision for %q: got %t want %t", tc.message, retriable, tc.retriable)
			}
		})
	}
}
