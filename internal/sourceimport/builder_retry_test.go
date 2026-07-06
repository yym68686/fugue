package sourceimport

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestRunBuilderJobWithRetryRetriesTransientFailures(t *testing.T) {
	t.Parallel()

	attempts := 0
	err := runBuilderJobWithRetry(context.Background(), "dockerfile", "build-demo", "registry.example/demo:upload-abc123", nil, func(context.Context, builderJobAttempt) error {
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

func TestRunBuilderJobWithRetryEscalatesEphemeralFailures(t *testing.T) {
	t.Parallel()

	attempts := []builderJobAttempt{}
	err := runBuilderJobWithRetry(context.Background(), "dockerfile", "build-demo", "registry.example/demo:upload-abc123", nil, func(_ context.Context, attempt builderJobAttempt) error {
		attempts = append(attempts, attempt)
		if len(attempts) < 2 {
			return errors.New("kaniko job build-demo: pod build-demo-abc on node node-a failed: Evicted: Pod ephemeral local storage usage exceeds the total limit of containers 8Gi")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected ephemeral builder error to recover, got %v", err)
	}
	got := []int{attempts[0].EphemeralRetryCount, attempts[1].EphemeralRetryCount}
	if want := []int{0, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ephemeral retry counts = %v, want %v; attempts=%+v", got, want, attempts)
	}
}

func TestRunBuilderJobWithRetryDoesNotRetryPermanentFailures(t *testing.T) {
	t.Parallel()

	attempts := 0
	wantErr := errors.New("kaniko job build-demo: invalid Dockerfile: unknown instruction FROOOM")
	err := runBuilderJobWithRetry(context.Background(), "dockerfile", "build-demo", "registry.example/demo:upload-abc123", nil, func(context.Context, builderJobAttempt) error {
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
			name:      "oom killed container",
			message:   "pod build-demo-abc on node node-a container executor failed: OOMKilled: exit_code=137",
			retriable: true,
		},
		{
			name:      "kubectl signal killed",
			message:   "kaniko job build-demo: kubectl -n fugue-system get job build-demo -o json: signal: killed",
			retriable: true,
		},
		{
			name:      "empty kubectl get job exit status",
			message:   "kaniko job build-demo: kubectl -n fugue-system get job build-demo -o json: exit status 1",
			retriable: true,
		},
		{
			name:      "permanent kubectl get job error",
			message:   "kaniko job build-demo: kubectl -n fugue-system get job build-demo -o json: error: the server doesn't have a resource type job",
			retriable: false,
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

func TestIsBuilderOOMFailure(t *testing.T) {
	t.Parallel()

	err := errors.New("pod build-demo-abc container executor failed: OOMKilled: exit_code=137")
	if !isBuilderOOMFailure(err, "oomkilled") {
		t.Fatal("expected OOMKilled builder failure to trigger memory escalation")
	}
}

func TestIsBuilderEphemeralStorageFailure(t *testing.T) {
	t.Parallel()

	err := errors.New("pod build-demo-abc failed: Evicted: Pod ephemeral local storage usage exceeds the total limit of containers 8Gi")
	if !isBuilderEphemeralStorageFailure(err, "evicted") {
		t.Fatal("expected ephemeral local storage eviction to trigger storage escalation")
	}
}
