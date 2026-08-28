package main

import (
	"os"
	"path/filepath"
	"testing"

	"fugue/internal/releaseguardian"
	"k8s.io/client-go/kubernetes/fake"
)

func TestNewAuthorityRuntimeIsExplicitAndGroupScoped(t *testing.T) {
	store, err := releaseguardian.NewAuthorityStore(fake.NewSimpleClientset(), "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	if runtime, err := newAuthorityRuntime(store, ""); err != nil || runtime != nil {
		t.Fatalf("empty authority configuration was not dormant: runtime=%v err=%v", runtime, err)
	}
	directory := t.TempDir()
	keyPath := filepath.Join(directory, "candidate-canary.key")
	if err := os.WriteFile(keyPath, []byte("0123456789abcdef0123456789abcdef"), 0o600); err != nil {
		t.Fatal(err)
	}
	runtime, err := newAuthorityRuntime(store, "edge-pool-a,candidate-canary-v1,"+keyPath)
	if err != nil {
		t.Fatal(err)
	}
	if runtime == nil || len(runtime.groups) != 1 || runtime.groups[0] != "edge-pool-a" || len(runtime.queues) != 1 {
		t.Fatalf("authority runtime is not exact-group scoped: %+v", runtime)
	}
}

func TestNewAuthorityRuntimeRejectsAmbiguousOrMissingKeys(t *testing.T) {
	store, err := releaseguardian.NewAuthorityStore(fake.NewSimpleClientset(), "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "missing.key")
	for _, value := range []string{
		"edge-pool-a,candidate-canary-v1," + missing,
		"edge-pool-a,candidate-canary-v1,relative.key",
		"edge-pool-a,candidate-canary-v1," + missing + ";edge-pool-a,candidate-canary-v2," + missing,
	} {
		if runtime, err := newAuthorityRuntime(store, value); err == nil || runtime != nil {
			t.Fatalf("invalid authority configuration was accepted: %q", value)
		}
	}
}

func TestAuthorityRuntimeCohortLimitLeavesPaginationHeadroom(t *testing.T) {
	for _, test := range []struct {
		nodes int
		want  int64
	}{
		{nodes: 1, want: 8},
		{nodes: 3, want: 16},
		{nodes: 100, want: 404},
		{nodes: 0, want: 8},
	} {
		if got := authorityRuntimeCohortLimit(test.nodes); got != test.want {
			t.Fatalf("authorityRuntimeCohortLimit(%d)=%d, want %d", test.nodes, got, test.want)
		}
	}
}
