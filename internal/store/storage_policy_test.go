package store

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStoragePolicyReloadAndInvalidRecovery(t *testing.T) {
	s := &Store{}
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.json")
	p := StoragePolicy{Version: 1, CompilerInputChunks: true, CheckpointPages: true, DatabaseMaxOpen: 32, DatabaseMaxIdle: 8, DatabaseIdleSeconds: 60}
	raw, _ := json.Marshal(p)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { s.RunStoragePolicy(ctx, path); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	deadline := time.Now().Add(time.Second)
	for s.StoragePolicy() != p && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.StoragePolicy() != p {
		t.Fatal("policy not loaded")
	}
	for _, raw := range []string{`{"version":2}`, `{"version":1,"database_max_open":2,"database_max_idle":3,"database_idle_seconds":60}`, `{"version":1,"compiler_input_chunks":true,"secret":"value"}`, `{} {}`} {
		if _, err := ParseStoragePolicy([]byte(raw)); err == nil {
			t.Fatal("invalid policy accepted", raw)
		}
	}
}
