package store

import (
	"errors"
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

func TestGetNodeUpdaterUsesExactOpaqueIDAndRedactsSecretMaterial(t *testing.T) {
	t.Parallel()

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	_, nodeKeySecret, err := stateStore.CreateScopedNodeKey("", "platform", model.NodeKeyScopePlatformNode)
	if err != nil {
		t.Fatalf("create platform node key: %v", err)
	}
	updater, _, err := stateStore.EnrollNodeUpdater(nodeKeySecret, "worker-a", "198.51.100.10", nil, "worker-a", "machine-a", model.NodeUpdaterCurrentVersion, "join-v1", []string{"heartbeat"})
	if err != nil {
		t.Fatalf("enroll node updater: %v", err)
	}

	got, err := stateStore.GetNodeUpdater(updater.ID)
	if err != nil {
		t.Fatalf("get node updater: %v", err)
	}
	if got.ID != updater.ID || got.ClusterNodeName != "worker-a" || got.TenantID != "" {
		t.Fatalf("unexpected exact node updater: %+v", got)
	}
	if got.TokenHash != "" {
		t.Fatal("exact node updater lookup exposed token hash")
	}
	if _, err := stateStore.GetNodeUpdater("missing-updater"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing updater error=%v, want ErrNotFound", err)
	}
	if _, err := stateStore.GetNodeUpdater(" "); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty updater error=%v, want ErrInvalidInput", err)
	}
}
