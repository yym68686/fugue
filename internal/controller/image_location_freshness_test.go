package controller

import (
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

func TestPresentImageLocationsFallsBackWhenAppScopedEvidenceIsExpired(t *testing.T) {
	t.Parallel()

	const fixture = "image-evidence"
	stateStore := newStoreTestDatabase(t, fixture)
	seedControllerStoreTenant(t, fixture, "tenant_shared")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	const imageRef = "registry.example/fugue-apps/shared:current"
	reconcileNow := time.Now().UTC().Add(3 * time.Hour)
	staleSeenAt := reconcileNow.Add(-3 * time.Hour)
	freshSeenAt := reconcileNow.Add(-5 * time.Minute)
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        "tenant_shared",
		AppID:           "app_current",
		ImageRef:        imageRef,
		Digest:          "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		ClusterNodeName: "worker-old",
		Status:          model.ImageLocationStatusPresent,
		LastSeenAt:      &staleSeenAt,
	}); err != nil {
		t.Fatalf("upsert stale app location: %v", err)
	}
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        "tenant_shared",
		AppID:           "app_other",
		ImageRef:        imageRef,
		Digest:          "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		ClusterNodeName: "worker-fresh",
		Status:          model.ImageLocationStatusPresent,
		LastSeenAt:      &freshSeenAt,
	}); err != nil {
		t.Fatalf("upsert fresh shared location: %v", err)
	}

	svc := &Service{
		Store:  stateStore,
		Config: config.ControllerConfig{ImageCacheInventoryTTL: 2 * time.Hour},
		now:    func() time.Time { return reconcileNow },
	}
	locations, err := svc.presentImageLocations(model.App{ID: "app_current", TenantID: "tenant_shared"}, imageRef)
	if err != nil {
		t.Fatalf("list present image locations: %v", err)
	}
	if len(locations) != 1 || locations[0].ClusterNodeName != "worker-fresh" {
		t.Fatalf("expected fresh tenant-scoped fallback, got %+v", locations)
	}
}
