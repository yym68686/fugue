package managedappstatus_test

import (
	"testing"
	"time"

	"fugue/internal/managedappstatus"
	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestPublishedObservationOwnsItsMutableData(t *testing.T) {
	var cache managedappstatus.Cache
	present := true
	app := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{Labels: map[string]string{"app": "original"}},
		Spec: runtime.ManagedAppSpec{
			Bindings: []model.ServiceBinding{{Env: map[string]string{"name": "original"}}},
		},
	}
	evidence := managedappstatus.Evidence{ImagePresent: &present, EvidenceSources: []string{"source"}}
	expires := time.Now().Add(time.Minute)
	items := map[string]runtime.ManagedAppObject{"app": app}
	cache.PublishApp("single", managedappstatus.AppSnapshot{
		Ok: true, Found: true, ClusterID: "cluster", ExpiresAt: expires, Managed: app, Evidence: evidence,
	})
	cache.PublishInventory(managedappstatus.NewInventory(managedappstatus.InventoryInput{
		Items: items, Evidence: map[string]managedappstatus.Evidence{"app": evidence},
		Ok: true, ClusterID: "cluster", ExpiresAt: expires,
	}))
	app.Metadata.Labels["app"] = "caller"
	app.Spec.Bindings[0].Env["name"] = "caller"
	evidence.EvidenceSources[0] = "caller"
	present = false
	delete(items, "app")

	inventory, ok, expired := cache.Inventory()
	if !ok || expired || !inventory.Has("app") {
		t.Fatal("expected a fresh published inventory")
	}
	found, ok, expired := cache.App("app")
	if !ok || expired || !found.Found {
		t.Fatal("expected a fresh per-app entry from inventory")
	}
	found.Managed.Spec.Bindings[0].Env["name"] = "reader"
	*found.Evidence.ImagePresent = false
	inventory.CopyItems()["app"].Metadata.Labels["app"] = "reader"
	inventory.EvidenceFor("app").EvidenceSources[0] = "reader"
	for _, result := range []managedappstatus.AppSnapshot{inventory.App("app"), lookupApp(t, &cache, "app")} {
		if result.Managed.Metadata.Labels["app"] != "original" ||
			result.Managed.Spec.Bindings[0].Env["name"] != "original" ||
			!*result.Evidence.ImagePresent || result.Evidence.EvidenceSources[0] != "source" {
			t.Fatalf("mutation escaped into cache: %+v", result)
		}
	}
	if removed := lookupApp(t, &cache, "single"); removed.Found || !removed.Ok {
		t.Fatalf("complete inventory must preserve negative evidence: %+v", removed)
	}
}

func TestUnidentifiedInventoryCannotReplaceObservation(t *testing.T) {
	var cache managedappstatus.Cache
	now := time.Now()
	item := runtime.ManagedAppObject{Spec: runtime.ManagedAppSpec{AppID: "app"}}
	cache.PublishInventory(managedappstatus.NewInventory(managedappstatus.InventoryInput{
		Items: map[string]runtime.ManagedAppObject{"app": item},
		Ok:    true, ClusterID: "cluster", RefreshedAt: now, ExpiresAt: now.Add(time.Minute),
	}))
	cache.PublishInventory(managedappstatus.NewInventory(managedappstatus.InventoryInput{
		Ok: true, RefreshedAt: now.Add(time.Second), ExpiresAt: now.Add(time.Minute),
	}))
	inventory, ok, expired := cache.Inventory()
	if !ok || expired || inventory.ClusterID != "cluster" || !inventory.Has("app") {
		t.Fatal("a read without cluster identity replaced a confirmed inventory")
	}
	cache.Invalidate()
	if _, ok, _ := cache.Inventory(); ok {
		t.Fatal("invalidated inventory is still available")
	}
	if _, ok, _ := cache.App("app"); ok {
		t.Fatal("invalidated app observation is still available")
	}
}

func TestManagedAppObservationSequenceIsMonotonic(t *testing.T) {
	t.Parallel()

	var cache managedappstatus.Cache
	first := cache.NextSequence()
	second := cache.NextSequence()
	third := cache.NextSequence()
	if !(first > 0 && first < second && second < third) {
		t.Fatalf("observation sequence is not monotonic: %d %d %d", first, second, third)
	}
}

func lookupApp(t *testing.T, cache *managedappstatus.Cache, key string) managedappstatus.AppSnapshot {
	t.Helper()
	entry, ok, expired := cache.App(key)
	if !ok || expired {
		t.Fatalf("expected a fresh entry for %s", key)
	}
	return entry
}
