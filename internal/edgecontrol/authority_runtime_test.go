package edgecontrol

import (
	"testing"
	"time"
)

func TestAuthorityRuntimeSkipsStableInputsUntilRefresh(t *testing.T) {
	runtime := &AuthorityRuntime{
		lastRouteIntentGen:  "route-1",
		lastInventoryDigest: "inventory-1",
		lastInventoryKnown:  true,
		lastReconcileAt:     time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC),
		hasBatch:            true,
	}

	if runtime.cachedBatchValid("route-1", "inventory-1", true, runtime.lastReconcileAt.Add(4*time.Minute+59*time.Second)) != true {
		t.Fatal("stable inputs should use the cached batch before the refresh interval")
	}
	if runtime.cachedBatchValid("route-1", "inventory-1", true, runtime.lastReconcileAt.Add(authorityRuntimeRefreshInterval)) {
		t.Fatal("cached batch must refresh at the bounded interval")
	}
	if runtime.cachedBatchValid("route-2", "inventory-1", true, runtime.lastReconcileAt.Add(time.Minute)) {
		t.Fatal("route intent changes must force reconciliation")
	}
	if runtime.cachedBatchValid("route-1", "inventory-2", true, runtime.lastReconcileAt.Add(time.Minute)) {
		t.Fatal("inventory changes must force reconciliation")
	}
	if runtime.cachedBatchValid("route-1", "inventory-1", false, runtime.lastReconcileAt.Add(time.Minute)) {
		t.Fatal("unknown inventory must never use a cached batch")
	}
}
