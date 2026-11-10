package controller

import (
	"context"
	"sync"
	"time"
)

// At most eight namespace discoveries are retained for a worker sweep. This
// context is deliberately distinct from initialReleaseDrainInventoryKey: a
// fresh post-observation capture must never inherit the shared discovery.
type retirementSweepInventory struct {
	sync.Mutex
	namespaces map[string]*retirementNamespaceDiscovery
}
type retirementNamespaceDiscovery struct {
	sync.Mutex
	initial  *initialReleaseDrainInventory
	presence *retirementResourcePresence
	at       time.Time
}
type retirementResourcePresence struct {
	deployments, services map[string]bool
}
type retirementSweepInventoryKey struct{}

func withRetirementSweepInventory(ctx context.Context) context.Context {
	return context.WithValue(ctx, retirementSweepInventoryKey{}, &retirementSweepInventory{namespaces: make(map[string]*retirementNamespaceDiscovery)})
}

func retirementNamespace(ctx context.Context, ns string) *retirementNamespaceDiscovery {
	sweep, _ := ctx.Value(retirementSweepInventoryKey{}).(*retirementSweepInventory)
	if sweep == nil {
		return nil
	}
	sweep.Lock()
	defer sweep.Unlock()
	if entry := sweep.namespaces[ns]; entry != nil && time.Since(entry.at) < 15*time.Second {
		return entry
	}
	if len(sweep.namespaces) >= 8 {
		for key := range sweep.namespaces {
			delete(sweep.namespaces, key)
			break
		}
	}
	entry := &retirementNamespaceDiscovery{initial: &initialReleaseDrainInventory{byNamespace: make(map[string]*releaseDrainInventory)}, at: time.Now()}
	sweep.namespaces[ns] = entry
	return entry
}

func retirementInitialDiscoveryContext(ctx context.Context, ns string) context.Context {
	if entry := retirementNamespace(ctx, ns); entry != nil {
		return context.WithValue(ctx, initialReleaseDrainInventoryKey{}, entry.initial)
	}
	return withInitialReleaseDrainInventory(ctx)
}
