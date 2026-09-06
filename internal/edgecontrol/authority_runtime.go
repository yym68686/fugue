package edgecontrol

import (
	"context"
	"errors"
	"sync"
	"time"
)

const authorityRuntimeRefreshInterval = 5 * time.Minute

type AuthorityRuntime struct {
	RouteIntents RouteIntentSource
	Compiler     GroupShadowCompiler
	Publisher    GroupAuthorityPublisher
	GroupIDs     []string
	Status       *AuthorityRuntimeState

	mu                  sync.Mutex
	lastRouteIntentGen  string
	lastInventoryDigest string
	lastInventoryKnown  bool
	lastReconcileAt     time.Time
	lastBatch           AuthorityRuntimeBatch
	hasBatch            bool
}

type AuthorityRuntimeBatch struct {
	Compiled  GroupShadowBatch    `json:"compiled"`
	Published GroupAuthorityBatch `json:"published"`
	Candidate GroupCandidateBatch `json:"candidate,omitempty"`
}

func (runtime *AuthorityRuntime) cachedBatchValid(routeIntentGeneration, inventoryDigest string, inventoryKnown bool, now time.Time) bool {
	if runtime == nil {
		return false
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.hasBatch && inventoryKnown && runtime.lastInventoryKnown && routeIntentGeneration == runtime.lastRouteIntentGen && inventoryDigest == runtime.lastInventoryDigest && now.Sub(runtime.lastReconcileAt) < authorityRuntimeRefreshInterval
}

func (runtime *AuthorityRuntime) RunOnce(ctx context.Context) (AuthorityRuntimeBatch, bool, error) {
	if runtime.RouteIntents == nil {
		return AuthorityRuntimeBatch{}, false, errors.New("edge-control authority RouteIntent source is nil")
	}
	snapshot, err := runtime.RouteIntents.FetchRouteIntents(ctx)
	if err != nil {
		return AuthorityRuntimeBatch{}, false, err
	}
	inventoryDigest := ""
	inventoryKnown := false
	if runtime.Compiler.Inventory != nil && len(runtime.GroupIDs) == 1 {
		if inventory, readErr := runtime.Compiler.Inventory.ReadGroupInventory(ctx, runtime.GroupIDs[0]); readErr == nil {
			inventoryDigest = groupInventorySemanticDigest(inventory)
			inventoryKnown = true
		}
	}
	now := time.Now().UTC()
	if runtime.cachedBatchValid(snapshot.Generation, inventoryDigest, inventoryKnown, now) {
		runtime.mu.Lock()
		batch := runtime.lastBatch
		runtime.mu.Unlock()
		return batch, true, nil
	}
	compiled, err := runtime.Compiler.Reconcile(ctx, snapshot, runtime.GroupIDs)
	if err != nil {
		return AuthorityRuntimeBatch{}, false, err
	}
	published, err := runtime.Publisher.Publish(ctx, compiled)
	if err != nil {
		return AuthorityRuntimeBatch{}, false, err
	}
	batch := AuthorityRuntimeBatch{Compiled: compiled, Published: published}
	runtime.mu.Lock()
	runtime.lastRouteIntentGen = snapshot.Generation
	runtime.lastInventoryDigest = inventoryDigest
	runtime.lastInventoryKnown = inventoryKnown
	runtime.lastReconcileAt = now
	runtime.lastBatch = batch
	runtime.hasBatch = true
	runtime.mu.Unlock()
	return batch, false, nil
}

type AuthorityRuntimeObservation struct {
	RouteIntentGeneration string `json:"route_intent_generation,omitempty"`
	Published             int    `json:"published"`
	Failed                int    `json:"failed"`
	CandidatePublished    int    `json:"candidate_published,omitempty"`
	FailureCode           string `json:"failure_code,omitempty"`
	SkippedUnchanged      bool   `json:"skipped_unchanged,omitempty"`
}

func (runtime *AuthorityRuntime) Run(ctx context.Context, interval time.Duration, observe func(AuthorityRuntimeObservation)) error {
	if ctx == nil {
		return errors.New("edge-control authority runtime context is nil")
	}
	if interval <= 0 {
		return errors.New("edge-control authority runtime interval must be positive")
	}
	if observe == nil {
		observe = func(AuthorityRuntimeObservation) {}
	}
	if err := ctx.Err(); err != nil {
		return nil
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		batch, skipped, err := runtime.RunOnce(ctx)
		observation := AuthorityRuntimeObservation{}
		if err != nil {
			observation.FailureCode = RouteIntentFailureCode(err)
		} else {
			observation.SkippedUnchanged = skipped
			observation.RouteIntentGeneration = batch.Published.RouteIntentGeneration
			observation.Published = batch.Published.Published
			observation.Failed = batch.Published.Failed
			for _, result := range batch.Published.Results {
				if result.Status == GroupAuthorityStatusFailed && result.FailureCode != "" {
					observation.FailureCode = result.FailureCode
					break
				}
			}
		}
		if runtime.Status != nil {
			runtime.Status.Observe(observation)
		}
		observe(observation)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
