package api

import (
	"context"
	"time"

	"fugue/internal/model"

	"golang.org/x/sync/errgroup"
)

// refreshDistributedProjectImageUsageSnapshots reads shared storage evidence
// once, then runs the same inventory aggregation independently for each tenant.
// It refreshes existing complete response caches without extending their TTL or
// publishing empty placeholders while a refresh is running.
func (s *Server) refreshDistributedProjectImageUsageSnapshots(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	started := time.Now()
	apps, err := s.store.ListAppSummaries("", true, false)
	if err != nil {
		return err
	}
	apps = visibleAppsForImageInventory(apps)
	principal := model.Principal{Scopes: map[string]struct{}{"platform.admin": {}}}
	var (
		operations map[string][]model.Operation
		evidence   distributedImageUsageEvidence
	)
	reads, readCtx := errgroup.WithContext(ctx)
	reads.Go(func() error {
		var err error
		operations, err = s.loadProjectImageUsageOperations(readCtx, principal, apps)
		return err
	})
	reads.Go(func() error {
		var err error
		evidence, err = s.loadDistributedImageUsageEvidence(readCtx, apps)
		return err
	})
	if err := reads.Wait(); err != nil {
		return err
	}
	inventories, err := s.buildDistributedImageUsageInventories(ctx, apps, operations, evidence)
	if err != nil {
		return err
	}
	byTenant := make(map[string][]projectImageUsageInventoryResult)
	observedByTenant := make(map[string]time.Time)
	for _, inventory := range inventories {
		tenantID := inventory.App.TenantID
		byTenant[tenantID] = append(byTenant[tenantID], inventory)
		observed := observedByTenant[tenantID]
		if evidence.manifestObservedAt.After(observed) {
			observed = evidence.manifestObservedAt
		}
		for _, location := range evidence.locationsByAppID[inventory.App.ID] {
			if locationObserved := distributedImageLocationObservedAt(location); locationObserved.After(observed) {
				observed = locationObserved
			}
		}
		observedByTenant[tenantID] = observed
	}
	responses := make(map[string]projectImageUsageResponse, len(byTenant)+1)
	for tenantID, tenantInventories := range byTenant {
		key := projectImageUsageCacheKey(model.Principal{TenantID: tenantID})
		responses[key] = aggregateProjectImageUsageInventories(newDistributedProjectImageUsageResponse(observedByTenant[tenantID]), tenantInventories)
	}
	responses[projectImageUsageCacheKey(principal)] = aggregateProjectImageUsageInventories(newDistributedProjectImageUsageResponse(evidence.observedAt), inventories)
	if err := ctx.Err(); err != nil {
		return err
	}
	for key, response := range responses {
		s.projectImageUsageCache.setUnlessUpdatedAfter(key, response, started)
	}
	if s.log != nil {
		s.log.Printf("project image snapshots refreshed duration_ms=%.1f apps=%d scopes=%d", float64(time.Since(started))/float64(time.Millisecond), len(apps), len(responses))
	}
	return nil
}
