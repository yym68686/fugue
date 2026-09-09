package api

import (
	"context"
	"strings"
	"time"
)

// Runtime observations already expire after 15 seconds. Refresh ahead of that
// deadline so a page can use the same complete, fresh evidence without waiting
// for a cluster-wide read. Failed refreshes never replace the previous snapshot.
func (s *Server) startConsoleObservationWarmLoop(ctx context.Context) {
	startLoop := func(interval time.Duration, refresh func()) {
		go func() {
			for {
				if ctx.Err() != nil {
					return
				}
				refresh()
				if ctx.Err() != nil {
					return
				}
				timer := time.NewTimer(interval)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}()
	}
	startLoop(max(s.managedAppStatusCache.cacheTTL()/2, time.Second), func() {
		if _, err := s.refreshManagedAppStatuses(ctx); err != nil && ctx.Err() == nil && s.log != nil {
			s.log.Printf("console runtime observation warm failed: %v", err)
		}
	})
	startLoop(30*time.Second, func() {
		if snapshots, err := s.loadClusterNodeInventory(ctx); err == nil {
			// The first runtime-list request should not have to materialize
			// locations that the inventory observer has already discovered.
			// This is the same hash-guarded reconciliation retained by GET.
			if s.store != nil && ctx.Err() == nil {
				if err := s.syncManagedSharedLocationRuntimesFromSnapshots(snapshots); err != nil && s.log != nil {
					s.log.Printf("console runtime location warm failed: %v", err)
				}
			}
			if _, err := s.loadPersistentVolumeUsagePolicies(ctx, snapshots); err != nil && ctx.Err() == nil && s.log != nil {
				s.log.Printf("console volume observation warm failed: %v", err)
			}
		}
	})
	if s.store != nil && strings.EqualFold(strings.TrimSpace(s.imageStoreMode), "distributed") {
		startLoop(defaultProjectImageUsageCacheTTL/2, func() {
			refreshCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			if err := s.refreshDistributedProjectImageUsageSnapshots(refreshCtx); err != nil && ctx.Err() == nil && s.log != nil {
				s.log.Printf("project image snapshot warm failed: %v", err)
			}
		})
	}
}
