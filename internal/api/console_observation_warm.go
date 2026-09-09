package api

import (
	"context"
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
			if _, err := s.loadPersistentVolumeUsagePolicies(ctx, snapshots); err != nil && ctx.Err() == nil && s.log != nil {
				s.log.Printf("console volume observation warm failed: %v", err)
			}
		}
	})
}
