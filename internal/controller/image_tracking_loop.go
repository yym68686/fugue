package controller

import (
	"context"
	"errors"
	"time"
)

// One worker owns image checks for this leadership epoch. It starts before full
// reconciliation, coalesces ticks without overlapping scans, and joins on stop.
func (s *Service) startAppImageTracking(ctx context.Context, trigger func()) func() {
	if s.Config.ImageTrackingInterval <= 0 {
		return func() {}
	}
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(s.Config.ImageTrackingInterval)
		defer ticker.Stop()
		for {
			if workerCtx.Err() != nil {
				return
			}
			if err := s.syncTrackedAppImages(workerCtx); err != nil {
				if !errors.Is(err, context.Canceled) && s.Logger != nil {
					s.Logger.Printf("image tracking sync error: %v", err)
				}
			} else if workerCtx.Err() == nil {
				trigger()
			}
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() { cancel(); <-done }
}
