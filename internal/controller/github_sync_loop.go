package controller

import (
	"context"
	"errors"
	"time"
)

// One worker owns all source checks for this leadership epoch. It starts before
// full reconciliation, never overlaps itself, coalesces ticks, and joins on stop.
func (s *Service) startGitHubSourceSync(ctx context.Context, trigger func()) func() {
	if s.Config.GitHubSyncInterval <= 0 {
		return func() {}
	}
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(s.Config.GitHubSyncInterval)
		defer ticker.Stop()
		for {
			if workerCtx.Err() != nil {
				return
			}
			if err := s.syncGitHubApps(workerCtx); err != nil {
				if !errors.Is(err, context.Canceled) && s.Logger != nil {
					s.Logger.Printf("github sync error: %v", err)
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

func (s *Service) gitHubSyncNow() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now().UTC()
}
