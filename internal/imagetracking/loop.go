package imagetracking

import (
	"context"
	"errors"
	"time"
)

// One worker owns image checks for this leadership epoch. It starts before full
// reconciliation, coalesces ticks without overlapping scans, and joins on stop.
func (s *Poller) Start(ctx context.Context, trigger func()) func() {
	if s.interval <= 0 {
		return func() {}
	}
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			if workerCtx.Err() != nil {
				return
			}
			if err := s.Sync(workerCtx); err != nil {
				if !errors.Is(err, context.Canceled) && s.logger != nil {
					s.logger.Printf("image tracking sync error: %v", err)
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
