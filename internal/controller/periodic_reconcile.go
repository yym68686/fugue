package controller

import (
	"context"
	"errors"
	"time"
)

// startPeriodicReconcile gives one maintenance task a serial worker. Ticks
// coalesce while it runs, so a slow or failed task cannot accumulate goroutines
// or block unrelated tasks. Store calls retain their own bounded DB deadlines;
// the task context also stops scans and Kubernetes requests on leadership loss.
func (s *Service) startPeriodicReconcile(ctx context.Context, interval time.Duration, name string, reconcile func(context.Context) error) func() {
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
				if workerCtx.Err() != nil {
					return
				}
				taskCtx, finish := context.WithTimeout(workerCtx, time.Minute)
				err := reconcile(taskCtx)
				finish()
				if err != nil && !errors.Is(err, context.Canceled) && s.Logger != nil {
					s.Logger.Printf("periodic %s reconcile error: %v", name, err)
				}
			}
		}
	}()
	return func() { cancel(); <-done }
}

func (s *Service) reconcileRuntimeFailovers(ctx context.Context) error {
	if err := s.markRuntimeOfflineStale(ctx); err != nil {
		return err
	}
	return s.queueAutomaticFailovers(ctx)
}
