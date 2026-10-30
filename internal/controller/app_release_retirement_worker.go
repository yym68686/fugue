package controller

import (
	"context"
	"errors"
	"sort"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

const appReleaseRetirementConcurrency = 100

// This worker belongs to the active leadership lifetime. Its serial app sweep
// and bounded per-app attempts limit concurrency; shutdown joins before handoff.
func (s *Service) startAppReleaseRetirementWorker(ctx context.Context) func() {
	if !s.Config.KubectlApply {
		return func() {}
	}
	s.retirementWorkerRunning.Store(true)
	lastApp := ""
	stop := s.startPeriodicReconcile(ctx, 5*time.Second, "app release retirement", func(ctx context.Context) error { return s.reconcileAppReleaseRetirements(ctx, &lastApp) })
	return func() { stop(); s.retirementWorkerRunning.Store(false) }
}

func (s *Service) reconcileAppReleaseRetirementInline(ctx context.Context, app model.App) error {
	if s.retirementWorkerRunning.Load() {
		return nil
	}
	return s.retryDrainingAppReleaseRetirement(ctx, app)
}

func (s *Service) reconcileAppReleaseRetirements(ctx context.Context, lastApp *string) error {
	// Metadata discovery omits every executable snapshot. Load app state and
	// full drain snapshots only for apps with a withdrawn lifecycle entry.
	targets, err := s.Store.ListAppReleaseRetirementTargets(ctx)
	if err != nil {
		return err
	}
	apps := map[string]string{}
	for _, target := range targets {
		apps[target.AppID] = target.TenantID
	}
	ids := make([]string, 0, len(apps))
	for id := range apps {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	start := sort.SearchStrings(ids, *lastApp)
	if start < len(ids) && ids[start] == *lastApp {
		start++
	}
	ids = append(append([]string{}, ids[start:]...), ids[:start]...)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		*lastApp = id
		app, err := s.Store.GetApp(id)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if app.TenantID != apps[id] {
			continue
		}
		if err := s.retryDrainingAppReleaseRetirement(ctx, app); err != nil && s.Logger != nil {
			s.Logger.Printf("release retirement app=%s deferred: %v", app.ID, err)
		}
	}
	return ctx.Err()
}
