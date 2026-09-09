package api

import (
	"context"
	"fmt"
	"sync"
	"time"

	"fugue/internal/model"
	"golang.org/x/sync/errgroup"
)

// A refresh observes the whole cluster. Load its durable evidence once instead
// of paying a database round trip for every app, image reference, and status.
type managedAppStoreSnapshot struct {
	policies  map[string]model.AppTrafficPolicy
	releases  map[string]model.AppRelease
	locations map[string][]model.ImageLocation
}

func (s *Server) loadManagedAppStoreSnapshot(ctx context.Context, appIDs ...string) (*managedAppStoreSnapshot, error) {
	started := time.Now()
	defer func() { serverTimingFromContext(ctx).Add("observation_store_snapshot", time.Since(started)) }()
	var policies []model.AppTrafficPolicy
	var releases []model.AppRelease
	var locations []model.ImageLocation
	group := new(errgroup.Group)
	group.Go(func() error {
		var err error
		policies, err = s.store.ListAppTrafficPolicies("", true)
		return err
	})
	group.Go(func() error {
		var err error
		releases, err = s.store.ListAppReleases(model.AppReleaseFilter{PlatformAdmin: true, ActiveOnly: true})
		return err
	})
	// The store treats an empty status as Present. Read every explicit status
	// so the batch preserves negative and pending evidence as well.
	var locationsMu sync.Mutex
	for _, status := range []string{model.ImageLocationStatusPresent, model.ImageLocationStatusPulling, model.ImageLocationStatusMissing, model.ImageLocationStatusFailed} {
		group.Go(func() error {
			items, err := s.store.ListImageLocations(model.ImageLocationFilter{PlatformAdmin: true, Status: status, AppIDs: appIDs})
			if err != nil {
				return err
			}
			locationsMu.Lock()
			locations = append(locations, items...)
			locationsMu.Unlock()
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, fmt.Errorf("load runtime observation store snapshot: %w", err)
	}
	result := &managedAppStoreSnapshot{
		policies:  make(map[string]model.AppTrafficPolicy, len(policies)),
		releases:  make(map[string]model.AppRelease, len(releases)),
		locations: make(map[string][]model.ImageLocation),
	}
	for _, policy := range policies {
		result.policies[policy.AppID] = policy
	}
	for _, release := range releases {
		result.releases[release.ID] = release
	}
	for _, location := range locations {
		result.locations[location.AppID] = append(result.locations[location.AppID], location)
	}
	return result, nil
}

func (snapshot *managedAppStoreSnapshot) imageLocations(filter model.ImageLocationFilter) ([]model.ImageLocation, error) {
	result := make([]model.ImageLocation, 0)
	for _, location := range snapshot.locations[filter.AppID] {
		if location.TenantID == filter.TenantID && location.ImageRef == filter.ImageRef &&
			location.RuntimeID == filter.RuntimeID && location.Status == filter.Status {
			result = append(result, location)
		}
	}
	return result, nil
}
