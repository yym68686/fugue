package api

import (
	"context"
	"fmt"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
	"golang.org/x/sync/errgroup"
	"strings"
)

type imageObservationKey struct{ tenant, app, runtime, image, status string }

// A refresh observes the whole cluster. Load its durable evidence once instead
// of paying a database round trip for every app, image reference, and status.
type managedAppStoreSnapshot struct {
	policies      map[string]model.AppTrafficPolicy
	releases      map[string]model.AppRelease
	locations     map[string][]model.ImageLocation
	locationIndex map[imageObservationKey][]model.ImageLocation
}

func (s *Server) loadManagedAppStoreSnapshot(ctx context.Context, appIDs ...string) (*managedAppStoreSnapshot, error) {
	return s.loadManagedAppStoreSnapshotScoped(ctx, nil, nil, appIDs...)
}
func (s *Server) loadManagedAppStoreSnapshotScoped(ctx context.Context, apps []model.App, observed map[string]runtime.ManagedAppObject, appIDs ...string) (*managedAppStoreSnapshot, error) {
	started := time.Now()
	defer func() { serverTimingFromContext(ctx).Add("observation_store_snapshot", time.Since(started)) }()
	var policies []model.AppTrafficPolicy
	var releases []model.AppRelease
	var locations []model.ImageLocation
	group, readCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		started := time.Now()
		defer func() { serverTimingFromContext(ctx).Add("observation_traffic_policies", time.Since(started)) }()
		var err error
		policies, err = s.store.ListAppTrafficPoliciesContext(readCtx, "", true)
		return err
	})
	group.Go(func() error {
		started := time.Now()
		defer func() { serverTimingFromContext(ctx).Add("observation_releases", time.Since(started)) }()
		var err error
		releases, err = s.store.ListAppReleaseMetadataContext(readCtx, model.AppReleaseFilter{PlatformAdmin: true, ActiveOnly: true})
		return err
	})
	// The store treats an empty status as Present. Read every explicit status
	// so the batch preserves negative and pending evidence as well.
	if apps == nil {
		var locationsMu sync.Mutex
		for _, status := range []string{model.ImageLocationStatusPresent, model.ImageLocationStatusPulling, model.ImageLocationStatusMissing, model.ImageLocationStatusFailed} {
			group.Go(func() error {
				started := time.Now()
				defer func() { serverTimingFromContext(ctx).Add("observation_locations_"+status, time.Since(started)) }()
				items, err := s.store.ListImageLocationsContext(readCtx, model.ImageLocationFilter{PlatformAdmin: true, Status: status, AppIDs: appIDs})
				if err != nil {
					return err
				}
				locationsMu.Lock()
				locations = append(locations, items...)
				locationsMu.Unlock()
				return nil
			})
		}
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
	if apps != nil {
		var scopes []store.ImageLocationScope
		for _, app := range apps {
			imageRef, runtimeID := app.Spec.Image, app.Spec.RuntimeID
			if managed, ok := observed[app.ID]; ok {
				imageRef = managed.Spec.AppSpec.Image
				if strings.TrimSpace(managed.Spec.AppSpec.RuntimeID) != "" {
					runtimeID = managed.Spec.AppSpec.RuntimeID
				}
			}
			if release, ok := s.servingReleaseTrafficTargetWithSnapshot(app, result); ok {
				imageRef = release.ResolvedImageRef
			}
			refs := []string{imageRef}
			if app.Source != nil {
				refs = append(refs, app.Source.ImageRef, app.Source.ResolvedImageRef)
			}
			for _, ref := range refs {
				scopes = append(scopes, store.ImageLocationScope{TenantID: app.TenantID, AppID: app.ID, RuntimeID: runtimeID, ImageRef: ref})
			}
		}
		var err error
		locations, err = s.store.ListImageLocationObservations(ctx, scopes)
		if err != nil {
			return nil, err
		}
	}
	result.locationIndex = make(map[imageObservationKey][]model.ImageLocation)
	for _, location := range locations {
		key := imageObservationKey{location.TenantID, location.AppID, location.RuntimeID, location.ImageRef, location.Status}
		result.locationIndex[key] = append(result.locationIndex[key], location)
		result.locations[location.AppID] = append(result.locations[location.AppID], location)
	}
	return result, nil
}

func (snapshot *managedAppStoreSnapshot) imageLocations(filter model.ImageLocationFilter) ([]model.ImageLocation, error) {
	if snapshot.locationIndex != nil {
		return append([]model.ImageLocation(nil), snapshot.locationIndex[imageObservationKey{filter.TenantID, filter.AppID, filter.RuntimeID, filter.ImageRef, filter.Status}]...), nil
	}
	result := make([]model.ImageLocation, 0)
	for _, location := range snapshot.locations[filter.AppID] {
		if location.TenantID == filter.TenantID && location.ImageRef == filter.ImageRef &&
			location.RuntimeID == filter.RuntimeID && location.Status == filter.Status {
			result = append(result, location)
		}
	}
	return result, nil
}
