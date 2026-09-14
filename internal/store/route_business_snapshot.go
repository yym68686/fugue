package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"

	"fugue/internal/model"
)

// RouteBusinessSnapshot is a detached, consistent read of the business inputs
// needed by route migration. Kubernetes observations are captured separately.
type RouteBusinessSnapshot struct {
	CapturedAt      time.Time                 `json:"captured_at"`
	Revision        string                    `json:"revision"`
	Apps            []model.App               `json:"apps"`
	Domains         []model.AppDomain         `json:"domains"`
	RouteTables     []model.ProjectRouteTable `json:"route_tables"`
	Runtimes        []model.Runtime           `json:"runtimes"`
	RoutePolicies   []model.EdgeRoutePolicy   `json:"route_policies"`
	Releases        []model.AppRelease        `json:"releases"`
	TrafficPolicies []model.AppTrafficPolicy  `json:"traffic_policies"`
}

func (s *Store) CaptureRouteBusinessSnapshot(ctx context.Context) (RouteBusinessSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return RouteBusinessSnapshot{}, err
	}
	if s.usingDatabase() {
		return s.pgCaptureRouteBusinessSnapshot(ctx)
	}
	var snapshot RouteBusinessSnapshot
	err := s.withLockedState(false, func(state *model.State) error {
		snapshot = RouteBusinessSnapshot{CapturedAt: time.Now().UTC(), Apps: state.Apps, Domains: state.AppDomains, RouteTables: state.ProjectRouteTables, Runtimes: state.Runtimes, RoutePolicies: state.EdgeRoutePolicies, Releases: state.AppReleases, TrafficPolicies: state.AppTrafficPolicies}
		raw, err := json.Marshal(snapshot)
		if err != nil {
			return err
		}
		// Detach maps/slices while still holding the file-store read lock.
		return json.Unmarshal(raw, &snapshot)
	})
	if err != nil {
		return RouteBusinessSnapshot{}, err
	}
	if err = ctx.Err(); err != nil {
		return RouteBusinessSnapshot{}, err
	}
	snapshot.normalize()
	material := snapshot
	material.CapturedAt = time.Time{}
	raw, err := json.Marshal(material)
	if err != nil {
		return RouteBusinessSnapshot{}, err
	}
	digest := sha256.Sum256(raw)
	snapshot.Revision = "json:" + hex.EncodeToString(digest[:])
	return snapshot, nil
}

func (snapshot *RouteBusinessSnapshot) normalize() {
	apps := make([]model.App, 0, len(snapshot.Apps))
	for _, app := range snapshot.Apps {
		normalizeAppStatusForRead(&app)
		if isDeletedApp(app) {
			continue
		}
		app.Bindings = nil
		app.BackingServices = nil
		apps = append(apps, app)
	}
	snapshot.Apps = apps
	domains := make([]model.AppDomain, 0, len(snapshot.Domains))
	for _, domain := range snapshot.Domains {
		if domain.Status == model.AppDomainStatusVerified {
			domains = append(domains, domain)
		}
	}
	snapshot.Domains = domains
	sort.Slice(snapshot.Apps, func(i, j int) bool { return snapshot.Apps[i].ID < snapshot.Apps[j].ID })
	sort.Slice(snapshot.Domains, func(i, j int) bool { return snapshot.Domains[i].Hostname < snapshot.Domains[j].Hostname })
	sort.Slice(snapshot.Runtimes, func(i, j int) bool { return snapshot.Runtimes[i].ID < snapshot.Runtimes[j].ID })
	sortProjectRouteTables(snapshot.RouteTables)
	sortEdgeRoutePolicies(snapshot.RoutePolicies)
	for i := range snapshot.RoutePolicies {
		snapshot.RoutePolicies[i].ExclusionLifecycle = model.EdgeRoutePolicyExclusionLifecycleAt(snapshot.RoutePolicies[i], snapshot.CapturedAt)
	}
	sort.Slice(snapshot.Releases, func(i, j int) bool { return snapshot.Releases[i].ID < snapshot.Releases[j].ID })
	sort.Slice(snapshot.TrafficPolicies, func(i, j int) bool { return snapshot.TrafficPolicies[i].AppID < snapshot.TrafficPolicies[j].AppID })
}
