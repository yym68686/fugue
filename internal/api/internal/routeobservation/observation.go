// Package routeobservation owns API-local observations of route production. Its
// state is diagnostic only: it cannot change an intent, route, or serving LKG.
package routeobservation

import (
	"strings"
	"sync"

	"fugue/internal/model"
)

const maxRememberedObservations = 4096

// Decisions is the per-process change log filter for route decisions and
// intent observations. Its zero value is ready to use.
type Decisions struct {
	mu   sync.Mutex
	last map[string]string
}

func (d *Decisions) RouteChanged(route model.EdgeRouteBinding) bool {
	key := strings.Join([]string{
		routeHost(route.Hostname),
		model.NormalizeAppRoutePathPrefix(route.PathPrefix),
		strings.TrimSpace(route.RouteKind),
		strings.TrimSpace(route.EdgeGroupID),
	}, "\x00")
	return d.changed(key, strings.TrimSpace(route.DecisionID))
}

func (d *Decisions) IntentChanged(appID, hostname, pathPrefix, kind, observationID string) bool {
	key := strings.Join([]string{"intent_observation", appID, routeHost(hostname), model.NormalizeAppRoutePathPrefix(pathPrefix), kind}, "\x00")
	return d.changed(key, observationID)
}

func routeHost(hostname string) string {
	return strings.Trim(strings.TrimSpace(strings.ToLower(hostname)), ".")
}

func (d *Decisions) changed(key, observationID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.last[key] == observationID {
		return false
	}
	if d.last == nil || len(d.last) >= maxRememberedObservations {
		d.last = make(map[string]string)
	}
	d.last[key] = observationID
	return true
}
