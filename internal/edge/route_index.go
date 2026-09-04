package edge

import (
	"strings"

	"fugue/internal/model"
)

// edgeRouteIndex is immutable after publication. Bundle updates build a new
// index and atomically replace the previous snapshot, so proxy requests never
// contend with bundle lifecycle or telemetry updates.
type edgeRouteIndex struct {
	bundleVersion string
	edgeGroupID   string
	publication   routePublicationMetadata
	byHost        map[string][]edgeIndexedRoute
}

type edgeIndexedRoute struct {
	route      model.EdgeRouteBinding
	pathPrefix string
}

func buildEdgeRouteIndex(bundle model.EdgeRouteBundle, edgeGroupID string, publication routePublicationMetadata) *edgeRouteIndex {
	index := &edgeRouteIndex{
		bundleVersion: strings.TrimSpace(bundle.Version),
		edgeGroupID:   strings.TrimSpace(edgeGroupID),
		publication:   publication,
		byHost:        make(map[string][]edgeIndexedRoute),
	}
	for _, route := range bundle.Routes {
		if !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) {
			continue
		}
		host := normalizeRouteHost(route.Hostname)
		if host == "" {
			continue
		}
		index.byHost[host] = append(index.byHost[host], edgeIndexedRoute{
			route:      route,
			pathPrefix: model.NormalizeAppRoutePathPrefix(route.PathPrefix),
		})
	}
	return index
}

func (index *edgeRouteIndex) routeForHost(host string) (model.EdgeRouteBinding, bool, bool) {
	if index == nil {
		return model.EdgeRouteBinding{}, false, false
	}
	routes := index.byHost[normalizeRouteHost(host)]
	var fallbackActive model.EdgeRouteBinding
	var fallbackInactive model.EdgeRouteBinding
	for _, indexed := range routes {
		route := indexed.route
		if routeMatchesCurrentEdgeGroup(route, index.edgeGroupID) {
			if strings.EqualFold(strings.TrimSpace(route.Status), model.EdgeRouteStatusActive) {
				return route, true, false
			}
			if fallbackInactive.Hostname == "" {
				fallbackInactive = route
			}
			continue
		}
		if strings.EqualFold(strings.TrimSpace(route.Status), model.EdgeRouteStatusActive) {
			if fallbackActive.Hostname == "" {
				fallbackActive = route
			}
			continue
		}
		if fallbackInactive.Hostname == "" {
			fallbackInactive = route
		}
	}
	if fallbackActive.Hostname != "" {
		return fallbackActive, true, true
	}
	if fallbackInactive.Hostname != "" {
		return fallbackInactive, true, true
	}
	return model.EdgeRouteBinding{}, false, false
}

func (index *edgeRouteIndex) routeForRequest(host, requestPath string) (model.EdgeRouteBinding, bool, bool, string, routePublicationMetadata) {
	if index == nil {
		return model.EdgeRouteBinding{}, false, false, "", routePublicationMetadata{}
	}
	requestPath = model.NormalizeAppRoutePathPrefix(requestPath)
	routes := index.byHost[normalizeRouteHost(host)]
	bestPrefixLen := -1
	var currentActive model.EdgeRouteBinding
	var fallbackActive model.EdgeRouteBinding
	var inactive model.EdgeRouteBinding
	inactiveFallbackHit := false
	for _, indexed := range routes {
		if !routePathPrefixMatchesNormalized(indexed.pathPrefix, requestPath) {
			continue
		}
		prefixLen := len(indexed.pathPrefix)
		if prefixLen > bestPrefixLen {
			bestPrefixLen = prefixLen
			currentActive = model.EdgeRouteBinding{}
			fallbackActive = model.EdgeRouteBinding{}
			inactive = model.EdgeRouteBinding{}
			inactiveFallbackHit = false
		}
		if prefixLen < bestPrefixLen {
			continue
		}

		route := indexed.route
		currentEdgeGroup := routeMatchesCurrentEdgeGroup(route, index.edgeGroupID)
		active := strings.EqualFold(strings.TrimSpace(route.Status), model.EdgeRouteStatusActive)
		switch {
		case currentEdgeGroup && active:
			if currentActive.Hostname == "" {
				currentActive = route
			}
		case active:
			if fallbackActive.Hostname == "" {
				fallbackActive = route
			}
		case inactive.Hostname == "":
			inactive = route
			inactiveFallbackHit = true
		}
	}
	if currentActive.Hostname != "" {
		return currentActive, true, false, index.bundleVersion, index.publication
	}
	if fallbackActive.Hostname != "" {
		return fallbackActive, true, true, index.bundleVersion, index.publication
	}
	if inactive.Hostname != "" {
		return inactive, true, inactiveFallbackHit, index.bundleVersion, index.publication
	}
	return model.EdgeRouteBinding{}, false, false, index.bundleVersion, index.publication
}

func routePathPrefixMatchesNormalized(prefix, requestPath string) bool {
	if prefix == "/" {
		return true
	}
	return requestPath == prefix || strings.HasPrefix(requestPath, prefix+"/")
}
