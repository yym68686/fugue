package api

import (
	"encoding/json"
	"log"
	"strings"

	"fugue/internal/model"
)

type platformRoutesEnvelope struct {
	Routes []model.PlatformRoute `json:"routes"`
}

func parsePlatformRoutes(raw string, logger *log.Logger) []model.PlatformRoute {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var routes []model.PlatformRoute
	if err := json.Unmarshal([]byte(raw), &routes); err != nil {
		var envelope platformRoutesEnvelope
		if envelopeErr := json.Unmarshal([]byte(raw), &envelope); envelopeErr != nil {
			if logger != nil {
				logger.Printf("ignoring FUGUE_PLATFORM_ROUTES_JSON: %v", err)
			}
			return nil
		}
		routes = envelope.Routes
	}
	out := make([]model.PlatformRoute, 0, len(routes))
	for _, route := range routes {
		normalized, ok := normalizePlatformRoute(route)
		if ok {
			out = append(out, normalized)
		}
	}
	return out
}

func normalizePlatformRoute(route model.PlatformRoute) (model.PlatformRoute, bool) {
	route.Hostname = normalizeExternalAppDomain(route.Hostname)
	route.Kind = strings.TrimSpace(route.Kind)
	route.UpstreamKind = strings.TrimSpace(route.UpstreamKind)
	route.UpstreamScope = strings.TrimSpace(route.UpstreamScope)
	route.UpstreamURL = strings.TrimSpace(route.UpstreamURL)
	route.TLSPolicy = strings.TrimSpace(route.TLSPolicy)
	rawRoutePolicy := strings.TrimSpace(route.RoutePolicy)
	if rawRoutePolicy == "" {
		route.RoutePolicy = model.EdgeRoutePolicyEnabled
	} else {
		route.RoutePolicy = model.NormalizeEdgeRoutePolicy(rawRoutePolicy)
	}
	route.EdgeGroupMode = strings.TrimSpace(route.EdgeGroupMode)
	route.EdgeGroupID = strings.TrimSpace(route.EdgeGroupID)
	route.Status = strings.TrimSpace(route.Status)
	route.StatusReason = strings.TrimSpace(route.StatusReason)

	if route.Hostname == "" || route.UpstreamURL == "" {
		return model.PlatformRoute{}, false
	}
	if route.Kind == "" {
		route.Kind = model.EdgeRouteKindPlatformRoute
	}
	if route.UpstreamKind == "" {
		route.UpstreamKind = model.EdgeRouteUpstreamKindKubernetesService
	}
	if route.UpstreamScope == "" {
		route.UpstreamScope = model.EdgeRouteUpstreamScopeCluster
	}
	if route.TLSPolicy == "" {
		route.TLSPolicy = model.EdgeRouteTLSPolicyPlatform
	}
	if route.RoutePolicy == "" {
		return model.PlatformRoute{}, false
	}
	if route.EdgeGroupMode == "" {
		route.EdgeGroupMode = model.PlatformRouteEdgeGroupModeAllHealthy
	}
	switch route.EdgeGroupMode {
	case model.PlatformRouteEdgeGroupModeAllHealthy, model.PlatformRouteEdgeGroupModeRegionAware:
		route.EdgeGroupID = ""
	case model.PlatformRouteEdgeGroupModePinned:
		if route.EdgeGroupID == "" {
			return model.PlatformRoute{}, false
		}
	default:
		return model.PlatformRoute{}, false
	}
	if route.Status == "" {
		route.Status = model.EdgeRouteStatusActive
	}
	switch route.Status {
	case model.EdgeRouteStatusActive, model.EdgeRouteStatusDisabled, model.EdgeRouteStatusUnavailable:
	default:
		return model.PlatformRoute{}, false
	}
	if route.TTL <= 0 {
		route.TTL = defaultEdgeDNSTTL
	}
	return route, true
}
