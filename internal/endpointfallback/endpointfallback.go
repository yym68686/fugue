package endpointfallback

import (
	"fmt"
	"strings"
	"time"

	"fugue/internal/localwal"
	"fugue/internal/model"
)

const (
	DefaultFallbackTTL = 5 * time.Minute
)

type Request struct {
	Hostname        string
	PathPrefix      string
	RouteGeneration string
	ServiceIdentity string
	AllowStateful   bool
	Now             time.Time
}

func BuildLKG(route model.EdgeRouteBinding, endpoints []model.EndpointLKGEndpoint, generatedAt time.Time, ttl time.Duration) model.EndpointLKG {
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	}
	if ttl <= 0 {
		ttl = DefaultFallbackTTL
	}
	serviceIdentity := ServiceIdentity(route)
	policy := model.EndpointFallbackPolicyDisabled
	stateless := routeAllowsDefaultStatelessFallback(route)
	if stateless {
		policy = model.EndpointFallbackPolicyStatelessHTTPDefault
	}
	return model.EndpointLKG{
		SchemaVersion:   model.AutonomySchemaVersionV1,
		Kind:            model.AutonomyArtifactKindEndpointLKG,
		Hostname:        strings.TrimSpace(route.Hostname),
		PathPrefix:      strings.TrimSpace(route.PathPrefix),
		RouteGeneration: strings.TrimSpace(route.RouteGeneration),
		ServiceIdentity: serviceIdentity,
		AppID:           strings.TrimSpace(route.AppID),
		RuntimeID:       strings.TrimSpace(route.RuntimeID),
		ServicePort:     route.ServicePort,
		StatelessHTTP:   stateless,
		FallbackPolicy:  policy,
		Endpoints:       normalizeEndpoints(endpoints),
		GeneratedAt:     generatedAt.UTC(),
		ValidUntil:      generatedAt.UTC().Add(ttl),
		Issuer:          model.BundleIssuerFugue,
	}
}

func Evaluate(lkg model.EndpointLKG, req Request) model.EndpointFallbackDecision {
	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	evidence := map[string]string{
		"route_generation": strings.TrimSpace(lkg.RouteGeneration),
		"service_identity": strings.TrimSpace(lkg.ServiceIdentity),
		"fallback_policy":  strings.TrimSpace(lkg.FallbackPolicy),
	}
	if strings.TrimSpace(lkg.SchemaVersion) != model.AutonomySchemaVersionV1 || strings.TrimSpace(lkg.Kind) != model.AutonomyArtifactKindEndpointLKG {
		return blocked(lkg, "invalid_endpoint_lkg_schema", evidence)
	}
	if lkg.ValidUntil.IsZero() || !now.Before(lkg.ValidUntil) {
		return model.EndpointFallbackDecision{
			Status:        model.EndpointFallbackStatusExpired,
			Reason:        "endpoint_lkg_ttl_expired",
			Hostname:      strings.TrimSpace(lkg.Hostname),
			PathPrefix:    strings.TrimSpace(lkg.PathPrefix),
			EndpointCount: readyEndpointCount(lkg.Endpoints),
			Evidence:      evidence,
		}
	}
	if req.Hostname != "" && !strings.EqualFold(strings.TrimSpace(req.Hostname), strings.TrimSpace(lkg.Hostname)) {
		return blocked(lkg, "hostname_mismatch", evidence)
	}
	if req.RouteGeneration != "" && strings.TrimSpace(req.RouteGeneration) != strings.TrimSpace(lkg.RouteGeneration) {
		return blocked(lkg, "route_generation_mismatch", evidence)
	}
	if req.ServiceIdentity != "" && strings.TrimSpace(req.ServiceIdentity) != strings.TrimSpace(lkg.ServiceIdentity) {
		return blocked(lkg, "service_identity_mismatch", evidence)
	}
	if !lkg.StatelessHTTP && !req.AllowStateful && strings.TrimSpace(lkg.FallbackPolicy) != model.EndpointFallbackPolicyStatefulExplicit {
		return blocked(lkg, "stateful_route_requires_explicit_policy", evidence)
	}
	ready := readyEndpointCount(lkg.Endpoints)
	if ready == 0 {
		return blocked(lkg, "no_ready_endpoint", evidence)
	}
	return model.EndpointFallbackDecision{
		Status:        model.EndpointFallbackStatusAllowed,
		Reason:        "control_plane_unavailable_endpoint_lkg_fallback",
		Hostname:      strings.TrimSpace(lkg.Hostname),
		PathPrefix:    strings.TrimSpace(lkg.PathPrefix),
		EndpointCount: ready,
		TTLSeconds:    int64(lkg.ValidUntil.Sub(now).Seconds()),
		Evidence:      evidence,
	}
}

func RecordWAL(path, nodeID string, lkg model.EndpointLKG, decision model.EndpointFallbackDecision, now time.Time) error {
	path = strings.TrimSpace(path)
	if path == "" || decision.Status == "" {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	expiresAt := lkg.ValidUntil
	evidence := map[string]string{
		"hostname":         strings.TrimSpace(decision.Hostname),
		"path_prefix":      strings.TrimSpace(decision.PathPrefix),
		"status":           strings.TrimSpace(decision.Status),
		"reason":           strings.TrimSpace(decision.Reason),
		"route_generation": strings.TrimSpace(lkg.RouteGeneration),
		"service_identity": strings.TrimSpace(lkg.ServiceIdentity),
		"endpoint_count":   fmt.Sprintf("%d", decision.EndpointCount),
	}
	record, err := localwal.NewRecord("edge-worker", nodeID, "endpoint_lkg_fallback", evidence, lkg.RouteGeneration, &expiresAt, now)
	if err != nil {
		return err
	}
	record.Subject = firstNonEmpty(lkg.Hostname, lkg.ServiceIdentity)
	record.SafetyClass = model.EdgeRepairSafetyL1TemporaryFilter
	return localwal.Append(path, record)
}

func ServiceIdentity(route model.EdgeRouteBinding) string {
	parts := []string{
		strings.TrimSpace(route.TenantID),
		strings.TrimSpace(route.AppID),
		strings.TrimSpace(route.RuntimeID),
		strings.TrimSpace(route.UpstreamKind),
		strings.TrimSpace(route.UpstreamScope),
		strings.TrimSpace(route.UpstreamURL),
		fmt.Sprintf("port:%d", route.ServicePort),
	}
	return strings.Join(parts, "|")
}

func routeAllowsDefaultStatelessFallback(route model.EdgeRouteBinding) bool {
	if strings.TrimSpace(route.UpstreamKind) != model.EdgeRouteUpstreamKindKubernetesService {
		return false
	}
	if strings.TrimSpace(route.UpstreamScope) == model.EdgeRouteUpstreamScopeMesh {
		return false
	}
	return !route.Streaming
}

func normalizeEndpoints(endpoints []model.EndpointLKGEndpoint) []model.EndpointLKGEndpoint {
	out := make([]model.EndpointLKGEndpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		endpoint.IP = strings.TrimSpace(endpoint.IP)
		if endpoint.IP == "" || endpoint.Port <= 0 {
			continue
		}
		endpoint.PodName = strings.TrimSpace(endpoint.PodName)
		endpoint.NodeName = strings.TrimSpace(endpoint.NodeName)
		endpoint.PodCIDR = strings.TrimSpace(endpoint.PodCIDR)
		out = append(out, endpoint)
	}
	return out
}

func readyEndpointCount(endpoints []model.EndpointLKGEndpoint) int {
	count := 0
	for _, endpoint := range endpoints {
		if strings.TrimSpace(endpoint.IP) != "" && endpoint.Port > 0 && endpoint.Ready {
			count++
		}
	}
	return count
}

func blocked(lkg model.EndpointLKG, reason string, evidence map[string]string) model.EndpointFallbackDecision {
	return model.EndpointFallbackDecision{
		Status:        model.EndpointFallbackStatusBlocked,
		Reason:        reason,
		Hostname:      strings.TrimSpace(lkg.Hostname),
		PathPrefix:    strings.TrimSpace(lkg.PathPrefix),
		EndpointCount: readyEndpointCount(lkg.Endpoints),
		Evidence:      evidence,
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}
