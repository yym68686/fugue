package platformconfig

import (
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

// OriginObservation is a fixed runtime fact, never a serving configuration.
type OriginObservation struct {
	Ref                string    `json:"ref"`
	ObservedAt         time.Time `json:"observed_at"`
	Status             string    `json:"status"`
	StatusReason       string    `json:"status_reason,omitempty"`
	RuntimeID          string    `json:"runtime_id,omitempty"`
	RuntimeType        string    `json:"runtime_type,omitempty"`
	RuntimeEdgeGroupID string    `json:"runtime_edge_group_id,omitempty"`
	RuntimeClusterNode string    `json:"runtime_cluster_node,omitempty"`
}

// CompiledRoute adds fixed origin placement to the declared route. It exists
// only inside the immutable artifact; its observed status is not persisted
// back into PlatformIntent.
type CompiledRoute struct {
	RouteIntent
	RuntimeType             string     `json:"runtime_type,omitempty"`
	RuntimeEdgeGroupID      string     `json:"runtime_edge_group_id,omitempty"`
	RuntimeClusterNode      string     `json:"runtime_cluster_node,omitempty"`
	DNSPlacementEdgeGroupID string     `json:"dns_placement_edge_group_id,omitempty"`
	ExcludedEdgeIDs         []string   `json:"excluded_edge_ids,omitempty"`
	ExcludedEdgeGroupIDs    []string   `json:"excluded_edge_group_ids,omitempty"`
	ExclusionReason         string     `json:"exclusion_reason,omitempty"`
	ExclusionExpiresAt      *time.Time `json:"exclusion_expires_at,omitempty"`
	ExclusionLifecycle      string     `json:"exclusion_lifecycle,omitempty"`
	MinHealthyEdgeNodes     int        `json:"min_healthy_edge_nodes,omitempty"`
}

func ResolveRouteOrigins(routes []RouteIntent, snapshot RuntimeSnapshot, policy PolicySnapshot) ([]CompiledRoute, error) {
	if len(snapshot.Origins) > 10000 {
		return nil, fmt.Errorf("runtime snapshot has too many origins")
	}
	byRef := make(map[string]OriginObservation, len(snapshot.Origins))
	for _, origin := range snapshot.Origins {
		if origin.Ref == "" || origin.Ref != strings.TrimSpace(origin.Ref) {
			return nil, fmt.Errorf("origin observation requires a canonical ref")
		}
		if _, exists := byRef[origin.Ref]; exists {
			return nil, fmt.Errorf("duplicate origin observation ref")
		}
		if snapshot.CapturedAt == nil || snapshot.CapturedAt.IsZero() || origin.ObservedAt.IsZero() || origin.ObservedAt.After(*snapshot.CapturedAt) {
			return nil, fmt.Errorf("origin observation requires a fixed captured_at at or after observed_at")
		}
		if snapshot.CapturedAt.Sub(origin.ObservedAt).Seconds() > float64(policy.MaxStaleSeconds) {
			return nil, fmt.Errorf("origin observation exceeds policy freshness")
		}
		switch origin.Status {
		case model.EdgeRouteStatusActive, model.EdgeRouteStatusDisabled, model.EdgeRouteStatusUnavailable:
		default:
			return nil, fmt.Errorf("origin observation has invalid status")
		}
		byRef[origin.Ref] = origin
	}
	out := make([]CompiledRoute, 0, len(routes))
	for _, route := range routes {
		compiled := CompiledRoute{RouteIntent: route}
		if route.OriginRef != "" {
			origin, exists := byRef[route.OriginRef]
			if !exists {
				return nil, fmt.Errorf("route origin_ref is missing from runtime snapshot")
			}
			if route.RuntimeID == "" || route.RuntimeID != origin.RuntimeID {
				return nil, fmt.Errorf("route runtime does not match origin observation")
			}
			compiled.RuntimeType = origin.RuntimeType
			compiled.RuntimeEdgeGroupID = origin.RuntimeEdgeGroupID
			compiled.RuntimeClusterNode = origin.RuntimeClusterNode
			// Retain non-active origin diagnostics even for a disabled route.
			// Enabled remains a separate immutable intent gate: an observed
			// recovery can never re-enable it. Explicit maintenance status wins.
			if (route.Status == "" || route.Status == model.EdgeRouteStatusActive) && origin.Status != model.EdgeRouteStatusActive {
				compiled.Status = origin.Status
				compiled.StatusReason = origin.StatusReason
			}
		}
		out = append(out, compiled)
	}
	return out, nil
}
