package api

import (
	"errors"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"net/http"
	"strings"
	"time"
)

func (s *Server) handleGetEdgeQualityRank(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect edge quality rank")
		return
	}
	httpx.WriteError(w, http.StatusGone, "legacy composite ranking retired; use physical quality and actual DNS receipts")
}

type edgeQualityRankScope struct {
	Kind    string
	Value   string
	Country string
	Region  string
	ASN     string
}

func edgeQualityRankCandidateForNode(node model.EdgeNode, policy model.EdgeRoutePolicy, now time.Time, quarantineByNode map[string]model.NodeDeepHealthResult) model.EdgeQualityRankCandidate {
	tlsReady := strings.EqualFold(strings.TrimSpace(node.TLSStatus), model.EdgeTLSStatusReady) || node.TLSReadyAt != nil
	routeReady := strings.TrimSpace(node.RouteBundleVersion) != "" && node.CaddyRouteCount > 0
	heartbeatFresh := edgeNodeHeartbeatFresh(node, now)
	candidate := model.EdgeQualityRankCandidate{
		EdgeID:      strings.TrimSpace(node.ID),
		EdgeGroupID: strings.TrimSpace(node.EdgeGroupID),
		Region:      strings.TrimSpace(node.Region),
		Country:     strings.TrimSpace(node.Country),
		Healthy:     node.Healthy && heartbeatFresh,
		Draining:    node.Draining,
		RouteReady:  routeReady,
		TLSReady:    tlsReady,
	}
	if !heartbeatFresh {
		candidate.Reason = "edge node heartbeat stale"
	}
	excluded, reason := edgeQualityNodeExcludedByPolicy(node, policy, now)
	candidate.Excluded = excluded
	candidate.ExclusionReason = reason
	if edgeNodeQuarantined(node, quarantineByNode) {
		candidate.Excluded = true
		if quarantine, ok := quarantineByNode[strings.TrimSpace(node.ID)]; ok {
			candidate.ExclusionReason = firstNonEmpty(quarantine.QuarantineReason, "node quarantined by deep health")
		} else {
			candidate.ExclusionReason = "node quarantined by deep health"
		}
	}
	return candidate
}

func edgeQualityRankCandidateHardGated(candidate model.EdgeQualityRankCandidate) bool {
	return candidate.Excluded || !candidate.Healthy || candidate.Draining || !candidate.RouteReady || !candidate.TLSReady
}

func edgeQualityRankGateReason(candidate model.EdgeQualityRankCandidate) string {
	switch {
	case candidate.Excluded:
		return "excluded by service edge policy"
	case !candidate.Healthy:
		return "edge node unhealthy"
	case candidate.Draining:
		return "edge node draining"
	case !candidate.RouteReady:
		return "edge node route bundle not ready"
	case !candidate.TLSReady:
		return "edge node TLS not ready"
	default:
		return ""
	}
}

func edgeQualityNodeExcludedByPolicy(node model.EdgeNode, policy model.EdgeRoutePolicy, now time.Time) (bool, string) {
	if strings.TrimSpace(policy.Hostname) == "" || !policy.Enabled {
		return false, ""
	}
	expires := policy.ExclusionExpiresAt
	exclusionActive := expires == nil || expires.IsZero() || now.Before(expires.UTC())
	if pinned := strings.TrimSpace(policy.EdgeGroupID); pinned != "" && !strings.EqualFold(pinned, node.EdgeGroupID) {
		return true, "edge group is not selected by service policy"
	}
	if exclusionActive {
		for _, edgeID := range policy.ExcludedEdgeIDs {
			if strings.EqualFold(strings.TrimSpace(edgeID), strings.TrimSpace(node.ID)) {
				return true, firstNonEmpty(strings.TrimSpace(policy.ExclusionReason), "edge node excluded by service policy")
			}
		}
		for _, edgeGroupID := range policy.ExcludedEdgeGroupIDs {
			if strings.EqualFold(strings.TrimSpace(edgeGroupID), strings.TrimSpace(node.EdgeGroupID)) {
				return true, firstNonEmpty(strings.TrimSpace(policy.ExclusionReason), "edge group excluded by service policy")
			}
		}
	}
	return false, ""
}

func parseEdgeQualityRankScope(raw string) (edgeQualityRankScope, error) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" || raw == "global" {
		return edgeQualityRankScope{Kind: "global", Value: "global"}, nil
	}
	kind, value, ok := strings.Cut(raw, ":")
	if !ok {
		return edgeQualityRankScope{}, errors.New("scope must be global, country:<country>, region:<country>:<region>, or asn:<asn>")
	}
	value = strings.TrimSpace(value)
	switch kind {
	case "asn":
		if value == "" {
			return edgeQualityRankScope{}, errors.New("asn scope value is required")
		}
		return edgeQualityRankScope{Kind: "asn", Value: value, ASN: value}, nil
	case "country":
		if value == "" {
			return edgeQualityRankScope{}, errors.New("country scope value is required")
		}
		return edgeQualityRankScope{Kind: "country", Value: value, Country: value}, nil
	case "region":
		value = strings.ReplaceAll(value, "-", ":")
		parts := strings.Split(value, ":")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return edgeQualityRankScope{}, errors.New("region scope must be region:<country>:<region>")
		}
		return edgeQualityRankScope{Kind: "region", Value: parts[0] + ":" + parts[1], Country: parts[0], Region: parts[1]}, nil
	default:
		return edgeQualityRankScope{}, errors.New("scope must be global, country:<country>, region:<country>:<region>, or asn:<asn>")
	}
}

func (scope edgeQualityRankScope) key() string {
	switch scope.Kind {
	case "asn":
		return "asn:" + strings.TrimSpace(scope.ASN)
	case "region":
		return "region:" + strings.TrimSpace(scope.Country) + ":" + strings.TrimSpace(scope.Region)
	case "country":
		return "country:" + strings.TrimSpace(scope.Country)
	default:
		return "global"
	}
}

func edgeQualityPathPrefixBucket(pathPrefix string) string {
	pathPrefix = model.NormalizeAppRoutePathPrefix(pathPrefix)
	switch {
	case pathPrefix == "" || pathPrefix == "/":
		return ""
	case strings.HasPrefix(pathPrefix, "/_next/static"):
		return "/_next/static/*"
	case strings.HasPrefix(pathPrefix, "/assets"):
		return "/assets/*"
	case strings.HasPrefix(pathPrefix, "/api"):
		return "/api/*"
	case strings.HasPrefix(pathPrefix, "/upload"):
		return "/upload/*"
	case strings.HasPrefix(pathPrefix, "/stream"):
		return "/stream/*"
	default:
		return pathPrefix
	}
}
