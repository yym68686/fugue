package platformconfig

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"fugue/internal/model"
)

// UpstreamIntent records desired targets and traffic weights. Observed health
// and readiness belong to the fixed runtime snapshot, never this input.
type UpstreamIntent struct {
	Role                 string `json:"role,omitempty"`
	ReleaseID            string `json:"release_id,omitempty"`
	Weight               int    `json:"weight"`
	UpstreamKind         string `json:"upstream_kind,omitempty"`
	UpstreamScope        string `json:"upstream_scope,omitempty"`
	UpstreamURL          string `json:"upstream_url"`
	ServicePort          int    `json:"service_port,omitempty"`
	RuntimeID            string `json:"runtime_id,omitempty"`
	DeploymentGeneration string `json:"deployment_generation,omitempty"`
}

func ValidateUpstreamIntents(upstreams []UpstreamIntent) error {
	if len(upstreams) > 16 {
		return fmt.Errorf("route intent supports at most 16 weighted upstreams")
	}
	total := 0
	seen := make(map[string]bool, len(upstreams))
	for _, upstream := range upstreams {
		for _, value := range []string{upstream.Role, upstream.ReleaseID, upstream.RuntimeID, upstream.DeploymentGeneration, upstream.UpstreamURL} {
			if value != strings.TrimSpace(value) {
				return fmt.Errorf("route intent upstream fields must not contain surrounding whitespace")
			}
		}
		parsed, err := url.Parse(upstream.UpstreamURL)
		if err != nil || parsed.Hostname() == "" || parsed.User != nil || parsed.Fragment != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return fmt.Errorf("route intent upstream requires an HTTP URL without credentials or fragment")
		}
		if port := parsed.Port(); port != "" {
			n, err := strconv.Atoi(port)
			if err != nil || n < 1 || n > 65535 {
				return fmt.Errorf("route intent upstream URL port is invalid")
			}
		}
		if upstream.ServicePort < 0 || upstream.ServicePort > 65535 || upstream.Weight < 0 || upstream.Weight > 100 {
			return fmt.Errorf("route intent upstream weight or service port is out of bounds")
		}
		switch upstream.UpstreamKind {
		case "", model.EdgeRouteUpstreamKindKubernetesService, model.EdgeRouteUpstreamKindMesh:
		default:
			return fmt.Errorf("route intent upstream kind is invalid")
		}
		switch upstream.UpstreamScope {
		case "", model.EdgeRouteUpstreamScopeLocalService, model.EdgeRouteUpstreamScopeCluster, model.EdgeRouteUpstreamScopeMesh:
		default:
			return fmt.Errorf("route intent upstream scope is invalid")
		}
		// Edge's selector uses this identity. Two targets with the same key
		// cannot produce an unambiguous per-release selection or receipt.
		key := firstNonEmpty(upstream.ReleaseID, upstream.Role, upstream.UpstreamURL)
		if seen[key] {
			return fmt.Errorf("route intent has duplicate upstream selection identity")
		}
		seen[key] = true
		total += upstream.Weight
	}
	if len(upstreams) > 0 && total != 100 {
		return fmt.Errorf("route intent upstream weights must total 100")
	}
	return nil
}

// ProjectUpstreamIntents preserves the ordered partition used by Edge's
// weighted selector. Empty status does not assert observed runtime health.
func ProjectUpstreamIntents(upstreams []UpstreamIntent) []model.EdgeRouteUpstream {
	if len(upstreams) == 0 {
		return nil
	}
	out := make([]model.EdgeRouteUpstream, 0, len(upstreams))
	for _, upstream := range upstreams {
		out = append(out, model.EdgeRouteUpstream{
			Role: upstream.Role, ReleaseID: upstream.ReleaseID, Weight: upstream.Weight,
			UpstreamKind: upstream.UpstreamKind, UpstreamScope: upstream.UpstreamScope,
			UpstreamURL: upstream.UpstreamURL, ServicePort: upstream.ServicePort,
			RuntimeID: upstream.RuntimeID, DeploymentGeneration: upstream.DeploymentGeneration,
		})
	}
	return out
}
