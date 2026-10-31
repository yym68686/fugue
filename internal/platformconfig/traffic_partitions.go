package platformconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"fugue/internal/model"
)

const RetainedTrafficPartitionsMetadata = "retained_traffic_partitions"

// The envelope metadata is signed, immutable and accepted by v1 consumers.
// Provenance is not an instruction for executors to resolve another artifact.
func RetainedTrafficPartitionsFromArtifact(a model.PlatformArtifact) ([]RetainedTrafficPartition, error) {
	raw, present := a.Metadata[RetainedTrafficPartitionsMetadata]
	if !present {
		return nil, nil
	}
	if len(raw) == 0 || len(raw) > 16<<20 {
		return nil, fmt.Errorf("retained traffic metadata exceeds bounds")
	}
	var parts []RetainedTrafficPartition
	d := json.NewDecoder(bytes.NewBufferString(raw))
	d.DisallowUnknownFields()
	if d.Decode(&parts) != nil || d.Decode(&struct{}{}) != io.EOF || len(parts) == 0 || len(parts) > 4096 {
		return nil, fmt.Errorf("retained traffic metadata invalid")
	}
	return parts, nil
}

// Retention is an observation of an exact previously verified binding, never
// evidence that the desired release is ready. The API independently validates
// the source signature, full fence, positive LKG and consumer convergence.
type RetainedTrafficPartition struct {
	AppID               string                                `json:"app_id"`
	TenantID            string                                `json:"tenant_id"`
	InputDigest         string                                `json:"input_digest"`
	Source              model.PlatformPublicationPrecondition `json:"source"`
	RouteArtifactID     string                                `json:"route_artifact_id"`
	RouteArtifactDigest string                                `json:"route_artifact_digest"`
	ObservedAt          time.Time                             `json:"observed_at"`
	ExpiresAt           time.Time                             `json:"expires_at"`
	Routes              []CompiledRoute                       `json:"routes"`
}

// Ignore other applications' desired changes, but retain shared policy, source,
// topology, TLS and cache dependencies. Global generation/time alone is not an
// application authorization change. All matching hostname/tenant rules remain.
func TrafficPartitionInputDigest(intent PlatformIntent, policy PolicySnapshot, tenant, app string) (string, error) {
	in := normalizeIntent(intent)
	p := normalizePolicy(policy)
	hosts := map[string]bool{}
	routes := []RouteIntent{}
	for _, r := range in.Routes {
		if r.AppID == app && r.TenantID == tenant {
			routes = append(routes, r)
			hosts[r.Hostname] = true
		}
	}
	if tenant == "" || app == "" || len(routes) == 0 {
		return "", fmt.Errorf("traffic partition owner has no routes")
	}
	in.Routes = routes
	in.Generation, in.CreatedAt = "", time.Time{}
	// DNS records and their observation policy are downstream consumers, not
	// authority to change this app's exact HTTP binding. They are independently
	// compiled/admitted against the new signed DNS inputs (never copied here).
	in.DNS, in.DNSConsumers, in.ACMEChallenges = nil, nil, nil
	in.DNSRouteSources, in.CellRoutePublications, in.RouteAuthorityTransition = nil, nil, nil
	tls := in.TLS[:0:0]
	for _, rule := range in.TLS {
		if hosts[rule.Hostname] {
			tls = append(tls, rule)
		}
	}
	in.TLS = tls
	p.Generation, p.CreatedAt = "", time.Time{}
	p.DNSPlacementMode, p.ConsumerTopologyDigest = "", ""
	p.DNSQueryPolicy, p.DNSReadiness, p.TLSReadiness = nil, nil, nil
	p.DNSAuthorities, p.DNSClientPolicies, p.DNSAnswerRules, p.DNSRouteStateConstraints = nil, nil, nil, nil
	p.TrafficRolloutCohorts = nil
	p.TrafficConstraints = nil
	for _, rule := range policy.TrafficConstraints {
		if rule.AppID == app {
			p.TrafficConstraints = append(p.TrafficConstraints, rule)
		}
	}
	p.RouteConstraints = nil
	for _, rule := range policy.RouteConstraints {
		if hosts[rule.Hostname] {
			p.RouteConstraints = append(p.RouteConstraints, rule)
		}
	}
	p.EdgeSelectionConstraints = nil
	for _, rule := range policy.EdgeSelectionConstraints {
		if hosts[rule.Hostname] {
			p.EdgeSelectionConstraints = append(p.EdgeSelectionConstraints, rule)
		}
	}
	p = normalizePolicy(p)
	return Digest(struct {
		Intent PlatformIntent
		Policy PolicySnapshot
	}{in, p})
}

func trafficRouteKey(r RouteIntent) string {
	return r.TenantID + "\x00" + r.AppID + "\x00" + r.Hostname + "\x00" + model.NormalizeAppRoutePathPrefix(r.PathPrefix)
}

func retainedTrafficRoutes(routes []CompiledRoute, policy PolicySnapshot, snapshot RuntimeSnapshot) (map[string]CompiledRoute, error) {
	out := map[string]CompiledRoute{}
	if len(snapshot.RetainedTrafficPartitions) > 4096 {
		return nil, fmt.Errorf("too many retained traffic partitions")
	}
	owners := map[string]bool{}
	members := make(map[string]bool, len(routes))
	counts := make(map[string]int)
	for _, r := range routes {
		members[trafficRouteKey(r.RouteIntent)] = true
		counts[r.TenantID+"\x00"+r.AppID]++
	}
	for _, part := range snapshot.RetainedTrafficPartitions {
		owner := part.TenantID + "\x00" + part.AppID
		if !snapshot.IsolateTrafficAvailability || owners[owner] || part.AppID == "" || part.TenantID == "" || len(part.Routes) == 0 || len(part.Routes) > 4096 || part.Source.ArtifactID == "" || part.Source.ReleaseID == "" || part.Source.FencingToken <= 0 || !trafficConsumerDigest.MatchString(part.Source.ContentHash) || part.RouteArtifactID == "" || !trafficConsumerDigest.MatchString(part.RouteArtifactDigest) {
			return nil, fmt.Errorf("retained traffic partition identity invalid")
		}
		owners[owner] = true
		if snapshot.CapturedAt == nil || part.ObservedAt.IsZero() || part.ObservedAt.After(*snapshot.CapturedAt) || snapshot.CapturedAt.Sub(part.ObservedAt).Seconds() > float64(policy.MaxStaleSeconds) || !part.ExpiresAt.After(*snapshot.CapturedAt) {
			return nil, fmt.Errorf("retained traffic partition evidence expired or unavailable")
		}
		// Digest is checked against the complete immutable intent by Compile and
		// capture. This helper also checks exact membership for callers resolving
		// a policy-filtered subset (e.g. origin readiness projection).
		if counts[owner] != len(part.Routes) {
			return nil, fmt.Errorf("retained traffic partition route membership changed")
		}
		for _, r := range part.Routes {
			key := trafficRouteKey(r.RouteIntent)
			if r.AppID != part.AppID || r.TenantID != part.TenantID || out[key].Hostname != "" || !r.Enabled || r.Status != "" && r.Status != model.EdgeRouteStatusActive || r.RoutePolicy != "" && !model.EdgeRoutePolicyAllowsTraffic(r.RoutePolicy) || len(r.Upstreams) == 0 || ValidateUpstreamIntents(r.Upstreams) != nil || r.ExclusionExpiresAt != nil && !r.ExclusionExpiresAt.After(*snapshot.CapturedAt) {
				return nil, fmt.Errorf("retained traffic route is not an active exact partition")
			}
			if !members[key] {
				return nil, fmt.Errorf("retained traffic route outside intent")
			}
			out[key] = r
		}
	}
	return out, nil
}

func ValidateTrafficPartitionInputs(intent PlatformIntent, policy PolicySnapshot, snapshot RuntimeSnapshot) error {
	for _, part := range snapshot.RetainedTrafficPartitions {
		digest, err := TrafficPartitionInputDigest(intent, policy, part.TenantID, part.AppID)
		if err != nil || digest != part.InputDigest {
			return fmt.Errorf("retained traffic partition input authorization changed")
		}
	}
	return nil
}

func NormalizeTrafficPartitionRoutes(routes []CompiledRoute) []CompiledRoute {
	out := append([]CompiledRoute(nil), routes...)
	sort.Slice(out, func(i, j int) bool { return trafficRouteKey(out[i].RouteIntent) < trafficRouteKey(out[j].RouteIntent) })
	return out
}
