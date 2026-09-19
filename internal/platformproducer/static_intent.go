package platformproducer

import (
	"bytes"
	"encoding/json"
	"fmt"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// StaticIntentInput adapts the supported static subset without consulting any
// environment, workload, or business table. Unsupported fields reject the input
// instead of silently dropping executable intent during migration.
type StaticIntentInput struct {
	Routes []model.PlatformRoute
	DNS    []model.EdgeDNSRecord
}

func DecodeStaticIntent(a model.PlatformArtifact) (StaticIntentInput, error) {
	fail := func() (StaticIntentInput, error) {
		return StaticIntentInput{}, fmt.Errorf("static intent identity, scope or supported fields invalid")
	}
	if a.ArtifactKind != model.PlatformArtifactKindPlatformIntent || a.ScopeKey != "global" {
		return fail()
	}
	raw, err := json.Marshal(a.Content)
	if err != nil {
		return StaticIntentInput{}, err
	}
	var intent platformconfig.PlatformIntent
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&intent) != nil {
		return fail()
	}
	if intent.SchemaVersion != platformconfig.SchemaVersion || intent.Scope != a.ScopeKey || intent.Generation != a.Generation || platformconfig.ValidatePlatformIntent(intent) != nil || len(intent.DNSConsumers) > 0 || len(intent.ACMEChallenges) > 0 || len(intent.TLS) > 0 || len(intent.CachePolicies) > 0 {
		return fail()
	}
	out := StaticIntentInput{}
	for _, r := range intent.Routes {
		if r.PathPrefix != "" && r.PathPrefix != "/" || r.ServicePort != 0 || r.Streaming != nil || len(r.Upstreams) > 0 || r.CachePolicyID != "" || r.CacheNamespace != "" || r.DeploymentGeneration != "" || len(r.RequestBodyPolicies) > 0 || r.AppID != "" || r.TenantID != "" || r.RuntimeID != "" || r.OriginRef != "" {
			return fail()
		}
		status := r.Status
		if !r.Enabled {
			status = model.EdgeRouteStatusDisabled
		}
		route, ok := platformconfig.NormalizePlatformRoute(model.PlatformRoute{Hostname: r.Hostname, Kind: r.Kind, UpstreamKind: r.UpstreamKind, UpstreamScope: r.UpstreamScope, UpstreamURL: r.UpstreamURL, TLSPolicy: r.TLSPolicy, RoutePolicy: r.RoutePolicy, EdgeGroupMode: r.EdgeGroupMode, EdgeGroupID: r.EdgeGroupID, TTL: r.TTL, Status: status, StatusReason: r.StatusReason})
		if !ok {
			return fail()
		}
		out.Routes = append(out.Routes, route)
	}
	for _, r := range intent.DNS {
		if r.Route != nil || r.Application != nil || r.Flatten != nil {
			return fail()
		}
		switch r.Type {
		case "A", "AAAA", "CNAME", "TXT", "MX", "NS", "SOA", "CAA", "SRV", "PTR":
		default:
			return fail()
		}
		out.DNS = append(out.DNS, model.EdgeDNSRecord{Name: r.Hostname, Type: r.Type, Values: append([]string(nil), r.Values...), TTL: r.TTL, ValueExpirations: r.ValueExpirations, RecordKind: r.RecordKind, Status: r.Status, StatusReason: r.StatusReason, AppID: r.AppID, TenantID: r.TenantID, EdgeGroupID: r.EdgeGroupID, FallbackEdgeGroupID: r.FallbackEdgeGroupID})
	}
	return out, nil
}
