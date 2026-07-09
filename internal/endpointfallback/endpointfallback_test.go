package endpointfallback

import (
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/localwal"
	"fugue/internal/model"
)

func TestEvaluateEndpointLKGAllowsOnlyFreshMatchingStatelessRoute(t *testing.T) {
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	route := model.EdgeRouteBinding{
		Hostname:        "api.example.com",
		AppID:           "app_1",
		TenantID:        "tenant_1",
		RuntimeID:       "runtime_1",
		RouteGeneration: "routegen_1",
		UpstreamKind:    model.EdgeRouteUpstreamKindKubernetesService,
		UpstreamScope:   model.EdgeRouteUpstreamScopeLocalService,
		UpstreamURL:     "http://svc.ns.svc.cluster.local",
		ServicePort:     8080,
	}
	lkg := BuildLKG(route, []model.EndpointLKGEndpoint{{IP: "10.42.1.10", Port: 8080, Ready: true, NodeName: "node-a", PodCIDR: "10.42.1.0/24"}}, now, time.Minute)
	decision := Evaluate(lkg, Request{Hostname: "api.example.com", RouteGeneration: "routegen_1", ServiceIdentity: ServiceIdentity(route), Now: now.Add(10 * time.Second)})
	if decision.Status != model.EndpointFallbackStatusAllowed || decision.EndpointCount != 1 {
		t.Fatalf("expected allowed fallback, got %+v", decision)
	}
	expired := Evaluate(lkg, Request{Hostname: "api.example.com", RouteGeneration: "routegen_1", ServiceIdentity: ServiceIdentity(route), Now: now.Add(2 * time.Minute)})
	if expired.Status != model.EndpointFallbackStatusExpired {
		t.Fatalf("expected expired fallback, got %+v", expired)
	}
	mismatch := Evaluate(lkg, Request{Hostname: "api.example.com", RouteGeneration: "routegen_other", ServiceIdentity: ServiceIdentity(route), Now: now.Add(10 * time.Second)})
	if mismatch.Status != model.EndpointFallbackStatusBlocked || mismatch.Reason != "route_generation_mismatch" {
		t.Fatalf("expected route generation mismatch, got %+v", mismatch)
	}
}

func TestEvaluateEndpointLKGBlocksStatefulWithoutExplicitPolicyAndRecordsWAL(t *testing.T) {
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	lkg := model.EndpointLKG{
		SchemaVersion:   model.AutonomySchemaVersionV1,
		Kind:            model.AutonomyArtifactKindEndpointLKG,
		Hostname:        "db.example.com",
		RouteGeneration: "routegen_db",
		ServiceIdentity: "tenant|app|runtime|db",
		FallbackPolicy:  model.EndpointFallbackPolicyDisabled,
		GeneratedAt:     now,
		ValidUntil:      now.Add(time.Minute),
		Endpoints:       []model.EndpointLKGEndpoint{{IP: "10.42.2.3", Port: 5432, Ready: true}},
	}
	decision := Evaluate(lkg, Request{Hostname: "db.example.com", RouteGeneration: "routegen_db", ServiceIdentity: "tenant|app|runtime|db", Now: now.Add(10 * time.Second)})
	if decision.Status != model.EndpointFallbackStatusBlocked || decision.Reason != "stateful_route_requires_explicit_policy" {
		t.Fatalf("expected stateful fallback block, got %+v", decision)
	}
	path := filepath.Join(t.TempDir(), "autonomy.wal")
	if err := RecordWAL(path, "edge-1", lkg, decision, now); err != nil {
		t.Fatalf("record wal: %v", err)
	}
	records, err := localwal.ReadAll(path)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	if len(records) != 1 || records[0].Action != "endpoint_lkg_fallback" || records[0].Evidence["reason"] != "stateful_route_requires_explicit_policy" {
		t.Fatalf("unexpected wal records: %+v", records)
	}
}
