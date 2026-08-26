package trafficepoch

import (
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestCompileRouteBindingCanonicalMaterial(t *testing.T) {
	now := time.Date(2026, 8, 26, 4, 0, 0, 0, time.UTC)
	binding := CompileRouteBinding(RouteBindingInput{
		Hostname: "API.Example.test.", PathPrefix: "/v1/", RouteKind: model.EdgeRouteKindPlatform,
		AppID: "app-api", TenantID: "tenant-platform", RuntimeID: "runtime-us", RuntimeType: model.RuntimeTypeManagedShared,
		RuntimeEdgeGroupID: "edge-group-country-us", RuntimeClusterNode: "node-us-1", Status: model.EdgeRouteStatusActive,
		Upstream:    RouteUpstreamFact{Kind: model.EdgeRouteUpstreamKindKubernetesService, Scope: model.EdgeRouteUpstreamScopeLocalService, URL: "http://api.internal:8080", Status: model.EdgeRouteStatusActive},
		ServicePort: 8080, TLSPolicy: model.EdgeRouteTLSPolicyPlatform, CachePolicyID: "static-assets-immutable-v1",
		DeploymentGeneration: "operation-42", RequestBodyPoliciesEnvelope: `[{"name":"uploads","methods":["POST"],"paths":["/upload"],"max_bytes":1024,"timeout_seconds":30,"max_concurrent":2,"retry_after_seconds":5}]`,
		CreatedAt: now.Add(-time.Hour), UpdatedAt: now,
	})
	wantPolicies, err := model.ParseEdgeRequestBodyPolicies(`[{"name":"uploads","methods":["POST"],"paths":["/upload"],"max_bytes":1024,"timeout_seconds":30,"max_concurrent":2,"retry_after_seconds":5}]`)
	if err != nil {
		t.Fatal(err)
	}
	if binding.Hostname != "api.example.test" || binding.PathPrefix != "/v1" || binding.EdgeGroupID != "edge-group-country-us" ||
		binding.FallbackEdgeGroupID != "edge-group-default" || binding.UpstreamURL != "http://api.internal:8080" ||
		binding.CacheNamespace != "app-api_operation-42" || !reflect.DeepEqual(binding.RequestBodyPolicies, wantPolicies) {
		t.Fatalf("unexpected canonical binding: %+v", binding)
	}
	if binding.RouteGeneration != "routegen_65de108f0c9fbb53" {
		t.Fatalf("canonical generation changed: %s", binding.RouteGeneration)
	}
}

func TestCompileRouteBindingFailsInvalidRequestPolicyClosed(t *testing.T) {
	binding := CompileRouteBinding(RouteBindingInput{
		Hostname: "api.example.test", RouteKind: model.EdgeRouteKindPlatform, AppID: "app-api", TenantID: "tenant-platform",
		RuntimeID: "runtime-us", RuntimeEdgeGroupID: "edge-group-country-us", Status: model.EdgeRouteStatusActive,
		Upstream:    RouteUpstreamFact{Kind: model.EdgeRouteUpstreamKindKubernetesService, URL: "http://api.internal", Status: model.EdgeRouteStatusActive},
		ServicePort: 80, TLSPolicy: model.EdgeRouteTLSPolicyPlatform, RequestBodyPoliciesEnvelope: `[{"name":"unsafe"}]`,
	})
	if binding.Status != model.EdgeRouteStatusUnavailable || binding.StatusReason != "invalid app edge request body policy" || binding.UpstreamURL != "" {
		t.Fatalf("invalid request body policy did not fail closed: %+v", binding)
	}
}
