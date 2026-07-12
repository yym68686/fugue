package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDeriveEdgeRouteBindingCarriesAppScopedRequestBodyPolicies(t *testing.T) {
	t.Parallel()

	_, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app.Spec.Env = map[string]string{
		model.AppEdgeRequestBodyPoliciesEnv: `[{
			"name":"source-imports",
			"methods":["POST"],
			"paths":["/api/fugue/apps/import-upload","/api/fugue/projects/create-and-import-upload"],
			"max_bytes":167772160,
			"timeout_seconds":300,
			"max_concurrent":2,
			"retry_after_seconds":5
		}]`,
	}
	app.Spec.Replicas = 1
	app.Status.CurrentReplicas = 1
	runtimeObj := model.Runtime{ID: app.Spec.RuntimeID, Type: model.RuntimeTypeManagedShared}
	now := time.Now().UTC()
	binding := server.deriveEdgeRouteBinding(
		httptest.NewRequest(http.MethodGet, "/v1/edge/routes", nil),
		app,
		"demo.fugue.pro",
		model.EdgeRouteKindPlatform,
		model.EdgeRouteTLSPolicyPlatform,
		now,
		now,
		map[string]model.Runtime{runtimeObj.ID: runtimeObj},
		nil,
	)

	if binding.Status != model.EdgeRouteStatusActive {
		t.Fatalf("expected valid policy route to stay active, got %+v", binding)
	}
	if len(binding.RequestBodyPolicies) != 1 {
		t.Fatalf("expected request body policy in signed route material, got %+v", binding.RequestBodyPolicies)
	}
	policy := binding.RequestBodyPolicies[0]
	if policy.Name != "source-imports" || policy.MaxBytes != 167772160 || policy.TimeoutSeconds != 300 || policy.MaxConcurrent != 2 {
		t.Fatalf("unexpected request body policy: %+v", policy)
	}
}

func TestDeriveEdgeRouteBindingFailsInvalidOptInPolicyClosed(t *testing.T) {
	t.Parallel()

	_, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app.Spec.Env = map[string]string{model.AppEdgeRequestBodyPoliciesEnv: `[{"name":"unsafe"}]`}
	app.Spec.Replicas = 1
	app.Status.CurrentReplicas = 1
	runtimeObj := model.Runtime{ID: app.Spec.RuntimeID, Type: model.RuntimeTypeManagedShared}
	now := time.Now().UTC()
	binding := server.deriveEdgeRouteBinding(
		httptest.NewRequest(http.MethodGet, "/v1/edge/routes", nil),
		app,
		"demo.fugue.pro",
		model.EdgeRouteKindPlatform,
		model.EdgeRouteTLSPolicyPlatform,
		now,
		now,
		map[string]model.Runtime{runtimeObj.ID: runtimeObj},
		nil,
	)

	if binding.Status != model.EdgeRouteStatusUnavailable || binding.UpstreamURL != "" {
		t.Fatalf("expected invalid opt-in policy to fail route closed, got %+v", binding)
	}
	if binding.StatusReason != "invalid app edge request body policy" {
		t.Fatalf("unexpected invalid policy reason %q", binding.StatusReason)
	}
}
