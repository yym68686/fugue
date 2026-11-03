package api

import (
	"net/http"
	"testing"

	"fugue/internal/model"
)

func TestPatchAppNodeLossIsExplicitIdempotentAndValidatedBeforeOtherSettings(t *testing.T) {
	s, server, key, app := setupAppConfigTestServer(t, model.AppSpec{Image: "registry.example/service:one", Replicas: 1, RuntimeID: "runtime_managed_shared"})
	for _, policy := range []any{map[string]any{"mode": "unexpected"}, map[string]any{"mode": "wait", "toleration_seconds": 0}, map[string]any{"mode": "replace", "toleration_seconds": -1}} {
		r := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID, key, map[string]any{"node_loss": policy, "image_mirror_limit": 99})
		if r.Code != http.StatusBadRequest {
			t.Fatalf("invalid node loss: %d %s", r.Code, r.Body.String())
		}
		current, _ := s.GetApp(app.ID)
		if current.Spec.ImageMirrorLimit != app.Spec.ImageMirrorLimit {
			t.Fatal("invalid policy partially changed configuration")
		}
	}
	body := map[string]any{"node_loss": map[string]any{"mode": "replace", "toleration_seconds": 0}}
	r := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID, key, body)
	if r.Code != http.StatusOK {
		t.Fatalf("patch: %d %s", r.Code, r.Body.String())
	}
	var response struct {
		Operation      *model.Operation `json:"operation"`
		AlreadyCurrent bool             `json:"already_current"`
	}
	mustDecodeJSON(t, r, &response)
	if response.Operation == nil || response.Operation.Type != model.OperationTypeDeploy {
		t.Fatal("policy did not go through normal deployment")
	}
	op, err := s.GetOperation(response.Operation.ID)
	if err != nil || op.DesiredSpec.NodeLoss == nil || op.DesiredSpec.NodeLoss.TolerationSeconds == nil || *op.DesiredSpec.NodeLoss.TolerationSeconds != 0 {
		t.Fatal("explicit immediate replacement lost", err)
	}
	if op.DesiredSpec.Replicas != 1 || op.DesiredSpec.RuntimeID != app.Spec.RuntimeID || op.DesiredSpec.Failover != nil {
		t.Fatal("policy provisioned implicit HA")
	}
	completeNextManagedOperation(t, s)
	r = performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID, key, body)
	if r.Code != http.StatusOK {
		t.Fatal(r.Body.String())
	}
	mustDecodeJSON(t, r, &response)
	if !response.AlreadyCurrent {
		t.Fatal("equivalent policy repeatedly deployed")
	}
}
