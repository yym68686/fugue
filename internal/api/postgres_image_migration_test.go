package api

import (
	"fugue/internal/model"
	"fugue/internal/pgimage"
	"net/http"
	"reflect"
	"testing"
)

func TestPostgresImageMigrationPlanRequiresExplicitApplyAndReusesIntent(t *testing.T) {
	s, server, key, app, svc := managedPostgresLifecycleFixture(t, 0)
	url := "/v1/backing-services/" + svc.ID + "/migrate-image"
	before, _ := s.GetBackingService(svc.ID)
	opsBefore, _ := s.ListOperationsByApp(app.TenantID, false, app.ID)
	planResponse := performJSONRequest(t, server, http.MethodPost, url, key, map[string]any{})
	if planResponse.Code != 200 {
		t.Fatalf("plan: %d %s", planResponse.Code, planResponse.Body.String())
	}
	var plan struct {
		Plan   pgimage.Plan `json:"plan"`
		DryRun bool         `json:"dry_run"`
	}
	mustDecodeJSON(t, planResponse, &plan)
	ops, _ := s.ListOperationsByApp(app.TenantID, false, app.ID)
	if !plan.DryRun || len(ops) != len(opsBefore) || !plan.Plan.LivePreflightPending || !plan.Plan.RequiresDowntime {
		t.Fatalf("plan mutated state or hid preflight: %+v", plan)
	}
	for _, request := range []map[string]any{{"apply": true}, {"apply": true, "acknowledge_downtime": true, "expected_config_hash": "stale"}} {
		res := performJSONRequest(t, server, http.MethodPost, url, key, request)
		if res.Code != 400 && res.Code != 409 {
			t.Fatalf("unsafe apply accepted: %d", res.Code)
		}
	}
	request := map[string]any{"apply": true, "acknowledge_downtime": true, "expected_config_hash": plan.Plan.ConfigHash}
	var first model.Operation
	for i := 0; i < 2; i++ {
		res := performJSONRequest(t, server, http.MethodPost, url, key, request)
		if res.Code != 202 {
			t.Fatalf("apply: %d %s", res.Code, res.Body.String())
		}
		assertResponseOmitsSecrets(t, res.Body.String(), "lifecycle-secret")
		var body struct {
			Operation model.Operation `json:"operation"`
		}
		mustDecodeJSON(t, res, &body)
		if i == 0 {
			first = body.Operation
		} else if body.Operation.ID != first.ID {
			t.Fatal("retry created duplicate")
		}
	}
	after, _ := s.GetBackingService(svc.ID)
	if !reflect.DeepEqual(before.Spec, after.Spec) {
		t.Fatal("target image persisted before execution")
	}
	deleted := performJSONRequest(t, server, http.MethodDelete, "/v1/apps/"+app.ID+"?force=true", key, nil)
	if deleted.Code != http.StatusConflict {
		t.Fatalf("force delete bypassed migration interlock: %d %s", deleted.Code, deleted.Body.String())
	}
}
