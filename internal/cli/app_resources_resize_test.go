package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"fugue/internal/model"
)

func TestRunAppResourcesApplyEmitsDatabaseNextStepWithoutFanout(t *testing.T) {
	t.Parallel()
	var mutations atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-01T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/resources/apply-recommendation":
			mutations.Add(1)
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{
  "recommendation": {
    "app": {"target_kind":"app","target_id":"app_123","window_hours":168,"sample_count":100,"current":{"cpu_millicores":100,"memory_mebibytes":256,"cpu_limit_millicores":200,"memory_limit_mebibytes":512},"recommended":{"cpu_millicores":200,"memory_mebibytes":512,"cpu_limit_millicores":400,"memory_limit_mebibytes":768},"policy":{"min_samples":12},"ready":true},
    "backing_services": [{"target_kind":"backing-service","target_id":"svc_pg","target_name":"main-db","service_type":"postgres","window_hours":168,"sample_count":100,"current":{"cpu_millicores":100,"memory_mebibytes":512,"cpu_limit_millicores":200,"memory_limit_mebibytes":768},"recommended":{"cpu_millicores":250,"memory_mebibytes":768,"cpu_limit_millicores":500,"memory_limit_mebibytes":1280},"policy":{"min_samples":12},"ready":true}]
  },
  "already_current": false,
  "operation": {"id":"op_deploy","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"pending","execution_mode":"managed","desired_spec":{"postgres":{"password":"operation-password-456"}},"created_at":"2026-08-01T00:01:00Z","updated_at":"2026-08-01T00:01:00Z"}
}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var jsonOut, jsonErr bytes.Buffer
	if err := runWithStreams([]string{
		"--base-url", server.URL, "--token", "token", "--json",
		"app", "resources", "apply", "demo", "--wait=false",
	}, &jsonOut, &jsonErr); err != nil {
		t.Fatalf("JSON app resources apply: %v stderr=%s", err, jsonErr.String())
	}
	if strings.Contains(jsonOut.String(), "operation-password-456") {
		t.Fatalf("app resources JSON leaked operation secret: %s", jsonOut.String())
	}
	var payload struct {
		Operation struct {
			ID          string `json:"id"`
			DesiredSpec struct {
				Postgres struct {
					Password string `json:"password"`
				} `json:"postgres"`
			} `json:"desired_spec"`
		} `json:"operation"`
		NextSteps []appResourceApplyNextStep `json:"next_steps"`
	}
	if err := json.Unmarshal(jsonOut.Bytes(), &payload); err != nil {
		t.Fatalf("decode app resources JSON: %v body=%s", err, jsonOut.String())
	}
	if payload.Operation.ID != "op_deploy" || payload.Operation.DesiredSpec.Postgres.Password != redactedSecretValue || len(payload.NextSteps) != 1 {
		t.Fatalf("unexpected apply payload: %+v", payload)
	}
	next := payload.NextSteps[0]
	if next.Kind != "reapply_after_app_deploy" || next.AfterOperationID != "op_deploy" ||
		len(next.PendingDatabases) != 1 || next.PendingDatabases[0].ServiceID != "svc_pg" ||
		!strings.Contains(next.Command, "fugue app resources apply app_123") ||
		!strings.Contains(next.Command, "--window-hours 168") || !strings.HasSuffix(next.Command, "--wait=false") {
		t.Fatalf("unexpected database next step: %+v", next)
	}

	var textOut, textErr bytes.Buffer
	if err := runWithStreams([]string{
		"--base-url", server.URL, "--token", "token",
		"app", "resources", "apply", "demo", "--wait=false",
	}, &textOut, &textErr); err != nil {
		t.Fatalf("text app resources apply: %v stderr=%s", err, textErr.String())
	}
	if !strings.Contains(textOut.String(), "database_resize_next_step=fugue app resources apply app_123") ||
		!strings.Contains(textOut.String(), "after operation op_deploy is terminal") {
		t.Fatalf("text output did not make the second durable step explicit:\n%s", textOut.String())
	}
	if mutations.Load() != 2 {
		t.Fatalf("two CLI invocations made %d mutations; expected exactly one server apply each and no DB fanout", mutations.Load())
	}
}

func TestAppResourceApplyNextStepFailsClosedForPartialRecommendation(t *testing.T) {
	t.Parallel()
	current := model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 512, CPULimitMilliCores: 200, MemoryLimitMebibytes: 768}
	partial := model.ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 768, CPULimitMilliCores: 500}
	response := appResourceRecommendationApplyResponse{
		Operation: &model.Operation{ID: "op_deploy", Type: model.OperationTypeDeploy},
		Recommendation: model.AppRightSizingRecommendation{BackingServices: []model.ResourceRightSizingRecommendation{{
			TargetID: "svc_pg", ServiceType: model.BackingServiceTypePostgres,
			Ready: true, Current: &current, Recommended: &partial,
		}}},
	}
	if steps := appResourceApplyDatabaseNextSteps("app_123", 168, 12, response); len(steps) != 0 {
		t.Fatalf("empty recommendation produced next steps: %+v", steps)
	}
}
