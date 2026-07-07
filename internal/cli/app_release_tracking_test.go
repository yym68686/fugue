package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunAppReleaseTrackingSyncWaitsForAlreadyCurrentOperation(t *testing.T) {
	t.Parallel()

	operationPolls := 0
	appPolls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"deploying","current_replicas":0}}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/image-sync":
			_, _ = w.Write([]byte(`{"app_id":"app_123","digest":"sha256:abc123","changed":false,"already_current":true,"rollout_pending":true,"app_phase":"deploying","operation":{"id":"op_deploy","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"running"},"message":"tracked image digest already matches app desired source; active operation is still in progress"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_deploy":
			operationPolls++
			_, _ = w.Write([]byte(`{"operation":{"id":"op_deploy","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","result_message":"deployment ready"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			appPolls++
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"deployed","current_replicas":1,"last_operation_id":"op_deploy"}}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"app", "release", "tracking", "sync", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run tracking sync: %v stderr=%s", err, stderr.String())
	}
	if operationPolls == 0 {
		t.Fatal("expected sync --wait to poll the returned operation")
	}
	if appPolls == 0 {
		t.Fatal("expected sync --wait to refresh app status after operation completion")
	}
	out := stdout.String()
	for _, want := range []string{"operation_id=op_deploy", "app_phase=deployed"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
	if strings.Contains(out, "rollout_pending=true") {
		t.Fatalf("expected rollout_pending to clear after wait, got %q", out)
	}
}

func TestRunAppReleaseTrackingSyncWaitsForQueuedDeployAfterImport(t *testing.T) {
	t.Parallel()

	operationPolls := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"deploying","current_replicas":0}}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/image-sync":
			_, _ = w.Write([]byte(`{"app_id":"app_123","digest":"sha256:def456","changed":true,"already_current":false,"release_attempt":{"id":"rel_123","tenant_id":"tenant_123","project_id":"project_123","app_id":"app_123","trigger_type":"image_tracking_manual_sync","trigger_actor_type":"user","status":"importing","confidence":"evidence_backed","started_at":"2026-07-03T00:00:00Z","created_at":"2026-07-03T00:00:00Z","updated_at":"2026-07-03T00:00:00Z"},"operation":{"id":"op_import","tenant_id":"tenant_123","app_id":"app_123","type":"import","status":"pending"},"message":"tracked image digest changed; import queued"}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/operations/"):
			opID := strings.TrimPrefix(r.URL.Path, "/v1/operations/")
			operationPolls[opID]++
			switch opID {
			case "op_import":
				_, _ = w.Write([]byte(`{"operation":{"id":"op_import","tenant_id":"tenant_123","app_id":"app_123","type":"import","status":"completed","result_message":"import build completed; queued deploy operation op_deploy"}}`))
			case "op_deploy":
				_, _ = w.Write([]byte(`{"operation":{"id":"op_deploy","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","result_message":"deployment ready"}}`))
			default:
				t.Fatalf("unexpected operation id %s", opID)
			}
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"deployed","current_replicas":1,"last_operation_id":"op_deploy"}}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"app", "release", "tracking", "sync", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run tracking sync: %v stderr=%s", err, stderr.String())
	}
	if operationPolls["op_import"] == 0 || operationPolls["op_deploy"] == 0 {
		t.Fatalf("expected import and queued deploy to be polled, got %v", operationPolls)
	}
	for _, want := range []string{"operation_id=op_deploy", "release_attempt_id=rel_123", "release_attempt_status=importing", "phase=deploy_rollout"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("expected final output to contain %q, got %q", want, stdout.String())
		}
	}
}

func TestRunAppReleaseTrackingSyncRetriesAfterDeferredActiveOperation(t *testing.T) {
	t.Parallel()

	syncAttempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"deploying","current_replicas":0}}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/image-sync":
			syncAttempts++
			if syncAttempts == 1 {
				_, _ = w.Write([]byte(`{"app_id":"app_123","digest":"sha256:def456","changed":false,"already_current":false,"rollout_pending":true,"operation":{"id":"op_blocking","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"running"},"message":"app has an active operation; image sync was not queued"}`))
				return
			}
			_, _ = w.Write([]byte(`{"app_id":"app_123","digest":"sha256:def456","changed":false,"already_current":true,"app_phase":"deployed","message":"tracked image digest already matches app desired source"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_blocking":
			_, _ = w.Write([]byte(`{"operation":{"id":"op_blocking","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","result_message":"deployment ready"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"deployed","current_replicas":1,"last_operation_id":"op_blocking"}}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"app", "release", "tracking", "sync", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run tracking sync: %v stderr=%s", err, stderr.String())
	}
	if syncAttempts != 2 {
		t.Fatalf("expected sync to retry after deferred active operation, got %d attempts", syncAttempts)
	}
	if !strings.Contains(stdout.String(), "already_current=true") {
		t.Fatalf("expected final retry result, got %q", stdout.String())
	}
}

func TestRunAppReleaseTrafficSupportsStableReleaseFlag(t *testing.T) {
	t.Parallel()

	var gotPatch appTrafficPatchCLIRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/traffic":
			_, _ = w.Write([]byte(`{"app_id":"app_123","traffic":{"id":"tp_123","tenant_id":"tenant_123","app_id":"app_123","mode":"canary","stable_release_id":"apprel_old","candidate_release_id":"apprel_candidate","stable_weight":50,"candidate_weight":50}}`))
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/apps/app_123/traffic":
			if err := json.NewDecoder(r.Body).Decode(&gotPatch); err != nil {
				t.Fatalf("decode patch body: %v", err)
			}
			_, _ = w.Write([]byte(`{"app_id":"app_123","traffic":{"id":"tp_123","tenant_id":"tenant_123","app_id":"app_123","mode":"single","stable_release_id":"apprel_stable","stable_weight":100,"candidate_weight":0}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"deployed","current_replicas":1}}]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"app", "release", "traffic", "demo",
		"--stable-release", "apprel_stable",
		"--stable", "100",
		"--candidate", "0",
		"--mode", "single",
		"--json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app release traffic: %v stderr=%s", err, stderr.String())
	}
	if gotPatch.StableReleaseID != "apprel_stable" || gotPatch.CandidateReleaseID != "apprel_candidate" || gotPatch.StableWeight == nil || *gotPatch.StableWeight != 100 || gotPatch.CandidateWeight == nil || *gotPatch.CandidateWeight != 0 {
		t.Fatalf("unexpected traffic patch body: %+v", gotPatch)
	}
	if !strings.Contains(stdout.String(), `"stable_release_id": "apprel_stable"`) {
		t.Fatalf("expected JSON output to include stable release, got %s", stdout.String())
	}
}
