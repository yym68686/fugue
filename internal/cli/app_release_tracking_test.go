package cli

import (
	"bytes"
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
			_, _ = w.Write([]byte(`{"app_id":"app_123","digest":"sha256:def456","changed":true,"already_current":false,"operation":{"id":"op_import","tenant_id":"tenant_123","app_id":"app_123","type":"import","status":"pending"},"message":"tracked image digest changed; import queued"}`))
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
	if !strings.Contains(stdout.String(), "operation_id=op_deploy") {
		t.Fatalf("expected final operation to be queued deploy, got %q", stdout.String())
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
