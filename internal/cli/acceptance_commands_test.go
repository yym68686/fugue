package cli

import (
	"archive/zip"
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunWorkflowExecutesMultiStepHTTPFlow(t *testing.T) {
	t.Setenv("TEST_TRACE_ID", "trace-123")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/register":
			if got := r.Header.Get("X-Trace-Id"); got != "trace-123" {
				t.Fatalf("expected trace header, got %q", got)
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read request body: %v", err)
			}
			if !strings.Contains(string(body), `"email":"user@example.com"`) {
				t.Fatalf("expected workflow body to include email, got %q", string(body))
			}
			w.Header().Add("Set-Cookie", "session=abc123; Path=/; HttpOnly")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"user":{"id":"user_123"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/users/user_123":
			if got := r.Header.Get("Cookie"); got != "session=abc123" {
				t.Fatalf("expected cookie header, got %q", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	workflowPath := filepath.Join(t.TempDir(), "signup.yaml")
	workflowYAML := `
name: signup
base_urls:
  default: ` + server.URL + `
steps:
  - id: register
    method: POST
    path: /register
    headers:
      X-Trace-Id: "{{env.TEST_TRACE_ID}}"
    body_json:
      email: user@example.com
    expect_status: [201]
    extract:
      - name: user_id
        from: body
        path: $.user.id
      - name: session_value
        from: cookie
        cookie: session
  - id: fetch
    method: GET
    path: /users/{{user_id}}
    headers:
      X-Trace-Id: "{{env.TEST_TRACE_ID}}"
    cookie: "session={{session_value}}"
    expect_status: [200]
`
	if err := os.WriteFile(workflowPath, []byte(workflowYAML), 0o644); err != nil {
		t.Fatalf("write workflow file: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--redact=false",
		"--confirm-raw-output",
		"-o", "json",
		"workflow", "run", workflowPath,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run workflow: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		`"schema_version": "fugue.workflow-run.v1"`,
		`"status": "success"`,
		`"user_id": "user_123"`,
		`"session_value": "abc123"`,
		`"id": "fetch"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected workflow output to contain %q, got %q", want, out)
		}
	}
}

func TestRunDiagnoseFSClassifiesContainerNotReady(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1,"workspace":{"mount_path":"/workspace"}},"status":{"phase":"degraded","current_replicas":1},"created_at":"2026-04-20T00:00:00Z","updated_at":"2026-04-20T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1,"workspace":{"mount_path":"/workspace"}},"status":{"phase":"degraded","current_replicas":1},"created_at":"2026-04-20T00:00:00Z","updated_at":"2026-04-20T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-pods":
			_, _ = w.Write([]byte(`{"component":"app","namespace":"tenant-123","selector":"app=demo","container":"app","groups":[{"owner_kind":"ReplicaSet","owner_name":"demo-abc","pods":[{"namespace":"tenant-123","name":"demo-abc","phase":"Running","node_name":"gcp1","ready":false,"start_time":"2026-04-20T00:10:00Z","containers":[{"name":"app","image":"ghcr.io/example/demo:latest","ready":true,"restart_count":0,"state":"running"},{"name":"fugue-workspace","image":"ghcr.io/example/workspace:latest","ready":false,"restart_count":1,"state":"waiting","reason":"ContainerCreating","message":"container is waiting to start"}]}]}],"warnings":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/diagnosis":
			_, _ = w.Write([]byte(`{"diagnosis":{"category":"workspace-sidecar-starting","summary":"workspace sidecar is still starting","hint":"retry after the sidecar becomes ready","component":"app","namespace":"tenant-123","selector":"app=demo","implicated_pod":"demo-abc","evidence":["workspace sidecar is waiting in ContainerCreating"],"warnings":[],"events":[{"namespace":"tenant-123","name":"demo-abc.1","type":"Warning","reason":"Failed","message":"workspace sidecar is still creating","object_kind":"Pod","object_name":"demo-abc","last_timestamp":"2026-04-20T00:11:00Z"}]}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/filesystem/tree":
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"filesystem target is not ready; retry shortly"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/cluster/exec":
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"unable to upgrade connection: container is waiting to start: ContainerCreating"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/logs":
			_, _ = w.Write([]byte(`{"namespace":"tenant-123","pod":"demo-abc","container":"fugue-workspace","logs":"2026-04-20T00:11:01Z workspace sidecar waiting: ContainerCreating\n"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/events":
			_, _ = w.Write([]byte(`{"events":[{"namespace":"tenant-123","name":"demo-abc.1","type":"Warning","reason":"Failed","message":"workspace sidecar is still creating","object_kind":"Pod","object_name":"demo-abc","last_timestamp":"2026-04-20T00:11:00Z"}]}`))
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
		"-o", "json",
		"diagnose", "fs", "demo",
		"--source", "persistent",
		"--path", "data",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected diagnose fs to fail")
	}
	if got := ExitCodeForError(err); got != ExitCodeSystemFault {
		t.Fatalf("expected system fault exit code, got %d for %v", got, err)
	}

	out := stdout.String()
	for _, want := range []string{
		`"failure_class": "container-not-ready"`,
		`"selected_container": "fugue-workspace"`,
		`"raw_exec_error": "unable to upgrade connection: container is waiting to start: ContainerCreating"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected diagnose fs output to contain %q, got %q", want, out)
		}
	}
}

func TestGlobalOutputFileMirrorsCommandOutput(t *testing.T) {
	t.Parallel()

	outputPath := filepath.Join(t.TempDir(), "tenants.json")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/tenants" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme","status":"active","updated_at":"2026-04-02T00:00:00Z"}]}`))
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--output-file", outputPath,
		"-o", "json",
		"tenant", "ls",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run tenant ls with output file: %v", err)
	}

	fileBytes, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if got, want := string(fileBytes), stdout.String(); got != want {
		t.Fatalf("expected mirrored output file contents, got %q want %q", got, want)
	}
}

func TestRunLogsCollectAggregatesRelatedSources(t *testing.T) {
	t.Parallel()

	server := newDiagnosticEvidenceTestServer(t)
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"-o", "json",
		"logs", "collect", "demo",
		"--request-id", "req-123",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run logs collect: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		`"schema_version": "fugue.logs-collect.v1"`,
		`"name": "runtime-app"`,
		`"name": "control-plane-api"`,
		`"operation_id": "op_deploy_123"`,
		`req-123`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected logs collect output to contain %q, got %q", want, out)
		}
	}
}

func TestRunDebugBundleCreatesArchive(t *testing.T) {
	t.Parallel()

	server := newDiagnosticEvidenceTestServer(t)
	defer server.Close()

	archivePath := filepath.Join(t.TempDir(), "bundle.zip")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"-o", "json",
		"debug", "bundle", "demo",
		"--request-id", "req-123",
		"--archive", archivePath,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run debug bundle: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		`"schema_version": "fugue.debug-bundle.v1"`,
		archivePath,
		`"path": "evidence.json"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected debug bundle output to contain %q, got %q", want, out)
		}
	}

	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		t.Fatalf("open archive: %v", err)
	}
	defer reader.Close()

	found := map[string]bool{}
	for _, file := range reader.File {
		found[file.Name] = true
	}
	for _, want := range []string{"evidence.json", "manifest.json", "logs/runtime-app.log"} {
		if !found[want] {
			t.Fatalf("expected archive to contain %q, got %+v", want, found)
		}
	}
}

func newDiagnosticEvidenceTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"degraded","current_replicas":1,"last_operation_id":"op_deploy_123"},"created_at":"2026-04-20T00:00:00Z","updated_at":"2026-04-20T00:30:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"degraded","current_replicas":1,"last_operation_id":"op_deploy_123"},"created_at":"2026-04-20T00:00:00Z","updated_at":"2026-04-20T00:30:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/domains":
			_, _ = w.Write([]byte(`{"domains":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/bindings":
			_, _ = w.Write([]byte(`{"bindings":[],"backing_services":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			_, _ = w.Write([]byte(`{"operations":[{"id":"op_deploy_123","tenant_id":"tenant_123","type":"deploy","status":"failed","execution_mode":"async","requested_by_type":"user","requested_by_id":"user_123","app_id":"app_123","result_message":"deploy failed while serving req-123","created_at":"2026-04-20T00:20:00Z","updated_at":"2026-04-20T00:25:00Z","started_at":"2026-04-20T00:21:00Z","completed_at":"2026-04-20T00:25:00Z"},{"id":"op_import_123","tenant_id":"tenant_123","type":"import","status":"completed","execution_mode":"async","requested_by_type":"user","requested_by_id":"user_123","app_id":"app_123","result_message":"import completed for req-123","created_at":"2026-04-20T00:10:00Z","updated_at":"2026-04-20T00:15:00Z","started_at":"2026-04-20T00:11:00Z","completed_at":"2026-04-20T00:15:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/images":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"image inventory not available"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-pods":
			_, _ = w.Write([]byte(`{"component":"app","namespace":"tenant-123","selector":"app=demo","container":"app","groups":[{"owner_kind":"ReplicaSet","owner_name":"demo-rs","pods":[{"namespace":"tenant-123","name":"demo-abc","phase":"Running","node_name":"gcp1","ready":false,"start_time":"2026-04-20T00:18:00Z","containers":[{"name":"app","image":"ghcr.io/example/demo:latest","ready":false,"restart_count":2,"state":"waiting","reason":"CrashLoopBackOff","message":"req-123 crashed"}]}]}],"warnings":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/diagnosis":
			_, _ = w.Write([]byte(`{"diagnosis":{"category":"crash-loop","summary":"runtime is crash looping after request req-123","hint":"inspect runtime logs","component":"app","namespace":"tenant-123","selector":"app=demo","implicated_pod":"demo-abc","evidence":["req-123 triggered a crash loop"],"warnings":[],"events":[{"namespace":"tenant-123","name":"demo-abc.1","type":"Warning","reason":"BackOff","message":"Back-off restarting failed container after req-123","object_kind":"Pod","object_name":"demo-abc","last_timestamp":"2026-04-20T00:24:00Z"}]}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_deploy_123/diagnosis":
			_, _ = w.Write([]byte(`{"diagnosis":{"category":"deploy-runtime-failed","summary":"deploy op_deploy_123 failed after req-123","hint":"compare runtime and controller logs","service":"demo","evidence":["controller recorded req-123 before rollout failure"]}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-logs":
			if got := r.URL.Query().Get("component"); got != "app" {
				t.Fatalf("expected runtime log component app, got %q", got)
			}
			_, _ = w.Write([]byte(`{"component":"app","namespace":"tenant-123","selector":"app=demo","container":"app","pods":["demo-abc"],"logs":"2026-04-20T00:24:01Z ERROR req-123 backend panic\n2026-04-20T00:24:02Z INFO retrying\n","warnings":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/build-logs":
			_, _ = w.Write([]byte(`{"operation_id":"op_import_123","operation_status":"completed","job_name":"build-demo","available":true,"source":"builder","logs":"2026-04-20T00:15:00Z build op_import_123 finished for req-123\n","summary":"build completed for req-123","build_strategy":"dockerfile","error_message":"","result_message":"done","last_updated_at":"2026-04-20T00:15:00Z"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/control-plane":
			_, _ = w.Write([]byte(`{"control_plane":{"namespace":"fugue-system","release_instance":"fugue","version":"v1.0.0","status":"healthy","observed_at":"2026-04-20T00:30:00Z","components":[]}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/pods":
			selector := r.URL.Query().Get("label_selector")
			switch selector {
			case "app.kubernetes.io/component=api":
				_, _ = w.Write([]byte(`{"cluster_pods":[{"namespace":"fugue-system","name":"fugue-api-abc","phase":"Running","node_name":"gcp1","ready":true,"start_time":"2026-04-20T00:00:00Z","containers":[{"name":"api","image":"ghcr.io/example/fugue-api:latest","ready":true,"restart_count":0,"state":"running"}]}]}`))
			case "app.kubernetes.io/component=controller":
				_, _ = w.Write([]byte(`{"cluster_pods":[{"namespace":"fugue-system","name":"fugue-controller-abc","phase":"Running","node_name":"gcp1","ready":true,"start_time":"2026-04-20T00:00:00Z","containers":[{"name":"controller","image":"ghcr.io/example/fugue-controller:latest","ready":true,"restart_count":0,"state":"running"}]}]}`))
			default:
				t.Fatalf("unexpected cluster pod selector %q", selector)
			}
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/logs":
			switch r.URL.Query().Get("pod") {
			case "fugue-api-abc":
				_, _ = w.Write([]byte(`{"namespace":"fugue-system","pod":"fugue-api-abc","container":"api","logs":"2026-04-20T00:24:00Z req-123 reached fugue-api\n"}`))
			case "fugue-controller-abc":
				_, _ = w.Write([]byte(`{"namespace":"fugue-system","pod":"fugue-controller-abc","container":"controller","logs":"2026-04-20T00:24:03Z req-123 rollout failed in controller\n"}`))
			default:
				t.Fatalf("unexpected cluster log pod %q", r.URL.Query().Get("pod"))
			}
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
}
