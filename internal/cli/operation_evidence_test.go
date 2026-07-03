package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunOperationEvidenceTimelineAndDebugBundle(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_123/evidence":
			if r.URL.Query().Get("include_payload") != "true" {
				t.Fatalf("expected include_payload=true query, got %s", r.URL.RawQuery)
			}
			_, _ = w.Write([]byte(`{"evidence":[{"id":"evid_1","tenant_id":"tenant_123","operation_id":"op_123","type":"rollout_previous_logs","source":"app_logs","severity":"error","confidence":"confirmed","observed_at":"2026-07-03T00:00:00Z","collected_at":"2026-07-03T00:00:01Z","summary":"captured previous logs","redaction_status":"redacted","payload_version":1,"created_at":"2026-07-03T00:00:01Z","payload":{"log_tail":"startup failed"}}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_123/timeline":
			_, _ = w.Write([]byte(`{"timeline":[{"id":"op_123:created","operation_id":"op_123","type":"operation_created","summary":"operation created","at":"2026-07-03T00:00:00Z"},{"id":"evid_1","operation_id":"op_123","type":"rollout_previous_logs","source":"app_logs","severity":"error","confidence":"confirmed","summary":"captured previous logs","evidence_id":"evid_1","at":"2026-07-03T00:00:01Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_123/debug-bundle":
			if r.URL.Query().Get("format") == "zip" {
				w.Header().Set("Content-Type", "application/zip")
				_, _ = w.Write([]byte("operation-zip-bytes"))
				return
			}
			_, _ = w.Write([]byte(`{"bundle":{"metadata":{"kind":"operation_debug_bundle"},"operation":{"id":"op_123","tenant_id":"tenant_123","type":"deploy","status":"failed"},"timeline":[],"evidence":[],"redaction_report":[{"status":"redacted"}]}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithStreams([]string{"--base-url", server.URL, "--token", "token", "operation", "evidence", "op_123", "--include-payload"}, &stdout, &stderr); err != nil {
		t.Fatalf("run operation evidence: %v stderr=%s", err, stderr.String())
	}
	if out := stdout.String(); !strings.Contains(out, "rollout_previous_logs") || !strings.Contains(out, "confirmed") || !strings.Contains(out, "evid_1") {
		t.Fatalf("expected evidence table, got %q", out)
	}

	stdout.Reset()
	stderr.Reset()
	if err := runWithStreams([]string{"--base-url", server.URL, "--token", "token", "--json", "operation", "timeline", "op_123"}, &stdout, &stderr); err != nil {
		t.Fatalf("run operation timeline: %v stderr=%s", err, stderr.String())
	}
	if out := stdout.String(); !strings.Contains(out, `"timeline"`) || !strings.Contains(out, "operation_created") || !strings.Contains(out, "evid_1") {
		t.Fatalf("expected timeline JSON, got %q", out)
	}

	outputPath := filepath.Join(t.TempDir(), "bundle.json")
	stdout.Reset()
	stderr.Reset()
	if err := runWithStreams([]string{"--base-url", server.URL, "--token", "token", "operation", "debug-bundle", "op_123", "--output", outputPath}, &stdout, &stderr); err != nil {
		t.Fatalf("run operation debug-bundle: %v stderr=%s", err, stderr.String())
	}
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read debug bundle: %v", err)
	}
	if !strings.Contains(string(data), "operation_debug_bundle") || !strings.Contains(stdout.String(), "wrote debug bundle") {
		t.Fatalf("expected debug bundle file/output, stdout=%q data=%s", stdout.String(), string(data))
	}

	zipPath := filepath.Join(t.TempDir(), "bundle.zip")
	stdout.Reset()
	stderr.Reset()
	if err := runWithStreams([]string{"--base-url", server.URL, "--token", "token", "operation", "debug-bundle", "op_123", "--output", zipPath}, &stdout, &stderr); err != nil {
		t.Fatalf("run operation debug-bundle zip: %v stderr=%s", err, stderr.String())
	}
	zipData, err := os.ReadFile(zipPath)
	if err != nil {
		t.Fatalf("read debug bundle zip: %v", err)
	}
	if string(zipData) != "operation-zip-bytes" {
		t.Fatalf("expected zip payload, got %q", string(zipData))
	}
}

func TestRunAppReleaseAttemptsStatusExplainAndDebugBundle(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1}}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/release-attempts":
			_, _ = w.Write([]byte(`{"release_attempts":[{"id":"rel_1","tenant_id":"tenant_123","project_id":"project_123","app_id":"app_123","trigger_type":"image_tracking_manual_sync","trigger_actor_type":"user","status":"failed","confidence":"confirmed","failure_operation_id":"op_deploy","failure_evidence_id":"evid_1","started_at":"2026-07-03T00:00:00Z","created_at":"2026-07-03T00:00:00Z","updated_at":"2026-07-03T00:00:01Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/release-attempts/rel_1":
			_, _ = w.Write([]byte(`{"release_attempt":{"id":"rel_1","tenant_id":"tenant_123","project_id":"project_123","app_id":"app_123","trigger_type":"image_tracking_manual_sync","trigger_actor_type":"user","status":"failed","confidence":"confirmed","failure_operation_id":"op_deploy","failure_evidence_id":"evid_1","started_at":"2026-07-03T00:00:00Z","created_at":"2026-07-03T00:00:00Z","updated_at":"2026-07-03T00:00:01Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/release-attempts/rel_1/timeline":
			_, _ = w.Write([]byte(`{"timeline":[{"id":"step_1","release_attempt_id":"rel_1","operation_id":"op_deploy","type":"rollout_wait","status":"failed","summary":"rollout failed","evidence_id":"evid_1","at":"2026-07-03T00:00:01Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/release-attempts/rel_1/evidence":
			_, _ = w.Write([]byte(`{"evidence":[{"id":"evid_1","tenant_id":"tenant_123","operation_id":"op_deploy","release_attempt_id":"rel_1","type":"rollout_previous_logs","source":"app_logs","severity":"error","confidence":"confirmed","observed_at":"2026-07-03T00:00:01Z","collected_at":"2026-07-03T00:00:01Z","summary":"captured previous logs","redaction_status":"redacted","payload_version":1,"created_at":"2026-07-03T00:00:01Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/release-attempts/rel_1/debug-bundle":
			if r.URL.Query().Get("format") == "zip" {
				w.Header().Set("Content-Type", "application/zip")
				_, _ = w.Write([]byte("release-zip-bytes"))
				return
			}
			_, _ = w.Write([]byte(`{"bundle":{"metadata":{"kind":"release_debug_bundle"},"release_attempt":{"id":"rel_1","tenant_id":"tenant_123","project_id":"project_123","app_id":"app_123","trigger_type":"image_tracking_manual_sync","trigger_actor_type":"user","status":"failed","confidence":"confirmed","started_at":"2026-07-03T00:00:00Z","created_at":"2026-07-03T00:00:00Z","updated_at":"2026-07-03T00:00:01Z"},"release_timeline":[],"evidence":[],"redaction_report":[{"status":"redacted"}]}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithStreams([]string{"--base-url", server.URL, "--token", "token", "app", "release", "attempts", "demo"}, &stdout, &stderr); err != nil {
		t.Fatalf("run release attempts: %v stderr=%s", err, stderr.String())
	}
	if out := stdout.String(); !strings.Contains(out, "rel_1") || !strings.Contains(out, "image_tracking_manual_sync") {
		t.Fatalf("expected attempts table, got %q", out)
	}

	stdout.Reset()
	stderr.Reset()
	if err := runWithStreams([]string{"--base-url", server.URL, "--token", "token", "app", "release", "explain", "demo"}, &stdout, &stderr); err != nil {
		t.Fatalf("run release explain: %v stderr=%s", err, stderr.String())
	}
	if out := stdout.String(); !strings.Contains(out, "release_attempt_id=rel_1") || !strings.Contains(out, "[timeline]") || !strings.Contains(out, "[evidence]") || !strings.Contains(out, "evid_1") {
		t.Fatalf("expected release explanation, got %q", out)
	}

	outputPath := filepath.Join(t.TempDir(), "release-bundle.json")
	stdout.Reset()
	stderr.Reset()
	if err := runWithStreams([]string{"--base-url", server.URL, "--token", "token", "app", "release", "debug-bundle", "demo", "--output", outputPath}, &stdout, &stderr); err != nil {
		t.Fatalf("run release debug-bundle: %v stderr=%s", err, stderr.String())
	}
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read release debug bundle: %v", err)
	}
	if !strings.Contains(string(data), "release_debug_bundle") {
		t.Fatalf("expected release debug bundle data, got %s", string(data))
	}

	zipPath := filepath.Join(t.TempDir(), "release-bundle.zip")
	stdout.Reset()
	stderr.Reset()
	if err := runWithStreams([]string{"--base-url", server.URL, "--token", "token", "app", "release", "debug-bundle", "demo", "--output", zipPath}, &stdout, &stderr); err != nil {
		t.Fatalf("run release debug-bundle zip: %v stderr=%s", err, stderr.String())
	}
	zipData, err := os.ReadFile(zipPath)
	if err != nil {
		t.Fatalf("read release debug bundle zip: %v", err)
	}
	if string(zipData) != "release-zip-bytes" {
		t.Fatalf("expected release zip payload, got %q", string(zipData))
	}
}
