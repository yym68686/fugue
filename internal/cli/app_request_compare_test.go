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

func TestFinalizeAppRequestCompareResultDetectsStaticFallback(t *testing.T) {
	t.Parallel()

	result := appRequestCompareResult{
		PublicRouteConfigured: true,
		Public: appHTTPProbe{
			Target: "public_route",
			rawHTTPDiagnostic: rawHTTPDiagnostic{
				Method:     http.MethodGet,
				URL:        "https://demo.apps.example.com/admin",
				Status:     "200 OK",
				StatusCode: http.StatusOK,
				Headers: map[string][]string{
					"Content-Type": {"text/html; charset=utf-8"},
				},
				Body:         "<!doctype html><html><body>fallback</body></html>",
				BodyEncoding: "utf-8",
				BodySize:     50,
			},
		},
		Internal: appHTTPProbe{
			Target: "internal_service",
			rawHTTPDiagnostic: rawHTTPDiagnostic{
				Method:     http.MethodGet,
				URL:        "http://demo.tenant-123.svc.cluster.local:3000/admin",
				Status:     "404 Not Found",
				StatusCode: http.StatusNotFound,
				Headers: map[string][]string{
					"Content-Type": {"application/json"},
				},
				Body:         `{"error":"route not found"}`,
				BodyEncoding: "utf-8",
				BodySize:     27,
			},
		},
	}
	app := model.App{
		ID:   "app_123",
		Name: "demo",
		Source: &model.AppSource{
			Type:          "upload",
			BuildStrategy: model.AppBuildStrategyStaticSite,
		},
		Route: &model.AppRoute{PublicURL: "https://demo.apps.example.com"},
		InternalService: &model.AppInternalService{
			Name:      "demo",
			Namespace: "tenant-123",
			Host:      "demo.tenant-123.svc.cluster.local",
			Port:      3000,
		},
	}

	finalizeAppRequestCompareResult(&result, app, nil)

	if result.Category != "static-fallback" {
		t.Fatalf("expected static-fallback category, got %q", result.Category)
	}
	if !strings.Contains(result.Summary, "static-site rule likely consumed this path") {
		t.Fatalf("expected static fallback summary, got %q", result.Summary)
	}
	if !containsString(result.Evidence, "app build strategy is static-site") {
		t.Fatalf("expected evidence to mention static-site build strategy, got %+v", result.Evidence)
	}
}

func TestRunAppRequestCompareExplainsPublicRouteMismatch(t *testing.T) {
	t.Parallel()

	publicServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/admin" {
			t.Fatalf("unexpected public request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("route not found"))
	}))
	defer publicServer.Close()

	var gotInternalBody struct {
		Method string              `json:"method"`
		Path   string              `json:"path"`
		Query  map[string][]string `json:"query"`
	}
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","route":{"public_url":"` + publicServer.URL + `"},"spec":{"runtime_id":"runtime_shared","ports":[3000],"replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-21T00:00:00Z","updated_at":"2026-04-21T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","route":{"public_url":"` + publicServer.URL + `"},"internal_service":{"name":"demo","namespace":"tenant-123","host":"demo.tenant-123.svc.cluster.local","port":3000},"source":{"type":"github-public","repo_url":"https://github.com/acme/demo","build_strategy":"dockerfile"},"spec":{"runtime_id":"runtime_shared","ports":[3000],"replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_shared","current_replicas":1,"last_operation_id":"op_123"},"created_at":"2026-04-21T00:00:00Z","updated_at":"2026-04-21T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/request":
			if err := json.NewDecoder(r.Body).Decode(&gotInternalBody); err != nil {
				t.Fatalf("decode internal probe body: %v", err)
			}
			_, _ = w.Write([]byte(`{
				"method":"GET",
				"url":"http://demo.tenant-123.svc.cluster.local:3000/admin",
				"status":"200 OK",
				"status_code":200,
				"headers":{"Content-Type":["application/json"]},
				"body":"{\"ok\":true}",
				"body_encoding":"utf-8",
				"body_size":11,
				"timing":{"total":"4ms"}
			}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer apiServer.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", apiServer.URL,
		"--token", "token",
		"app", "request", "compare", "demo", "/admin",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app request compare: %v", err)
	}

	if gotInternalBody.Method != http.MethodGet || gotInternalBody.Path != "/admin" {
		t.Fatalf("unexpected internal compare request %+v", gotInternalBody)
	}
	out := stdout.String()
	for _, want := range []string{
		"category=public-route-not-forwarding",
		"summary=the public route returned 404 while the internal service returned 200 OK; the public ingress is not forwarding this path to the app",
		"evidence=public route " + publicServer.URL + "/admin -> 404 Not Found",
		"evidence=internal service http://demo.tenant-123.svc.cluster.local:3000/admin -> 200 OK",
		"related_object=route ref=" + publicServer.URL,
		"next_action=fugue app route show demo",
		"public_route",
		"status=404 Not Found",
		"internal_service",
		"status=200 OK",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppRequestCompareBlocksOnMissingEnvPreflight(t *testing.T) {
	t.Parallel()

	var internalProbeCalls atomic.Int32
	var publicProbeCalls atomic.Int32

	publicServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		publicProbeCalls.Add(1)
		t.Fatalf("unexpected public probe %s %s", r.Method, r.URL.Path)
	}))
	defer publicServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","route":{"public_url":"` + publicServer.URL + `"},"spec":{"runtime_id":"runtime_shared","ports":[3000],"replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-21T00:00:00Z","updated_at":"2026-04-21T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","route":{"public_url":"` + publicServer.URL + `"},"internal_service":{"name":"demo","namespace":"tenant-123","host":"demo.tenant-123.svc.cluster.local","port":3000},"spec":{"runtime_id":"runtime_shared","ports":[3000],"replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_shared","current_replicas":1},"created_at":"2026-04-21T00:00:00Z","updated_at":"2026-04-21T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/env":
			_, _ = w.Write([]byte(`{"env":{"SERVICE_KEY":"secret"},"entries":[{"key":"SERVICE_KEY","value":"secret","source":"binding","source_ref":"postgres"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/request":
			internalProbeCalls.Add(1)
			t.Fatalf("unexpected internal probe")
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer apiServer.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", apiServer.URL,
		"--token", "token",
		"app", "request", "compare", "demo", "/install",
		"--header-from-env", "Authorization=SERVICE_KEY",
		"--require-env", "PUBLIC_BASE_URL",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app request compare with preflight: %v", err)
	}
	if internalProbeCalls.Load() != 0 {
		t.Fatalf("expected no internal probe calls, got %d", internalProbeCalls.Load())
	}
	if publicProbeCalls.Load() != 0 {
		t.Fatalf("expected no public probe calls, got %d", publicProbeCalls.Load())
	}

	out := stdout.String()
	for _, want := range []string{
		"category=missing-env-prerequisite",
		"request_header_injection=Authorization<-SERVICE_KEY (resolved)",
		"env_requirement=PUBLIC_BASE_URL missing used_by=request prerequisite",
		"env_requirement=SERVICE_KEY present source=binding ref=postgres used_by=header Authorization",
		"next_action=fugue app env ls demo",
		"next_action=fugue app env set demo PUBLIC_BASE_URL=<value>",
		"error=probe skipped because required app env is missing",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}
