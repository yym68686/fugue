package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunServiceSuspendWaitsByDefaultAndRedactsJSON(t *testing.T) {
	t.Parallel()

	var detailCalls, operationCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services":
			_, _ = w.Write([]byte(`{"backing_services":[{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","password":"list-secret","service_name":"demo-postgres","instances":1}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services/svc_pg":
			detailCalls++
			if detailCalls == 1 {
				_, _ = w.Write([]byte(`{"backing_service":{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","password":"detail-secret","service_name":"demo-postgres","instances":1}},"runtime_status":{"phase":"active","ready_instances":1,"desired_instances":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
				return
			}
			_, _ = w.Write([]byte(`{"backing_service":{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","password":"final-secret","service_name":"demo-postgres","instances":1,"suspended":true}},"runtime_status":{"phase":"suspended","message":"database hibernated; storage retained","ready_instances":0,"desired_instances":0},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:02Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/backing-services/svc_pg/suspend":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read suspend body: %v", err)
			}
			if len(bytes.TrimSpace(body)) != 0 && string(bytes.TrimSpace(body)) != "null" {
				t.Fatalf("expected empty suspend body, got %q", body)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"backing_service":{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","password":"response-secret","service_name":"demo-postgres","instances":1,"suspended":true}},"runtime_status":{"phase":"suspending","ready_instances":1,"desired_instances":0},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:01Z"},"operation":{"id":"op_suspend","tenant_id":"tenant_123","service_id":"svc_pg","type":"database-suspend","status":"pending","desired_spec":{"env":{"OP_SECRET":"operation-secret"},"postgres":{"password":"operation-db-secret"}},"created_at":"2026-04-02T00:00:01Z","updated_at":"2026-04-02T00:00:01Z"},"already_current":false}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_suspend":
			operationCalls++
			_, _ = w.Write([]byte(`{"operation":{"id":"op_suspend","tenant_id":"tenant_123","service_id":"svc_pg","type":"database-suspend","status":"completed","desired_spec":{"env":{"OP_SECRET":"operation-secret"},"postgres":{"password":"operation-db-secret"}},"created_at":"2026-04-02T00:00:01Z","updated_at":"2026-04-02T00:00:02Z","completed_at":"2026-04-02T00:00:02Z"}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--json",
		"service", "suspend", "main-db",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run service suspend: %v stderr=%s", err, stderr.String())
	}
	if detailCalls != 2 || operationCalls != 1 {
		t.Fatalf("expected default wait and final service refresh, detail_calls=%d operation_calls=%d", detailCalls, operationCalls)
	}
	for _, secret := range []string{"list-secret", "detail-secret", "response-secret", "final-secret", "operation-secret", "operation-db-secret"} {
		if strings.Contains(stdout.String(), secret) {
			t.Fatalf("JSON output leaked %q: %s", secret, stdout.String())
		}
	}
	if !strings.Contains(stdout.String(), redactedSecretValue) {
		t.Fatalf("expected redacted values in JSON output: %s", stdout.String())
	}
	var response struct {
		BackingService struct {
			Spec struct {
				Postgres struct {
					Suspended bool `json:"suspended"`
				} `json:"postgres"`
			} `json:"spec"`
			RuntimeStatus struct {
				Phase            string `json:"phase"`
				ReadyInstances   int    `json:"ready_instances"`
				DesiredInstances int    `json:"desired_instances"`
			} `json:"runtime_status"`
		} `json:"backing_service"`
		Operation struct {
			Status string `json:"status"`
		} `json:"operation"`
		AlreadyCurrent bool `json:"already_current"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
		t.Fatalf("decode suspend JSON: %v body=%s", err, stdout.String())
	}
	if !response.BackingService.Spec.Postgres.Suspended || response.BackingService.RuntimeStatus.Phase != "suspended" {
		t.Fatalf("unexpected final backing service state: %+v", response.BackingService)
	}
	if response.BackingService.RuntimeStatus.ReadyInstances != 0 || response.BackingService.RuntimeStatus.DesiredInstances != 0 {
		t.Fatalf("unexpected suspended instance counts: %+v", response.BackingService.RuntimeStatus)
	}
	if response.Operation.Status != "completed" || response.AlreadyCurrent {
		t.Fatalf("unexpected lifecycle result: %+v", response)
	}
}

func TestRunServiceResumeWithoutWaitRendersDesiredAndRuntimeState(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services":
			_, _ = w.Write([]byte(`{"backing_services":[{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"database":"demo","service_name":"demo-postgres","suspended":true}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services/svc_pg":
			_, _ = w.Write([]byte(`{"backing_service":{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"database":"demo","service_name":"demo-postgres","suspended":true}},"runtime_status":{"phase":"suspended","ready_instances":0,"desired_instances":0},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/backing-services/svc_pg/resume":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"backing_service":{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"database":"demo","service_name":"demo-postgres","suspended":false}},"runtime_status":{"phase":"resuming","message":"waiting for primary readiness","ready_instances":0,"desired_instances":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:01Z"},"operation":{"id":"op_resume","tenant_id":"tenant_123","service_id":"svc_pg","type":"database-resume","status":"pending","created_at":"2026-04-02T00:00:01Z","updated_at":"2026-04-02T00:00:01Z"},"already_current":false}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/projects":
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"apps","slug":"apps","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"service", "resume", "main-db",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run service resume: %v stderr=%s", err, stderr.String())
	}
	for _, want := range []string{
		"operation_id=op_resume",
		"operation_status=pending",
		"desired_suspended=false",
		"runtime_status_phase=resuming",
		"runtime_status_instances=0/1",
		"runtime_status_message=waiting for primary readiness",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("expected text output to contain %q, got %s", want, stdout.String())
		}
	}
}

func TestRunServicePostgresOrphanList(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/backing-services/orphans" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"orphans":[{"app_id":"app_b","tenant_id":"tenant_b","project_id":"project_b","name":"river","namespace":"tenant-b","managed_app_name":"river","phase":"disabled","message":"retained for audit","backing_services":[{"id":"svc_b","name":"river-postgres","type":"postgres","runtime_id":"runtime_us","service_name":"river-postgres","storage_size":"20Gi","suspended":false}]},{"app_id":"app_a","tenant_id":"tenant_a","project_id":"project_a","name":"ember","namespace":"tenant-a","managed_app_name":"ember","phase":"disabled","backing_services":[{"id":"svc_a","name":"ember-postgres","type":"postgres","runtime_id":"runtime_us","service_name":"ember-postgres","storage_size":"1Gi","suspended":true}]}]}`))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"service", "postgres", "orphan", "ls",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run orphan ls: %v stderr=%s", err, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{"APP_ID", "MANAGED_APP", "app_a", "ember-postgres", "1/1", "app_b", "river-postgres", "0/1", "retained for audit"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected orphan table to contain %q, got %s", want, out)
		}
	}
	if strings.Index(out, "app_a") > strings.Index(out, "app_b") {
		t.Fatalf("expected orphan rows sorted by name, got %s", out)
	}
}

func TestRunServicePostgresOrphanAdoptRedactsJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/backing-services/orphans/app_123/adopt" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"adopted-app","source":{"type":"docker-image"},"spec":{"env":{"APP_SECRET":"app-secret"},"postgres":{"password":"app-db-secret"}},"backing_services":[{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","owner_app_id":"app_123","name":"adopted-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"password":"nested-service-secret"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"backing_services":[{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","owner_app_id":"app_123","name":"adopted-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"password":"service-secret"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"already_adopted":false}`))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--json",
		"service", "postgres", "orphan", "adopt", "app_123",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run orphan adopt: %v stderr=%s", err, stderr.String())
	}
	for _, secret := range []string{"app-secret", "app-db-secret", "nested-service-secret", "service-secret"} {
		if strings.Contains(stdout.String(), secret) {
			t.Fatalf("adopt JSON leaked %q: %s", secret, stdout.String())
		}
	}
	if strings.Count(stdout.String(), redactedSecretValue) < 4 {
		t.Fatalf("expected all app/service secrets redacted: %s", stdout.String())
	}
	var response struct {
		App struct {
			ID string `json:"id"`
		} `json:"app"`
		AlreadyAdopted bool `json:"already_adopted"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
		t.Fatalf("decode adopt JSON: %v body=%s", err, stdout.String())
	}
	if response.App.ID != "app_123" || response.AlreadyAdopted {
		t.Fatalf("unexpected adopt response: %+v", response)
	}
}

func TestServiceLifecycleAndOrphanHelp(t *testing.T) {
	t.Parallel()

	cases := []struct {
		args []string
		want []string
	}{
		{args: []string{"service", "suspend", "--help"}, want: []string{"persistent storage", "--wait=false", "observed runtime phase"}},
		{args: []string{"service", "resume", "--help"}, want: []string{"retained persistent storage", "--wait=false"}},
		{args: []string{"service", "postgres", "orphan", "ls", "--help"}, want: []string{"platform-admin or bootstrap key", "orphan ls --json"}},
		{args: []string{"service", "postgres", "orphan", "adopt", "--help"}, want: []string{"platform-admin or bootstrap key", "JSON output redacts", "orphan adopt app_123"}},
	}
	for _, tc := range cases {
		var stdout, stderr bytes.Buffer
		if err := runWithStreams(tc.args, &stdout, &stderr); err != nil {
			t.Fatalf("run %v: %v stderr=%s", tc.args, err, stderr.String())
		}
		for _, want := range tc.want {
			if !strings.Contains(stdout.String(), want) {
				t.Fatalf("help %v missing %q: %s", tc.args, want, stdout.String())
			}
		}
	}
}
