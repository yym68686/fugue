package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
)

const resizeServiceListJSON = `{"backing_services":[{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","owner_app_id":"app_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","service_name":"demo-postgres","resources":{"cpu_millicores":100,"memory_mebibytes":512,"cpu_limit_millicores":200,"memory_limit_mebibytes":768},"runtime_resources":{"cpu_millicores":150,"memory_mebibytes":640,"cpu_limit_millicores":300,"memory_limit_mebibytes":1024}}},"created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-01T00:00:00Z"}]}`

const resizeServiceSecretJSON = `{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","owner_app_id":"app_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","password":"service-password-123","service_name":"demo-postgres","resources":{"cpu_millicores":100,"memory_mebibytes":512,"cpu_limit_millicores":200,"memory_limit_mebibytes":768},"runtime_resources":{"cpu_millicores":150,"memory_mebibytes":640,"cpu_limit_millicores":300,"memory_limit_mebibytes":1024}}},"created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-01T00:00:00Z"}`

func TestRunServicePostgresResizeValidatesAndRedactsJSON(t *testing.T) {
	t.Parallel()
	var request backingServiceResizeRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services":
			_, _ = w.Write([]byte(resizeServiceListJSON))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services/svc_pg":
			_, _ = fmt.Fprintf(w, `{"backing_service":%s}`, resizeServiceSecretJSON)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/backing-services/svc_pg/resize":
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatal(err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = fmt.Fprintf(w, `{"backing_service":%s,"operation":{"id":"op_resize","tenant_id":"tenant_123","app_id":"app_123","service_id":"svc_pg","type":"database-resize","status":"pending","execution_mode":"managed","desired_spec":{"postgres":{"password":"operation-password-456","runtime_resources":{"cpu_millicores":250,"memory_mebibytes":768,"cpu_limit_millicores":500,"memory_limit_mebibytes":1280}}},"created_at":"2026-08-01T00:01:00Z","updated_at":"2026-08-01T00:01:00Z"}}`, resizeServiceSecretJSON)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL, "--token", "token", "--json",
		"service", "postgres", "resize", "main-db",
		"--cpu-millicores", "250", "--memory-mebibytes", "768",
		"--cpu-limit-millicores", "500", "--memory-limit-mebibytes", "1280",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run resize: %v stderr=%s", err, stderr.String())
	}
	want := model.ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 768, CPULimitMilliCores: 500, MemoryLimitMebibytes: 1280}
	if request.RuntimeResources != want {
		t.Fatalf("resize request=%+v want=%+v", request.RuntimeResources, want)
	}
	if strings.Contains(stdout.String(), "service-password-123") || strings.Contains(stdout.String(), "operation-password-456") {
		t.Fatalf("resize JSON leaked a secret: %s", stdout.String())
	}
	var payload struct {
		BackingService model.BackingService `json:"backing_service"`
		Operation      model.Operation      `json:"operation"`
		Requested      model.ResourceSpec   `json:"requested_runtime_resources"`
		NextStep       string               `json:"next_step"`
		Resources      struct {
			Bootstrap postgresResourceEnvelopeOutput `json:"bootstrap"`
			Runtime   postgresResourceEnvelopeOutput `json:"runtime_resources"`
			Live      postgresResourceEnvelopeOutput `json:"live"`
		} `json:"postgres_resources"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode resize output: %v body=%s", err, stdout.String())
	}
	if payload.Requested != want || payload.Operation.ID != "op_resize" || payload.NextStep != "fugue operation watch op_resize" {
		t.Fatalf("unexpected resize output: %+v", payload)
	}
	if payload.BackingService.Spec.Postgres == nil || payload.BackingService.Spec.Postgres.Password != redactedSecretValue ||
		payload.Operation.DesiredSpec == nil || payload.Operation.DesiredSpec.Postgres == nil || payload.Operation.DesiredSpec.Postgres.Password != redactedSecretValue {
		t.Fatalf("response was not structurally redacted: service=%+v operation=%+v", payload.BackingService, payload.Operation)
	}
	if payload.Resources.Bootstrap.Status != "complete" || payload.Resources.Runtime.Status != "complete" || payload.Resources.Live.Status != "unknown" {
		t.Fatalf("unexpected resource semantics: %+v", payload.Resources)
	}
}

func TestRunServicePostgresResizeRejectsIncompleteEnvelopeBeforeHTTP(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()

	tests := [][]string{
		{"--cpu-millicores", "250", "--memory-mebibytes", "768", "--cpu-limit-millicores", "500"},
		{"--cpu-millicores", "0", "--memory-mebibytes", "768", "--cpu-limit-millicores", "500", "--memory-limit-mebibytes", "1280"},
		{"--cpu-millicores", "500", "--memory-mebibytes", "768", "--cpu-limit-millicores", "250", "--memory-limit-mebibytes", "1280"},
	}
	for _, flags := range tests {
		args := []string{"--base-url", server.URL, "--token", "token", "service", "postgres", "resize", "main-db"}
		args = append(args, flags...)
		var stdout, stderr bytes.Buffer
		if err := runWithStreams(args, &stdout, &stderr); err == nil {
			t.Fatalf("invalid flags unexpectedly succeeded: %v", flags)
		}
	}
	client, err := newClientWithOptions(server.URL, "token", clientOptions{RequireToken: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ResizeBackingService("svc_pg", model.ResourceSpec{
		CPUMilliCores: 100, MemoryMebibytes: 512, CPULimitMilliCores: 200,
	}); err == nil {
		t.Fatal("client accepted an incomplete four-field envelope")
	}
	if requests.Load() != 0 {
		t.Fatalf("invalid local envelope made %d HTTP requests", requests.Load())
	}
}

func TestRunServicePostgresResizeSanitizes409Body(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/backing-services":
			_, _ = w.Write([]byte(resizeServiceListJSON))
		case "/v1/backing-services/svc_pg":
			_, _ = fmt.Fprintf(w, `{"backing_service":%s}`, resizeServiceSecretJSON)
		case "/v1/backing-services/svc_pg/resize":
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"service-password-123","code":"resize_disabled","category":"conflict","retryable":true}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL, "--token", "token", "service", "postgres", "resize", "main-db",
		"--cpu-millicores", "250", "--memory-mebibytes", "768",
		"--cpu-limit-millicores", "500", "--memory-limit-mebibytes", "1280",
	}, &stdout, &stderr)
	if err == nil || strings.Contains(err.Error(), "service-password-123") ||
		strings.Contains(err.Error(), "resize_disabled") || strings.Contains(err.Error(), "conflict") ||
		!strings.Contains(err.Error(), "status=409") {
		t.Fatalf("unsafe or incomplete 409 error: %v", err)
	}
	var resizeErr *backingServiceResizeClientError
	if !errors.As(err, &resizeErr) || resizeErr.StatusCode != http.StatusConflict || !resizeErr.Retryable {
		t.Fatalf("409 classification was not retained safely: %#v", err)
	}
}

func TestRunServicePostgresResizeWaitsThroughDeferredAndInfeasibleUntilCompleted(t *testing.T) {
	previousPollInterval := deployWaitPollInterval
	deployWaitPollInterval = time.Millisecond
	defer func() { deployWaitPollInterval = previousPollInterval }()

	var operationReads atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services":
			_, _ = w.Write([]byte(resizeServiceListJSON))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services/svc_pg":
			_, _ = fmt.Fprintf(w, `{"backing_service":%s}`, resizeServiceSecretJSON)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/backing-services/svc_pg/resize":
			w.WriteHeader(http.StatusAccepted)
			_, _ = fmt.Fprintf(w, `{"backing_service":%s,"operation":%s}`, resizeServiceSecretJSON, resizeOperationJSON("pending", "queued"))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_resize":
			attempt := operationReads.Add(1)
			status, message := "running", "Deferred: waiting for node capacity"
			if attempt == 2 {
				message = "Infeasible: capacity is not currently available"
			}
			if attempt >= 3 {
				status, message = "completed", "resize verified"
			}
			_, _ = fmt.Fprintf(w, `{"operation":%s}`, resizeOperationJSON(status, message))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL, "--token", "token", "--json", "service", "postgres", "resize", "main-db",
		"--cpu-millicores", "250", "--memory-mebibytes", "768",
		"--cpu-limit-millicores", "500", "--memory-limit-mebibytes", "1280", "--wait",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("wait resize: %v stderr=%s", err, stderr.String())
	}
	if operationReads.Load() != 3 {
		t.Fatalf("wait returned before terminal status after %d reads", operationReads.Load())
	}
	var payload struct {
		Operation model.Operation `json:"operation"`
		NextStep  string          `json:"next_step"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil || payload.Operation.Status != model.OperationStatusCompleted || payload.NextStep != "" {
		t.Fatalf("unexpected terminal JSON: payload=%+v err=%v body=%s", payload, err, stdout.String())
	}
}

func TestRunServicePostgresResizeTerminalFailureDoesNotLeakEvidence(t *testing.T) {
	previousPollInterval := deployWaitPollInterval
	deployWaitPollInterval = time.Millisecond
	defer func() { deployWaitPollInterval = previousPollInterval }()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services":
			_, _ = w.Write([]byte(resizeServiceListJSON))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services/svc_pg":
			_, _ = fmt.Fprintf(w, `{"backing_service":%s}`, resizeServiceSecretJSON)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/backing-services/svc_pg/resize":
			w.WriteHeader(http.StatusAccepted)
			_, _ = fmt.Fprintf(w, `{"backing_service":%s,"operation":%s}`, resizeServiceSecretJSON, resizeOperationJSON("pending", "queued"))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_resize":
			_, _ = fmt.Fprintf(w, `{"operation":%s}`, resizeOperationJSON("failed", "controller-secret-789"))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/build-logs":
			_, _ = w.Write([]byte(`{"logs":"build-log-secret-987"}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/operations/op_resize/evidence"):
			http.NotFound(w, r)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			http.NotFound(w, r)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL, "--token", "token", "service", "postgres", "resize", "main-db",
		"--cpu-millicores", "250", "--memory-mebibytes", "768",
		"--cpu-limit-millicores", "500", "--memory-limit-mebibytes", "1280", "--wait",
	}, &stdout, &stderr)
	if err == nil || strings.Contains(err.Error(), "controller-secret-789") || strings.Contains(err.Error(), "build-log-secret-987") ||
		!strings.Contains(err.Error(), "fugue operation show op_resize") {
		t.Fatalf("unsafe terminal error: %v", err)
	}
}

func TestWaitForOperationsTreatsCanceledAsTerminal(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_canceled/evidence" {
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet || r.URL.Path != "/v1/operations/op_canceled" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
		_, _ = w.Write([]byte(`{"operation":{"id":"op_canceled","tenant_id":"tenant_123","app_id":"app_123","service_id":"svc_pg","type":"database-resize","status":"canceled","execution_mode":"managed","created_at":"2026-08-01T00:01:00Z","updated_at":"2026-08-01T00:02:00Z"}}`))
	}))
	defer server.Close()
	client, err := newClientWithOptions(server.URL, "token", clientOptions{RequireToken: true})
	if err != nil {
		t.Fatal(err)
	}
	cli := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
	_, err = cli.waitForOperations(client, []model.Operation{{ID: "op_canceled", Status: model.OperationStatusRunning}})
	if err == nil || !strings.Contains(err.Error(), "operation op_canceled was canceled") {
		t.Fatalf("canceled operation was not terminal: %v", err)
	}
}

func TestRunServiceShowLabelsPartialRuntimeAndUnknownLiveResources(t *testing.T) {
	t.Parallel()
	partial := `{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","password":"service-password-123","service_name":"demo-postgres","resources":{"cpu_millicores":100,"memory_mebibytes":512,"memory_limit_mebibytes":768}}},"created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-01T00:00:00Z"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/backing-services":
			_, _ = fmt.Fprintf(w, `{"backing_services":[%s]}`, partial)
		case "/v1/backing-services/svc_pg":
			_, _ = fmt.Fprintf(w, `{"backing_service":%s}`, partial)
		case "/v1/projects":
			_, _ = w.Write([]byte(`{"projects":[]}`))
		case "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var jsonOut, jsonErr bytes.Buffer
	if err := runWithStreams([]string{"--base-url", server.URL, "--token", "token", "--json", "service", "show", "main-db"}, &jsonOut, &jsonErr); err != nil {
		t.Fatalf("JSON service show: %v stderr=%s", err, jsonErr.String())
	}
	if strings.Contains(jsonOut.String(), "service-password-123") {
		t.Fatalf("service show leaked password: %s", jsonOut.String())
	}
	var payload struct {
		Resources backingServicePostgresResourcesOutput `json:"postgres_resources"`
	}
	if err := json.Unmarshal(jsonOut.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Resources.Bootstrap.Status != "partial" ||
		strings.Join(payload.Resources.Bootstrap.MissingFields, ",") != "cpu_limit_millicores" ||
		payload.Resources.Runtime.Status != "unknown" || payload.Resources.Live.Status != "unknown" {
		t.Fatalf("unexpected JSON resource state: %+v", payload.Resources)
	}

	var textOut, textErr bytes.Buffer
	if err := runWithStreams([]string{"--base-url", server.URL, "--token", "token", "service", "status", "main-db"}, &textOut, &textErr); err != nil {
		t.Fatalf("text service status: %v stderr=%s", err, textErr.String())
	}
	for _, line := range []string{
		"bootstrap_resources_status=partial",
		"bootstrap_resources_missing=cpu_limit_millicores",
		"runtime_resources_status=unknown",
		"live_resources_status=unknown",
	} {
		if !strings.Contains(textOut.String(), line) {
			t.Fatalf("missing %q in service status:\n%s", line, textOut.String())
		}
	}
}

func resizeOperationJSON(status, message string) string {
	return fmt.Sprintf(`{"id":"op_resize","tenant_id":"tenant_123","app_id":"app_123","service_id":"svc_pg","type":"database-resize","status":%q,"execution_mode":"managed","result_message":%q,"created_at":"2026-08-01T00:01:00Z","updated_at":"2026-08-01T00:01:00Z"}`, status, message)
}
