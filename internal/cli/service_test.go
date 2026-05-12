package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRunServiceLocalizeUsesBackingServiceLocalizeEndpoint(t *testing.T) {
	t.Parallel()

	var gotBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_b","tenant_id":"tenant_123","name":"target-vps","type":"managed-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services":
			_, _ = w.Write([]byte(`{"backing_services":[{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","service_name":"demo-postgres","failover_target_runtime_id":"runtime_b"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services/svc_pg":
			_, _ = w.Write([]byte(`{"backing_service":{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","service_name":"demo-postgres","failover_target_runtime_id":"runtime_b"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/backing-services/svc_pg/localize":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode localize body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"backing_service":{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","service_name":"demo-postgres","failover_target_runtime_id":"runtime_b"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"operation":{"id":"op_localize","tenant_id":"tenant_123","app_id":"app_123","service_id":"svc_pg","type":"database-localize","status":"pending","execution_mode":"managed","target_runtime_id":"runtime_b","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"-o", "json",
		"service", "localize", "main-db",
		"--to", "target-vps",
		"--node", "ns101351",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run service localize: %v stderr=%s", err, stderr.String())
	}

	if gotBody["target_runtime_id"] != "runtime_b" || gotBody["target_node_name"] != "ns101351" {
		t.Fatalf("expected localize target runtime/node, got %+v", gotBody)
	}
	var response struct {
		Operation struct {
			ID              string `json:"id"`
			Type            string `json:"type"`
			ServiceID       string `json:"service_id"`
			TargetRuntimeID string `json:"target_runtime_id"`
		} `json:"operation"`
		TargetNodeName string `json:"target_node_name"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
		t.Fatalf("decode stdout JSON: %v body=%s", err, stdout.String())
	}
	if response.Operation.ID != "op_localize" || response.Operation.Type != "database-localize" || response.Operation.ServiceID != "svc_pg" {
		t.Fatalf("unexpected operation response: %+v", response.Operation)
	}
	if response.Operation.TargetRuntimeID != "runtime_b" || response.TargetNodeName != "ns101351" {
		t.Fatalf("unexpected localize response target: %+v", response)
	}
}
