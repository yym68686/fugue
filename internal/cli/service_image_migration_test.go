package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServicePostgresImageMigrationDefaultsToReadOnlyPlan(t *testing.T) {
	var request postgresImageMigrationRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/backing-services":
			w.Write([]byte(resizeServiceListJSON))
		case "/v1/backing-services/svc_pg/migrate-image":
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatal(err)
			}
			w.Write([]byte(`{"dry_run":true,"plan":{"service_id":"svc_pg","app_id":"app_123","ownership":"independent","target_image":"verified","config_hash":"config-hash","requires_downtime":true,"live_preflight_pending":true,"stages":[]}}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	if err := runWithStreams([]string{"--base-url", server.URL, "--token", "test", "--json", "service", "postgres", "migrate-image", "main-db"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if request.Apply || request.Acknowledge || request.Expected != "" {
		t.Fatalf("default plan requested mutation: %+v", request)
	}
	var response postgresImageMigrationResponse
	if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !response.DryRun || response.Operation != nil || response.Plan.Ownership != "independent" || response.Plan.AppID != "app_123" {
		t.Fatal("plan returned an operation")
	}
}

func TestServicePostgresImageMigrationRejectsUnacknowledgedApplyLocally(t *testing.T) {
	for _, flags := range [][]string{{"--apply"}, {"--apply", "--acknowledge-downtime"}, {"--apply", "--expected-config-hash", "hash"}} {
		var stdout, stderr bytes.Buffer
		args := append([]string{"--base-url", "http://127.0.0.1:1", "--token", "test", "service", "postgres", "migrate-image", "main-db"}, flags...)
		if err := runWithStreams(args, &stdout, &stderr); err == nil {
			t.Fatalf("unsafe apply accepted: %v", flags)
		}
	}
}
