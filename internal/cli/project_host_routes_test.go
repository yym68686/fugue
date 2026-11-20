package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"fugue/internal/model"
)

func TestProjectHostRouteCLIPlanAndApplyWait(t *testing.T) {
	plan := model.ProjectHostRoutePlan{
		ProjectID: "project_123", PlanID: "plan-id",
		Edit:   model.ProjectHostRouteEdit{Hostname: "customer.example.net", Paths: map[string]string{"/": "web", "/api": "api"}},
		Before: map[string]string{"/": "api"}, After: map[string]string{"/": "web", "/api": "api"},
		HostnameOwnerAppID: "api", State: "planned",
	}
	var operations []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Tenant","slug":"tenant"}]}`))
		case "/v1/projects":
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","name":"demo","slug":"demo","tenant_id":"tenant_123"}]}`))
		case "/v1/projects/project_123/routes/host-plan":
			operations = append(operations, "plan")
			var edit model.ProjectHostRouteEdit
			if err := json.NewDecoder(r.Body).Decode(&edit); err != nil || !reflect.DeepEqual(edit, plan.Edit) {
				t.Errorf("unexpected route plan request: %+v %v", edit, err)
			}
			_ = json.NewEncoder(w).Encode(plan)
		case "/v1/projects/project_123/routes/host-apply":
			operations = append(operations, "apply")
			var request model.ProjectHostRoutePlan
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || !reflect.DeepEqual(request, plan) {
				t.Errorf("apply did not send the reviewed plan: %+v %v", request, err)
			}
			saved := plan
			saved.State = "saved"
			_ = json.NewEncoder(w).Encode(saved)
		case "/v1/projects/project_123/routes/host-status":
			operations = append(operations, "status")
			_ = json.NewEncoder(w).Encode(model.ProjectHostRoutePublicationStatus{State: "published", ReleaseID: "full-release", Reason: "confirmed"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	run := func(args ...string) []byte {
		t.Helper()
		var out, stderr bytes.Buffer
		if err := runWithStreams(append([]string{"--base-url", server.URL, "--token", "test-token", "project", "routes"}, args...), &out, &stderr); err != nil {
			t.Fatalf("run routes: %v %s", err, stderr.String())
		}
		return out.Bytes()
	}
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := os.WriteFile(path, run("plan", "demo", "--host", "customer.example.net", "--route", "/=web", "--route", "/api=api"), 0600); err != nil {
		t.Fatal(err)
	}
	got := run("apply", "demo", "--plan", path, "--wait")
	if !bytes.Contains(got, []byte("full-release")) || !reflect.DeepEqual(operations, []string{"plan", "status", "apply", "status"}) {
		t.Fatalf("unexpected wait result %s, operations %v", got, operations)
	}
}
