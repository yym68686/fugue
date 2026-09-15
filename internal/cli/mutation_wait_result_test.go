package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestMutationWaitReturnsCompletedOperation(t *testing.T) {
	previousPoll := deployWaitPollInterval
	deployWaitPollInterval = time.Millisecond
	defer func() { deployWaitPollInterval = previousPoll }()
	for _, args := range [][]string{
		{"app", "env", "set", "demo", "TEST_VALUE=value"},
		{"app", "env", "unset", "demo", "TEST_VALUE"},
		{"app", "storage", "set", "demo", "--mount", "/new-data", "--size", "2Gi"},
		{"app", "storage", "disable", "demo"},
		{"app", "storage", "reset", "demo"},
	} {
		t.Run(args[1]+"_"+args[2], func(t *testing.T) {
			app := model.App{ID: "app_demo", Name: "demo", TenantID: "tenant_demo", ProjectID: "project_demo"}
			app.Spec.PersistentStorage = &model.AppPersistentStorageSpec{StorageSize: "1Gi", Mounts: []model.AppPersistentStorageMount{{Path: "/data"}}}
			polls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/apps":
					_ = json.NewEncoder(w).Encode(map[string]any{"apps": []model.App{app}})
				case "/v1/apps/app_demo":
					_ = json.NewEncoder(w).Encode(map[string]any{"app": app})
				case "/v1/apps/app_demo/env":
					out := map[string]any{"env": map[string]string{"TEST_VALUE": "value"}}
					if r.Method != http.MethodGet {
						out["operation"] = model.Operation{ID: "op_demo", Status: "pending"}
					}
					_ = json.NewEncoder(w).Encode(out)
				case "/v1/apps/app_demo/deploy":
					_ = json.NewEncoder(w).Encode(map[string]any{"operation": model.Operation{ID: "op_demo", Status: "pending"}})
				case "/v1/operations/op_demo":
					polls++
					status := "running"
					if polls >= 2 {
						status = "completed"
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"operation": model.Operation{ID: "op_demo", Status: status, ResultMessage: "runtime verified"}})
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			var stdout, stderr bytes.Buffer
			command := append([]string{"--base-url", server.URL, "--token", "test", "--json"}, args...)
			if err := runWithStreams(command, &stdout, &stderr); err != nil {
				t.Fatalf("run: %v; %s", err, stderr.String())
			}
			var result struct {
				Operation model.Operation `json:"operation"`
			}
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result.Operation.Status != "completed" || result.Operation.ResultMessage != "runtime verified" || polls != 2 {
				t.Fatalf("stale result: %+v; polls=%d", result.Operation, polls)
			}
		})
	}
}
