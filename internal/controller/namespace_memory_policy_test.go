package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"fugue/internal/config"
)

func TestNamespaceMemoryPolicyUsesIndependentConfigurationAndPreservesOverrides(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "install default", true: "preserve configured request"}[existing], func(t *testing.T) {
			creates := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.URL.Path == "/api/v1/namespaces/system/configmaps/fugue-workload-memory-policy":
					_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{"policy.json": `{"apiVersion":"constraints.fugue.dev/v1","kind":"WorkloadMemoryPolicy","namespacePrefixes":["fg-"],"defaultMemoryRequest":"64Mi"}`}})
				case r.URL.Path == "/api/v1/namespaces/fg-example/limitranges" && r.Method == http.MethodGet:
					if existing {
						_, _ = w.Write([]byte(`{"items":[{"spec":{"limits":[{"defaultRequest":{"memory":"96Mi"}}]}}]}`))
					} else {
						_, _ = w.Write([]byte(`{"items":[]}`))
					}
				case r.URL.Path == "/api/v1/namespaces/fg-example/limitranges" && r.Method == http.MethodPost:
					creates++
					var body map[string]any
					_ = json.NewDecoder(r.Body).Decode(&body)
					rule := body["spec"].(map[string]any)["limits"].([]any)[0].(map[string]any)
					if rule["default"] != nil || rule["max"] != nil || rule["defaultRequest"].(map[string]any)["memory"] != "64Mi" {
						t.Errorf("reservation changed limits: %+v", rule)
					}
					w.WriteHeader(http.StatusCreated)
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client := &kubeClient{client: server.Client(), baseURL: server.URL}
			s := &Service{Config: config.ControllerConfig{ControlPlaneNamespace: "system"}}
			if err := s.ensureNamespaceMemoryPolicy(context.Background(), client, "fg-example"); err != nil {
				t.Fatal(err)
			}
			if (creates == 1) == existing {
				t.Fatalf("unexpected creates %d", creates)
			}
		})
	}
}
