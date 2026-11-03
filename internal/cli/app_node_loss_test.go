package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAppNodeLossSetRequiresExplicitPolicyAndDoesNotEnableHA(t *testing.T) {
	patches := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			w.Write([]byte(`{"apps":[{"id":"app_example","tenant_id":"tenant_example","name":"service","spec":{"replicas":1,"runtime_id":"runtime_example"}}]}`))
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/apps/app_example":
			patches++
			var body map[string]json.RawMessage
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			var policy struct {
				Mode    string `json:"mode"`
				Seconds *int64 `json:"toleration_seconds"`
			}
			if err := json.Unmarshal(body["node_loss"], &policy); err != nil || len(body) != 1 || policy.Mode != "replace" || policy.Seconds == nil || *policy.Seconds != 0 {
				t.Errorf("unexpected lifecycle/resource mutation %s %v", body, err)
			}
			w.Write([]byte(`{"app":{"id":"app_example","spec":{"replicas":1}},"operation":{"id":"op_example","type":"deploy","status":"pending"}}`))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	for _, args := range [][]string{{"--wait=false"}, {"--mode", "wait", "--toleration-seconds", "0", "--wait=false"}, {"--mode", "replace", "--toleration-seconds", "-1", "--wait=false"}} {
		var out, stderr bytes.Buffer
		base := []string{"--base-url", server.URL, "--token", "synthetic-token", "app", "node-loss", "set", "service"}
		if err := runWithStreams(append(base, args...), &out, &stderr); err == nil {
			t.Fatal("invalid or implicit policy accepted")
		}
	}
	var out, stderr bytes.Buffer
	if err := runWithStreams([]string{"--base-url", server.URL, "--token", "synthetic-token", "app", "node-loss", "set", "service", "--mode", "replace", "--toleration-seconds", "0", "--wait=false"}, &out, &stderr); err != nil {
		t.Fatal(err, stderr.String())
	}
	if patches != 1 {
		t.Fatal("unexpected number of deployments", patches)
	}
}
