package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"fugue/internal/model"
)

func TestNodeBuildPolicyCommandRejectsIgnoredOrUnappliedIntent(t *testing.T) {
	for _, scenario := range []string{"ignored", "not-applied", "missing-policy", "already-converged"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPatch || r.URL.Path != "/v1/cluster/nodes/storage-node/policy" {
					t.Errorf("unexpected API request %s %s", r.Method, r.URL.Path)
				}
				var requested setClusterNodePolicyRequest
				if err := json.NewDecoder(r.Body).Decode(&requested); err != nil {
					t.Error(err)
				}
				if requested.AllowBuilds == nil || *requested.AllowBuilds || requested.AllowAppRuntime != nil || requested.AllowSharedPool != nil {
					t.Errorf("changed unrelated policy: %+v", requested)
				}
				policy := &model.ClusterNodePolicy{AllowSharedPool: true, EffectiveSharedPool: true, AllowAppRuntime: true, EffectiveAppRuntime: true}
				switch scenario {
				case "ignored":
					policy.AllowBuilds = true
					policy.EffectiveBuilds = true
				case "not-applied":
					policy.EffectiveBuilds = true
				case "missing-policy":
					policy = nil
				}
				_ = json.NewEncoder(w).Encode(setClusterNodePolicyResponse{ClusterNode: model.ClusterNode{Name: "storage-node", Policy: policy}, NodeReconciled: false})
			}))
			defer server.Close()
			var stdout, stderr bytes.Buffer
			err := runWithStreams([]string{"--base-url", server.URL, "--token", "bootstrap-fixture", "admin", "cluster", "node-policy", "set", "storage-node", "--builds", "off", "--json"}, &stdout, &stderr)
			if (err == nil) != (scenario == "already-converged") {
				t.Fatalf("false success or rejected no-op: scenario=%s err=%v output=%s", scenario, err, stdout.String())
			}
		})
	}
}
