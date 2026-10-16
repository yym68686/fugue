package api

import (
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

func TestClusterNodePolicyDisablesBuildsWhileKeepingSharedRuntime(t *testing.T) {
	t.Parallel()
	s := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{runtimepkg.SharedPoolLabelKey: runtimepkg.SharedPoolLabelValue, runtimepkg.BuildNodeLabelKey: runtimepkg.BuildNodeLabelValue, runtimepkg.BuilderRoleLabelKey: runtimepkg.NodeRoleLabelValue, runtimepkg.AppRuntimeRoleLabelKey: runtimepkg.NodeRoleLabelValue}
	kube := newBootstrapControlPlaneKubeServerWithLabels(t, labels)
	defer kube.Close()
	server := NewServer(s, auth.New(s, "bootstrap-secret"), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{client: kube.Client(), baseURL: kube.URL, bearerToken: "test-token"}, nil
	}
	for range 2 {
		response := performJSONRequest(t, server, http.MethodPatch, "/v1/cluster/nodes/gcp1/policy", "bootstrap-secret", map[string]any{"allow_builds": false})
		if response.Code != http.StatusOK {
			t.Fatalf("policy write failed: %d %s", response.Code, response.Body.String())
		}
		read := performJSONRequest(t, server, http.MethodGet, "/v1/cluster/node-policies/gcp1", "bootstrap-secret", nil)
		if read.Code != http.StatusOK {
			t.Fatalf("policy read failed: %d", read.Code)
		}
		var result struct {
			NodePolicy model.ClusterNodePolicyStatus `json:"node_policy"`
		}
		mustDecodeJSON(t, read, &result)
		p := result.NodePolicy
		if p.Policy == nil || p.Policy.AllowBuilds || p.Policy.EffectiveBuilds || !p.Policy.AllowSharedPool || !p.Policy.EffectiveSharedPool || !p.Policy.AllowAppRuntime || !p.Policy.EffectiveAppRuntime || !p.Reconciled {
			t.Fatalf("build isolation did not preserve shared serving: %+v", p)
		}
		if p.Labels[runtimepkg.BuildNodeLabelKey] != "false" || p.Labels[runtimepkg.BuilderRoleLabelKey] != "false" {
			t.Fatalf("stale build labels: %v", p.Labels)
		}
	}
}
