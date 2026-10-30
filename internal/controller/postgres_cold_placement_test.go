package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

func TestColdRecoveryPreflightsReplicaStorageBeforeAnyMutation(t *testing.T) {
	for _, scenario := range []string{"ready", "csi-only-replica", "unready-replica", "same-node", "capacity", "local-storage"} {
		t.Run(scenario, func(t *testing.T) {
			state := store.New(filepath.Join(t.TempDir(), "state.json"))
			if err := state.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, _ := state.CreateTenant("Storage recovery")
			primary, _, _ := state.CreateRuntime(tenant.ID, "primary", model.RuntimeTypeManagedOwned, "", nil)
			replica, _, _ := state.CreateRuntime(tenant.ID, "replica", model.RuntimeTypeManagedOwned, "", nil)
			pg := model.AppPostgresSpec{ServiceName: "database", RuntimeID: primary.ID, FailoverTargetRuntimeID: replica.ID, PrimaryNodeName: "primary-node", StorageClassName: "recovery-storage", StorageSize: "3Gi", Instances: 2, SynchronousReplicas: 1}
			nodes := []any{}
			for _, n := range []struct{ name, runtime string }{{"primary-node", primary.ID}, {"replica-node", replica.ID}} {
				if scenario == "same-node" && n.name == "replica-node" {
					continue
				}
				nodes = append(nodes, map[string]any{"metadata": map[string]any{"name": n.name, "labels": map[string]string{runtimepkg.RuntimeIDLabelKey: n.runtime, runtimepkg.TenantIDLabelKey: tenant.ID, kubeHostnameLabelKey: n.name}}, "status": map[string]any{"conditions": []any{map[string]string{"type": "Ready", "status": "True"}}}})
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("preflight wrote %s %s", r.Method, r.URL.Path)
					http.Error(w, "forbidden", 403)
					return
				}
				var out any
				switch {
				case r.URL.Path == "/api/v1/nodes":
					out = map[string]any{"items": nodes}
				case strings.Contains(r.URL.Path, "/persistentvolumeclaims/"):
					http.NotFound(w, r)
					return
				case strings.Contains(r.URL.Path, "/storageclasses/"):
					provisioner := longhornCSIDriver
					if scenario == "local-storage" {
						provisioner = "rancher.io/local-path"
					}
					out = map[string]any{"metadata": map[string]string{"name": pg.StorageClassName}, "provisioner": provisioner}
				case strings.Contains(r.URL.Path, "/csidrivers/"):
					out = map[string]any{}
				case strings.HasSuffix(r.URL.Path, "/csinodes"):
					out = map[string]any{"items": []any{map[string]any{"metadata": map[string]string{"name": "primary-node"}, "spec": map[string]any{"drivers": []any{map[string]string{"name": longhornCSIDriver}}}}, map[string]any{"metadata": map[string]string{"name": "replica-node"}, "spec": map[string]any{"drivers": []any{map[string]string{"name": longhornCSIDriver}}}}}}
				case strings.HasSuffix(r.URL.Path, "/csistoragecapacities"):
					items := []any{}
					if scenario == "capacity" {
						items = append(items, map[string]any{"storageClassName": pg.StorageClassName, "nodeTopology": map[string]any{"matchLabels": map[string]string{kubeHostnameLabelKey: "replica-node"}}, "capacity": "1Gi"})
					}
					out = map[string]any{"items": items}
				case r.URL.Path == "/apis/longhorn.io/v1beta2/nodes":
					items := []any{map[string]any{"metadata": map[string]string{"name": "primary-node"}, "status": map[string]any{"conditions": []any{map[string]string{"type": "Ready", "status": "True"}}}}}
					if scenario != "csi-only-replica" {
						ready := "True"
						if scenario == "unready-replica" {
							ready = "False"
						}
						items = append(items, map[string]any{"metadata": map[string]string{"name": "replica-node"}, "status": map[string]any{"conditions": []any{map[string]string{"type": "Ready", "status": ready}}}})
					}
					out = map[string]any{"items": items}
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				json.NewEncoder(w).Encode(out)
			}))
			defer server.Close()
			c := &kubeClient{baseURL: server.URL, client: server.Client()}
			svc := &Service{Store: state}
			placements, err := svc.coldRecoveryPlacements(context.Background(), c, model.App{TenantID: tenant.ID}, pg, "primary-node")
			good := scenario == "ready" || scenario == "local-storage"
			if (err == nil) != good {
				t.Fatalf("placements=%+v err=%v", placements, err)
			}
			if good && (len(placements) != 2 || placements[0].NodeSelector[kubeHostnameLabelKey] != "primary-node" || placements[1].NodeSelector[kubeHostnameLabelKey] != "replica-node") {
				t.Fatal("storage-compatible distinct hosts were not pinned", placements)
			}
		})
	}
}
