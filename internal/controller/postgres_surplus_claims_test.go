package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/runtime"
)

func TestSurplusUnboundClaimRetentionPreservesServingPrimaryAndData(t *testing.T) {
	for _, scenario := range []string{"surplus", "bound", "used-pod", "used-job", "initialized", "foreign-pvc", "foreign-cluster", "foreign-pod", "primary-unready", "scaling", "switchover", "deleting", "generation-changed", "identity-changed", "conflict"} {
		t.Run(scenario, func(t *testing.T) {
			var cluster kubeCloudNativePGCluster
			if err := json.Unmarshal([]byte(`{"metadata":{"name":"database","uid":"cluster-id","generation":3,"ownerReferences":[{"apiVersion":"fugue.pro/v1alpha1","kind":"ManagedApp","name":"app","uid":"app-id"}]},"spec":{"instances":1,"storage":{"storageClass":"database-storage"}},"status":{"instances":2,"readyInstances":1,"currentPrimary":"database-1","targetPrimary":"database-1","danglingPVC":["database-2"]}}`), &cluster); err != nil {
				t.Fatal(err)
			}
			managed := runtime.ManagedAppObject{}
			managed.Metadata.Name, managed.Metadata.UID = "app", "app-id"
			if scenario == "foreign-cluster" {
				managed.Metadata.UID = "different"
			}
			if scenario == "scaling" {
				cluster.Spec.Instances = 2
			}
			if scenario == "switchover" {
				cluster.Status.TargetPrimary = "database-2"
			}
			if scenario == "deleting" {
				cluster.Metadata.DeletionTimestamp = "2026-01-01T00:00:00Z"
			}
			var primary kubePod
			if err := json.Unmarshal([]byte(`{"metadata":{"name":"database-1","ownerReferences":[{"apiVersion":"postgresql.cnpg.io/v1","kind":"Cluster","uid":"cluster-id"}]},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}`), &primary); err != nil {
				t.Fatal(err)
			}
			if scenario == "primary-unready" {
				primary.Status.Conditions[0].Status = "False"
			}
			if scenario == "foreign-pod" {
				primary.ObservedOwnerReferences = nil
			}
			patched, clusterReads, pvcReads := false, 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				volumes := []any{map[string]any{"persistentVolumeClaim": map[string]string{"claimName": "database-2"}}}
				switch {
				case strings.HasSuffix(r.URL.Path, "/clusters/database"):
					clusterReads++
					fresh := cluster
					if scenario == "generation-changed" && clusterReads > 1 {
						fresh.Metadata.Generation++
					}
					json.NewEncoder(w).Encode(fresh)
				case strings.HasSuffix(r.URL.Path, "/pods"):
					items := []any{primary}
					if scenario == "used-pod" {
						items = append(items, map[string]any{"spec": map[string]any{"volumes": volumes}})
					}
					json.NewEncoder(w).Encode(map[string]any{"items": items})
				case strings.HasSuffix(r.URL.Path, "/jobs"):
					items := []any{}
					if scenario == "used-job" {
						items = append(items, map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"volumes": volumes}}}})
					}
					json.NewEncoder(w).Encode(map[string]any{"items": items})
				case strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims"):
					json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"metadata": map[string]string{"name": "database-1"}}, map[string]any{"metadata": map[string]string{"name": "database-2"}}}})
				case strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims/database-2") && r.Method == http.MethodGet:
					pvcReads++
					uid, state, owner, volume := "claim-id", "initializing", "cluster-id", ""
					if scenario == "identity-changed" && pvcReads > 1 {
						uid = "replacement"
					}
					if scenario == "initialized" {
						state = "ready"
					}
					if scenario == "foreign-pvc" {
						owner = "other"
					}
					if scenario == "bound" {
						volume = "valuable-volume"
					}
					json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": uid, "resourceVersion": "42", "labels": map[string]string{"cnpg.io/cluster": "database", "cnpg.io/instanceRole": "replica", "cnpg.io/pvcRole": "PG_DATA"}, "annotations": map[string]string{"cnpg.io/pvcStatus": state}, "ownerReferences": []any{map[string]string{"apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster", "uid": owner}, map[string]string{"kind": "AuditOwner", "uid": "audit-id"}}}, "spec": map[string]string{"storageClassName": "database-storage", "volumeName": volume}, "status": map[string]string{"phase": "Pending"}})
				case strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims/database-2") && r.Method == http.MethodPatch:
					var body map[string]any
					json.NewDecoder(r.Body).Decode(&body)
					if len(body) != 1 || body["metadata"] == nil {
						t.Errorf("modified non-metadata state: %v", body)
					}
					meta := body["metadata"].(map[string]any)
					if meta["uid"] != "claim-id" || meta["resourceVersion"] != "42" || len(meta["ownerReferences"].([]any)) != 1 {
						t.Errorf("lost identity or unrelated owner: %v", meta)
					}
					if scenario == "conflict" {
						w.WriteHeader(http.StatusConflict)
						return
					}
					patched = true
					json.NewEncoder(w).Encode(map[string]any{})
				default:
					t.Errorf("unexpected request (including any primary mutation): %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			err := retainSurplusUnboundClaims(context.Background(), &kubeClient{baseURL: server.URL, client: server.Client()}, "tenant", managed, cluster, []kubePod{primary})
			if err != nil || patched != (scenario == "surplus") {
				t.Fatalf("patched=%v err=%v", patched, err)
			}
		})
	}
}
