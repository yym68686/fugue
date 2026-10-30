package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestMigrationRetentionKeepsSourceVolumeAndOnlyRemovesUnstartedReplica(t *testing.T) {
	for _, scenario := range []string{"retain", "resume-retained", "primary-unready", "same-class", "another-user", "pending-job", "foreign-claim", "claim-replaced", "cluster-changed", "pod-assigned", "conflict"} {
		t.Run(scenario, func(t *testing.T) {
			var cluster kubeCloudNativePGCluster
			json.Unmarshal([]byte(`{"metadata":{"name":"db","uid":"cluster","generation":2,"labels":{"app.kubernetes.io/managed-by":"fugue","fugue.pro/tenant-id":"tenant","fugue.pro/project-id":"project","fugue.pro/backing-service-id":"service"}},"spec":{"instances":1,"storage":{"storageClass":"replicated"}},"status":{"instances":3,"readyInstances":2,"currentPrimary":"db-3","targetPrimary":"db-3"}}`), &cluster)
			var primary, old kubePod
			json.Unmarshal([]byte(`{"metadata":{"name":"db-3","uid":"primary","ownerReferences":[{"apiVersion":"postgresql.cnpg.io/v1","kind":"Cluster","uid":"cluster"}]},"spec":{"nodeName":"destination"},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}`), &primary)
			oldJSON := `{"metadata":{"name":"db-1","uid":"old-pod","resourceVersion":"7","ownerReferences":[{"apiVersion":"postgresql.cnpg.io/v1","kind":"Cluster","uid":"cluster"}]},"spec":{"volumes":[{"persistentVolumeClaim":{"claimName":"db-1"}}]},"status":{"phase":"Pending"}}`
			json.Unmarshal([]byte(oldJSON), &old)
			if scenario == "primary-unready" {
				primary.Status.Conditions = nil
			}
			patches, deletes, claimReads := 0, 0, 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims/db-3"):
					json.NewEncoder(w).Encode(map[string]any{"spec": map[string]string{"storageClassName": "replicated"}})
				case strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims/db-1") && r.Method == http.MethodGet:
					claimReads++
					class, owner, uid := "source-local", "cluster", "old-claim"
					if scenario == "same-class" {
						class = "replicated"
					}
					if scenario == "foreign-claim" {
						owner = "foreign"
					}
					if scenario == "claim-replaced" && claimReads > 1 {
						uid = "replacement"
					}
					meta := map[string]any{"name": "db-1", "uid": uid, "resourceVersion": "9", "labels": map[string]string{"cnpg.io/cluster": "db", "cnpg.io/pvcRole": "PG_DATA"}, "ownerReferences": []any{map[string]string{"apiVersion": runtime.CloudNativePGAPIVersion, "kind": "Cluster", "uid": owner}, map[string]string{"kind": "AuditOwner", "uid": "audit"}}}
					if scenario == "resume-retained" {
						meta["labels"] = map[string]string{}
						meta["ownerReferences"] = []any{map[string]string{"kind": "AuditOwner", "uid": "audit"}}
						meta["annotations"] = map[string]string{"fugue.pro/recovery-retained-cluster-uid": "cluster", "fugue.pro/recovery-retained-reason": "superseded-local-replica"}
					}
					json.NewEncoder(w).Encode(map[string]any{"metadata": meta, "spec": map[string]string{"storageClassName": class, "volumeName": "valuable-source-data"}, "status": map[string]string{"phase": "Bound"}})
				case strings.HasSuffix(r.URL.Path, "/persistentvolumes/valuable-source-data"):
					w.Write([]byte(`{"spec":{"nodeAffinity":{"required":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":["source"]}]}]}}}}`))
				case strings.HasSuffix(r.URL.Path, "/pods"):
					items := []any{}
					if scenario == "another-user" {
						items = append(items, map[string]any{"metadata": map[string]string{"uid": "backup"}, "spec": old.Spec})
					}
					json.NewEncoder(w).Encode(map[string]any{"items": items})
				case strings.HasSuffix(r.URL.Path, "/jobs"):
					items := []any{}
					if scenario == "pending-job" {
						items = append(items, map[string]any{"spec": map[string]any{"template": map[string]any{"spec": old.Spec}}})
					}
					json.NewEncoder(w).Encode(map[string]any{"items": items})
				case strings.HasSuffix(r.URL.Path, "/clusters/db"):
					copy := cluster
					if scenario == "cluster-changed" {
						copy.Metadata.Generation++
					}
					json.NewEncoder(w).Encode(copy)
				case strings.HasSuffix(r.URL.Path, "/pods/db-1") && r.Method == http.MethodGet:
					data := oldJSON
					if scenario == "pod-assigned" {
						data = strings.Replace(data, `"spec":{`, `"spec":{"nodeName":"source",`, 1)
					}
					w.Write([]byte(data))
				case strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims/db-1") && r.Method == http.MethodPatch:
					var body map[string]any
					json.NewDecoder(r.Body).Decode(&body)
					meta := body["metadata"].(map[string]any)
					if len(body) != 1 || meta["uid"] != "old-claim" || meta["resourceVersion"] != "9" || len(meta["ownerReferences"].([]any)) != 1 {
						t.Fatalf("data or identity modified: %v", body)
					}
					if scenario == "conflict" {
						w.WriteHeader(409)
						return
					}
					patches++
					w.Write([]byte(`{}`))
				case strings.HasSuffix(r.URL.Path, "/pods/db-1") && r.Method == http.MethodDelete:
					var body map[string]any
					json.NewDecoder(r.Body).Decode(&body)
					preconditions := body["preconditions"].(map[string]any)
					if preconditions["uid"] != "old-pod" || preconditions["resourceVersion"] != "7" {
						t.Fatalf("missing Pod identity fence: %v", body)
					}
					deletes++
					w.Write([]byte(`{}`))
				default:
					t.Fatalf("unexpected request, including any PVC/PV deletion: %s %s", r.Method, r.URL.Path)
				}
			}))
			defer srv.Close()
			app := model.App{ID: "app", TenantID: "tenant", ProjectID: "project"}
			deployment := runtime.ManagedBackingServiceDeployment{ServiceID: "service", ResourceName: "db", DesiredInstances: 1, StorageClassName: "replicated"}
			err := retainUnscheduledMigrationReplica(context.Background(), &kubeClient{baseURL: srv.URL, client: srv.Client()}, "tenant", app, deployment, cluster, []kubePod{primary, old})
			want := 0
			if scenario == "retain" {
				want = 1
			}
			wantDeletes := want
			if scenario == "resume-retained" {
				wantDeletes = 1
			}
			if err != nil || patches != want || deletes != wantDeletes {
				t.Fatalf("patches=%d deletes=%d err=%v", patches, deletes, err)
			}
		})
	}
}
