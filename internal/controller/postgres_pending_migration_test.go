package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestRecoverUnboundPostgresMigrationReplicasProtectsDataAndObjectIdentity(t *testing.T) {
	for _, tc := range []struct {
		name                                               string
		bound, selected, scheduled, conflict, currentClass bool
		want                                               []string
	}{
		{name: "unbound old replica", want: []string{"claim", "job"}},
		{name: "bound data", bound: true},
		{name: "provisioning in progress", selected: true},
		{name: "scheduled pod", scheduled: true},
		{name: "claim changed after observation", conflict: true, want: []string{"claim"}},
		{name: "target intent not yet applied", currentClass: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var deletes []string
			pod := map[string]any{
				"metadata": map[string]any{"name": "sample-db-2-join-abcd", "uid": "pod-id", "ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "sample-db-2-join", "uid": "job-id", "controller": true}}},
				"spec":     map[string]any{"volumes": []any{map[string]any{"name": "pgdata", "persistentVolumeClaim": map[string]any{"claimName": "sample-db-2"}}}},
				"status":   map[string]any{"phase": "Pending"},
			}
			if tc.scheduled {
				pod["spec"].(map[string]any)["nodeName"] = "node-target"
			}
			annotations := map[string]string{}
			if tc.selected {
				annotations["volume.kubernetes.io/selected-node"] = "node-target"
			}
			pvc := map[string]any{
				"metadata": map[string]any{"name": "sample-db-2", "uid": "claim-id", "resourceVersion": "12", "labels": map[string]string{"cnpg.io/cluster": "sample-db", "cnpg.io/pvcRole": "PG_DATA"}, "annotations": annotations},
				"spec":     map[string]any{"storageClassName": "source-storage"}, "status": map[string]any{"phase": "Pending"},
			}
			if tc.bound {
				pvc["spec"].(map[string]any)["volumeName"] = "data-pv"
				pvc["status"].(map[string]any)["phase"] = "Bound"
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/pods"):
					json.NewEncoder(w).Encode(map[string]any{"items": []any{pod}})
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims/sample-db-2"):
					json.NewEncoder(w).Encode(pvc)
				case r.Method == http.MethodDelete:
					var body struct {
						Preconditions map[string]string `json:"preconditions"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if strings.Contains(r.URL.Path, "/persistentvolumeclaims/") {
						deletes = append(deletes, "claim")
						if body.Preconditions["uid"] != "claim-id" || body.Preconditions["resourceVersion"] != "12" {
							t.Errorf("unsafe claim deletion: %#v", body)
						}
						if tc.conflict {
							http.Error(w, "changed", http.StatusConflict)
							return
						}
					} else {
						deletes = append(deletes, "job")
						if body.Preconditions["uid"] != "job-id" {
							t.Errorf("unsafe job deletion: %#v", body)
						}
					}
					json.NewEncoder(w).Encode(map[string]any{})
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client := &kubeClient{client: server.Client(), baseURL: server.URL}
			var cluster kubeCloudNativePGCluster
			cluster.Status.CurrentPrimary = "sample-db-1"
			cluster.Status.ReadyInstances = 1
			cluster.Spec.Storage.StorageClass = "target-storage"
			if tc.currentClass {
				cluster.Spec.Storage.StorageClass = "source-storage"
			}
			err := recoverUnboundPostgresMigrationReplicas(context.Background(), client, "tenant-demo", "sample-db", cluster, managedPostgresStorageTarget{StorageClassName: "target-storage", StorageSize: "1Gi"})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(deletes, tc.want) {
				t.Fatalf("deletes %v, want %v", deletes, tc.want)
			}
		})
	}
}

func TestPostgresExpansionLeavesMigrationSourceAndPendingTargetUntouched(t *testing.T) {
	for _, sourceClass := range []string{"source-storage", "target-storage"} {
		t.Run(sourceClass, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("unexpected mutation %s %s", r.Method, r.URL.Path)
					http.Error(w, "unexpected mutation", 500)
					return
				}
				if strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims") {
					json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"metadata": map[string]any{"name": "sample-db-2"}}}})
					return
				}
				json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "sample-db-2"}, "spec": map[string]any{"storageClassName": sourceClass, "resources": map[string]any{"requests": map[string]string{"storage": "1Gi"}}}, "status": map[string]string{"phase": "Pending"}})
			}))
			defer server.Close()
			client := &kubeClient{client: server.Client(), baseURL: server.URL}
			svc := &Service{}
			target := managedPostgresStorageTarget{StorageClassName: "target-storage", StorageSize: "1Gi"}
			if err := svc.prepareManagedPostgresInPlaceStorageExpansion(context.Background(), client, "tenant-demo", "sample-db", target); err != nil {
				t.Fatal(err)
			}
			if sourceClass != "target-storage" {
				if err := svc.prepareManagedPostgresInPlaceStorageExpansionForExistingCluster(context.Background(), client, "tenant-demo", "sample-db", target); err == nil {
					t.Fatal("explicit in-place expansion must reject changing storage class")
				}
			}
		})
	}
}
