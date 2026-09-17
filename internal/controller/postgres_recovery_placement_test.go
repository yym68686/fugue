package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestRecoverySelectsReadyStorageNodeWithinRuntime(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/storageclasses/"):
			json.NewEncoder(w).Encode(map[string]any{"provisioner": "driver.longhorn.io"})
		case r.URL.Path == "/apis/longhorn.io/v1beta2/nodes":
			var items []any
			for _, name := range []string{"foreign", "unready", "eligible"} {
				ready := "True"
				if name == "unready" {
					ready = "False"
				}
				items = append(items, map[string]any{"metadata": map[string]string{"name": name}, "status": map[string]any{"conditions": []any{map[string]string{"type": "Ready", "status": ready}}}})
			}
			json.NewEncoder(w).Encode(map[string]any{"items": items})
		case strings.HasPrefix(r.URL.Path, "/api/v1/nodes/"):
			pool := "internal"
			if strings.HasSuffix(r.URL.Path, "/foreign") {
				pool = "private"
			}
			json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"labels": map[string]string{"fugue.io/shared-pool": pool}}, "status": map[string]any{"conditions": []any{map[string]string{"type": "Ready", "status": "True"}}}})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := &kubeClient{baseURL: server.URL, client: server.Client()}
	rt := model.Runtime{ID: "shared", Type: model.RuntimeTypeManagedShared}
	got, err := recoveryStorageTargetNode(context.Background(), client, rt, "network-storage", "")
	if err != nil || got != "eligible" {
		t.Fatalf("selection=%q error=%v", got, err)
	}
	for _, requested := range []string{"csi-only", "foreign", "unready"} {
		if _, err := recoveryStorageTargetNode(context.Background(), client, rt, "network-storage", requested); err == nil {
			t.Fatalf("accepted unsupported requested node %s", requested)
		}
	}
}

func TestRecoveryReschedulesOnlyUnstartedJoinAndPreservesPVC(t *testing.T) {
	for _, scenario := range []string{"unstarted", "started", "primary", "foreign-owner", "correct-node"} {
		t.Run(scenario, func(t *testing.T) {
			deleted := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/clusters/db"):
					json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]string{"uid": "cluster-id"}, "spec": map[string]any{"storage": map[string]string{"storageClass": "network-storage"}}, "status": map[string]any{"currentPrimary": "db-1", "readyInstances": 1}})
				case strings.HasSuffix(r.URL.Path, "/pods"):
					claim, node := "db-2", "unsupported"
					if scenario == "primary" {
						claim = "db-1"
					}
					if scenario == "correct-node" {
						node = "eligible"
					}
					status := map[string]any{"phase": "Pending"}
					if scenario == "started" {
						status["initContainerStatuses"] = []any{map[string]any{"state": map[string]any{"terminated": map[string]any{"exitCode": 0}}}}
					}
					json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"metadata": map[string]any{"name": "db-2-join-pod", "ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "db-2-join", "uid": "job-id", "controller": true}}}, "spec": map[string]any{"nodeName": node, "volumes": []any{map[string]any{"name": "pgdata", "persistentVolumeClaim": map[string]string{"claimName": claim}}}}, "status": status}}})
				case strings.Contains(r.URL.Path, "/persistentvolumeclaims/"):
					if r.Method != http.MethodGet {
						t.Fatal("recovery deleted a data claim")
					}
					json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"labels": map[string]string{"cnpg.io/cluster": "db", "cnpg.io/pvcRole": "PG_DATA"}}, "spec": map[string]string{"storageClassName": "network-storage", "volumeName": "retained-pv"}})
				case strings.HasSuffix(r.URL.Path, "/jobs/db-2-join") && r.Method == http.MethodGet:
					owner := "cluster-id"
					if scenario == "foreign-owner" {
						owner = "other"
					}
					json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": "job-id", "resourceVersion": "42", "ownerReferences": []any{map[string]any{"apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster", "uid": owner, "controller": true}}}})
				case strings.HasSuffix(r.URL.Path, "/jobs/db-2-join") && r.Method == http.MethodDelete:
					var body struct {
						Preconditions map[string]string `json:"preconditions"`
					}
					json.NewDecoder(r.Body).Decode(&body)
					if body.Preconditions["uid"] != "job-id" || body.Preconditions["resourceVersion"] != "42" {
						t.Errorf("missing identity preconditions: %+v", body)
					}
					deleted = true
					json.NewEncoder(w).Encode(map[string]any{})
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			err := rescheduleUnstartedRecoveryJoins(context.Background(), &kubeClient{baseURL: server.URL, client: server.Client()}, "tenant", "db", "eligible", managedPostgresStorageTarget{StorageClassName: "network-storage"})
			if err != nil || deleted != (scenario == "unstarted") {
				t.Fatalf("deleted=%v error=%v", deleted, err)
			}
		})
	}
}

func TestRecoverySourceResizeDoesNotWaitForUnstartedDestination(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims"):
			json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"metadata": map[string]string{"name": "db-1"}}, map[string]any{"metadata": map[string]string{"name": "db-2"}}}})
		case strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims/db-1"):
			json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]string{"name": "db-1"}, "spec": map[string]any{"resources": map[string]any{"requests": map[string]string{"storage": "22Gi"}}}, "status": map[string]any{"capacity": map[string]string{"storage": "22Gi"}}})
		case strings.HasSuffix(r.URL.Path, "/pods"):
			json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"metadata": map[string]string{"name": "db-1"}, "spec": map[string]any{"nodeName": "source", "volumes": []any{map[string]any{"persistentVolumeClaim": map[string]string{"claimName": "db-1"}}}}}, map[string]any{"metadata": map[string]string{"name": "db-2-join"}, "spec": map[string]any{"volumes": []any{map[string]any{"persistentVolumeClaim": map[string]string{"claimName": "db-2"}}}}}}})
		case strings.HasSuffix(r.URL.Path, "/stats/summary"):
			json.NewEncoder(w).Encode(map[string]any{"pods": []any{map[string]any{"podRef": map[string]string{"name": "db-1", "namespace": "tenant"}, "volume": []any{map[string]any{"pvcRef": map[string]string{"name": "db-1", "namespace": "tenant"}, "capacityBytes": int64(22 << 30)}}}}})
		default:
			t.Errorf("unexpected %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	converged, detail, err := inspectManagedPostgresStorageExpansion(context.Background(), &kubeClient{baseURL: server.URL, client: server.Client()}, "tenant", "db", managedPostgresStorageTarget{StorageSize: "22Gi"}, "db-1")
	if err != nil || !converged {
		t.Fatalf("source blocked by destination: %v %s %v", converged, detail, err)
	}
}

func TestRecoveryPreservesUnspecifiedCNPGResizePolicy(t *testing.T) {
	current := map[string]any{"spec": map[string]any{"storage": map[string]any{"resizeInUseVolumes": false}}}
	desired := map[string]any{"apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster", "spec": map[string]any{"storage": map[string]any{"size": "40Gi"}}}
	preserveCloudNativePGResizePolicy(current, desired)
	storage := desired["spec"].(map[string]any)["storage"].(map[string]any)
	if storage["resizeInUseVolumes"] != false {
		t.Fatal("spec replacement unfreezes source expansion")
	}
	storage["resizeInUseVolumes"] = true
	preserveCloudNativePGResizePolicy(current, desired)
	if storage["resizeInUseVolumes"] != true {
		t.Fatal("explicit configuration was overwritten")
	}
}

func TestRecoveryRetainsOnlyAbandonedInitializingReplicaClaims(t *testing.T) {
	for _, scenario := range []string{"abandoned", "primary", "target-primary", "ready", "pod-used", "job-used", "identity-changed"} {
		t.Run(scenario, func(t *testing.T) {
			patched, reads := false, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				volumes := []any{map[string]any{"persistentVolumeClaim": map[string]string{"claimName": "db-2"}}}
				switch {
				case strings.HasSuffix(r.URL.Path, "/clusters/db"):
					primary, target := "db-1", "db-1"
					if scenario == "primary" {
						primary = "db-2"
					}
					if scenario == "target-primary" {
						target = "db-2"
					}
					json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]string{"uid": "cluster-id"}, "spec": map[string]any{"storage": map[string]string{"storageClass": "network-storage"}}, "status": map[string]any{"currentPrimary": primary, "targetPrimary": target, "readyInstances": 1}})
				case strings.HasSuffix(r.URL.Path, "/pods"):
					items := []any{}
					if scenario == "pod-used" {
						items = append(items, map[string]any{"spec": map[string]any{"volumes": volumes}})
					}
					json.NewEncoder(w).Encode(map[string]any{"items": items})
				case strings.HasSuffix(r.URL.Path, "/jobs"):
					items := []any{}
					if scenario == "job-used" {
						items = append(items, map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"volumes": volumes}}}})
					}
					json.NewEncoder(w).Encode(map[string]any{"items": items})
				case strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims"):
					json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"metadata": map[string]string{"name": "db-2"}}}})
				case strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims/db-2") && r.Method == http.MethodGet:
					reads++
					status, uid := "initializing", "claim-id"
					if scenario == "ready" {
						status = "ready"
					}
					if scenario == "identity-changed" && reads > 1 {
						uid = "replacement-id"
					}
					json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "db-2", "uid": uid, "resourceVersion": "42", "labels": map[string]string{"cnpg.io/cluster": "db", "cnpg.io/instanceRole": "replica", "cnpg.io/pvcRole": "PG_DATA"}, "annotations": map[string]string{"cnpg.io/pvcStatus": status}, "ownerReferences": []any{map[string]string{"uid": "cluster-id", "kind": "Cluster"}}}, "spec": map[string]string{"storageClassName": "network-storage", "volumeName": "retained-pv"}})
				case r.Method == http.MethodPatch:
					var body map[string]any
					json.NewDecoder(r.Body).Decode(&body)
					m := body["metadata"].(map[string]any)
					if m["uid"] != "claim-id" || m["resourceVersion"] != "42" || len(m["ownerReferences"].([]any)) != 0 {
						t.Errorf("unsafe retention patch: %v", body)
					}
					if _, ok := body["spec"]; ok {
						t.Fatal("retention must not change the bound volume")
					}
					patched = true
					json.NewEncoder(w).Encode(map[string]any{})
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			err := retainAbandonedRecoveryClaims(context.Background(), &kubeClient{baseURL: server.URL, client: server.Client()}, "tenant", "db", managedPostgresStorageTarget{StorageClassName: "network-storage"})
			if err != nil || patched != (scenario == "abandoned") {
				t.Fatalf("patched=%v error=%v", patched, err)
			}
		})
	}
}
