package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMigrationStoragePreservesCompatibleClassAndSelectsApprovedFallback(t *testing.T) {
	for _, scenario := range []string{"preserve", "fallback", "unapproved", "destination-not-ready", "unknown-capacity", "observation-failed"} {
		t.Run(scenario, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Fatalf("preflight performed mutation: %s", r.Method)
				}
				var result any
				switch r.URL.Path {
				case "/api/v1/nodes/destination":
					result = map[string]any{"metadata": map[string]any{"name": "destination", "labels": map[string]string{"kubernetes.io/hostname": "destination"}}}
				case "/apis/storage.k8s.io/v1/storageclasses":
					approved := "true"
					if scenario == "unapproved" {
						approved = "false"
					}
					result = map[string]any{"items": []any{map[string]any{"metadata": map[string]any{"name": "source-local"}, "provisioner": "local.csi.example"}, map[string]any{"metadata": map[string]any{"name": "replicated", "labels": map[string]string{"fugue.pro/managed-postgres": approved}}, "provisioner": longhornCSIDriver, "allowVolumeExpansion": true}}}
				case "/apis/storage.k8s.io/v1/csinodes":
					result = map[string]any{"items": []any{map[string]any{"metadata": map[string]string{"name": "destination"}, "spec": map[string]any{"drivers": []any{map[string]string{"name": "local.csi.example"}, map[string]string{"name": longhornCSIDriver}}}}}}
				case "/apis/storage.k8s.io/v1/csistoragecapacities":
					if scenario == "observation-failed" {
						http.Error(w, "unavailable", 503)
						return
					}
					capacity := "0"
					if scenario == "preserve" {
						capacity = "10Gi"
					}
					items := []any{}
					if scenario != "unknown-capacity" {
						items = append(items, map[string]any{"storageClassName": "source-local", "capacity": capacity, "nodeTopology": map[string]any{"matchLabels": map[string]string{"kubernetes.io/hostname": "destination"}}})
					}
					result = map[string]any{"items": items}
				case "/apis/longhorn.io/v1beta2/nodes":
					ready := "True"
					if scenario == "destination-not-ready" {
						ready = "False"
					}
					result = map[string]any{"items": []any{map[string]any{"metadata": map[string]string{"name": "destination"}, "status": map[string]any{"conditions": []any{map[string]string{"type": "Ready", "status": ready}}}}}}
				default:
					t.Fatalf("unexpected request: %s", r.URL.Path)
				}
				json.NewEncoder(w).Encode(result)
			}))
			defer srv.Close()
			class, err := migrationStorageClass(context.Background(), &kubeClient{baseURL: srv.URL, client: srv.Client()}, "destination", "source-local", "5Gi", true)
			if scenario == "unapproved" || scenario == "destination-not-ready" || scenario == "observation-failed" {
				if err == nil {
					t.Fatalf("unsafe fallback accepted: %s", class)
				}
			} else {
				want := "source-local"
				if scenario == "fallback" {
					want = "replicated"
				}
				if err != nil || class != want {
					t.Fatalf("class=%q want=%q err=%v", class, want, err)
				}
			}
		})
	}
}

func TestUnstartedMigrationReplicaExcludesAnyServingInstance(t *testing.T) {
	var cluster kubeCloudNativePGCluster
	json.Unmarshal([]byte(`{"metadata":{"uid":"cluster"},"status":{"currentPrimary":"db-2","targetPrimary":"db-2"}}`), &cluster)
	fixture := `{"metadata":{"name":"db-1","uid":"pod","ownerReferences":[{"apiVersion":"postgresql.cnpg.io/v1","kind":"Cluster","uid":"cluster"}]},"spec":{},"status":{"phase":"Pending"}}`
	for _, scenario := range []string{"unstarted", "primary", "assigned", "started", "foreign"} {
		t.Run(scenario, func(t *testing.T) {
			var pod kubePod
			json.Unmarshal([]byte(fixture), &pod)
			switch scenario {
			case "primary":
				pod.Metadata.Name = "db-2"
			case "assigned":
				pod.Spec.NodeName = "source"
			case "started":
				json.Unmarshal([]byte(strings.Replace(fixture, `"phase":"Pending"`, `"phase":"Pending","containerStatuses":[{"name":"postgres","state":{"waiting":{}}}]`, 1)), &pod)
			case "foreign":
				pod.ObservedOwnerReferences = nil
			}
			if got := unstartedMigrationReplica(pod, cluster); got != (scenario == "unstarted") {
				t.Fatalf("eligible=%v", got)
			}
		})
	}
}
