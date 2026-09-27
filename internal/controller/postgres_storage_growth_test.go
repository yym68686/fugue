package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
	"github.com/jackc/pgx/v5"
)

type storageSQLRow struct{ started string }

func (r storageSQLRow) Scan(dest ...any) error {
	if len(dest) != 4 {
		return fmt.Errorf("unexpected query destinations")
	}
	*dest[0].(*bool) = false
	*dest[1].(*string) = "off"
	*dest[2].(*string) = "192.0.2.10"
	*dest[3].(*string) = r.started
	return nil
}

func TestStorageGrowthOnlyMutatesCapacityAndPreservesLegacyAffinity(t *testing.T) {
	for _, scenario := range []string{"growth", "partial-retry", "pod-replaced", "postmaster-restarted", "sql-unavailable", "cluster-conflict", "affinity-changed", "late-replacement"} {
		t.Run(scenario, func(t *testing.T) {
			state := store.New(filepath.Join(t.TempDir(), "state.json"))
			if err := state.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, err := state.CreateTenant("Storage test")
			if err != nil {
				t.Fatal(err)
			}
			project, err := state.CreateProject(tenant.ID, "sample", "")
			if err != nil {
				t.Fatal(err)
			}
			pg := &model.AppPostgresSpec{RuntimeID: model.DefaultManagedRuntimeID, StorageClassName: "expandable", StorageSize: "5Gi", PrimaryNodeName: "worker", Instances: 1, Database: "sample", User: "sample", Password: "test-password-123", ServiceName: "sample-pg"}
			app, err := state.CreateApp(tenant.ID, project.ID, "sample", "", model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Image: "example/app:v1", Replicas: 1, Postgres: pg})
			if err != nil {
				t.Fatal(err)
			}
			desired := app.Spec
			desired.Postgres = model.CloneAppPostgresSpec(store.OwnedManagedPostgresSpec(app))
			desired.Postgres.StorageSize = "10Gi"
			op, err := state.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDatabaseLocalize, TargetRuntimeID: model.DefaultManagedRuntimeID, DesiredSpec: &desired})
			if err != nil {
				t.Fatal(err)
			}
			ns := runtimepkg.NamespaceForTenant(tenant.ID)
			clusterName := desired.Postgres.ServiceName
			podName := clusterName + "-1"
			affinity := map[string]any{"nodeAffinity": map[string]any{"requiredDuringSchedulingIgnoredDuringExecution": map[string]any{"nodeSelectorTerms": []any{map[string]any{"matchExpressions": []any{
				map[string]any{"key": "fugue.io/runtime-id", "operator": "In", "values": []string{model.DefaultManagedRuntimeID}},
				map[string]any{"key": "fugue.io/tenant-id", "operator": "In", "values": []string{tenant.ID}},
			}}}}}}
			cluster := map[string]any{"metadata": map[string]any{"name": clusterName, "uid": "cluster-uid", "resourceVersion": "1"}, "spec": map[string]any{"instances": 1, "affinity": affinity, "storage": map[string]any{"size": "5Gi", "storageClass": "expandable", "resizeInUseVolumes": true}}, "status": map[string]any{"currentPrimary": podName, "targetPrimary": podName, "readyInstances": 1}}
			pod := map[string]any{"metadata": map[string]any{"name": podName, "namespace": ns, "uid": "pod-original", "resourceVersion": "1"}, "spec": map[string]any{"nodeName": "worker", "affinity": affinity, "containers": []any{map[string]any{"name": "postgres"}}, "volumes": []any{map[string]any{"name": "pgdata", "persistentVolumeClaim": map[string]any{"claimName": podName}}}}, "status": map[string]any{"phase": "Running", "podIP": "192.0.2.10", "conditions": []any{map[string]any{"type": "Ready", "status": "True"}}, "containerStatuses": []any{map[string]any{"name": "postgres", "ready": true, "restartCount": 2, "state": map[string]any{"running": map[string]any{"startedAt": "2026-01-01T00:00:00Z"}}}}}}
			pvcSize, clusterPatches, pvcPatches := "5Gi", 0, 0
			if scenario == "partial-retry" {
				pvcSize = "10Gi"
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				write := func(v any) { _ = json.NewEncoder(w).Encode(v) }
				switch {
				case r.URL.Path == "/apis/postgresql.cnpg.io/v1/namespaces/"+ns+"/clusters/"+clusterName:
					if r.Method != http.MethodGet && r.Method != http.MethodPatch {
						t.Errorf("forbidden cluster mutation: %s", r.Method)
						http.Error(w, "forbidden", 500)
						return
					}
					if r.Method == http.MethodPatch {
						clusterPatches++
						if r.Header.Get("Content-Type") != "application/json-patch+json" {
							t.Error("not a guarded JSON patch")
						}
						var patch []map[string]any
						if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
							t.Error(err)
						}
						if len(patch) != 4 || patch[0]["path"] != "/metadata/uid" || patch[1]["path"] != "/metadata/resourceVersion" || patch[2]["path"] != "/spec/storage/size" || patch[3]["path"] != "/spec/storage/size" || patch[3]["op"] != "replace" {
							t.Errorf("unexpected patch: %+v", patch)
						}
						if scenario == "cluster-conflict" {
							http.Error(w, "conflict", 409)
							return
						}
						cluster["spec"].(map[string]any)["storage"].(map[string]any)["size"] = "10Gi"
						if scenario == "pod-replaced" {
							pod["metadata"].(map[string]any)["uid"] = "replacement"
						}
						if scenario == "affinity-changed" {
							cluster["spec"].(map[string]any)["affinity"] = map[string]any{"unexpected": "drift"}
						}
					}
					write(cluster)
				case r.URL.Path == "/api/v1/namespaces/"+ns+"/persistentvolumeclaims":
					write(map[string]any{"items": []any{map[string]any{"metadata": map[string]any{"name": podName}}}})
				case r.URL.Path == "/api/v1/namespaces/"+ns+"/persistentvolumeclaims/"+podName:
					if r.Method != http.MethodGet && r.Method != http.MethodPatch {
						t.Errorf("forbidden PVC mutation: %s", r.Method)
						http.Error(w, "forbidden", 500)
						return
					}
					if r.Method == http.MethodPatch {
						pvcPatches++
						pvcSize = "10Gi"
					}
					write(map[string]any{"metadata": map[string]any{"name": podName}, "spec": map[string]any{"storageClassName": "expandable", "resources": map[string]any{"requests": map[string]string{"storage": pvcSize}}}, "status": map[string]any{"capacity": map[string]string{"storage": pvcSize}}})
				case r.Method == http.MethodGet && r.URL.Path == "/apis/storage.k8s.io/v1/storageclasses/expandable":
					write(map[string]any{"allowVolumeExpansion": true})
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+ns+"/pods/"+podName:
					write(pod)
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/"+ns+"/pods":
					write(map[string]any{"items": []any{pod}})
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes/worker":
					write(map[string]any{"metadata": map[string]any{"name": "worker", "labels": map[string]string{"fugue.io/shared-pool": "internal"}}})
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes/worker/proxy/stats/summary":
					if scenario == "late-replacement" {
						pod["metadata"].(map[string]any)["uid"] = "replacement"
					}
					write(map[string]any{"pods": []any{map[string]any{"podRef": map[string]any{"name": podName, "namespace": ns}, "volume": []any{map[string]any{"pvcRef": map[string]any{"name": podName, "namespace": ns}, "capacityBytes": int64(10 << 30)}}}}})
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					http.Error(w, "unexpected", 500)
				}
			}))
			defer server.Close()
			client := &kubeClient{client: server.Client(), baseURL: server.URL, namespace: ns}
			svc := &Service{Store: state, Config: config.ControllerConfig{KubectlApply: true, ManagedAppRolloutTimeout: time.Second}, Logger: log.New(io.Discard, "", 0), newKubeClient: func(string) (*kubeClient, error) { return client, nil }, postgresPrimarySQLConnect: func(context.Context, string) (managedPostgresPrimarySQLConnection, error) {
				if scenario == "sql-unavailable" {
					return nil, fmt.Errorf("connection refused")
				}
				started := "2026-01-01 00:00:00+00"
				if scenario == "postmaster-restarted" && clusterPatches > 0 {
					started = "2026-01-02 00:00:00+00"
				}
				return &fakeManagedPostgresPrimarySQLConnection{row: storageSQLRow{started: started}}, nil
			}}
			err = svc.executeManagedDatabaseLocalizeOperation(context.Background(), op, app)
			success := scenario == "growth" || scenario == "partial-retry"
			if (err == nil) != success {
				t.Fatalf("success=%t err=%v", success, err)
			}
			if scenario == "sql-unavailable" && (pvcPatches != 0 || clusterPatches != 0) {
				t.Fatal("mutation before healthy baseline")
			}
			if scenario == "partial-retry" && pvcPatches != 0 {
				t.Fatal("already enlarged PVC was patched again")
			}
			if scenario != "affinity-changed" && !reflect.DeepEqual(cluster["spec"].(map[string]any)["affinity"], affinity) {
				t.Fatal("affinity changed")
			}
			if success {
				completed, err := state.GetOperation(op.ID)
				if err != nil || completed.Status != model.OperationStatusCompleted {
					t.Fatalf("completion=%+v err=%v", completed, err)
				}
				updated, getErr := state.GetApp(app.ID)
				if getErr != nil {
					t.Fatal(getErr)
				}
				actual := store.OwnedManagedPostgresSpec(updated)
				if actual == nil || actual.StorageSize != "10Gi" || actual.PrimaryNodeName != "worker" {
					t.Fatalf("lost size/pin: %+v", actual)
				}
			}
			if scenario == "pod-replaced" || scenario == "postmaster-restarted" {
				persisted, baselineErr := svc.storageGrowthBaseline(op, app, ns, postgresStorageWitness{PodUID: "new", ClusterUID: "new"})
				if baselineErr != nil {
					t.Fatal(baselineErr)
				}
				if persisted.PodUID != "pod-original" {
					t.Fatal("retry replaced original continuity evidence")
				}
				if !strings.Contains(err.Error(), "continuity") {
					t.Fatalf("missing continuity error: %v", err)
				}
			}
		})
	}
}

var _ pgx.Row = storageSQLRow{}

func TestStorageGrowthSQLWitnessWithLocalPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("requires disposable loopback PostgreSQL")
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Hostname() != "127.0.0.1" || !strings.Contains(parsed.Path, "fugue_test") {
		t.Fatal("requires disposable loopback fugue_test database")
	}
	readOnly := false
	svc := &Service{postgresPrimarySQLConnect: func(ctx context.Context, _ string) (managedPostgresPrimarySQLConnection, error) {
		conn, err := pgx.Connect(ctx, address)
		if err != nil {
			return nil, err
		}
		if readOnly {
			if _, err = conn.Exec(ctx, "SET default_transaction_read_only=on"); err != nil {
				conn.Close(ctx)
				return nil, err
			}
		}
		return conn, nil
	}}
	first, err := svc.storageGrowthSQLWitness(context.Background(), "ignored", "127.0.0.1", model.AppPostgresSpec{})
	if err != nil || first == "" {
		t.Fatalf("SQL witness %q: %v", first, err)
	}
	second, err := svc.storageGrowthSQLWitness(context.Background(), "ignored", "127.0.0.1", model.AppPostgresSpec{})
	if err != nil || second != first {
		t.Fatalf("unstable postmaster witness %q %q: %v", first, second, err)
	}
	readOnly = true
	if _, err = svc.storageGrowthSQLWitness(context.Background(), "ignored", "127.0.0.1", model.AppPostgresSpec{}); err == nil || !strings.Contains(err.Error(), "transaction_read_only") {
		t.Fatalf("read-only primary accepted: %v", err)
	}
}
