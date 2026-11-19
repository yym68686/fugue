package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/pgretire"
	"fugue/internal/runtime"
	"fugue/internal/store"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
)

type replicaRetirementSQLRow struct{ started, sync string }

func (r replicaRetirementSQLRow) Scan(dest ...any) error {
	if len(dest) != 5 {
		return fmt.Errorf("expected PostgreSQL process and synchronous replication witnesses")
	}
	if err := (storageSQLRow{started: r.started}).Scan(dest[:4]...); err != nil {
		return err
	}
	*dest[4].(*string) = r.sync
	return nil
}

func TestManagedPostgresReplicaRetirement(t *testing.T) {
	for _, scenario := range []string{"sync-then-scale", "stale-replica", "volume-pending-resumes", "retained-volume", "primary-replaced", "postmaster-restarted"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			db := store.New(filepath.Join(t.TempDir(), "state.json"))
			if err := db.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, _ := db.CreateTenant("Retirement test")
			project, _ := db.CreateProject(tenant.ID, "fixture", "")
			rt, _, err := db.CreateRuntime(tenant.ID, "primary-runtime", model.RuntimeTypeManagedOwned, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			pg := &model.AppPostgresSpec{ServiceName: "fixture-postgres", RuntimeID: rt.ID, PrimaryNodeName: "node-main", Database: "fixture", User: "fixture", Password: "test-secret", StorageSize: "1Gi", Instances: 2}
			if scenario == "sync-then-scale" {
				pg.SynchronousReplicas = 1
			}
			app, err := db.CreateApp(tenant.ID, project.ID, "fixture", "", model.AppSpec{RuntimeID: rt.ID, Image: "example.invalid/fixture:2", Replicas: 1, Postgres: pg})
			if err != nil || len(app.BackingServices) != 1 {
				t.Fatalf("create app: %v, services=%d", err, len(app.BackingServices))
			}
			service := app.BackingServices[0]
			pg = service.Spec.Postgres
			ns, name := runtime.NamespaceForTenant(app.TenantID), pg.ServiceName
			p1, p2 := name+"-1", name+"-2"
			req := model.AppPostgresSpec{RetireReplicaNode: "node-standby", RetireReplicaPodUID: "pod-2"}
			if scenario == "stale-replica" {
				req.RetireReplicaPodUID = "old-uid"
			}
			op, err := db.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, ServiceID: service.ID,
				Type: model.OperationTypeDatabaseReplicaRetire, DesiredSpec: &model.AppSpec{Postgres: &req}})
			if err != nil {
				t.Fatalf("queue retirement: %v", err)
			}
			op, claimed, err := db.TryClaimPendingOperation(op.ID)
			if err != nil || !claimed {
				t.Fatalf("claim retirement: %v, claimed=%v", err, claimed)
			}
			owner := []any{map[string]any{"uid": "cluster-uid", "apiVersion": runtime.CloudNativePGAPIVersion, "kind": "Cluster"}}
			healthy := []any{map[string]any{"type": "Ready", "status": "True"}}
			affinity := map[string]any{"enablePodAntiAffinity": true, "topologyKey": "kubernetes.io/hostname"}
			cluster := map[string]any{
				"metadata": map[string]any{"uid": "cluster-uid", "resourceVersion": "1", "name": name, "annotations": map[string]any{"example.invalid/keep": "unchanged"},
					"labels": map[string]any{runtime.FugueLabelBackingServiceID: service.ID, runtime.FugueLabelOwnerAppID: app.ID, runtime.FugueLabelProjectID: project.ID, runtime.FugueLabelTenantID: tenant.ID}},
				"spec":   map[string]any{"instances": 2, "minSyncReplicas": pg.SynchronousReplicas, "maxSyncReplicas": pg.SynchronousReplicas, "affinity": affinity, "imageName": "example.invalid/postgres:stable", "storage": map[string]any{"size": "1Gi"}},
				"status": map[string]any{"instances": 2, "readyInstances": 2, "instanceNames": []any{p1, p2}, "phase": "Cluster in healthy state", "currentPrimary": p1, "targetPrimary": p1},
			}
			pod := func(n, uid, node, role, ip, serial string) map[string]any {
				return map[string]any{
					"metadata": map[string]any{"uid": uid, "resourceVersion": "1", "name": n, "ownerReferences": owner,
						"annotations": map[string]any{"cnpg.io/nodeSerial": serial}, "labels": map[string]any{"cnpg.io/instanceRole": role}},
					"spec": map[string]any{"nodeName": node, "volumes": []any{map[string]any{"name": "pgdata", "persistentVolumeClaim": map[string]any{"claimName": n}}}},
					"status": map[string]any{"phase": "Running", "podIP": ip, "conditions": healthy, "containerStatuses": []any{map[string]any{
						"name": "postgres", "ready": true, "restartCount": 0, "state": map[string]any{"running": map[string]any{"startedAt": "2026-01-01T00:00:00Z"}}}}},
				}
			}
			primary := pod(p1, "pod-1", "node-main", "primary", "192.0.2.10", "1")
			standby := pod(p2, "pod-2", "node-standby", "replica", "192.0.2.11", "2")
			pvc := func(n, id string) any {
				return map[string]any{"metadata": map[string]any{"name": n, "resourceVersion": "1", "uid": "claim-" + id, "ownerReferences": owner},
					"spec": map[string]any{"volumeName": "pv-" + id}, "status": map[string]any{"phase": "Bound"}}
			}
			pv := func(n, id, node, policy, phase string) any {
				terms := []any{map[string]any{"matchExpressions": []any{map[string]any{"key": kubeHostnameLabelKey, "operator": "In", "values": []any{node}}}}}
				return map[string]any{"metadata": map[string]any{"name": "pv-" + id, "resourceVersion": "1", "uid": "volume-" + id},
					"spec": map[string]any{"nodeAffinity": map[string]any{"required": map[string]any{"nodeSelectorTerms": terms}}, "persistentVolumeReclaimPolicy": policy,
						"claimRef": map[string]any{"namespace": ns, "name": n, "uid": "claim-" + id}}, "status": map[string]any{"phase": phase}}
			}
			clusterPath := cloudNativePGClusterAPIPath(ns, name)
			var mutations, sqlProbes atomic.Int32
			var scaled, allowReclaim atomic.Bool
			if scenario != "volume-pending-resumes" {
				allowReclaim.Store(true)
			}
			policy := "Delete"
			if scenario == "retained-volume" {
				policy = "Retain"
			}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodPatch && r.URL.Path == clusterPath {
					count := mutations.Add(1)
					proofs, e := db.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: tenant.ID, OperationID: op.ID, Types: []string{replicaRetirementIntent}})
					if e != nil || len(proofs) != 1 {
						t.Error("patch was not preceded by durable intent")
					}
					payload, _ := io.ReadAll(r.Body)
					var changes []map[string]any
					_ = json.Unmarshal(payload, &changes)
					if len(changes) < 5 || changes[0]["path"] != "/metadata/uid" || changes[1]["path"] != "/metadata/resourceVersion" || changes[2]["op"] != "test" {
						t.Errorf("missing CAS: %s", payload)
					}
					for _, change := range changes {
						switch change["path"] {
						case "/metadata/uid", "/metadata/resourceVersion", "/metadata/annotations", "/spec/instances", "/spec/minSyncReplicas", "/spec/maxSyncReplicas":
						default:
							t.Errorf("patch touched unrelated field: %v", change)
						}
					}
					patch, err := jsonpatch.DecodePatch(payload)
					original, _ := json.Marshal(cluster)
					next, err2 := patch.Apply(original)
					if err != nil || err2 != nil || json.Unmarshal(next, &cluster) != nil {
						t.Errorf("invalid patch: %v %v", err, err2)
						http.Error(w, "invalid patch", 500)
						return
					}
					pgretire.Meta(cluster)["resourceVersion"] = fmt.Sprint(1 + count)
					spec := pgretire.Object(cluster["spec"])
					if spec["instances"] == float64(1) {
						if scenario == "sync-then-scale" && (count != 2 || sqlProbes.Load() < 3 || spec["maxSyncReplicas"] != float64(0)) {
							t.Error("standby removed before primary observed sync disable")
						}
						scaled.Store(true)
						cluster["status"] = map[string]any{"instances": 1, "readyInstances": 1, "instanceNames": []any{p1}, "phase": "Cluster in healthy state", "currentPrimary": p1, "targetPrimary": p1}
						if scenario == "primary-replaced" {
							pgretire.Meta(primary)["uid"] = "replacement-primary"
						}
					}
					w.WriteHeader(http.StatusOK)
					return
				}
				if r.Method != http.MethodGet {
					t.Errorf("unrelated mutation %s %s", r.Method, r.URL.Path)
					http.Error(w, "forbidden", 500)
					return
				}
				var object any
				switch r.URL.Path {
				case clusterPath:
					object = cluster
				case "/api/v1/nodes/node-main":
					object = map[string]any{"metadata": map[string]any{"name": "node-main", "labels": map[string]any{runtime.RuntimeIDLabelKey: rt.ID}}}
				case pgretire.Path(ns, "pods", ""):
					pods := []any{primary}
					if !scaled.Load() {
						pods = append(pods, standby)
					}
					object = map[string]any{"items": pods}
				case pgretire.Path(ns, "pods", p1):
					object = primary
				case pgretire.Path(ns, "pods", p2):
					if !scaled.Load() {
						object = standby
					}
				case pgretire.Path(ns, "persistentvolumeclaims", p1):
					object = pvc(p1, "1")
				case pgretire.Path(ns, "persistentvolumeclaims", p2):
					if !scaled.Load() {
						object = pvc(p2, "2")
					}
				case "/api/v1/persistentvolumes/pv-1":
					object = pv(p1, "1", "node-main", "Delete", "Bound")
				case "/api/v1/persistentvolumes/pv-2":
					if !scaled.Load() || !allowReclaim.Load() {
						object = pv(p2, "2", "node-standby", policy, "Bound")
					} else if policy == "Retain" {
						object = pv(p2, "2", "node-standby", policy, "Released")
					}
				case pgretire.Path(ns, "endpoints", model.PostgresEndpointName(*pg)):
					object = map[string]any{"subsets": []any{map[string]any{"addresses": []any{map[string]any{"ip": "192.0.2.10"}}}}}
				default:
					t.Errorf("unanticipated GET %s", r.URL)
				}
				if object == nil {
					http.NotFound(w, r)
				} else {
					_ = json.NewEncoder(w).Encode(object)
				}
			}))
			defer api.Close()
			kube := &kubeClient{client: api.Client(), baseURL: api.URL, namespace: ns}
			s := &Service{Store: db, Config: config.ControllerConfig{KubectlApply: true}, Logger: log.New(io.Discard, "", 0), newKubeClient: func(string) (*kubeClient, error) { return kube, nil },
				postgresPrimarySQLConnect: func(_ context.Context, connectionURL string) (managedPostgresPrimarySQLConnection, error) {
					if !strings.Contains(connectionURL, model.PostgresEndpointName(*pg)+".") {
						t.Error("SQL did not probe the configured serving endpoint")
					}
					probe := sqlProbes.Add(1)
					start, syncStandbys := "original", ""
					if scenario == "sync-then-scale" && probe < 3 {
						syncStandbys = "ANY 1 (standby)"
					}
					if scenario == "postmaster-restarted" && scaled.Load() {
						start = "restarted"
					}
					return &fakeManagedPostgresPrimarySQLConnection{row: replicaRetirementSQLRow{started: start, sync: syncStandbys}}, nil
				}}
			run := func(ctx context.Context) error {
				return s.executePostgresReplicaRetirement(ctx, op, app)
			}
			if scenario == "volume-pending-resumes" {
				ctx, cancel := context.WithTimeout(context.Background(), 3500*time.Millisecond)
				err = run(ctx)
				cancel()
				current, _ := db.GetOperation(op.ID)
				if !errors.Is(err, errPostgresReplicaRetirementUnsettled) || current.Status != model.OperationStatusRunning || mutations.Load() != 1 {
					t.Fatalf("operation must wait for standby storage: %s, mutations=%d, %v", current.Status, mutations.Load(), err)
				}
				allowReclaim.Store(true)
			}
			err = run(context.Background())
			after, _ := db.GetBackingService(service.ID)
			current, _ := db.GetOperation(op.ID)
			switch scenario {
			case "stale-replica":
				if err == nil || errors.Is(err, errPostgresReplicaRetirementUnsettled) || mutations.Load() != 0 {
					t.Fatalf("stale UID unexpectedly mutated or became unsettled: %v", err)
				}
			case "primary-replaced", "postmaster-restarted":
				if !errors.Is(err, errPostgresReplicaRetirementUnsettled) || after.Spec.Postgres.Instances != 2 || current.Status != model.OperationStatusRunning {
					t.Fatalf("primary continuity failure was hidden: %v, %s", err, current.Status)
				}
			default:
				if err != nil || after.Spec.Postgres.Instances != 1 || after.Spec.Postgres.SynchronousReplicas != 0 || current.Status != model.OperationStatusCompleted {
					t.Fatalf("expected committed retirement: err=%v status=%s db=%+v", err, current.Status, after.Spec.Postgres)
				}
				want := int32(1)
				if scenario == "sync-then-scale" {
					want = 2
				}
				if mutations.Load() != want {
					t.Fatalf("unexpected additional mutation: %d", mutations.Load())
				}
				rendered := runtime.PostgresAffinity(*after.Spec.Postgres, []runtime.SchedulingConstraints{runtime.SchedulingForRuntime(rt)})
				desired := map[string]any{"apiVersion": runtime.CloudNativePGAPIVersion, "kind": runtime.CloudNativePGClusterKind, "spec": map[string]any{"instances": 1, "affinity": rendered}}
				preserveRetiredPostgresReplicaAffinity(cluster, desired)
				if !reflect.DeepEqual(pgretire.Object(desired["spec"])["affinity"], affinity) {
					t.Fatal("ordinary reconcile would rewrite retired Cluster affinity")
				}
				replacement := map[string]any{"enablePodAntiAffinity": false}
				pgretire.Object(desired["spec"])["affinity"] = replacement
				preserveRetiredPostgresReplicaAffinity(cluster, desired)
				if !reflect.DeepEqual(pgretire.Object(desired["spec"])["affinity"], replacement) {
					t.Fatal("retirement interfered with an explicit new placement")
				}
			}
		})
	}
}
