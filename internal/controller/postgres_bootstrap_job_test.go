package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/pgretire"
	"fugue/internal/runtime"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"
)

func bootstrapJSON(t *testing.T, obj any) string {
	t.Helper()
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func bootstrapObject(t *testing.T, raw string) map[string]any {
	t.Helper()
	var obj map[string]any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		t.Fatal(err)
	}
	return obj
}

type bootstrapJobFixture struct {
	name, role, cluster, namespace, instance, clusterUID string
	job, claim, pod                                      map[string]any
}

func newBootstrapJobFixture(t *testing.T, namespace, cluster, uid string, serial int, role string) bootstrapJobFixture {
	instance := fmt.Sprintf("%s-%d", cluster, serial)
	name := instance + "-" + role
	owner := fmt.Sprintf(`"ownerReferences":[{"apiVersion":"postgresql.cnpg.io/v1","kind":"Cluster","uid":%q,"name":%q,"controller":true}]`, uid, cluster)
	labels := fmt.Sprintf(`"cnpg.io/cluster":%q,"cnpg.io/instanceName":%q`, cluster, instance)
	volume := fmt.Sprintf(`"volumes":[{"name":"pgdata","persistentVolumeClaim":{"claimName":%q}}]`, instance)
	return bootstrapJobFixture{
		name: name, instance: instance, role: role, cluster: cluster, clusterUID: uid, namespace: namespace,
		job:   bootstrapObject(t, fmt.Sprintf(`{"metadata":{"uid":"join-uid","resourceVersion":"20","name":%q,%s,"labels":{%s,"cnpg.io/jobRole":%q}},"spec":{"suspend":false,"template":{"spec":{"containers":[{"name":%q}],%s}}},"status":{"active":1}}`, name, owner, labels, role, role, volume)),
		claim: bootstrapObject(t, fmt.Sprintf(`{"metadata":{"uid":"claim-uid","resourceVersion":"21","name":%q,%s,"labels":{%s,"cnpg.io/pvcRole":"PG_DATA","example.org/keep":"true"},"annotations":{"cnpg.io/pvcStatus":"initializing"}},"spec":{"storageClassName":"test-local","resources":{"requests":{"storage":"1Gi"}}},"status":{"phase":"Pending"}}`, instance, owner, labels)),
		pod:   bootstrapObject(t, fmt.Sprintf(`{"metadata":{"uid":"pod-uid","resourceVersion":"22","name":"worker","ownerReferences":[{"apiVersion":"batch/v1","kind":"Job","uid":"join-uid","name":%q,"controller":true}]},"spec":{%s},"status":{"phase":"Pending","conditions":[{"type":"PodScheduled","status":"False","reason":"Unschedulable"}]}}`, name, volume)),
	}
}

func patchBootstrapFixture(t *testing.T, req *http.Request, obj map[string]any) (map[string]any, error) {
	t.Helper()
	raw, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	var ops []map[string]any
	if err := json.Unmarshal(raw, &ops); err != nil || len(ops) < 3 || ops[0]["path"] != "/metadata/uid" || ops[1]["path"] != "/metadata/resourceVersion" {
		t.Fatalf("unguarded mutation: %s (%v)", raw, err)
	}
	p, err := jsonpatch.DecodePatch(raw)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := p.Apply([]byte(bootstrapJSON(t, obj)))
	if err != nil {
		return nil, err
	}
	updated := bootstrapObject(t, string(changed))
	pgretire.Meta(updated)["resourceVersion"] = fmt.Sprint(postgresStageInt(pgretire.Meta(obj)["resourceVersion"]) + 1)
	return updated, nil
}

func (f *bootstrapJobFixture) settle(t *testing.T) {
	f.pod = nil
	f.job["status"] = bootstrapObject(t, `{"conditions":[{"type":"Suspended","status":"True"}]}`)
}

func bootstrapList(t *testing.T, items ...map[string]any) *http.Response {
	list := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if item != nil {
			list = append(list, item)
		}
	}
	return okJSONResponse(bootstrapJSON(t, map[string]any{"items": list}))
}

func TestOrphanPostgresNeverInitializedPausesOnlyExactUnscheduledInitDB(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"safe", "scheduled", "bound", "selected", "foreign job", "other volume", "stale ManagedApp", "old failed attempt", "resumed"} {
		t.Run(scenario, func(t *testing.T) {
			s, managed := orphanLifecycleFixture(t)
			managed.Spec.BackingServices[0].Spec.Postgres.Suspended = scenario != "resumed"
			managed.Metadata.Annotations[runtime.FugueAnnotationOrphanPostgresLifecycle] = runtime.OrphanPostgresLifecycleAutomatic
			ns := managed.Metadata.Namespace
			h := "on"
			if scenario == "resumed" {
				h = "off"
			}
			cluster := bootstrapObject(t, orphanClusterJSON(managed, "retained-postgres", h))
			cluster["status"] = bootstrapObject(t, `{"phase":"Setting up primary","targetPrimary":"retained-postgres-1","conditions":[{"type":"cnpg.io/hibernation","status":"False","reason":"WaitingForHealthy"}]}`)
			f := newBootstrapJobFixture(t, ns, "retained-postgres", "cluster-uid-retained-postgres", 1, "initdb")
			var extra map[string]any
			switch scenario {
			case "scheduled":
				pgretire.Object(f.pod["spec"])["nodeName"] = "node-a"
			case "bound":
				pgretire.Object(f.claim["spec"])["volumeName"] = "bound-pv"
			case "selected":
				pgretire.Object(pgretire.Meta(f.claim)["annotations"])["volume.kubernetes.io/selected-node"] = "node-a"
			case "foreign job":
				pgretire.Objects(pgretire.Meta(f.job)["ownerReferences"])[0]["uid"] = "replacement"
			case "other volume":
				extra = cloneKubeMap(f.claim)
				pgretire.Meta(extra)["name"] = "wal"
			case "old failed attempt":
				pgretire.Object(f.job["status"])["failed"] = 1
			case "resumed":
				pgretire.Object(f.job["spec"])["suspend"] = true
				pgretire.Meta(f.job)["annotations"] = map[string]any{postgresBootstrapJobHold: "orphan:managed-uid:" + f.clusterUID}
				f.settle(t)
			}
			writes := 0
			c := &kubeClient{baseURL: "http://kube.test", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodPatch && req.URL.Path == postgresJobPath(ns, f.name) {
					next, err := patchBootstrapFixture(t, req, f.job)
					if err != nil {
						t.Fatal(err)
					}
					f.job = next
					writes++
					return okJSONResponse("{}"), nil
				}
				if req.Method != http.MethodGet {
					t.Fatalf("unexpected write: %s %s", req.Method, req.URL)
				}
				switch req.URL.Path {
				case cloudNativePGClusterAPIPath(ns, f.cluster):
					return okJSONResponse(bootstrapJSON(t, cluster)), nil
				case postgresJobPath(ns, f.name):
					return okJSONResponse(bootstrapJSON(t, f.job)), nil
				case postgresJobPath(ns, ""):
					return bootstrapList(t, f.job), nil
				case pgretire.Path(ns, "pods", ""):
					return bootstrapList(t, f.pod), nil
				case pgretire.Path(ns, "persistentvolumeclaims", ""):
					return bootstrapList(t, f.claim, extra), nil
				case "/apis/storage.k8s.io/v1/storageclasses/test-local":
					return okJSONResponse(`{"volumeBindingMode":"WaitForFirstConsumer"}`), nil
				case managedAppAPIPath(ns, managed.Metadata.Name):
					fresh := managed
					if scenario == "stale ManagedApp" {
						fresh.Metadata.ResourceVersion = "99"
					}
					return okJSONResponse(bootstrapJSON(t, fresh)), nil
				default:
					t.Fatalf("unexpected read %s", req.URL)
					return nil, nil
				}
			})}}
			reconcile := func() ([]runtime.ManagedBackingServiceStatus, error) {
				return s.reconcileOrphanManagedPostgresLifecycle(context.Background(), c, ns, managed, runtime.AppFromManagedApp(managed))
			}
			status, err := reconcile()
			switch scenario {
			case "safe":
				if err != nil || writes != 1 || status[0].Phase == model.ManagedPostgresRuntimePhaseSuspended || pgretire.Object(f.job["spec"])["suspend"] != true {
					t.Fatalf("requested pause is not yet observed: writes=%d status=%+v err=%v", writes, status, err)
				}
				f.settle(t)
				for i := 0; i < 2; i++ {
					status, err = reconcile()
					if err != nil || writes != 1 || status[0].Phase != model.ManagedPostgresRuntimePhaseSuspended || !strings.Contains(status[0].Message, "unbound storage claim retained") {
						t.Fatalf("pause did not settle idempotently: writes=%d status=%+v err=%v", writes, status, err)
					}
				}
			case "resumed":
				if err != nil || writes != 1 || pgretire.Object(f.job["spec"])["suspend"] != false {
					t.Fatalf("Fugue-owned hold did not release: writes=%d err=%v", writes, err)
				}
			default:
				if writes != 0 || (len(status) != 0 && status[0].Phase == model.ManagedPostgresRuntimePhaseSuspended) {
					t.Fatalf("unsafe suspension for %s: writes=%d status=%+v err=%v", scenario, writes, status, err)
				}
			}
		})
	}
}

func TestPostgresPlacementRollbackSettlesJoinBeforeReportingSuccess(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"safe", "journal replay", "job already removed", "unallocated", "scheduled", "bound", "executed", "replaced job", "other claim user", "other job", "old stage", "primary changed", "cluster race", "no delayed binding"} {
		t.Run(scenario, func(t *testing.T) {
			s, op, ns := postgresRollbackTestOperation(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ctx = context.WithValue(ctx, postgresStageRollbackContextKey{}, true)
			f := newBootstrapJobFixture(t, ns, "sample-postgres", "cluster-uid", 2, "join")
			cluster := bootstrapObject(t, `{"apiVersion":"postgresql.cnpg.io/v1","kind":"Cluster","metadata":{"name":"sample-postgres","uid":"cluster-uid","resourceVersion":"10","annotations":{"cnpg.io/reconcilePodSpec":"disabled","example.org/keep":"true"}},"spec":{"instances":2,"affinity":{"nodeSelector":{"test.example/placement":"staged"}}},"status":{"currentPrimary":"sample-postgres-1","targetPrimary":"sample-postgres-1","latestGeneratedNode":2}}`)
			desired := bootstrapObject(t, `{"apiVersion":"postgresql.cnpg.io/v1","kind":"Cluster","spec":{"instances":1,"affinity":{"nodeSelector":{"test.example/placement":"source"}}}}`)
			witness := &postgresStageWitness{Operation: op.ID, UID: f.clusterUID, Primary: "sample-postgres-1", Instances: 1, Serial: 1}
			before := cloneKubeMap(f.claim)
			var otherPod, otherJob map[string]any
			delay := "WaitForFirstConsumer"
			switch scenario {
			case "journal replay":
				witness.Rollback = true
				pgretire.Object(f.job["spec"])["suspend"] = true
				pgretire.Meta(f.job)["annotations"] = map[string]any{postgresBootstrapJobHold: "rollback:" + op.ID + ":" + f.clusterUID}
				f.settle(t)
			case "job already removed":
				witness.Rollback = true
				f.job, f.pod = nil, nil
				pgretire.Object(cluster["spec"])["instances"] = 1
				delete(pgretire.Object(pgretire.Meta(cluster)["annotations"]), runtime.CloudNativePGReconcilePodSpecAnno)
				pgretire.Meta(f.claim)["ownerReferences"] = []any{}
				pgretire.Meta(f.claim)["labels"] = map[string]any{"example.org/keep": "true"}
				pgretire.Meta(f.claim)["annotations"] = map[string]any{postgresRollbackRetainedClaim: "rollback:" + op.ID + ":" + f.clusterUID}
			case "unallocated":
				pgretire.Object(cluster["status"])["latestGeneratedNode"] = 1
				f.job, f.claim, f.pod = nil, nil, nil
			case "scheduled":
				pgretire.Object(f.pod["spec"])["nodeName"] = "destination"
			case "bound":
				pgretire.Object(f.claim["spec"])["volumeName"] = "protected-pv"
			case "executed":
				pgretire.Object(f.job["status"])["succeeded"] = 1
			case "other claim user":
				otherPod = cloneKubeMap(f.pod)
				pgretire.Meta(otherPod)["ownerReferences"] = nil
			case "other job":
				otherJob = cloneKubeMap(f.job)
				pgretire.Meta(otherJob)["uid"] = "another-job-uid"
			case "old stage":
				witness = nil
			case "primary changed":
				pgretire.Object(cluster["status"])["targetPrimary"] = f.instance
			case "no delayed binding":
				delay = "Immediate"
			}
			pgretire.Meta(cluster)["annotations"] = postgresStageAnnotation(cluster, witness)
			clusterPath, claimPath, jobPath := cloudNativePGClusterAPIPath(ns, f.cluster), pgretire.Path(ns, "persistentvolumeclaims", f.instance), postgresJobPath(ns, f.name)
			var actions []string
			c := &kubeClient{baseURL: "http://kube.test", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				path := req.URL.Path
				if req.Method == http.MethodGet {
					switch path {
					case clusterPath:
						return okJSONResponse(bootstrapJSON(t, cluster)), nil
					case claimPath:
						return okJSONResponse(bootstrapJSON(t, f.claim)), nil
					case jobPath:
						if f.job == nil {
							return orphanLifecycleHTTPResponse(404, "{}"), nil
						}
						return okJSONResponse(bootstrapJSON(t, f.job)), nil
					case postgresJobPath(ns, ""):
						return bootstrapList(t, f.job, otherJob), nil
					case pgretire.Path(ns, "pods", ""):
						return bootstrapList(t, f.pod, otherPod), nil
					case "/apis/storage.k8s.io/v1/storageclasses/test-local":
						return okJSONResponse(bootstrapJSON(t, map[string]any{"volumeBindingMode": delay})), nil
					}
				}
				if req.Method == http.MethodPatch {
					var target *map[string]any
					var action string
					switch path {
					case clusterPath:
						target, action = &cluster, "cluster"
					case claimPath:
						target, action = &f.claim, "retain claim"
					case jobPath:
						target, action = &f.job, "pause job"
					default:
						t.Fatalf("unexpected patch %s", path)
					}
					if action == "cluster" && scenario == "cluster race" {
						pgretire.Meta(cluster)["resourceVersion"] = "999"
					}
					next, err := patchBootstrapFixture(t, req, *target)
					if err != nil {
						return orphanLifecycleHTTPResponse(409, "{}"), nil
					}
					*target = next
					actions = append(actions, action)
					if action == "pause job" {
						f.settle(t)
						if scenario == "replaced job" {
							pgretire.Meta(f.job)["uid"] = "replacement"
						}
					}
					return okJSONResponse("{}"), nil
				}
				if req.Method == http.MethodDelete && path == jobPath {
					var body map[string]any
					if err := json.NewDecoder(req.Body).Decode(&body); err != nil ||
						pgretire.Object(body["preconditions"])["uid"] != pgretire.Meta(f.job)["uid"] ||
						pgretire.Object(body["preconditions"])["resourceVersion"] != pgretire.Meta(f.job)["resourceVersion"] ||
						pgretire.Object(cluster["spec"])["instances"] != float64(1) {
						t.Fatalf("job removal was not fenced or started before topology restore: %+v (%v)", body, err)
					}
					f.job = nil
					actions = append(actions, "delete job")
					return okJSONResponse("{}"), nil
				}
				t.Fatalf("unexpected API request: %s %s", req.Method, req.URL)
				return nil, nil
			})}}
			patch, err := managedPostgresPlacementOperationPatch(cluster, desired)
			if err != nil {
				t.Fatal(err)
			}
			err = s.applyGuardedPostgresPlacement(ctx, c, op, ns, f.cluster, cloneKubeMap(cluster), desired, patch)
			want := map[string][]string{
				"safe":                {"cluster", "pause job", "retain claim", "cluster", "delete job", "cluster"},
				"journal replay":      {"retain claim", "cluster", "delete job", "cluster"},
				"job already removed": {"cluster", "cluster"},
				"unallocated":         {"cluster"},
				"replaced job":        {"cluster", "pause job"},
				"other job":           {"cluster", "pause job"},
			}[scenario]
			success := scenario == "safe" || scenario == "journal replay" || scenario == "job already removed" || scenario == "unallocated"
			if (err == nil) != success || !reflect.DeepEqual(want, actions) {
				t.Fatalf("rollback outcome mismatch: got actions %v err=%v, want %v (success=%v)", actions, err, want, success)
			}
			if f.claim != nil && !reflect.DeepEqual(f.claim["spec"], before["spec"]) && scenario != "bound" {
				t.Fatalf("claim storage config changed: %+v", f.claim)
			}
			if success && (pgretire.Object(cluster["spec"])["instances"] != float64(1) ||
				pgretire.Object(pgretire.Meta(cluster)["annotations"])[postgresStageWitnessAnnotation] != nil || f.job != nil) {
				t.Fatalf("roll back returned before topology, stage marker and job settled: %+v", cluster)
			}
		})
	}
}

func postgresRollbackTestOperation(t *testing.T) (*Service, model.Operation, string) {
	t.Helper()
	st := newStoreTestDatabase(t, "")
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := st.CreateTenant("rollback")
	if err != nil {
		t.Fatal(err)
	}
	project, err := st.CreateProject(tenant.ID, "rollback", "")
	if err != nil {
		t.Fatal(err)
	}
	a, err := st.CreateApp(tenant.ID, project.ID, "sample", "", model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Image: "example.invalid/sample:latest", Replicas: 1})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	// Only operation ownership is consumed by the rollback helper. The
	// placement operation's dispatch is covered by its full-path test.
	op, err := st.CreateOperation(model.Operation{Type: model.OperationTypeDeploy, TenantID: tenant.ID, AppID: a.ID, DesiredSpec: &a.Spec})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	claimed, ok, err := st.ClaimNextPendingOperation()
	if err != nil || !ok || op.ID != claimed.ID {
		t.Fatalf("could not claim rollback fixture operation %s: %v", op.ID, err)
	}
	return &Service{Store: st}, claimed, runtime.NamespaceForTenant(tenant.ID)
}

func TestOrdinaryApplyCannotForgetUnsettledPostgresPlacement(t *testing.T) {
	t.Parallel()
	cluster := bootstrapObject(t, `{"apiVersion":"postgresql.cnpg.io/v1","kind":"Cluster","metadata":{"name":"sample","uid":"cluster-uid","annotations":{"fugue.pro/postgres-placement-stage":"unsettled","cnpg.io/reconcilePodSpec":"disabled"}},"spec":{"instances":2}}`)
	desired := bootstrapObject(t, `{"apiVersion":"postgresql.cnpg.io/v1","kind":"Cluster","spec":{"instances":1}}`)
	c := &kubeClient{baseURL: "http://kube.test", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodGet {
			t.Fatalf("ordinary apply tried to restore placement before staged resources settled: %s", req.Method)
		}
		return okJSONResponse(bootstrapJSON(t, cluster)), nil
	})}}
	why, skip, err := c.shouldSkipApply(context.Background(), cloudNativePGClusterAPIPath("test", "sample"), desired)
	if err != nil || !skip || why != "apply_skipped_postgres_placement_unsettled" {
		t.Fatalf("unexpected approval for a normal apply: %q skip=%v err=%v", why, skip, err)
	}
}
