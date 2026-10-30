package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/storagerecovery"
	"fugue/internal/store"
)

func TestColdStorageRetryCannotChangeHistoryOrActivatedDestination(t *testing.T) {
	pg := model.AppPostgresSpec{ServiceName: "database", RuntimeID: "destination", PrimaryNodeName: "destination-node", StorageClassName: "old-class", StorageSize: "1Gi", Instances: 2, SynchronousReplicas: 1, FailoverTargetRuntimeID: "replica", Database: "records", User: "owner", Password: "fixture"}
	st := &coldPostgresState{AppID: "app", OfflineSource: true, TargetRuntime: pg.RuntimeID, TargetNode: pg.PrimaryNodeName, TargetClass: pg.StorageClassName, IntentHash: store.PostgresSpecFingerprint(pg), Phase: "restoring"}
	pg.StorageClassName = "new-class"
	if !coldStorageReplanAllowed(st, "app", pg) {
		t.Fatal("class-only unpublished retry rejected")
	}
	for _, phase := range []string{"planned", "copying", "validated", "cutover", "complete", "unknown"} {
		changed := *st
		changed.Phase = phase
		if coldStorageReplanAllowed(&changed, "app", pg) {
			t.Fatalf("accepted phase %s", phase)
		}
	}
	for _, change := range []func(*model.AppPostgresSpec){
		func(p *model.AppPostgresSpec) { p.RuntimeID = "elsewhere" },
		func(p *model.AppPostgresSpec) { p.PrimaryNodeName = "elsewhere" },
		func(p *model.AppPostgresSpec) { p.Instances = 1; p.SynchronousReplicas = 0 },
		func(p *model.AppPostgresSpec) { p.Password = "different" },
		func(p *model.AppPostgresSpec) { p.StorageSize = "10Gi" },
		func(p *model.AppPostgresSpec) { p.FailoverTargetRuntimeID = "elsewhere" },
		func(p *model.AppPostgresSpec) { p.StorageClassName = "old-class" },
	} {
		changed := *model.CloneAppPostgresSpec(&pg)
		change(&changed)
		if coldStorageReplanAllowed(st, "app", changed) {
			t.Fatal("accepted unrelated intent change")
		}
	}
	for _, change := range []func(*coldPostgresState){func(p *coldPostgresState) { p.OfflineSource = false }, func(p *coldPostgresState) { p.IntentHash = "" }, func(p *coldPostgresState) { p.AppID = "foreign" }, func(p *coldPostgresState) { p.RetainedAttempts = make([]coldRetainedAttempt, 4) }} {
		changed := *st
		change(&changed)
		if coldStorageReplanAllowed(&changed, "app", pg) {
			t.Fatal("accepted unproven/beyond-bound retry")
		}
	}
	st.ServiceID, st.SourceUID, st.SourcePVCUID, st.SourceReaderUID = "service", "source-uid", "pvc-uid", "reader-uid"
	st.TargetName, st.TargetUID, st.SeedName, st.SeedUID = "previous-target", "target-uid", "previous-seed", "seed-uid"
	st.SystemID, st.ControlDigest, st.FileDigest = "123", strings.Repeat("a", 64), strings.Repeat("b", 64)
	before := *st
	next := coldReplannedState(st, pg)
	if !reflect.DeepEqual(*st, before) || next.SourcePVCUID != st.SourcePVCUID || next.SourceReaderUID != st.SourceReaderUID || next.FileDigest != st.FileDigest || next.ControlDigest != st.ControlDigest || next.SystemID != st.SystemID {
		t.Fatal("retry changed source or verified receipt")
	}
	if next.TargetName == st.TargetName || next.SeedName == st.SeedName || next.TargetUID != "" || next.SeedUID != "" || next.Phase != "planned" || len(next.RetainedAttempts) != 1 || next.RetainedAttempts[0].TargetUID != st.TargetUID || next.RetainedAttempts[0].SeedUID != st.SeedUID {
		t.Fatal("retry overwrites a destination or loses retained data identities", next)
	}
}

func TestColdStorageRetryRefusesChangedServingEndpoint(t *testing.T) {
	for _, selected := range []string{"source", "target", "foreign"} {
		t.Run(selected, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Fatal("retry changed serving endpoint")
				}
				json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]string{"uid": "service-uid"}, "spec": map[string]any{"selector": map[string]string{"cnpg.io/cluster": selected, "cnpg.io/instanceRole": "primary"}}})
			}))
			defer srv.Close()
			err := coldEndpointStillSelectsSource(context.Background(), &kubeClient{baseURL: srv.URL, client: srv.Client()}, "ns", &coldPostgresState{SourceName: "source", Endpoint: "stable"})
			if (err == nil) != (selected == "source") {
				t.Fatal(err)
			}
		})
	}
}

func TestColdStorageRetryFencesOnlyOwnedUnpublishedTargetAndRetainsClaims(t *testing.T) {
	for _, scenario := range []string{"primary", "join", "foreign-cluster", "foreign-pod", "foreign-job", "unhealthy-node", "inactive"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			state := store.New(filepath.Join(t.TempDir(), "state.json"))
			if err := state.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, _ := state.CreateTenant("Recovery")
			project, _ := state.CreateProject(tenant.ID, "project", "")
			rt, _, _ := state.CreateRuntime(tenant.ID, "runtime", model.RuntimeTypeManagedOwned, "", nil)
			app, err := state.CreateApp(tenant.ID, project.ID, "application", "", model.AppSpec{RuntimeID: rt.ID, Image: "example.test/app:v1", Replicas: 1, Postgres: &model.AppPostgresSpec{ServiceName: "database", RuntimeID: rt.ID, Database: "records", User: "owner", Password: "fixture", StorageSize: "1Gi", Instances: 1}})
			if err != nil {
				t.Fatal(err)
			}
			op, err := state.CreateOperation(model.Operation{AppID: app.ID, TenantID: tenant.ID, Type: storagerecovery.OperationType})
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "inactive" {
				state.FailOperation(op.ID, "test")
			}
			st := &coldPostgresState{ServiceID: "service", TargetName: "target", TargetUID: "target-uid"}
			patched, deleted := false, false
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				path := r.URL.Path
				if r.Method == http.MethodPatch {
					if !strings.HasSuffix(path, "/clusters/target") {
						t.Fatalf("unexpected mutation %s", path)
					}
					var body map[string]any
					json.NewDecoder(r.Body).Decode(&body)
					meta := normalizeKubeMap(body["metadata"])
					ann := normalizeKubeMap(meta["annotations"])
					if len(body) != 1 || meta["uid"] != st.TargetUID || meta["resourceVersion"] != "42" || ann[coldReconciliationAnnotation] != coldReconciliationDisabled || ann["cnpg.io/fencedInstances"] != `["*"]` {
						t.Fatal("fence lost scope/preconditions", body)
					}
					patched = true
					w.Write([]byte(`{}`))
					return
				}
				if r.Method == http.MethodDelete {
					if !strings.HasSuffix(path, "/pods/target-1") && !strings.HasSuffix(path, "/jobs/target-join") {
						t.Fatalf("deleted data/unrelated resource %s", path)
					}
					var body map[string]any
					json.NewDecoder(r.Body).Decode(&body)
					if body["gracePeriodSeconds"] != nil || normalizeKubeMap(body["preconditions"])["uid"] == nil {
						t.Fatal("force/unconditional deletion", body)
					}
					deleted = true
					w.Write([]byte(`{}`))
					return
				}
				var out any
				switch {
				case strings.HasSuffix(path, "/clusters/target"):
					uid := st.TargetUID
					if scenario == "foreign-cluster" {
						uid = "foreign"
					}
					ann := map[string]string{coldMigrationAnnotation: coldStateName(st.ServiceID)}
					if patched {
						ann[coldReconciliationAnnotation] = coldReconciliationDisabled
						ann["cnpg.io/fencedInstances"] = `["*"]`
						ann[runtimepkg.CloudNativePGHibernationAnno] = runtimepkg.CloudNativePGHibernationOn
					}
					out = map[string]any{"metadata": map[string]any{"name": "target", "uid": uid, "resourceVersion": "42", "annotations": ann}}
				case strings.HasSuffix(path, "/pods"):
					items := []any{}
					if !deleted {
						owner := map[string]any{"apiVersion": runtimepkg.CloudNativePGAPIVersion, "kind": "Cluster", "name": "target", "uid": st.TargetUID, "controller": true}
						if scenario == "join" || scenario == "foreign-job" {
							owner = map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": "target-join", "uid": "job-uid", "controller": true}
						}
						if scenario == "foreign-pod" {
							owner["uid"] = "foreign"
						}
						items = append(items, map[string]any{"metadata": map[string]any{"name": "target-1", "uid": "pod-uid", "ownerReferences": []any{owner}}, "spec": map[string]string{"nodeName": "target-node"}, "status": map[string]string{"phase": "Running"}})
					}
					out = map[string]any{"items": items}
				case strings.HasSuffix(path, "/nodes/target-node"):
					ready := "True"
					if scenario == "unhealthy-node" {
						ready = "False"
					}
					out = map[string]any{"status": map[string]any{"conditions": []any{map[string]string{"type": "Ready", "status": ready}}}}
				case strings.HasSuffix(path, "/jobs/target-join"):
					uid := st.TargetUID
					if scenario == "foreign-job" {
						uid = "foreign"
					}
					out = map[string]any{"metadata": map[string]any{"uid": "job-uid", "resourceVersion": "5", "ownerReferences": []any{map[string]any{"apiVersion": runtimepkg.CloudNativePGAPIVersion, "kind": "Cluster", "uid": uid, "controller": true}}}}
				default:
					t.Errorf("unexpected read %s", path)
					http.NotFound(w, r)
					return
				}
				json.NewEncoder(w).Encode(out)
			}))
			defer srv.Close()
			good := scenario == "primary" || scenario == "join"
			timeout := 100 * time.Millisecond
			if good {
				timeout = 10 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			err = (&Service{Store: state}).stopUnpublishedColdTarget(ctx, &kubeClient{baseURL: srv.URL, client: srv.Client()}, "ns", st, op.ID)
			if (err == nil) != good {
				t.Fatalf("normal shutdown not safely acknowledged: %v", err)
			}
			wantDelete := scenario == "primary" || scenario == "join"
			if deleted != wantDelete || (scenario == "foreign-cluster" || scenario == "inactive") && patched {
				t.Fatalf("unsafe target change: patched=%t deleted=%t err=%v", patched, deleted, err)
			}
		})
	}
}
