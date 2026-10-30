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
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func orphanLifecycleHTTPResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

func orphanLifecycleFixture(t *testing.T) (*Service, runtime.ManagedAppObject) {
	t.Helper()
	stateStore := store.New(t.TempDir() + "/store.json")
	if err := stateStore.Init(); err != nil {
		t.Fatal(err)
	}
	app := model.App{
		ID: "app_retained", TenantID: "tenant_retained", ProjectID: "project_retained", Name: "retained",
		Spec: model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 0},
		BackingServices: []model.BackingService{{
			ID: "service_retained", TenantID: "tenant_retained", ProjectID: "project_retained", OwnerAppID: "app_retained",
			Name: "retained", Type: model.BackingServiceTypePostgres, Provisioner: model.BackingServiceProvisionerManaged,
			Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{ServiceName: "retained-postgres", RuntimeID: model.DefaultManagedRuntimeID, Instances: 1, StorageSize: "20Gi", Password: "retained-secret"}},
		}},
		Bindings: []model.ServiceBinding{{ID: "binding_retained", TenantID: "tenant_retained", AppID: "app_retained", ServiceID: "service_retained"}},
	}
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatal(err)
	}
	managed.Metadata.UID, managed.Metadata.ResourceVersion, managed.Metadata.Generation = "managed-uid", "10", 2
	managed.Metadata.Annotations = map[string]string{"unrelated.example/annotation": "preserved"}
	managed.Status = managedAppDisabledOrphanStatus(managed, app, "orphaned managed app: app not found in store; disabled workload and retained storage for audit", nil)
	managed.Status.Conditions[0].LastTransitionTime = "2026-01-01T00:00:00Z"
	return &Service{Store: stateStore, now: func() time.Time { return time.Date(2026, 1, 1, 0, 6, 0, 0, time.UTC) }}, managed
}

func orphanClusterJSON(managed runtime.ManagedAppObject, name, state string) string {
	return fmt.Sprintf(`{"metadata":{"name":%q,"uid":%q,"resourceVersion":"20","labels":{"app.kubernetes.io/managed-by":"fugue","fugue.pro/tenant-id":%q,"fugue.pro/project-id":%q,"fugue.pro/owner-app-id":%q,"fugue.pro/backing-service-id":"service_retained","fugue.pro/backing-service-type":"postgres"},"ownerReferences":[{"apiVersion":"fugue.pro/v1alpha1","kind":"ManagedApp","name":%q,"uid":%q}],"annotations":{"cnpg.io/hibernation":%q}},"spec":{"instances":1},"status":{"instances":0,"readyInstances":0,"conditions":[{"type":"cnpg.io/hibernation","status":"True"}]}}`, name, "cluster-uid-"+name, managed.Spec.TenantID, managed.Spec.ProjectID, managed.Spec.AppID, managed.Metadata.Name, managed.Metadata.UID, state)
}

func TestOrphanPostgresHibernateGraceRequiresCurrentTimedZeroProof(t *testing.T) {
	t.Parallel()
	s, managed := orphanLifecycleFixture(t)
	for _, test := range []struct {
		name   string
		change func(*runtime.ManagedAppObject)
		want   bool
	}{
		{"elapsed", func(*runtime.ManagedAppObject) {}, true},
		{"recent", func(m *runtime.ManagedAppObject) { m.Status.Conditions[0].LastTransitionTime = "2026-01-01T00:05:00Z" }, false},
		{"future", func(m *runtime.ManagedAppObject) { m.Status.Conditions[0].LastTransitionTime = "2026-01-02T00:00:00Z" }, false},
		{"invalid", func(m *runtime.ManagedAppObject) { m.Status.Conditions[0].LastTransitionTime = "invalid" }, false},
		{"stale generation", func(m *runtime.ManagedAppObject) { m.Status.ObservedGeneration-- }, false},
		{"no proof", func(m *runtime.ManagedAppObject) { m.Status.Conditions = nil }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			copy := managed
			copy.Status.Conditions = append([]runtime.ManagedAppCondition(nil), managed.Status.Conditions...)
			test.change(&copy)
			if got := orphanPostgresHibernateGraceElapsed(copy, s.now()); got != test.want {
				t.Fatalf("got %v, want %v", got, test.want)
			}
		})
	}
}

func TestOrphanPostgresDefaultsToHibernateAndRepairsUniqueOwnedCluster(t *testing.T) {
	t.Parallel()
	for _, staleName := range []bool{false, true} {
		t.Run(fmt.Sprint("staleName=", staleName), func(t *testing.T) {
			s, managed := orphanLifecycleFixture(t)
			if staleName {
				managed.Spec.BackingServices[0].Spec.Postgres.ServiceName = "missing-recovery"
			}
			original := managed.Spec.BackingServices[0].Spec.Postgres
			var changes []map[string]any
			client := &kubeClient{baseURL: "http://kube.test", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch {
				case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/missing-recovery"):
					return orphanLifecycleHTTPResponse(http.StatusNotFound, `{"message":"not found"}`), nil
				case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/retained-postgres"):
					return okJSONResponse(orphanClusterJSON(managed, "retained-postgres", "off")), nil
				case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/clusters"):
					if req.URL.Query().Get("labelSelector") != "fugue.pro/owner-app-id=app_retained,fugue.pro/backing-service-id=service_retained" {
						t.Fatalf("unexpected identity selector: %s", req.URL)
					}
					return okJSONResponse(`{"items":[{"metadata":{"name":"retained-postgres"}}]}`), nil
				case req.Method == http.MethodPatch && req.URL.Path == managedAppAPIPath(managed.Metadata.Namespace, managed.Metadata.Name):
					if err := json.NewDecoder(req.Body).Decode(&changes); err != nil {
						t.Fatal(err)
					}
					return okJSONResponse(`{}`), nil
				default:
					t.Fatalf("unexpected request (must not modify CNPG, storage, or credentials): %s %s", req.Method, req.URL)
					return nil, nil
				}
			})}}
			if _, err := s.reconcileOrphanManagedPostgresLifecycle(context.Background(), client, managed.Metadata.Namespace, managed, runtime.AppFromManagedApp(managed)); err != nil {
				t.Fatal(err)
			}
			if len(changes) < 4 || changes[0]["path"] != "/metadata/uid" || changes[0]["value"] != "managed-uid" || changes[1]["path"] != "/metadata/resourceVersion" || changes[1]["value"] != "10" {
				t.Fatalf("missing identity/CAS fence: %+v", changes)
			}
			annotations := changes[2]["value"].(map[string]any)
			if annotations[runtime.FugueAnnotationOrphanPostgresLifecycle] != runtime.OrphanPostgresLifecycleAutomatic || annotations["unrelated.example/annotation"] != "preserved" {
				t.Fatalf("wrong annotations: %+v", annotations)
			}
			seenName := false
			for _, op := range changes[3:] {
				switch op["path"] {
				case "/spec/backingServices/0/spec/postgres/suspended":
					if op["value"] != true {
						t.Fatal(op)
					}
				case "/spec/backingServices/0/spec/postgres/service_name":
					seenName = op["value"] == "retained-postgres"
				default:
					t.Fatalf("unexpected spec change: %+v", op)
				}
			}
			if seenName != staleName {
				t.Fatalf("name repair=%v, stale=%v", seenName, staleName)
			}
			if original.Suspended || original.Password != "retained-secret" || original.StorageSize != "20Gi" {
				t.Fatal("mutated retained snapshot")
			}
		})
	}
}

func TestOrphanPostgresWaitsForGraceWithoutAnyRuntimeWrites(t *testing.T) {
	t.Parallel()
	s, managed := orphanLifecycleFixture(t)
	managed.Status.Conditions[0].LastTransitionTime = s.now().Format(time.RFC3339Nano)
	client := &kubeClient{baseURL: "http://kube.test", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("grace period must not touch runtime: %s %s", req.Method, req.URL)
		return nil, nil
	})}}
	if _, err := s.reconcileOrphanManagedPostgresLifecycle(context.Background(), client, managed.Metadata.Namespace, managed, runtime.AppFromManagedApp(managed)); err != nil {
		t.Fatal(err)
	}
}

func TestOrphanPostgresExplicitResumeSurvivesAutomaticReconcile(t *testing.T) {
	t.Parallel()
	s, managed := orphanLifecycleFixture(t)
	managed.Metadata.Annotations[runtime.FugueAnnotationOrphanPostgresLifecycle] = runtime.OrphanPostgresLifecycleExplicit
	var hibernation string
	reads := 0
	client := &kubeClient{baseURL: "http://kube.test", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(req.URL.Path, "/retained-postgres") {
			t.Fatalf("unexpected path %s", req.URL)
		}
		switch req.Method {
		case http.MethodGet:
			reads++
			state := "on"
			if reads > 1 {
				state = "off"
			}
			body := orphanClusterJSON(managed, "retained-postgres", state)
			if reads > 1 {
				body = strings.Replace(body, `"instances":0,"readyInstances":0`, `"instances":1,"readyInstances":1,"currentPrimary":"retained-postgres-1"`, 1)
				body = strings.Replace(body, `"status":"True"`, `"status":"False"`, 1)
			}
			return okJSONResponse(body), nil
		case http.MethodPatch:
			var patch struct {
				Metadata struct {
					UID             string            `json:"uid"`
					ResourceVersion string            `json:"resourceVersion"`
					Annotations     map[string]string `json:"annotations"`
				} `json:"metadata"`
			}
			if err := json.NewDecoder(req.Body).Decode(&patch); err != nil {
				t.Fatal(err)
			}
			if patch.Metadata.UID != "cluster-uid-retained-postgres" || patch.Metadata.ResourceVersion != "20" {
				t.Fatal("missing CNPG identity fence")
			}
			hibernation = patch.Metadata.Annotations[runtime.CloudNativePGHibernationAnno]
			return okJSONResponse(`{}`), nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			return nil, nil
		}
	})}}
	statuses, err := s.reconcileOrphanManagedPostgresLifecycle(context.Background(), client, managed.Metadata.Namespace, managed, runtime.AppFromManagedApp(managed))
	if err != nil {
		t.Fatal(err)
	}
	if hibernation != "off" || len(statuses) != 1 || statuses[0].ReadyInstances != 1 {
		t.Fatalf("explicit resume was not respected: %q %+v", hibernation, statuses)
	}
}

func TestOrphanPostgresAutomaticIntentReachesObservedHibernate(t *testing.T) {
	t.Parallel()
	s, managed := orphanLifecycleFixture(t)
	managed.Metadata.Annotations[runtime.FugueAnnotationOrphanPostgresLifecycle] = runtime.OrphanPostgresLifecycleAutomatic
	managed.Spec.BackingServices[0].Spec.Postgres.Suspended = true
	state := "off"
	writes := 0
	client := &kubeClient{baseURL: "http://kube.test", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(req.URL.Path, "/retained-postgres") {
			t.Fatalf("unexpected resource %s", req.URL)
		}
		if req.Method == http.MethodGet {
			return okJSONResponse(orphanClusterJSON(managed, "retained-postgres", state)), nil
		}
		if req.Method != http.MethodPatch {
			t.Fatalf("unexpected method %s", req.Method)
		}
		var patch map[string]any
		if err := json.NewDecoder(req.Body).Decode(&patch); err != nil {
			t.Fatal(err)
		}
		if len(patch) != 1 {
			t.Fatalf("hibernation must only patch metadata: %+v", patch)
		}
		metadata := patch["metadata"].(map[string]any)
		if metadata["uid"] != "cluster-uid-retained-postgres" || metadata["resourceVersion"] != "20" {
			t.Fatalf("missing CNPG identity fence: %+v", metadata)
		}
		state, _ = metadata["annotations"].(map[string]any)[runtime.CloudNativePGHibernationAnno].(string)
		writes++
		return okJSONResponse(`{}`), nil
	})}}
	statuses, err := s.reconcileOrphanManagedPostgresLifecycle(context.Background(), client, managed.Metadata.Namespace, managed, runtime.AppFromManagedApp(managed))
	if err != nil {
		t.Fatal(err)
	}
	if writes != 1 || state != "on" || len(statuses) != 1 || statuses[0].Phase != model.ManagedPostgresRuntimePhaseSuspended || statuses[0].ReadyInstances != 0 {
		t.Fatalf("automatic intent did not converge: writes=%d state=%q status=%+v", writes, state, statuses)
	}
	if _, err := s.reconcileOrphanManagedPostgresLifecycle(context.Background(), client, managed.Metadata.Namespace, managed, runtime.AppFromManagedApp(managed)); err != nil {
		t.Fatal(err)
	}
	if writes != 1 {
		t.Fatal("already hibernated cluster must not be rewritten")
	}
}

func TestOrphanPostgresClusterRecoveryRejectsAmbiguityAndForeignOwnership(t *testing.T) {
	t.Parallel()
	for _, foreign := range []bool{false, true} {
		t.Run(fmt.Sprint("foreign=", foreign), func(t *testing.T) {
			_, managed := orphanLifecycleFixture(t)
			deployment := runtime.ManagedBackingServiceDeployments(runtime.AppFromManagedApp(managed), managed.Spec.Scheduling)[0]
			deployment.ResourceName = "missing"
			client := &kubeClient{baseURL: "http://kube.test", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet {
					t.Fatalf("unexpected write %s", req.Method)
				}
				if strings.HasSuffix(req.URL.Path, "/missing") {
					return orphanLifecycleHTTPResponse(http.StatusNotFound, `{}`), nil
				}
				if strings.HasSuffix(req.URL.Path, "/clusters") {
					return okJSONResponse(`{"items":[{"metadata":{"name":"first"}},{"metadata":{"name":"second"}}]}`), nil
				}
				body := orphanClusterJSON(managed, req.URL.Path[strings.LastIndex(req.URL.Path, "/")+1:], "off")
				if foreign {
					body = strings.ReplaceAll(body, "managed-uid", "foreign-uid")
				}
				return okJSONResponse(body), nil
			})}}
			name, err := resolveOrphanPostgresCluster(context.Background(), client, managed.Metadata.Namespace, managed, deployment)
			if foreign && (err != nil || name != "") {
				t.Fatalf("foreign owner must not resolve: %q %v", name, err)
			}
			if !foreign && (err == nil || !strings.Contains(err.Error(), "multiple")) {
				t.Fatalf("ambiguous cluster must fail closed: %q %v", name, err)
			}
		})
	}
}

func TestOrphanPostgresClusterRecoveryDoesNotRedirectSameNamedForeignCluster(t *testing.T) {
	t.Parallel()
	_, managed := orphanLifecycleFixture(t)
	deployment := runtime.ManagedBackingServiceDeployments(runtime.AppFromManagedApp(managed), managed.Spec.Scheduling)[0]
	client := &kubeClient{baseURL: "http://kube.test", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodGet {
			t.Fatalf("unexpected write %s", req.Method)
		}
		body := orphanClusterJSON(managed, deployment.ResourceName, "off")
		body = strings.ReplaceAll(body, "managed-uid", "foreign-uid")
		return okJSONResponse(body), nil
	})}}
	if name, err := resolveOrphanPostgresCluster(context.Background(), client, managed.Metadata.Namespace, managed, deployment); err != nil || name != deployment.ResourceName {
		t.Fatalf("same-named foreign cluster must be left for ownership-conflict observation: %q %v", name, err)
	}
}

func TestOrphanPostgresStoreEvidenceFailsClosed(t *testing.T) {
	t.Parallel()
	s, managed := orphanLifecycleFixture(t)
	app := runtime.AppFromManagedApp(managed)
	deployments := runtime.ManagedBackingServiceDeployments(app, managed.Spec.Scheduling)
	if err := s.verifyOrphanPostgresHasNoStoreConsumers(app, deployments); err != nil {
		t.Fatal(err)
	}
	if err := (&Service{}).verifyOrphanPostgresHasNoStoreConsumers(app, deployments); err == nil {
		t.Fatal("missing store must fail closed")
	}
	tenant, err := s.Store.CreateTenant("retained tenant")
	if err != nil {
		t.Fatal(err)
	}
	project, err := s.Store.CreateProject(tenant.ID, "retained project", "")
	if err != nil {
		t.Fatal(err)
	}
	live, err := s.Store.CreateApp(tenant.ID, project.ID, "restored", "", model.AppSpec{Image: "ghcr.io/example/app:latest", RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1, Ports: []int{8080}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.verifyOrphanPostgresHasNoStoreConsumers(live, deployments); err == nil || !strings.Contains(err.Error(), "restored") {
		t.Fatalf("restored app must prevent hibernation: %v", err)
	}
	restored, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(live, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatal(err)
	}
	restored.Metadata.UID, restored.Metadata.ResourceVersion = "restored-uid", "30"
	restored.Metadata.Annotations = map[string]string{runtime.FugueAnnotationOrphanPostgresLifecycle: runtime.OrphanPostgresLifecycleExplicit, "unrelated.example/annotation": "preserved"}
	cleared := false
	client := &kubeClient{baseURL: "http://kube.test", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPatch || req.URL.Path != managedAppAPIPath(restored.Metadata.Namespace, restored.Metadata.Name) {
			t.Fatalf("unexpected adoption side effect: %s %s", req.Method, req.URL)
		}
		var ops []map[string]any
		if err := json.NewDecoder(req.Body).Decode(&ops); err != nil {
			t.Fatal(err)
		}
		annotations := ops[2]["value"].(map[string]any)
		if _, present := annotations[runtime.FugueAnnotationOrphanPostgresLifecycle]; present || annotations["unrelated.example/annotation"] != "preserved" {
			t.Fatalf("adoption did not clear only the orphan override: %+v", annotations)
		}
		cleared = true
		return okJSONResponse(`{}`), nil
	})}}
	if err := s.reconcileManagedAppObject(context.Background(), client, restored); err != nil {
		t.Fatal(err)
	}
	if !cleared {
		t.Fatal("adoption must reset the orphan lifecycle episode")
	}
	service, err := s.Store.CreateBackingService(tenant.ID, project.ID, "shared", "", model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{RuntimeID: model.DefaultManagedRuntimeID, StorageSize: "1Gi"}})
	if err != nil {
		t.Fatal(err)
	}
	app.TenantID, deployments[0].ServiceID = tenant.ID, service.ID
	if err := s.verifyOrphanPostgresHasNoStoreConsumers(app, deployments); err == nil || !strings.Contains(err.Error(), "exists in store") {
		t.Fatalf("standalone service must prevent hibernation: %v", err)
	}
	if _, err := s.Store.BindBackingService(tenant.ID, live.ID, service.ID, "postgres", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.verifyOrphanPostgresHasNoStoreConsumers(app, deployments); err == nil || !strings.Contains(err.Error(), "consumer") {
		t.Fatalf("bound consumer must prevent hibernation: %v", err)
	}
}

func TestOrphanPostgresLifecycleIntentPatchPreservesAnnotationsAndFencesConflicts(t *testing.T) {
	t.Parallel()
	_, managed := orphanLifecycleFixture(t)
	var ops []map[string]any
	client := &kubeClient{baseURL: "http://kube.test", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if err := json.NewDecoder(req.Body).Decode(&ops); err != nil {
			t.Fatal(err)
		}
		return orphanLifecycleHTTPResponse(http.StatusConflict, `{"message":"resourceVersion conflict"}`), nil
	})}}
	if err := client.patchOrphanPostgresLifecycleIntent(context.Background(), managed.Metadata.Namespace, managed, nil, ""); err == nil {
		t.Fatal("must propagate concurrent-write conflict")
	}
	if got := ops[2]["value"].(map[string]any); !reflect.DeepEqual(got, map[string]any{"unrelated.example/annotation": "preserved"}) {
		t.Fatalf("wrong annotation reset: %+v", got)
	}
}
