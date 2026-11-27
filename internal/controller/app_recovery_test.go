package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/releaseflow"
	"fugue/internal/runtime"
)

func TestRecoveryCapturePinsObservedImageAndSeparatesActualClaimNames(t *testing.T) {
	st := newStoreTestDatabase(t, "")
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := st.CreateTenant("recovery fixture")
	if err != nil {
		t.Fatal(err)
	}
	project, err := st.CreateProject(tenant.ID, "recovery", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	app, err := st.CreateApp(tenant.ID, project.ID, "gateway", "", model.AppSpec{Image: "registry.example/worker:moving", RuntimeID: "runtime_managed_shared", Replicas: 1, Ports: []int{8080}, PersistentStorage: &model.AppPersistentStorageSpec{StorageSize: "1Gi", Mounts: []model.AppPersistentStorageMount{{Kind: "directory", Path: "/state"}}}})
	if err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	ns := runtime.NamespaceForTenant(tenant.ID)
	deployment := runtime.RuntimeAppResourceName(app)
	service := runtime.RuntimeAppServiceName(app)
	labels := map[string]any{runtime.FugueLabelAppID: app.ID}
	owner := map[string]any{"kind": "ReplicaSet", "name": "original-rs", "uid": "original-rs-uid", "controller": true}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Error("capture mutated runtime")
		}
		var response any
		switch r.URL.Path {
		case recoveryObjectPath(ns, "deployments", deployment):
			response = map[string]any{"metadata": map[string]any{"uid": "original-deployment-uid", "labels": labels}, "spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "worker", "image": app.Spec.Image}}}}}}
		case recoveryObjectPath(ns, "services", service):
			response = map[string]any{"metadata": map[string]any{"uid": "original-service-uid", "labels": labels}}
		case "/api/v1/namespaces/" + ns + "/pods":
			response = map[string]any{"items": []any{map[string]any{"metadata": map[string]any{"name": "original-pod", "uid": "original-pod-uid", "ownerReferences": []any{owner}}, "spec": map[string]any{"nodeName": "worker"}, "status": map[string]any{"containerStatuses": []any{map[string]any{"name": "worker", "imageID": "registry.example/worker@" + digest, "containerID": "containerd://original", "restartCount": 0}}}}}}
		case "/api/v1/nodes/worker":
			response = map[string]any{"status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}}
		case recoveryObjectPath(ns, "replicasets", ""):
			http.Error(w, "unexpected trailing slash", 500)
			return
		case strings.TrimSuffix(recoveryObjectPath(ns, "replicasets", ""), "/"):
			response = map[string]any{"items": []any{map[string]any{"metadata": map[string]any{"name": "original-rs"}}}}
		case recoveryObjectPath(ns, "replicasets", "original-rs"):
			response = map[string]any{"metadata": map[string]any{"uid": "original-rs-uid", "ownerReferences": []any{map[string]any{"kind": "Deployment", "uid": "original-deployment-uid"}}}}
		default:
			t.Errorf("unexpected capture request %s", r.URL.Path)
			http.Error(w, "unexpected", 500)
			return
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	zero := int64(0)
	r := model.AppRecoveryRecord{AppRecovery: model.AppRecovery{ID: "recovery_fixture"}, SourceSpec: app.Spec, Request: model.AppRecoveryStartRequest{RetentionSeconds: &zero}}
	s := &Service{Store: st}
	if err := s.captureRecoverySource(context.Background(), &kubeClient{baseURL: server.URL, client: server.Client()}, app, &r); err != nil {
		t.Fatal(err)
	}
	replacement := app
	replacement.Spec = *r.ReplacementSpec
	if r.ImageRef != "registry.example/worker@"+digest || r.SourcePods[0].Containers[0].ID != "containerd://original" {
		t.Fatal("capture used mutable tag or lost evidence identity")
	}
	if r.SourcePVC == r.ReplacementPVC || runtime.PersistentStoragePVCName(replacement, *replacement.Spec.PersistentStorage) != r.ReplacementPVC {
		t.Fatal("actual rendered claim differs from isolated recorded identity")
	}
	if app.Spec.PersistentStorage.ClaimName != "" {
		t.Fatal("capture mutated original storage")
	}
}

func TestRecoveryFreezesControllerWithoutDeletingEvidencePod(t *testing.T) {
	r := model.AppRecoveryRecord{AppRecovery: model.AppRecovery{ID: "recovery_fixture", Namespace: "tenant-fixture", SourceDeployment: "source", SourcePods: []model.AppRecoveryPod{{Name: "source-pod", UID: "original-pod-uid"}}}, SourceDeploymentUID: "source-uid", SourceReplicaSets: map[string]string{"source-rs": "rs-uid"}}
	paused, orphaned := false, false
	replicas := 1
	sets := true
	writes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		p := request.URL.Path
		switch {
		case p == recoveryObjectPath(r.Namespace, "deployments", "source") && request.Method == http.MethodGet:
			json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": "source-uid", "resourceVersion": "1"}, "spec": map[string]any{"paused": paused, "replicas": replicas}})
		case p == recoveryObjectPath(r.Namespace, "deployments", "source") && request.Method == http.MethodPatch:
			var raw map[string]any
			json.NewDecoder(request.Body).Decode(&raw)
			spec := objectMapField(raw, "spec")
			writes++
			if value, ok := spec["paused"].(bool); ok {
				paused = value
			}
			if value, ok := spec["replicas"].(float64); ok {
				if !orphaned {
					t.Error("scaled source before orphaning original Pod")
				}
				replicas = int(value)
			}
			json.NewEncoder(w).Encode(map[string]any{})
		case strings.HasSuffix(p, "/replicasets"):
			items := []any{}
			if sets {
				items = append(items, map[string]any{"metadata": map[string]any{"name": "source-rs"}})
			}
			json.NewEncoder(w).Encode(map[string]any{"items": items})
		case p == recoveryObjectPath(r.Namespace, "replicasets", "source-rs") && request.Method == http.MethodDelete:
			var options map[string]any
			json.NewDecoder(request.Body).Decode(&options)
			if !paused || nestedObjectString(options, "preconditions", "uid") != "rs-uid" || objectStringField(options, "propagationPolicy") != "Orphan" {
				t.Error("unsafe source ReplicaSet deletion")
			}
			sets = false
			orphaned = true
			writes++
			json.NewEncoder(w).Encode(map[string]any{})
		case p == recoveryObjectPath(r.Namespace, "pods", "source-pod") && request.Method == http.MethodGet:
			owners := []any{}
			if !orphaned {
				owners = append(owners, map[string]any{"uid": "rs-uid", "name": "source-rs", "kind": "ReplicaSet", "controller": true})
			}
			json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "source-pod", "uid": "original-pod-uid", "ownerReferences": owners}})
		default:
			t.Errorf("unexpected mutation or API request: %s %s", request.Method, p)
			http.Error(w, "unexpected", 500)
		}
	}))
	defer server.Close()
	c := &kubeClient{baseURL: server.URL, client: server.Client()}
	for n := 0; n < 3; n++ {
		err := freezeRecoverySource(context.Background(), c, r)
		if n < 2 && !errors.Is(err, errRecoveryWaiting) {
			t.Fatalf("step %d error=%v", n, err)
		}
		if n == 2 && err != nil {
			t.Fatal(err)
		}
	}
	if !paused || !orphaned || replicas != 0 || writes != 3 {
		t.Fatal("source was not safely frozen", paused, orphaned, replicas, writes)
	}
}

func TestRecoveryFilterNeverMutatesBackingServicesOrAliasesBeforeProbe(t *testing.T) {
	r := model.AppRecoveryRecord{AppRecovery: model.AppRecovery{ID: "recovery_fixture"}, ReplacementService: "replacement"}
	objects := []map[string]any{
		{"kind": "Service", "metadata": map[string]any{"name": "database", "labels": map[string]string{runtime.FugueLabelAppID: "app_fixture", runtime.FugueLabelBackingServiceType: model.BackingServiceTypePostgres}}},
		{"kind": "Service", "metadata": map[string]any{"name": "compatibility-alias", "labels": map[string]string{runtime.FugueLabelAppID: "app_fixture"}}},
		{"kind": "Service", "metadata": map[string]any{"name": "replacement", "labels": map[string]string{runtime.FugueLabelAppID: "app_fixture"}}},
	}
	got, err := recoveryReplacementObjects(objects, r)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || nestedObjectString(got[0], "metadata", "labels", runtime.FugueLabelAppID) != "app_fixture" || nestedObjectString(got[0], "metadata", "labels", recoveryLabel) != r.ID {
		t.Fatal("lost ownership labels or included unrelated resources", got)
	}
}

func TestRecoveryDetectsContainerRestartAndPodReplacement(t *testing.T) {
	r := model.AppRecoveryRecord{AppRecovery: model.AppRecovery{Namespace: "tenant-fixture", EvidenceStatus: "intact", SourcePods: []model.AppRecoveryPod{{Name: "evidence", UID: "original", Containers: []model.AppRecoveryContainer{{Name: "worker", ID: "containerd://original", RestartCount: 2}}}}}}
	uid, id := "original", "containerd://original"
	count := 2
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": uid}, "status": map[string]any{"containerStatuses": []any{map[string]any{"name": "worker", "containerID": id, "restartCount": count}}}})
	}))
	defer server.Close()
	c := &kubeClient{baseURL: server.URL, client: server.Client()}
	s := &Service{}
	for _, want := range []string{"intact", "changed", "lost"} {
		state, err := s.observeRecoveryEvidence(context.Background(), c, r)
		if err != nil || state != want {
			t.Fatal(want, state, err)
		}
		if want == "intact" {
			id = "containerd://restarted"
			count++
		} else {
			uid = "replacement"
		}
	}
}

func TestRecoveryBusinessFailureCannotSatisfyCutover(t *testing.T) {
	// Readiness alone is insufficient. Even a successful HTTP status must
	// contain the explicit business assertion, with redirects disabled.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/business", http.StatusFound)
			return
		}
		w.Write([]byte("healthy"))
	}))
	defer server.Close()
	for _, p := range []string{"/business", "/redirect"} {
		evaluator := releaseflow.ReleaseGateEvaluator{HTTPClient: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
		result := evaluator.RunProbe(context.Background(), model.AppRelease{UpstreamURL: server.URL}, model.AppReleaseProbe{Path: p, ExpectedBodyContains: "transaction-complete"})
		if result.Status == model.AppReleaseGateStatusPass {
			t.Fatal("non-business response passed gate")
		}
	}
}

func TestRecoveryReadinessRejectsOriginalEndpointAndDifferentImage(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	image := "registry.example/worker@" + digest
	r := model.AppRecoveryRecord{AppRecovery: model.AppRecovery{Namespace: "tenant-fixture", ReplacementDeployment: "replacement", ImageRef: image}, ReplacementDeploymentUID: "new-deployment", ReplacementService: "replacement"}
	endpointUID, imageID := "original-pod", image
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case recoveryObjectPath(r.Namespace, "deployments", "replacement"):
			json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": "new-deployment", "generation": 1}, "spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "worker", "image": image}}}}}, "status": map[string]any{"observedGeneration": 1, "readyReplicas": 1, "availableReplicas": 1}})
		case "/api/v1/namespaces/tenant-fixture/pods":
			json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"metadata": map[string]any{"uid": "new-pod"}, "status": map[string]any{"containerStatuses": []any{map[string]any{"name": "worker", "ready": true, "imageID": imageID}}}}}})
		case recoveryObjectPath(r.Namespace, "endpoints", "replacement"):
			json.NewEncoder(w).Encode(map[string]any{"subsets": []any{map[string]any{"addresses": []any{map[string]any{"ip": "192.0.2.2", "targetRef": map[string]any{"uid": endpointUID}}}}}})
		default:
			t.Errorf("unexpected request %s", request.URL.Path)
			http.Error(w, "unexpected", 500)
		}
	}))
	defer server.Close()
	c := &kubeClient{baseURL: server.URL, client: server.Client()}
	s := &Service{}
	if err := s.recoveryReplacementReady(context.Background(), c, model.App{}, r); err == nil {
		t.Fatal("old endpoint accepted")
	}
	endpointUID = "new-pod"
	if err := s.recoveryReplacementReady(context.Background(), c, model.App{}, r); err != nil {
		t.Fatal(err)
	}
	r.ReplacementPodUID = "previous-replacement-pod"
	if err := s.recoveryReplacementReady(context.Background(), c, model.App{}, r); err == nil {
		t.Fatal("pre-cutover Pod replacement reused business proof")
	}
	now := time.Now()
	r.CutoverAt = &now
	if err := s.recoveryReplacementReady(context.Background(), c, model.App{}, r); err != nil {
		t.Fatal("normal post-cutover restart prevented release", err)
	}
	imageID = "registry.example/worker@sha256:" + strings.Repeat("b", 64)
	if err := s.recoveryReplacementReady(context.Background(), c, model.App{}, r); err == nil {
		t.Fatal("different executable image accepted")
	}
}

func TestRecoveryObjectsNeverAdoptCollidingNames(t *testing.T) {
	created := 0
	owner := "someone-else"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"labels": map[string]any{recoveryLabel: owner}}})
			return
		}
		created++
		t.Errorf("existing object was modified: %s", r.Method)
	}))
	defer server.Close()
	c := &kubeClient{baseURL: server.URL, client: server.Client()}
	objects := []map[string]any{{"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": map[string]any{"name": "independent-volume"}}}
	if err := createRecoveryObjects(context.Background(), c, "fixture", "recovery_fixture", objects); err == nil {
		t.Fatal("adopted unrelated storage")
	}
	owner = "recovery_fixture"
	if err := createRecoveryObjects(context.Background(), c, "fixture", owner, objects); err != nil {
		t.Fatal("restart could not reuse its own objects", err)
	}
	if created != 0 {
		t.Fatal("colliding object changed")
	}
}

func TestRecoveryExpiredIntentKeepsHoldAndCanCancelWithoutRuntimeWrites(t *testing.T) {
	st := newStoreTestDatabase(t, "")
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := st.CreateTenant("deadline fixture")
	if err != nil {
		t.Fatal(err)
	}
	project, err := st.CreateProject(tenant.ID, "recovery", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	app, err := st.CreateApp(tenant.ID, project.ID, "gateway", "", model.AppSpec{Image: "registry.example/worker:v1", RuntimeID: "runtime_managed_shared", Replicas: 1, Ports: []int{8080}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{CPUMilliCores: 1000, MemoryMebibytes: 2048}); err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	r, err := st.StartAppRecovery(app, model.AppRecoveryStartRequest{AuthorizeOverlap: true, StorageStrategy: "fresh", RetentionSeconds: &zero, Probes: []model.AppReleaseProbe{{Path: "/business", ExpectedBodyContains: "accepted"}}})
	if err != nil {
		t.Fatal(err)
	}
	next := r
	next.Deadline = time.Now().Add(-time.Second)
	if _, err := st.SaveAppRecovery(r, next, false); err != nil {
		t.Fatal(err)
	}
	s := &Service{Store: st}
	// A nil client makes any accidental Kubernetes call fail the test.
	if handled, err := s.reconcileAppRecovery(context.Background(), nil, runtime.ManagedAppObject{}, app); err != nil || !handled {
		t.Fatal(handled, err)
	}
	records, err := st.ListAppRecoveries(app.ID)
	if err != nil || len(records) != 1 || records[0].Phase != "failed" || !records[0].Active {
		t.Fatal("expired request lost hold", err)
	}
	if _, err := st.ReleaseAppRecovery(app.ID, r.ID); err != nil {
		t.Fatal(err)
	}
	if handled, err := s.reconcileAppRecovery(context.Background(), nil, runtime.ManagedAppObject{}, app); err != nil || !handled {
		t.Fatal(handled, err)
	}
	records, err = st.ListAppRecoveries(app.ID)
	if err != nil || records[0].Phase != "released" || records[0].Active {
		t.Fatal("pending cancellation did not finish", err)
	}
}
