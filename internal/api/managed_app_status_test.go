package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestOverlayManagedAppStatusesUsesKubernetesObservedState(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_demo",
			Replicas:  2,
		},
		Status: model.AppStatus{
			Phase:            "deployed",
			CurrentRuntimeID: "runtime_demo",
			CurrentReplicas:  2,
		},
	}

	managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managed["metadata"].(map[string]any)["generation"] = float64(1)
	managed["status"] = map[string]any{
		"phase":              runtime.ManagedAppPhaseProgressing,
		"readyReplicas":      1,
		"observedGeneration": 1,
		"message":            "rollout in progress",
	}

	server := newManagedAppTestServer(t, map[string]any{
		"items": []map[string]any{managed},
	})
	defer server.Close()

	apiServer := &Server{
		log: log.New(io.Discard, "", 0),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{
				client:      server.Client(),
				baseURL:     server.URL,
				bearerToken: "test",
			}, nil
		},
	}

	apps := apiServer.overlayManagedAppStatuses(context.Background(), []model.App{app})
	if len(apps) != 1 {
		t.Fatalf("expected one app, got %d", len(apps))
	}
	if apps[0].Status.Phase != "deploying" {
		t.Fatalf("expected phase deploying, got %q", apps[0].Status.Phase)
	}
	if apps[0].Status.CurrentReplicas != 1 {
		t.Fatalf("expected current replicas 1, got %d", apps[0].Status.CurrentReplicas)
	}
	if apps[0].Status.LastMessage != "rollout in progress" {
		t.Fatalf("unexpected last message: %q", apps[0].Status.LastMessage)
	}
}

func TestOverlayManagedAppStatusesPublishesCompleteRuntimeEvidence(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("runtime evidence")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	metadata := managed["metadata"].(map[string]any)
	metadata["generation"] = float64(3)
	managed["status"] = map[string]any{
		"phase":              runtime.ManagedAppPhaseReady,
		"desiredReplicas":    1,
		"readyReplicas":      1,
		"observedGeneration": 3,
		"message":            "ready",
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	deploymentName := runtime.RuntimeAppResourceName(app)
	serviceName := runtime.RuntimeAppServiceName(app)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writeManagedAppClusterIdentity(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/" + runtime.ManagedAppAPIGroup + "/v1alpha1/" + runtime.ManagedAppPlural:
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{managed}})
		case "/api/v1/namespaces":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{"metadata": map[string]any{"name": namespace}}}})
		case "/apis/apps/v1/deployments":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{
				"metadata": map[string]any{"name": deploymentName, "namespace": namespace, "generation": 4},
				"spec": map[string]any{
					"replicas": 1,
					"template": map[string]any{"spec": map[string]any{"containers": []map[string]any{{"name": "demo", "image": app.Spec.Image}}}},
				},
				"status": map[string]any{"replicas": 1, "updatedReplicas": 1, "readyReplicas": 1, "availableReplicas": 1, "observedGeneration": 4},
			}}})
		case "/api/v1/services":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{"metadata": map[string]any{"name": serviceName, "namespace": namespace}}}})
		case "/api/v1/endpoints":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{
				"metadata": map[string]any{"name": serviceName, "namespace": namespace},
				"subsets":  []map[string]any{{"addresses": []map[string]any{{"ip": "10.0.0.1"}}}},
			}}})
		default:
			if strings.Contains(r.URL.Path, "/"+runtime.ManagedAppPlural+"/") {
				_ = json.NewEncoder(w).Encode(managed)
			} else {
				http.NotFound(w, r)
			}
		}
	}))
	defer server.Close()
	apiServer := &Server{
		store: stateStore,
		log:   log.New(io.Discard, "", 0),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{client: server.Client(), baseURL: server.URL, bearerToken: "test"}, nil
		},
	}
	observed := apiServer.overlayManagedAppStatuses(context.Background(), []model.App{app})[0]
	if observed.Status.Phase != "deployed" || observed.ObservedStatus == nil || !observed.ObservedStatus.Fresh {
		t.Fatalf("expected fresh deployed observation, got %#v", observed)
	}
	status := observed.ObservedStatus
	for label, value := range map[string]*bool{
		"namespace": status.NamespacePresent,
		"service":   status.ServicePresent,
		"endpoint":  status.EndpointPresent,
		"ready":     status.EndpointReady,
		"image":     status.ImagePresent,
	} {
		if value != nil && !*value {
			t.Fatalf("expected %s evidence to be present, status=%#v", label, status)
		}
	}
	if status.PhysicalReplicas == nil || *status.PhysicalReplicas != 1 || status.Generation != 3 || status.ObservedGeneration != 3 {
		t.Fatalf("missing physical/generation evidence: %#v", status)
	}
	cached, ok, _ := apiServer.managedAppStatusCache.getList()
	if !ok {
		t.Fatal("expected successful refresh to populate list cache")
	}
	assertManagedAppFullRefreshSequence(t, cached.sequence)
}

func TestFetchManagedAppInventorySequenceOmitsUnperformedStages(t *testing.T) {
	t.Parallel()

	server := newManagedAppTestServer(t, map[string]any{"items": []map[string]any{}})
	defer server.Close()
	apiServer := &Server{
		log:                   log.New(io.Discard, "", 0),
		managedAppStatusCache: newManagedAppStatusCache(time.Minute, time.Second),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{client: server.Client(), baseURL: server.URL, bearerToken: "test"}, nil
		},
	}
	entry, err := apiServer.fetchManagedAppInventory(context.Background())
	if err != nil {
		t.Fatalf("fetch inventory: %v", err)
	}
	sequence := entry.sequence
	if !(sequence.refreshStarted > 0 && sequence.refreshStarted < sequence.managedAppsRead && sequence.managedAppsRead < sequence.refreshCompleted) {
		t.Fatalf("inventory sequence is not ordered: %+v", sequence)
	}
	if sequence.kubeSnapshotRead != 0 || sequence.durableAppsRead != 0 {
		t.Fatalf("inventory refresh fabricated unperformed stages: %+v", sequence)
	}
}

func TestFailedManagedAppInventoryRefreshDoesNotFabricateCompletion(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	var logs strings.Builder
	apiServer := &Server{
		log:                   log.New(&logs, "", 0),
		managedAppStatusCache: newManagedAppStatusCache(time.Minute, time.Second),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{client: server.Client(), baseURL: server.URL, bearerToken: "test"}, nil
		},
	}
	entry, err := apiServer.fetchManagedAppInventory(context.Background())
	if err == nil {
		t.Fatal("expected inventory refresh failure")
	}
	if entry.sequence != (managedAppObservationSequence{}) {
		t.Fatalf("failed refresh returned fabricated completion sequence: %+v", entry.sequence)
	}
	logOutput := logs.String()
	if !strings.Contains(logOutput, `"phase":"start"`) || !strings.Contains(logOutput, `"phase":"end"`) || !strings.Contains(logOutput, `"result":"error"`) {
		t.Fatalf("failed refresh did not record start/error boundary: %s", logOutput)
	}
	if strings.Contains(logOutput, `"result":"success"`) {
		t.Fatalf("failed refresh was logged as completed: %s", logOutput)
	}
}

func assertManagedAppFullRefreshSequence(t *testing.T, sequence managedAppObservationSequence) {
	t.Helper()
	if !(sequence.refreshStarted > 0 &&
		sequence.refreshStarted < sequence.managedAppsRead &&
		sequence.managedAppsRead < sequence.kubeSnapshotRead &&
		sequence.kubeSnapshotRead < sequence.durableAppsRead &&
		sequence.durableAppsRead < sequence.refreshCompleted) {
		t.Fatalf("managed app refresh sequence is not ordered: %+v", sequence)
	}
}

func TestOverlayManagedAppStatusesUsesEndpointSlicesWhenLegacyEndpointsAreUnavailable(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	app := model.App{ID: "app_endpoint_slice", TenantID: "tenant_slice", Name: "slice", Spec: model.AppSpec{
		Image: "nginx:1.27", Ports: []int{8080}, Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID,
	}, Status: model.AppStatus{Phase: "deployed", CurrentReplicas: 1}}
	managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managed["metadata"].(map[string]any)["generation"] = float64(2)
	managed["status"] = map[string]any{"phase": runtime.ManagedAppPhaseReady, "readyReplicas": 1, "desiredReplicas": 1, "observedGeneration": 2}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	serviceName := runtime.RuntimeAppServiceName(app)
	deploymentName := runtime.RuntimeAppResourceName(app)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writeManagedAppClusterIdentity(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/" + runtime.ManagedAppAPIGroup + "/v1alpha1/" + runtime.ManagedAppPlural:
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{managed}})
		case "/api/v1/namespaces":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{"metadata": map[string]any{"name": namespace}}}})
		case "/apis/apps/v1/deployments":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{
				"metadata": map[string]any{"name": deploymentName, "namespace": namespace, "generation": 2},
				"spec":     map[string]any{"replicas": 1, "template": map[string]any{"spec": map[string]any{"containers": []map[string]any{{"image": app.Spec.Image}}}}},
				"status":   map[string]any{"updatedReplicas": 1, "readyReplicas": 1, "availableReplicas": 1, "observedGeneration": 2},
			}}})
		case "/api/v1/services":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{"metadata": map[string]any{"name": serviceName, "namespace": namespace}}}})
		case "/api/v1/endpoints":
			http.NotFound(w, r)
		case "/apis/discovery.k8s.io/v1/endpointslices":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{
				"metadata":  map[string]any{"name": serviceName + "-slice", "namespace": namespace, "labels": map[string]any{"kubernetes.io/service-name": serviceName}},
				"endpoints": []map[string]any{{"addresses": []string{"10.0.0.8"}, "conditions": map[string]any{"ready": true}}},
			}}})
		default:
			if strings.Contains(r.URL.Path, "/"+runtime.ManagedAppPlural+"/") {
				_ = json.NewEncoder(w).Encode(managed)
			} else {
				http.NotFound(w, r)
			}
		}
	}))
	defer server.Close()
	apiServer := &Server{store: stateStore, log: log.New(io.Discard, "", 0), newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
		return &managedAppStatusClient{client: server.Client(), baseURL: server.URL, bearerToken: "test"}, nil
	}}
	updated := apiServer.overlayManagedAppStatus(context.Background(), app)
	if updated.ObservedStatus == nil || updated.Status.Phase != "deployed" {
		t.Fatalf("endpoint-slice observation should keep the app deployed: %+v", updated)
	}
	if updated.ObservedStatus.EndpointPresent == nil || !*updated.ObservedStatus.EndpointPresent || updated.ObservedStatus.EndpointReady == nil || !*updated.ObservedStatus.EndpointReady {
		t.Fatalf("endpoint-slice readiness was not recorded: %+v", updated.ObservedStatus)
	}
}

func TestManagedAppEvidenceDoesNotORStaleLegacyEndpointsWithEmptyEndpointSlices(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	app := model.App{ID: "app_slice_empty", TenantID: "tenant_slice_empty", Spec: model.AppSpec{
		Image: "nginx:1.27", Ports: []int{8080}, Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID,
	}}
	managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managedObject, err := runtime.ManagedAppObjectFromMap(managed)
	if err != nil {
		t.Fatalf("decode managed app: %v", err)
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	serviceKey := kubeNamespacedKey(namespace, runtime.RuntimeAppServiceName(app))
	ready := kubeEndpointRuntimeEvidence{Present: true, ReadyAddresses: 1}
	evidence, err := (&Server{store: stateStore}).buildManagedAppRuntimeEvidence(app, managedObject, true, managedAppKubeSnapshot{
		namespaces:              map[string]struct{}{namespace: {}},
		services:                map[string]struct{}{serviceKey: {}},
		endpoints:               map[string]kubeEndpointRuntimeEvidence{serviceKey: ready},
		endpointSlices:          map[string]kubeEndpointRuntimeEvidence{},
		endpointSlicesAvailable: true,
	})
	if err != nil {
		t.Fatalf("build runtime evidence: %v", err)
	}
	if evidence.endpointReady == nil || *evidence.endpointReady {
		t.Fatalf("empty current EndpointSlice inventory must override stale legacy readiness: %+v", evidence)
	}
}

func TestManagedAppEvidenceTreatsConfiguredRegistryPushAndPullRefsAsSameImage(t *testing.T) {
	t.Parallel()

	const (
		pushBase  = "fugue-fugue-registry.fugue-system.svc.cluster.local:5000"
		pullBase  = "registry.fugue.internal:5000"
		imagePath = "/fugue-apps/demo:image-abc123"
	)
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	app := model.App{
		ID: "app_image_alias", TenantID: "tenant_image_alias", Name: "demo",
		Spec:   model.AppSpec{Image: pullBase + imagePath, Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID},
		Source: &model.AppSource{ResolvedImageRef: pushBase + imagePath},
	}
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("decode managed app: %v", err)
	}
	var deployment kubeDeploymentRuntimeEvidence
	if err := decodeKubeObject(map[string]any{
		"metadata": map[string]any{
			"namespace": runtime.NamespaceForTenant(app.TenantID),
			"name":      runtime.RuntimeAppResourceName(app), "generation": 1,
		},
		"spec": map[string]any{
			"replicas": 1,
			"template": map[string]any{"spec": map[string]any{"containers": []map[string]any{{"image": pushBase + imagePath}}}},
		},
		"status": map[string]any{"updatedReplicas": 1, "readyReplicas": 1, "availableReplicas": 1, "observedGeneration": 1},
	}, &deployment); err != nil {
		t.Fatalf("decode deployment: %v", err)
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	evidence, err := (&Server{
		store: stateStore, registryPushBase: pushBase, registryPullBase: pullBase,
	}).buildManagedAppRuntimeEvidence(app, managed, true, managedAppKubeSnapshot{
		namespaces: map[string]struct{}{namespace: {}},
		deployments: map[string]kubeDeploymentRuntimeEvidence{
			kubeNamespacedKey(namespace, runtime.RuntimeAppResourceName(app)): deployment,
		},
	})
	if err != nil {
		t.Fatalf("build runtime evidence: %v", err)
	}
	if evidence.imagePresent == nil || !*evidence.imagePresent || slices.Contains(evidence.invariantViolations, "current_image_mismatch") {
		t.Fatalf("configured registry aliases must identify the same image: %+v", evidence)
	}
}

func TestManagedAppEvidenceDoesNotCountReadyReplicasFromPreviousRevision(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID: "app_previous_revision", TenantID: "tenant_previous_revision", Name: "demo",
		Spec:   model.AppSpec{Image: "registry.example/demo:v2", Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID},
		Status: model.AppStatus{Phase: "deployed", CurrentReplicas: 1},
	}
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("decode managed app: %v", err)
	}
	managed.Metadata.Generation = 2
	managed.Status = runtime.ManagedAppStatus{
		Phase: runtime.ManagedAppPhaseReady, DesiredReplicas: 1, ReadyReplicas: 1, ObservedGeneration: 2,
	}
	var deployment kubeDeploymentRuntimeEvidence
	if err := decodeKubeObject(map[string]any{
		"metadata": map[string]any{
			"namespace":  runtime.NamespaceForTenant(app.TenantID),
			"name":       runtime.RuntimeAppResourceName(app),
			"generation": 2,
		},
		"spec": map[string]any{
			"replicas": 1,
			"template": map[string]any{"spec": map[string]any{"containers": []map[string]any{{"image": app.Spec.Image}}}},
		},
		// Kubernetes can retain ready/available replicas from the old
		// ReplicaSet while updatedReplicas is still zero.
		"status": map[string]any{
			"updatedReplicas": 0, "readyReplicas": 1, "availableReplicas": 1, "observedGeneration": 2,
		},
	}, &deployment); err != nil {
		t.Fatalf("decode deployment: %v", err)
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	evidence, err := (&Server{}).buildManagedAppRuntimeEvidence(app, managed, true, managedAppKubeSnapshot{
		namespaces: map[string]struct{}{namespace: {}},
		deployments: map[string]kubeDeploymentRuntimeEvidence{
			kubeNamespacedKey(namespace, runtime.RuntimeAppResourceName(app)): deployment,
		},
	})
	if err != nil {
		t.Fatalf("build runtime evidence: %v", err)
	}
	if evidence.physicalReplicas == nil || *evidence.physicalReplicas != 0 {
		t.Fatalf("old-revision ready replicas must not count as current physical evidence: %+v", evidence)
	}
	status := runtime.CalculateAppObservedStatus(app, runtime.AppRuntimeObservation{
		ManagedApp: managed, Found: true, Complete: true, Fresh: true,
		ObservedAt: time.Now().UTC(), ClusterID: "cluster-current",
		NamespacePresent: evidence.namespacePresent, PhysicalReplicas: evidence.physicalReplicas,
		PhysicalDesiredReplicas: evidence.physicalDesiredReplicas,
		ImagePresent:            evidence.imagePresent, InvariantViolations: evidence.invariantViolations,
	})
	if status.Phase != "unavailable" || !slices.Contains(status.InvariantViolations, "physical_replicas_zero") {
		t.Fatalf("previous-revision replicas must fail the deployed invariant: %+v", status)
	}
}

func TestOverlayManagedAppStatusesTreatsChildQueryFailureAsUnknown(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	app := model.App{ID: "app_child_failure", TenantID: "tenant_demo", Name: "child-failure", Spec: model.AppSpec{Image: "nginx:1.27", Ports: []int{80}, Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID}, Status: model.AppStatus{Phase: "deployed", CurrentReplicas: 1}}
	managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managed["status"] = map[string]any{"phase": runtime.ManagedAppPhaseReady, "readyReplicas": 1, "desiredReplicas": 1, "observedGeneration": 1}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writeManagedAppClusterIdentity(w, r) {
			return
		}
		if r.URL.Path == "/api/v1/namespaces" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{}})
			return
		}
		if strings.HasPrefix(r.URL.Path, "/apis/"+runtime.ManagedAppAPIGroup+"/v1alpha1/"+runtime.ManagedAppPlural) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{managed}})
			return
		}
		http.Error(w, "kubernetes child query failed", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	apiServer := &Server{store: stateStore, log: log.New(io.Discard, "", 0), newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
		return &managedAppStatusClient{client: server.Client(), baseURL: server.URL, bearerToken: "test"}, nil
	}}
	updated := apiServer.overlayManagedAppStatuses(context.Background(), []model.App{app})[0]
	if updated.ObservedStatus == nil || updated.ObservedStatus.Phase != "unknown" || updated.ObservedStatus.Fresh || updated.ObservedStatus.RuntimeObjectPresent != nil {
		t.Fatalf("child query failure must be unknown without absence claims: %#v", updated.ObservedStatus)
	}
}

func TestAppMayUseManagedRuntimeChecksDesiredAndCurrentRuntimeDuringMigration(t *testing.T) {
	app := model.App{
		Spec:   model.AppSpec{RuntimeID: "runtime_target_managed"},
		Status: model.AppStatus{CurrentRuntimeID: "runtime_source_external"},
	}
	runtimes := map[string]model.Runtime{
		"runtime_source_external": {ID: "runtime_source_external", Type: model.RuntimeTypeExternalOwned},
		"runtime_target_managed":  {ID: "runtime_target_managed", Type: model.RuntimeTypeManagedOwned},
	}
	if !appMayUseManagedRuntime(app, runtimes) {
		t.Fatal("migration with a managed desired runtime must still receive live status observation")
	}
}

func TestCurrentManagedImagePresenceRequiresFreshExplicitEvidence(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	app := model.App{ID: "app_image_evidence", TenantID: "tenant_demo"}
	imageRef := "registry.fugue.internal:5000/fugue-apps/demo@sha256:" + strings.Repeat("a", 64)
	apiServer := &Server{store: stateStore}

	app.Spec.RuntimeID = "runtime-target"
	present, err := apiServer.currentManagedImagePresence(app, imageRef, app.Spec.RuntimeID)
	if err != nil {
		t.Fatalf("read empty image evidence: %v", err)
	}
	if present != nil {
		t.Fatalf("absence of a report must remain unknown, got %v", *present)
	}

	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:  app.TenantID,
		AppID:     app.ID,
		ImageRef:  imageRef,
		RuntimeID: "runtime-target",
		Status:    model.ImageLocationStatusMissing,
	}); err != nil {
		t.Fatalf("record missing image: %v", err)
	}
	present, err = apiServer.currentManagedImagePresence(app, imageRef, app.Spec.RuntimeID)
	if err != nil {
		t.Fatalf("read missing image evidence: %v", err)
	}
	if present == nil || *present {
		t.Fatalf("fresh explicit missing report must be false, got %v", present)
	}

	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:  app.TenantID,
		AppID:     app.ID,
		ImageRef:  imageRef,
		RuntimeID: "runtime-target",
		Status:    model.ImageLocationStatusPresent,
	}); err != nil {
		t.Fatalf("record present image: %v", err)
	}
	present, err = apiServer.currentManagedImagePresence(app, imageRef, app.Spec.RuntimeID)
	if err != nil {
		t.Fatalf("read present image evidence: %v", err)
	}
	if present == nil || !*present {
		t.Fatalf("fresh present report must be true, got %v", present)
	}
}

func TestCurrentManagedImagePresenceDoesNotUseSourceRuntimeEvidence(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	app := model.App{ID: "app_image_migration", TenantID: "tenant_demo", Spec: model.AppSpec{RuntimeID: "runtime-target"}}
	imageRef := "registry.fugue.internal:5000/fugue-apps/demo@sha256:" + strings.Repeat("b", 64)
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID: app.TenantID, AppID: app.ID, ImageRef: imageRef,
		RuntimeID: "runtime-source", Status: model.ImageLocationStatusPresent,
	}); err != nil {
		t.Fatalf("record source image evidence: %v", err)
	}
	apiServer := &Server{store: stateStore}
	present, err := apiServer.currentManagedImagePresence(app, imageRef, app.Spec.RuntimeID)
	if err != nil {
		t.Fatalf("read target image evidence: %v", err)
	}
	if present != nil {
		t.Fatalf("source-runtime evidence must not prove target image presence, got %v", *present)
	}
}

func TestManagedAppRuntimeEvidenceRejectsUnobservedDeploymentGeneration(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	app := model.App{
		ID:       "app_generation_guard",
		TenantID: "tenant_demo",
		Name:     "generation-guard",
		Spec: model.AppSpec{
			Image:     "registry.fugue.internal:5000/fugue-apps/generation-guard:latest",
			Replicas:  1,
			RuntimeID: model.DefaultManagedRuntimeID,
		},
		Status: model.AppStatus{Phase: "deployed", CurrentReplicas: 1},
	}
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("build managed app: %v", err)
	}
	managed.Metadata.Generation = 3
	managed.Status = runtime.ManagedAppStatus{
		Phase:              runtime.ManagedAppPhaseReady,
		DesiredReplicas:    1,
		ReadyReplicas:      1,
		ObservedGeneration: 3,
	}
	var deployment kubeDeploymentRuntimeEvidence
	if err := decodeKubeObject(map[string]any{
		"metadata": map[string]any{
			"namespace":  runtime.NamespaceForTenant(app.TenantID),
			"name":       runtime.RuntimeAppResourceName(app),
			"generation": 9,
		},
		"spec": map[string]any{
			"replicas": 1,
			"template": map[string]any{"spec": map[string]any{"containers": []map[string]any{{"name": app.Name, "image": app.Spec.Image}}}},
		},
		"status": map[string]any{"readyReplicas": 1, "availableReplicas": 1, "observedGeneration": 8},
	}, &deployment); err != nil {
		t.Fatalf("decode deployment: %v", err)
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	evidence, err := (&Server{store: stateStore}).buildManagedAppRuntimeEvidence(app, managed, true, managedAppKubeSnapshot{
		namespaces:  map[string]struct{}{namespace: {}},
		deployments: map[string]kubeDeploymentRuntimeEvidence{kubeNamespacedKey(namespace, runtime.RuntimeAppResourceName(app)): deployment},
		services:    map[string]struct{}{},
		endpoints:   map[string]kubeEndpointRuntimeEvidence{},
	})
	if err != nil {
		t.Fatalf("build evidence: %v", err)
	}
	if !slices.Contains(evidence.invariantViolations, "deployment_generation_unobserved") {
		t.Fatalf("missing deployment generation invariant: %+v", evidence.invariantViolations)
	}
	status := runtime.CalculateAppObservedStatus(app, runtime.AppRuntimeObservation{
		ManagedApp:              managed,
		Found:                   true,
		Complete:                true,
		Fresh:                   true,
		ObservedAt:              time.Now().UTC(),
		ClusterID:               "cluster-test-uid",
		NamespacePresent:        evidence.namespacePresent,
		PhysicalReplicas:        evidence.physicalReplicas,
		PhysicalDesiredReplicas: evidence.physicalDesiredReplicas,
		ImagePresent:            evidence.imagePresent,
		InvariantViolations:     evidence.invariantViolations,
	})
	if status.Phase != "unavailable" {
		t.Fatalf("unobserved deployment generation must fail closed, got %+v", status)
	}
}

func TestManagedAppRuntimeEvidenceRejectsForeignManagedAppIdentity(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	app := model.App{
		ID: "app_identity_guard", TenantID: "tenant_identity", Name: "identity-guard",
		Spec: model.AppSpec{Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID},
	}
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("decode managed app: %v", err)
	}
	managed.Spec.AppID = "app-foreign"
	managed.Metadata.Generation = 1
	managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseReady, ReadyReplicas: 1, ObservedGeneration: 1}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	evidence, err := (&Server{store: stateStore}).buildManagedAppRuntimeEvidence(app, managed, true, managedAppKubeSnapshot{
		namespaces:  map[string]struct{}{namespace: {}},
		deployments: map[string]kubeDeploymentRuntimeEvidence{},
		services:    map[string]struct{}{},
		endpoints:   map[string]kubeEndpointRuntimeEvidence{},
	})
	if err != nil {
		t.Fatalf("build identity evidence: %v", err)
	}
	status := runtime.CalculateAppObservedStatus(app, runtime.AppRuntimeObservation{
		ManagedApp: managed, Found: true, Complete: true, Fresh: true,
		ObservedAt: time.Now().UTC(), ClusterID: "cluster-identity",
		InvariantViolations: evidence.invariantViolations,
	})
	if status.Phase != "unavailable" || !slices.Contains(status.InvariantViolations, "managed_app_identity_mismatch") {
		t.Fatalf("foreign ManagedApp identity must fail closed: %+v", status)
	}
}

func TestOverlayManagedAppStatusesPublishesAuthoritativeAbsenceAndKeepsStoredState(t *testing.T) {
	app := model.App{
		ID:       "app_missing",
		TenantID: "tenant_demo",
		Name:     "missing",
		Spec:     model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1},
		Status:   model.AppStatus{Phase: "deployed", CurrentReplicas: 1},
	}
	server := newManagedAppTestServer(t, map[string]any{"items": []map[string]any{}})
	defer server.Close()
	apiServer := &Server{
		log: log.New(io.Discard, "", 0),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{client: server.Client(), baseURL: server.URL, bearerToken: "test"}, nil
		},
	}

	updated := apiServer.overlayManagedAppStatuses(context.Background(), []model.App{app})[0]
	if updated.Status.Phase != "unavailable" || updated.Status.CurrentReplicas != 0 {
		t.Fatalf("authoritative missing object must be unavailable/zero, got %+v", updated.Status)
	}
	if updated.StoredStatus == nil || updated.StoredStatus.Phase != "deployed" || updated.StoredStatus.CurrentReplicas != 1 {
		t.Fatalf("stored state was not preserved separately: %+v", updated.StoredStatus)
	}
	if updated.ObservedStatus == nil || !updated.ObservedStatus.Fresh || updated.ObservedStatus.ClusterID != "cluster-test-uid" {
		t.Fatalf("missing observation lacks complete evidence: %+v", updated.ObservedStatus)
	}
	if updated.ObservedStatus.RuntimeObjectPresent == nil || *updated.ObservedStatus.RuntimeObjectPresent {
		t.Fatalf("missing observation did not record runtime_object_present=false: %+v", updated.ObservedStatus)
	}
}

func TestOverlayManagedAppStatusConfirmsPoint404WithCompleteInventory(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_point_404",
		TenantID: "tenant_demo",
		Name:     "point-404",
		Spec:     model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1},
		Status:   model.AppStatus{Phase: "deployed", CurrentReplicas: 1},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writeManagedAppClusterIdentity(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/namespaces/") {
			http.NotFound(w, r)
			return
		}
		if r.URL.Path == "/apis/"+runtime.ManagedAppAPIGroup+"/v1alpha1/"+runtime.ManagedAppPlural {
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{}})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	apiServer := &Server{
		log: log.New(io.Discard, "", 0),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{client: server.Client(), baseURL: server.URL, bearerToken: "test"}, nil
		},
	}

	updated := apiServer.overlayManagedAppStatus(context.Background(), app)
	if updated.ObservedStatus == nil || updated.ObservedStatus.Phase != "unavailable" || !updated.ObservedStatus.Fresh {
		t.Fatalf("complete inventory absence must be unavailable: %+v", updated.ObservedStatus)
	}
	if updated.ObservedStatus.RuntimeObjectPresent == nil || *updated.ObservedStatus.RuntimeObjectPresent {
		t.Fatalf("complete inventory absence must record false runtime presence: %+v", updated.ObservedStatus)
	}
}

func TestManagedAppInventoryRejectsIncompleteIdentity(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{{"metadata": map[string]any{"name": "orphan"}}},
		})
	}))
	defer server.Close()
	client := &managedAppStatusClient{client: server.Client(), baseURL: server.URL, bearerToken: "test"}
	if _, err := client.listObservedManagedAppsByAppID(context.Background()); err == nil || !strings.Contains(err.Error(), "identity is incomplete") {
		t.Fatalf("malformed inventory must fail closed instead of proving app absence: %v", err)
	}
}

func TestOverlayManagedAppStatusDoesNotTreatCRD404AsAuthoritativeAbsence(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_crd_404",
		TenantID: "tenant_demo",
		Name:     "crd-404",
		Spec:     model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1},
		Status:   model.AppStatus{Phase: "deployed", CurrentReplicas: 1},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writeManagedAppClusterIdentity(w, r) {
			return
		}
		if strings.Contains(r.URL.Path, "/namespaces/") || r.URL.Path == "/apis/"+runtime.ManagedAppAPIGroup+"/v1alpha1/"+runtime.ManagedAppPlural {
			http.Error(w, "managed app API unavailable", http.StatusNotFound)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	apiServer := &Server{
		log: log.New(io.Discard, "", 0),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{client: server.Client(), baseURL: server.URL, bearerToken: "test"}, nil
		},
	}

	updated := apiServer.overlayManagedAppStatus(context.Background(), app)
	if updated.ObservedStatus == nil || updated.ObservedStatus.Phase != "unknown" || updated.ObservedStatus.Fresh {
		t.Fatalf("CRD/API 404 must remain unknown: %+v", updated.ObservedStatus)
	}
	if updated.ObservedStatus.RuntimeObjectPresent != nil {
		t.Fatalf("CRD/API 404 must not claim object absence: %+v", updated.ObservedStatus)
	}
}

func TestOverlayManagedAppStatusesReportsUnknownOnKubernetesQueryFailure(t *testing.T) {
	app := model.App{
		ID:       "app_unknown",
		TenantID: "tenant_demo",
		Name:     "unknown",
		Spec:     model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1},
		Status:   model.AppStatus{Phase: "deployed", CurrentReplicas: 1},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writeManagedAppClusterIdentity(w, r) {
			return
		}
		http.Error(w, "kubernetes unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	apiServer := &Server{
		log: log.New(io.Discard, "", 0),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{client: server.Client(), baseURL: server.URL, bearerToken: "test"}, nil
		},
	}

	updated := apiServer.overlayManagedAppStatuses(context.Background(), []model.App{app})[0]
	if updated.Status.Phase != "unknown" || updated.Status.CurrentReplicas != 0 {
		t.Fatalf("query failure must be unknown/zero projection, got %+v", updated.Status)
	}
	if updated.ObservedStatus == nil || updated.ObservedStatus.Fresh || updated.ObservedStatus.RuntimeObjectPresent != nil {
		t.Fatalf("query failure was treated as absence: %+v", updated.ObservedStatus)
	}
	if updated.StoredStatus == nil || updated.StoredStatus.Phase != "deployed" {
		t.Fatalf("query failure lost durable state: %+v", updated.StoredStatus)
	}
}

func TestOverlayManagedAppStatusesFollowsCompleteKubernetesPagination(t *testing.T) {
	app := model.App{
		ID:       "app_page_two",
		TenantID: "tenant_demo",
		Name:     "page-two",
		Spec:     model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1},
		Status:   model.AppStatus{Phase: "deployed", CurrentReplicas: 1},
	}
	other := app
	other.ID = "app_page_one"
	other.Name = "page-one"
	firstMap := runtime.BuildManagedAppObject(other, runtime.SchedulingConstraints{})
	secondMap := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	for _, raw := range []map[string]any{firstMap, secondMap} {
		metadata, _ := raw["metadata"].(map[string]any)
		metadata["generation"] = float64(1)
		raw["status"] = map[string]any{
			"phase":              runtime.ManagedAppPhaseReady,
			"desiredReplicas":    1,
			"readyReplicas":      1,
			"observedGeneration": 1,
		}
	}
	var listCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writeManagedAppClusterIdentity(w, r) {
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/apis/"+runtime.ManagedAppAPIGroup+"/v1alpha1/"+runtime.ManagedAppPlural) {
			http.NotFound(w, r)
			return
		}
		listCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("continue") == "page-two" {
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{secondMap}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items":    []map[string]any{firstMap},
			"metadata": map[string]any{"continue": "page-two"},
		})
	}))
	defer server.Close()
	apiServer := &Server{
		log: log.New(io.Discard, "", 0),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{client: server.Client(), baseURL: server.URL, bearerToken: "test"}, nil
		},
	}

	updated := apiServer.overlayManagedAppStatuses(context.Background(), []model.App{app})[0]
	if updated.ObservedStatus == nil || !updated.ObservedStatus.Fresh || updated.Status.Phase != "deployed" {
		t.Fatalf("object on final list page was not observed as deployed: %+v", updated)
	}
	if got := listCalls.Load(); got != 2 {
		t.Fatalf("expected complete pagination before absence decision, got %d list calls", got)
	}
}

func TestOverlayManagedAppStatusesReportsUnknownWhenClusterChangesDuringObservation(t *testing.T) {
	app := model.App{
		ID:       "app_cluster_change",
		TenantID: "tenant_demo",
		Name:     "cluster-change",
		Spec:     model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1},
		Status:   model.AppStatus{Phase: "deployed", CurrentReplicas: 1},
	}
	var identityCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/namespaces/kube-system" {
			identityCalls.Add(1)
			uid := "cluster-a"
			if identityCalls.Load() > 1 {
				uid = "cluster-b"
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": uid}})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{}})
	}))
	defer server.Close()
	apiServer := &Server{
		log: log.New(io.Discard, "", 0),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{client: server.Client(), baseURL: server.URL, bearerToken: "test"}, nil
		},
	}

	updated := apiServer.overlayManagedAppStatuses(context.Background(), []model.App{app})[0]
	if updated.ObservedStatus == nil || updated.ObservedStatus.Phase != "unknown" || updated.ObservedStatus.Fresh {
		t.Fatalf("cluster identity change must invalidate the observation, got %+v", updated.ObservedStatus)
	}
	if updated.ObservedStatus.RuntimeObjectPresent != nil {
		t.Fatalf("cluster identity change must not claim object absence: %+v", updated.ObservedStatus)
	}
}

func TestOverlayManagedAppStatusUsesSingleObjectLookup(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_demo",
			Replicas:  1,
		},
		Status: model.AppStatus{
			Phase:            "deployed",
			CurrentRuntimeID: "runtime_demo",
			CurrentReplicas:  1,
		},
	}

	managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managed["metadata"].(map[string]any)["generation"] = float64(1)
	managed["status"] = map[string]any{
		"phase":              runtime.ManagedAppPhaseReady,
		"readyReplicas":      1,
		"observedGeneration": 1,
		"message":            "deployment ready",
	}

	server := newManagedAppTestServer(t, managed)
	defer server.Close()

	apiServer := &Server{
		log: log.New(io.Discard, "", 0),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{
				client:      server.Client(),
				baseURL:     server.URL,
				bearerToken: "test",
			}, nil
		},
	}

	updated := apiServer.overlayManagedAppStatus(context.Background(), app)
	if updated.Status.Phase != "deployed" {
		t.Fatalf("expected phase deployed, got %q", updated.Status.Phase)
	}
	if updated.Status.CurrentReplicas != 1 {
		t.Fatalf("expected one ready replica, got %d", updated.Status.CurrentReplicas)
	}
	if updated.Status.LastMessage != "deployment ready" {
		t.Fatalf("unexpected last message: %q", updated.Status.LastMessage)
	}
}

func TestLoadConsoleAppsUsesManagedAppOverlay(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Console Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: tenantSharedRuntimeID,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managed["status"] = map[string]any{
		"phase":         runtime.ManagedAppPhaseError,
		"message":       "startup failed",
		"readyReplicas": 0,
	}
	setManagedAppTestObservedGeneration(managed, 1)

	server := newManagedAppTestServer(t, map[string]any{
		"items": []map[string]any{managed},
	})
	defer server.Close()

	apiServer := &Server{
		store: stateStore,
		log:   log.New(io.Discard, "", 0),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{
				client:      server.Client(),
				baseURL:     server.URL,
				bearerToken: "test",
			}, nil
		},
	}

	apps, err := apiServer.loadConsoleApps(context.Background(), model.Principal{
		TenantID: tenant.ID,
	}, true, false)
	if err != nil {
		t.Fatalf("load console apps: %v", err)
	}
	if len(apps) != 1 {
		t.Fatalf("expected one console app, got %d", len(apps))
	}
	if apps[0].Status.Phase != "failed" {
		t.Fatalf("expected failed phase from managed app overlay, got %q", apps[0].Status.Phase)
	}
	if apps[0].Status.LastMessage != "startup failed" {
		t.Fatalf("expected managed app message, got %q", apps[0].Status.LastMessage)
	}
}

func TestOverlayManagedAppStatusesUsesTTLCache(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec:     model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1},
		Status: model.AppStatus{
			Phase: "deployed",
		},
	}

	managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managed["status"] = map[string]any{
		"phase":         runtime.ManagedAppPhaseError,
		"message":       "cached failure",
		"readyReplicas": 0,
	}
	setManagedAppTestObservedGeneration(managed, 1)

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writeManagedAppClusterIdentity(w, r) {
			return
		}
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{managed},
		})
	}))
	defer server.Close()

	apiServer := &Server{
		log:                   log.New(io.Discard, "", 0),
		managedAppStatusCache: newManagedAppStatusCache(time.Minute, time.Second),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{
				client:      server.Client(),
				baseURL:     server.URL,
				bearerToken: "test",
			}, nil
		},
	}

	first := apiServer.overlayManagedAppStatuses(context.Background(), []model.App{app})
	second := apiServer.overlayManagedAppStatuses(context.Background(), []model.App{app})

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected one list lookup within TTL, got %d", got)
	}
	if first[0].Status.Phase != "failed" || second[0].Status.Phase != "failed" {
		t.Fatalf("expected cached managed status overlay, got %q and %q", first[0].Status.Phase, second[0].Status.Phase)
	}
}

func TestFetchManagedAppStatusesClosesIdleKubeConnections(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec:     model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1},
		Status: model.AppStatus{
			Phase: "deployed",
		},
	}

	managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	server := newManagedAppTestServer(t, map[string]any{
		"items": []map[string]any{managed},
	})
	defer server.Close()

	kubeClient := server.Client()
	tracker := &closeTrackingTransport{base: kubeClient.Transport}
	kubeClient.Transport = tracker

	apiServer := &Server{
		log:                   log.New(io.Discard, "", 0),
		managedAppStatusCache: newManagedAppStatusCache(time.Minute, time.Second),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{
				client:      kubeClient,
				baseURL:     server.URL,
				bearerToken: "test",
			}, nil
		},
	}

	entry, err := apiServer.fetchManagedAppStatuses(context.Background())
	if err != nil {
		t.Fatalf("fetch managed app statuses: %v", err)
	}
	if !entry.ok || len(entry.items) != 1 {
		t.Fatalf("expected fetched managed app status entry, got %#v", entry)
	}
	if tracker.closeCount.Load() == 0 {
		t.Fatal("expected managed app status refresh to close idle kubernetes connections")
	}
}

func TestOverlayManagedAppStatusFallsBackToStaleCacheOnRefreshError(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec:     model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1},
		Status: model.AppStatus{
			Phase: "deployed",
		},
	}

	managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managed["status"] = map[string]any{
		"phase":         runtime.ManagedAppPhaseError,
		"message":       "startup failed",
		"readyReplicas": 0,
	}
	setManagedAppTestObservedGeneration(managed, 1)

	server := newManagedAppTestServer(t, managed)
	defer server.Close()

	apiServer := &Server{
		log:                   log.New(io.Discard, "", 0),
		managedAppStatusCache: newManagedAppStatusCache(time.Minute, time.Second),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{
				client:      server.Client(),
				baseURL:     server.URL,
				bearerToken: "test",
			}, nil
		},
	}

	updated := apiServer.overlayManagedAppStatus(context.Background(), app)
	if updated.Status.Phase != "failed" {
		t.Fatalf("expected initial cached status to be failed, got %q", updated.Status.Phase)
	}

	key := managedAppStatusCacheKey(app)
	apiServer.managedAppStatusCache.mu.Lock()
	entry := apiServer.managedAppStatusCache.byApp[key]
	entry.expiresAt = time.Now().Add(-time.Second)
	apiServer.managedAppStatusCache.byApp[key] = entry
	apiServer.managedAppStatusCache.mu.Unlock()

	apiServer.newManagedAppStatusClient = func() (*managedAppStatusClient, error) {
		return nil, errors.New("boom")
	}

	stale := apiServer.overlayManagedAppStatus(context.Background(), app)
	if stale.Status.Phase != "unknown" || stale.ObservedStatus == nil || stale.ObservedStatus.Fresh {
		t.Fatalf("expected failed refresh to make the live status unknown, got %+v", stale)
	}
	if stale.ObservedStatus.Reason != runtime.AppObservationReasonObservationStale {
		t.Fatalf("expected stale observation reason, got %+v", stale.ObservedStatus)
	}
}

func TestOverlayManagedAppStatusUsesSingleflight(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec:     model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1},
		Status: model.AppStatus{
			Phase: "deployed",
		},
	}

	managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managed["status"] = map[string]any{
		"phase":         runtime.ManagedAppPhaseReady,
		"message":       "deployment ready",
		"readyReplicas": 1,
	}
	setManagedAppTestObservedGeneration(managed, 1)

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writeManagedAppClusterIdentity(w, r) {
			return
		}
		calls.Add(1)
		time.Sleep(150 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(managed)
	}))
	defer server.Close()

	apiServer := &Server{
		log:                   log.New(io.Discard, "", 0),
		managedAppStatusCache: newManagedAppStatusCache(time.Minute, time.Second),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{
				client:      server.Client(),
				baseURL:     server.URL,
				bearerToken: "test",
			}, nil
		},
	}

	const workers = 6
	var wg sync.WaitGroup
	results := make(chan model.App, workers)
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			results <- apiServer.overlayManagedAppStatus(context.Background(), app)
		}()
	}
	wg.Wait()
	close(results)

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected singleflight to collapse requests to one kube GET, got %d", got)
	}
	for result := range results {
		if result.Status.LastMessage != "deployment ready" {
			t.Fatalf("expected overlaid status for every waiter, got %q", result.Status.LastMessage)
		}
	}
}

func TestOverlayManagedAppStatusCachedReturnsImmediatelyAndRefreshesInBackground(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec:     model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1},
		Status: model.AppStatus{
			Phase:            "deployed",
			CurrentReplicas:  1,
			CurrentRuntimeID: "runtime_demo",
		},
	}

	managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managed["status"] = map[string]any{
		"phase":         runtime.ManagedAppPhaseError,
		"message":       "background refresh",
		"readyReplicas": 0,
	}
	setManagedAppTestObservedGeneration(managed, 1)

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writeManagedAppClusterIdentity(w, r) {
			return
		}
		calls.Add(1)
		if want := "/apis/" + runtime.ManagedAppAPIGroup + "/v1alpha1/" + runtime.ManagedAppPlural; r.URL.Path != want {
			t.Fatalf("expected background refresh to list managed apps at %s, got %s", want, r.URL.Path)
		}
		time.Sleep(150 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{managed},
		})
	}))
	defer server.Close()

	apiServer := &Server{
		log:                   log.New(io.Discard, "", 0),
		managedAppStatusCache: newManagedAppStatusCache(time.Minute, time.Second),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{
				client:      server.Client(),
				baseURL:     server.URL,
				bearerToken: "test",
			}, nil
		},
	}

	startedAt := time.Now()
	first := apiServer.overlayManagedAppStatusCached(app)
	if elapsed := time.Since(startedAt); elapsed > 75*time.Millisecond {
		t.Fatalf("expected hot-path read to return immediately, took %s", elapsed)
	}
	if first.Status.Phase != "unknown" || first.ObservedStatus == nil || first.ObservedStatus.Fresh {
		t.Fatalf("expected first hot-path read to report unknown live state, got %+v", first)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if calls.Load() == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected one background refresh, got %d", got)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cached, ok, _ := apiServer.managedAppStatusCache.getApp(managedAppStatusCacheKey(app))
		if ok && cached.found {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	second := apiServer.overlayManagedAppStatusCached(app)
	if second.Status.Phase != "failed" {
		t.Fatalf("expected hot-path read to use refreshed cache, got %q", second.Status.Phase)
	}
	if second.Status.LastMessage != "background refresh" {
		t.Fatalf("expected refreshed cache message, got %q", second.Status.LastMessage)
	}
}

func TestOverlayManagedAppStatusCachedCoalescesBackgroundListRefresh(t *testing.T) {
	t.Parallel()

	apps := []model.App{
		{ID: "app_demo_1", TenantID: "tenant_demo", Name: "demo-1", Spec: model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1}, Status: model.AppStatus{Phase: "deployed"}},
		{ID: "app_demo_2", TenantID: "tenant_demo", Name: "demo-2", Spec: model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1}, Status: model.AppStatus{Phase: "deployed"}},
		{ID: "app_demo_3", TenantID: "tenant_demo", Name: "demo-3", Spec: model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1}, Status: model.AppStatus{Phase: "deployed"}},
	}
	var managedItems []map[string]any
	for i, app := range apps {
		managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
		managed["status"] = map[string]any{
			"phase":         runtime.ManagedAppPhaseReady,
			"message":       "ready " + app.ID,
			"readyReplicas": i + 1,
		}
		setManagedAppTestObservedGeneration(managed, 1)
		managedItems = append(managedItems, managed)
	}

	var listCalls atomic.Int32
	var getCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case writeManagedAppClusterIdentity(w, r):
			return
		case r.URL.Path == "/apis/"+runtime.ManagedAppAPIGroup+"/v1alpha1/"+runtime.ManagedAppPlural:
			listCalls.Add(1)
			time.Sleep(150 * time.Millisecond)
			_ = json.NewEncoder(w).Encode(map[string]any{"items": managedItems})
		case strings.Contains(r.URL.Path, "/namespaces/"):
			getCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{})
		default:
			t.Fatalf("unexpected Kubernetes API path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	apiServer := &Server{
		log:                   log.New(io.Discard, "", 0),
		managedAppStatusCache: newManagedAppStatusCache(time.Minute, time.Second),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{
				client:      server.Client(),
				baseURL:     server.URL,
				bearerToken: "test",
			}, nil
		},
	}

	for _, app := range apps {
		first := apiServer.overlayManagedAppStatusCached(app)
		if first.Status.Phase != "unknown" || first.ObservedStatus == nil || first.ObservedStatus.Fresh {
			t.Fatalf("expected first hot-path read to report unknown live state, got %+v", first)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if listCalls.Load() == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("expected one coalesced list refresh, got %d", got)
	}
	if got := getCalls.Load(); got != 0 {
		t.Fatalf("expected no per-app managed app GETs, got %d", got)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cached, ok, _ := apiServer.managedAppStatusCache.getList()
		if ok && len(cached.items) == len(apps) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	updated := apiServer.overlayManagedAppStatusesCached(apps)
	if len(updated) != len(apps) {
		t.Fatalf("expected %d apps, got %d", len(apps), len(updated))
	}
	for i, app := range updated {
		if app.Status.Phase != "deployed" {
			t.Fatalf("expected app %d phase deployed from managed status, got %q", i, app.Status.Phase)
		}
		if app.Status.LastMessage != "ready "+apps[i].ID {
			t.Fatalf("expected app %d cached message, got %q", i, app.Status.LastMessage)
		}
	}
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("expected cached list to be reused, got %d list refreshes", got)
	}
}

func TestOverlayManagedAppStatusesForEdgeRoutesKeepsVerifiedErrorServingAndMarksMissingUnavailable(t *testing.T) {
	t.Parallel()

	present := model.App{
		ID:       "app_present",
		TenantID: "tenant_demo",
		Name:     "present",
		Spec: model.AppSpec{
			RuntimeID: model.DefaultManagedRuntimeID,
			Replicas:  1,
		},
		Status: model.AppStatus{Phase: "deployed", CurrentReplicas: 1},
	}
	missing := present
	missing.ID = "app_missing"
	missing.Name = "missing"

	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(present, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("build managed app: %v", err)
	}
	managed.Status = runtime.ManagedAppStatus{
		Phase:              runtime.ManagedAppPhaseError,
		Message:            "deploy image preflight failed",
		ReadyReplicas:      1,
		ObservedGeneration: 1,
	}
	managed.Metadata.Generation = 1

	apiServer := &Server{managedAppStatusCache: newManagedAppStatusCache(time.Minute, time.Second)}
	now := time.Now()
	apiServer.managedAppStatusCache.setList(managedAppStatusListCacheEntry{
		items:       map[string]runtime.ManagedAppObject{present.ID: managed},
		ok:          true,
		clusterID:   "cluster-test-uid",
		refreshedAt: now,
		expiresAt:   now.Add(time.Minute),
	})
	runtimes := map[string]model.Runtime{
		model.DefaultManagedRuntimeID: {
			ID:   model.DefaultManagedRuntimeID,
			Type: model.RuntimeTypeManagedShared,
		},
	}

	updated := apiServer.overlayManagedAppStatusesForEdgeRoutesCached([]model.App{present, missing}, runtimes)
	if updated[0].Status.CurrentReplicas != 1 || updated[0].Status.Phase != "failed" {
		t.Fatalf("expected verified serving replicas and the operator-visible error to coexist, got %+v", updated[0].Status)
	}
	if routeStatus, reason := edgeRouteStatus(updated[0], model.DefaultManagedRuntimeID, true); routeStatus != model.EdgeRouteStatusActive || reason != "" {
		t.Fatalf("expected verified serving error status to retain an active edge route, got status=%q reason=%q", routeStatus, reason)
	}
	if updated[1].Status.CurrentReplicas != 0 || updated[1].Status.Phase != "unavailable" || !strings.Contains(updated[1].Status.LastMessage, "not found") {
		t.Fatalf("expected missing managed app to become unavailable for edge publication, got %+v", updated[1].Status)
	}
}

func TestOverlayManagedAppStatusesForEdgeRoutesKeepsRecentExpiredObservationActive(t *testing.T) {
	t.Parallel()

	failedOperation := &model.AppOperationFailure{ID: "op_failed", Type: "deploy"}
	app := model.App{
		ID:       "app_recent_observation",
		TenantID: "tenant_demo",
		Name:     "recent-observation",
		Route:    &model.AppRoute{Hostname: "recent-observation.example", PathPrefix: "/", ServicePort: 8000},
		Spec: model.AppSpec{
			Image:     "registry.example/recent-observation:v1",
			Ports:     []int{8000},
			RuntimeID: model.DefaultManagedRuntimeID,
			Replicas:  1,
		},
		Status: model.AppStatus{
			Phase:               "unknown",
			CurrentReplicas:     1,
			LastOperationID:     failedOperation.ID,
			LastFailedOperation: failedOperation,
		},
	}
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("build managed app: %v", err)
	}
	managed.Metadata.Generation = 1
	managed.Status = runtime.ManagedAppStatus{
		Phase:              runtime.ManagedAppPhaseReady,
		ReadyReplicas:      1,
		ObservedGeneration: 1,
	}
	present := true
	replicas := 1
	now := time.Now().UTC()
	apiServer := &Server{
		log:                   log.New(io.Discard, "", 0),
		managedAppStatusCache: newManagedAppStatusCache(time.Second, time.Second),
	}
	apiServer.managedAppStatusCache.setList(managedAppStatusListCacheEntry{
		items:     map[string]runtime.ManagedAppObject{app.ID: managed},
		ok:        true,
		clusterID: "cluster-test-uid",
		evidence: map[string]managedAppRuntimeEvidence{
			app.ID: {
				namespacePresent:        &present,
				servicePresent:          &present,
				endpointPresent:         &present,
				endpointReady:           &present,
				physicalReplicas:        &replicas,
				physicalDesiredReplicas: &replicas,
				imagePresent:            &present,
				imageRef:                app.Spec.Image,
			},
		},
		refreshedAt: now.Add(-2 * time.Second),
		expiresAt:   now.Add(-time.Second),
	})
	runtimes := map[string]model.Runtime{
		model.DefaultManagedRuntimeID: {ID: model.DefaultManagedRuntimeID, Type: model.RuntimeTypeManagedShared},
	}

	updated := apiServer.overlayManagedAppStatusesForEdgeRoutesCached([]model.App{app}, runtimes)[0]
	if updated.ObservedStatus == nil || !updated.ObservedStatus.Fresh || updated.ObservedStatus.Phase != "deployed" {
		t.Fatalf("recent successful observation became stale during cache refresh: %+v", updated.ObservedStatus)
	}
	if status, reason := edgeRouteStatus(updated, model.DefaultManagedRuntimeID, true); status != model.EdgeRouteStatusActive || reason != "" {
		t.Fatalf("recent successful observation must keep route active during refresh, got status=%q reason=%q", status, reason)
	}
}

func TestOverlayManagedAppStatusesForEdgeRoutesRejectsEvidenceFromPreviousAppRevision(t *testing.T) {
	t.Parallel()

	previous := model.App{
		ID:       "app_revision_guard",
		TenantID: "tenant_revision_guard",
		Name:     "revision-guard",
		Route:    &model.AppRoute{Hostname: "revision-guard.example", PathPrefix: "/", ServicePort: 8080},
		Spec: model.AppSpec{
			Image:     "registry.example/revision-guard:v1",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: model.DefaultManagedRuntimeID,
		},
		Status: model.AppStatus{
			Phase:            "deploying",
			CurrentRuntimeID: model.DefaultManagedRuntimeID,
			CurrentReplicas:  1,
		},
	}
	current := previous
	current.Spec.Image = "registry.example/revision-guard:v2"
	current.Status.Phase = "deployed"

	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(current, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("build managed app: %v", err)
	}
	managed.Metadata.Generation = 2
	managed.Status = runtime.ManagedAppStatus{
		Phase:              runtime.ManagedAppPhaseReady,
		DesiredReplicas:    1,
		ReadyReplicas:      1,
		ObservedGeneration: 2,
	}
	var deployment kubeDeploymentRuntimeEvidence
	if err := decodeKubeObject(map[string]any{
		"metadata": map[string]any{
			"namespace":  runtime.NamespaceForTenant(current.TenantID),
			"name":       runtime.RuntimeAppResourceName(current),
			"generation": 2,
		},
		"spec": map[string]any{
			"replicas": 1,
			"template": map[string]any{"spec": map[string]any{"containers": []map[string]any{{"image": current.Spec.Image}}}},
		},
		"status": map[string]any{
			"updatedReplicas": 1, "readyReplicas": 1, "availableReplicas": 1, "observedGeneration": 2,
		},
	}, &deployment); err != nil {
		t.Fatalf("decode deployment: %v", err)
	}
	namespace := runtime.NamespaceForTenant(current.TenantID)
	serviceKey := kubeNamespacedKey(namespace, runtime.RuntimeAppServiceName(current))
	snapshot := managedAppKubeSnapshot{
		namespaces:  map[string]struct{}{namespace: {}},
		deployments: map[string]kubeDeploymentRuntimeEvidence{kubeNamespacedKey(namespace, runtime.RuntimeAppResourceName(current)): deployment},
		services:    map[string]struct{}{serviceKey: {}},
		endpoints: map[string]kubeEndpointRuntimeEvidence{
			serviceKey: {Present: true, ReadyAddresses: 1},
		},
	}
	apiServer := &Server{
		log:                   log.New(io.Discard, "", 0),
		managedAppStatusCache: newManagedAppStatusCache(time.Minute, time.Second),
	}
	previousEvidence, err := apiServer.buildManagedAppRuntimeEvidence(previous, managed, true, snapshot)
	if err != nil {
		t.Fatalf("build previous-revision evidence: %v", err)
	}
	if !slices.Contains(previousEvidence.invariantViolations, "current_image_mismatch") {
		t.Fatalf("test precondition did not reproduce image mismatch: %+v", previousEvidence)
	}
	if previousEvidence.appObservationKey == managedAppRuntimeEvidenceObservationKey(current) {
		t.Fatal("previous and current app revisions unexpectedly share an observation key")
	}
	now := time.Now().UTC()
	apiServer.managedAppStatusCache.setList(managedAppStatusListCacheEntry{
		items:       map[string]runtime.ManagedAppObject{current.ID: managed},
		evidence:    map[string]managedAppRuntimeEvidence{current.ID: previousEvidence},
		ok:          true,
		clusterID:   "cluster-test-uid",
		refreshedAt: now,
		expiresAt:   now.Add(time.Minute),
	})
	// Keep this unit test deterministic; production immediately schedules the
	// refresh, while the assertion below exercises the safe interim projection.
	apiServer.managedAppStatusCache.mu.Lock()
	apiServer.managedAppStatusCache.listRefreshNotBefore = now.Add(time.Hour)
	apiServer.managedAppStatusCache.mu.Unlock()
	runtimes := map[string]model.Runtime{
		model.DefaultManagedRuntimeID: {ID: model.DefaultManagedRuntimeID, Type: model.RuntimeTypeManagedShared},
	}

	updated := apiServer.overlayManagedAppStatusesForEdgeRoutesCached([]model.App{current}, runtimes)[0]
	if updated.ObservedStatus == nil || updated.ObservedStatus.Phase != "unknown" || updated.ObservedStatus.Fresh {
		t.Fatalf("previous-revision evidence must become unknown while it refreshes: %+v", updated.ObservedStatus)
	}
	if len(updated.ObservedStatus.InvariantViolations) != 0 || !strings.Contains(updated.ObservedStatus.Message, "different app revision") {
		t.Fatalf("previous-revision contradictions leaked into the current app: %+v", updated.ObservedStatus)
	}
	if status, reason := edgeRouteStatus(updated, model.DefaultManagedRuntimeID, true); status != model.EdgeRouteStatusActive || !strings.Contains(reason, "last-known-good") {
		t.Fatalf("revision refresh must preserve the serving route, got status=%q reason=%q", status, reason)
	}

	currentEvidence, err := apiServer.buildManagedAppRuntimeEvidence(current, managed, true, snapshot)
	if err != nil {
		t.Fatalf("build current-revision evidence: %v", err)
	}
	apiServer.managedAppStatusCache.setList(managedAppStatusListCacheEntry{
		items:       map[string]runtime.ManagedAppObject{current.ID: managed},
		evidence:    map[string]managedAppRuntimeEvidence{current.ID: currentEvidence},
		ok:          true,
		clusterID:   "cluster-test-uid",
		refreshedAt: now,
		expiresAt:   now.Add(time.Minute),
	})
	updated = apiServer.overlayManagedAppStatusesForEdgeRoutesCached([]model.App{current}, runtimes)[0]
	if updated.ObservedStatus == nil || updated.ObservedStatus.Phase != "deployed" || !updated.ObservedStatus.Fresh {
		t.Fatalf("current-revision evidence should restore a fresh deployed projection: %+v", updated.ObservedStatus)
	}
}

func TestOverlayManagedAppStatusesForEdgeRoutesRejectsAgedExpiredObservation(t *testing.T) {
	t.Parallel()

	failedOperation := &model.AppOperationFailure{ID: "op_failed", Type: "deploy"}
	app := model.App{
		ID:       "app_aged_observation",
		TenantID: "tenant_demo",
		Name:     "aged-observation",
		Spec: model.AppSpec{
			RuntimeID: model.DefaultManagedRuntimeID,
			Replicas:  1,
		},
		Status: model.AppStatus{
			Phase:               "unknown",
			CurrentReplicas:     1,
			LastOperationID:     failedOperation.ID,
			LastFailedOperation: failedOperation,
		},
	}
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("build managed app: %v", err)
	}
	managed.Metadata.Generation = 1
	managed.Status = runtime.ManagedAppStatus{
		Phase:              runtime.ManagedAppPhaseReady,
		ReadyReplicas:      1,
		ObservedGeneration: 1,
	}
	now := time.Now().UTC()
	apiServer := &Server{
		log:                   log.New(io.Discard, "", 0),
		managedAppStatusCache: newManagedAppStatusCache(time.Second, time.Second),
	}
	apiServer.managedAppStatusCache.setList(managedAppStatusListCacheEntry{
		items:       map[string]runtime.ManagedAppObject{app.ID: managed},
		ok:          true,
		clusterID:   "cluster-test-uid",
		refreshedAt: now.Add(-defaultAppObservedStatusMaxAge - time.Second),
		expiresAt:   now.Add(-defaultAppObservedStatusMaxAge),
	})
	runtimes := map[string]model.Runtime{
		model.DefaultManagedRuntimeID: {ID: model.DefaultManagedRuntimeID, Type: model.RuntimeTypeManagedShared},
	}

	updated := apiServer.overlayManagedAppStatusesForEdgeRoutesCached([]model.App{app}, runtimes)[0]
	if updated.ObservedStatus == nil || updated.ObservedStatus.Fresh || updated.ObservedStatus.Reason != runtime.AppObservationReasonObservationStale {
		t.Fatalf("aged observation must be stale: %+v", updated.ObservedStatus)
	}
	if status, reason := edgeRouteStatus(updated, model.DefaultManagedRuntimeID, true); status != model.EdgeRouteStatusUnavailable || !strings.Contains(reason, "stale") {
		t.Fatalf("aged observation after a current failed operation must fail closed, got status=%q reason=%q", status, reason)
	}
}

func TestOverlayManagedAppStatusesForEdgeRoutesRetainsStoreStateUntilGenerationObserved(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			RuntimeID: model.DefaultManagedRuntimeID,
			Replicas:  1,
		},
		Status: model.AppStatus{Phase: "deployed", CurrentReplicas: 1},
	}
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("build managed app: %v", err)
	}
	managed.Metadata.Generation = 2
	managed.Status = runtime.ManagedAppStatus{
		Phase:              runtime.ManagedAppPhaseError,
		Message:            "transient controller status",
		ReadyReplicas:      0,
		ObservedGeneration: 1,
	}
	updated := []model.App{runtime.ApplyAppObservedStatus(app, runtime.CalculateAppObservedStatus(app, runtime.AppRuntimeObservation{
		ManagedApp:     managed,
		Found:          true,
		Complete:       true,
		Fresh:          true,
		ObservedAt:     time.Now().UTC(),
		ClusterID:      "cluster-test-uid",
		EvidenceSource: runtime.AppObservationSourceKubernetesAPI,
	}))}
	if updated[0].Status.CurrentReplicas != 0 || updated[0].Status.Phase != "unknown" {
		t.Fatalf("unobserved generation must be unknown, got %+v", updated[0].Status)
	}

	managed.Status.ObservedGeneration = managed.Metadata.Generation
	updated = []model.App{runtime.ApplyAppObservedStatus(app, runtime.CalculateAppObservedStatus(app, runtime.AppRuntimeObservation{
		ManagedApp:     managed,
		Found:          true,
		Complete:       true,
		Fresh:          true,
		ObservedAt:     time.Now().UTC(),
		ClusterID:      "cluster-test-uid",
		EvidenceSource: runtime.AppObservationSourceKubernetesAPI,
	}))}
	if updated[0].Status.CurrentReplicas != 0 || updated[0].Status.Phase != "failed" {
		t.Fatalf("observed generation must publish authoritative managed status, got %+v", updated[0].Status)
	}
}

func TestEdgeRouteDoesNotServeHistoricalReplicasAfterFailedOperation(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:   "app_failed_history",
		Name: "failed-history",
		Spec: model.AppSpec{Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID},
		Status: model.AppStatus{
			Phase:           "unknown",
			CurrentReplicas: 1,
			LastFailedOperation: &model.AppOperationFailure{
				ID:   "op_failed_history",
				Type: "deploy",
			},
		},
	}
	status, reason := edgeRouteStatus(app, model.DefaultManagedRuntimeID, true)
	if status != model.EdgeRouteStatusUnavailable || reason == "" {
		t.Fatalf("historical replicas after a failed operation must not serve: status=%q reason=%q", status, reason)
	}
}

func TestEdgeRouteRejectsIncompleteDeployedObservation(t *testing.T) {
	t.Parallel()
	ready := 1
	present := true
	app := model.App{
		ID: "app_incomplete_deployed", Name: "incomplete-deployed",
		Spec: model.AppSpec{Image: "registry.example/app:v1", Ports: []int{8080}, Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID},
		ObservedStatus: &model.AppObservedStatus{
			Phase: "deployed", Fresh: true, ObservedAt: time.Now().UTC(),
			ClusterID: "cluster-current", Generation: 2, ObservedGeneration: 2,
			EvidenceSource: runtime.AppObservationSourceKubernetesAPI,
			ReadyReplicas:  &ready, RuntimeObjectPresent: &present, NamespacePresent: &present,
			// Endpoint/image/physical fields are deliberately absent. A phase
			// string cannot fill those evidence gaps.
		},
	}
	status, reason := edgeRouteStatus(app, model.DefaultManagedRuntimeID, true)
	if status != model.EdgeRouteStatusUnavailable || reason == "" {
		t.Fatalf("incomplete deployed observation must not publish an active route: status=%q reason=%q", status, reason)
	}
}

func TestAppObservedReadyForServingRequiresDesiredReplicaAgreement(t *testing.T) {
	t.Parallel()
	ready := 1
	present := true
	app := model.App{
		Spec: model.AppSpec{Image: "registry.example/app:v1", Ports: []int{8080}, Replicas: 1},
		ObservedStatus: &model.AppObservedStatus{
			Phase: "deployed", DesiredReplicas: 1, ReadyReplicas: &ready,
			RuntimeObjectPresent: &present, NamespacePresent: &present, ServicePresent: &present,
			EndpointPresent: &present, EndpointReady: &present, PhysicalReplicas: &ready,
			PhysicalDesired: &ready, ImagePresent: &present, Fresh: true, ObservedAt: time.Now().UTC(),
			ClusterID: "cluster-current", Generation: 2, ObservedGeneration: 2,
			EvidenceSource: runtime.AppObservationSourceKubernetesAPI,
		},
	}
	if !appObservedReadyForServing(app, time.Now().UTC()) {
		t.Fatal("complete matching runtime evidence should be publishable")
	}
	app.ObservedStatus.DesiredReplicas = 0
	if appObservedReadyForServing(app, time.Now().UTC()) {
		t.Fatal("observed desired replicas below the durable spec were accepted")
	}
}

func TestEdgeRoutePreservesAuthoritativeAbsenceWhenDesiredReplicasZero(t *testing.T) {
	ready := 0
	present := false
	app := model.App{
		Spec: model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 0},
		ObservedStatus: &model.AppObservedStatus{
			Phase: "unavailable", Fresh: true, ObservedAt: time.Now().UTC(),
			ClusterID: "cluster-uid", EvidenceSource: runtime.AppObservationSourceKubernetesAPI,
			ReadyReplicas: &ready, RuntimeObjectPresent: &present,
			Reason:  runtime.AppObservationReasonManagedAppNotFound,
			Message: "managed app runtime object not found",
		},
	}
	status, reason := edgeRouteStatus(app, model.DefaultManagedRuntimeID, true)
	if status != model.EdgeRouteStatusUnavailable || !strings.Contains(reason, "not found") {
		t.Fatalf("Edge relabeled authoritative absence as desired disabled state: status=%q reason=%q", status, reason)
	}
}

func TestOverlayManagedAppStatusCachedBacksOffAfterBackgroundListError(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Status: model.AppStatus{
			Phase: "deployed",
		},
	}

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "api server unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	apiServer := &Server{
		log:                   log.New(io.Discard, "", 0),
		managedAppStatusCache: newManagedAppStatusCache(time.Minute, time.Second),
		newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
			return &managedAppStatusClient{
				client:      server.Client(),
				baseURL:     server.URL,
				bearerToken: "test",
			}, nil
		},
	}

	_ = apiServer.overlayManagedAppStatusCached(app)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		apiServer.managedAppStatusCache.mu.RLock()
		blocked := time.Now().Before(apiServer.managedAppStatusCache.listRefreshNotBefore)
		apiServer.managedAppStatusCache.mu.RUnlock()
		if blocked {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected one failed background refresh, got %d", got)
	}

	_ = apiServer.overlayManagedAppStatusCached(app)
	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected refresh backoff to suppress immediate retry, got %d calls", got)
	}
}

func newManagedAppTestServer(t *testing.T, payload any) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writeManagedAppClusterIdentity(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Fatalf("encode payload: %v", err)
		}
	}))
}

func writeManagedAppClusterIdentity(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path != "/api/v1/namespaces/kube-system" {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"metadata": map[string]any{"uid": "cluster-test-uid"},
	})
	return true
}

func setManagedAppTestObservedGeneration(managed map[string]any, generation int64) {
	metadata, _ := managed["metadata"].(map[string]any)
	if metadata == nil {
		metadata = map[string]any{}
		managed["metadata"] = metadata
	}
	metadata["generation"] = generation
	status, _ := managed["status"].(map[string]any)
	if status == nil {
		status = map[string]any{}
		managed["status"] = status
	}
	status["observedGeneration"] = generation
}
