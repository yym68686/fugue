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
				"status": map[string]any{"replicas": 1, "readyReplicas": 1, "availableReplicas": 1, "observedGeneration": 4},
			}}})
		case "/api/v1/services":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{"metadata": map[string]any{"name": serviceName, "namespace": namespace}}}})
		case "/api/v1/endpoints":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{
				"metadata": map[string]any{"name": serviceName, "namespace": namespace},
				"subsets":  []map[string]any{{"addresses": []map[string]any{{"ip": "10.0.0.1"}}}},
			}}})
		default:
			http.NotFound(w, r)
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

func TestCurrentManagedImagePresenceRequiresFreshExplicitEvidence(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	app := model.App{ID: "app_image_evidence", TenantID: "tenant_demo"}
	imageRef := "registry.fugue.internal:5000/fugue-apps/demo@sha256:" + strings.Repeat("a", 64)
	apiServer := &Server{store: stateStore}

	present, err := apiServer.currentManagedImagePresence(app, imageRef)
	if err != nil {
		t.Fatalf("read empty image evidence: %v", err)
	}
	if present != nil {
		t.Fatalf("absence of a report must remain unknown, got %v", *present)
	}

	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID: app.TenantID,
		AppID:    app.ID,
		ImageRef: imageRef,
		Status:   model.ImageLocationStatusMissing,
	}); err != nil {
		t.Fatalf("record missing image: %v", err)
	}
	present, err = apiServer.currentManagedImagePresence(app, imageRef)
	if err != nil {
		t.Fatalf("read missing image evidence: %v", err)
	}
	if present == nil || *present {
		t.Fatalf("fresh explicit missing report must be false, got %v", present)
	}

	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID: app.TenantID,
		AppID:    app.ID,
		ImageRef: imageRef,
		Status:   model.ImageLocationStatusPresent,
	}); err != nil {
		t.Fatalf("record present image: %v", err)
	}
	present, err = apiServer.currentManagedImagePresence(app, imageRef)
	if err != nil {
		t.Fatalf("read present image evidence: %v", err)
	}
	if present == nil || !*present {
		t.Fatalf("fresh present report must be true, got %v", present)
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
