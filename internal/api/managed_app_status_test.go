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
	managed["status"] = map[string]any{
		"phase":         runtime.ManagedAppPhaseProgressing,
		"readyReplicas": 1,
		"message":       "rollout in progress",
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
	managed["status"] = map[string]any{
		"phase":         runtime.ManagedAppPhaseReady,
		"readyReplicas": 1,
		"message":       "deployment ready",
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

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	if stale.Status.Phase != "failed" {
		t.Fatalf("expected stale cached phase to be reused, got %q", stale.Status.Phase)
	}
	if stale.Status.LastMessage != "startup failed" {
		t.Fatalf("expected stale cached message to be reused, got %q", stale.Status.LastMessage)
	}
}

func TestOverlayManagedAppStatusUsesSingleflight(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
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

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	if first.Status.LastMessage != "" {
		t.Fatalf("expected first hot-path read to use store state, got %q", first.Status.LastMessage)
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
		{ID: "app_demo_1", TenantID: "tenant_demo", Name: "demo-1", Status: model.AppStatus{Phase: "deployed"}},
		{ID: "app_demo_2", TenantID: "tenant_demo", Name: "demo-2", Status: model.AppStatus{Phase: "deployed"}},
		{ID: "app_demo_3", TenantID: "tenant_demo", Name: "demo-3", Status: model.AppStatus{Phase: "deployed"}},
	}
	var managedItems []map[string]any
	for i, app := range apps {
		managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
		managed["status"] = map[string]any{
			"phase":         runtime.ManagedAppPhaseReady,
			"message":       "ready " + app.ID,
			"readyReplicas": i + 1,
		}
		managedItems = append(managedItems, managed)
	}

	var listCalls atomic.Int32
	var getCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
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
		if first.Status.LastMessage != "" {
			t.Fatalf("expected first hot-path read to use store state, got %q", first.Status.LastMessage)
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

func TestOverlayManagedAppStatusesForEdgeRoutesMarksMissingManagedAppUnavailable(t *testing.T) {
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
		Phase:         runtime.ManagedAppPhaseReady,
		ReadyReplicas: 1,
	}

	apiServer := &Server{managedAppStatusCache: newManagedAppStatusCache(time.Minute, time.Second)}
	apiServer.managedAppStatusCache.setList(managedAppStatusListCacheEntry{
		items:     map[string]runtime.ManagedAppObject{present.ID: managed},
		ok:        true,
		expiresAt: time.Now().Add(time.Minute),
	})
	runtimes := map[string]model.Runtime{
		model.DefaultManagedRuntimeID: {
			ID:   model.DefaultManagedRuntimeID,
			Type: model.RuntimeTypeManagedShared,
		},
	}

	updated := apiServer.overlayManagedAppStatusesForEdgeRoutesCached([]model.App{present, missing}, runtimes)
	if updated[0].Status.CurrentReplicas != 1 {
		t.Fatalf("expected present managed app to remain ready, got %+v", updated[0].Status)
	}
	if updated[1].Status.CurrentReplicas != 0 || updated[1].Status.Phase != "unavailable" || !strings.Contains(updated[1].Status.LastMessage, "not found") {
		t.Fatalf("expected missing managed app to become unavailable for edge publication, got %+v", updated[1].Status)
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
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Fatalf("encode payload: %v", err)
		}
	}))
}
