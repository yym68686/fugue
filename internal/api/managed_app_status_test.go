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

func newManagedAppTestServer(t *testing.T, payload any) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Fatalf("encode payload: %v", err)
		}
	}))
}
