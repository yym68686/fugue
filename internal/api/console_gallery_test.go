package api

import (
	"errors"
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestBuildConsoleProjectLifecycleUsesUpdatingForMixedLiveAndPending(t *testing.T) {
	t.Parallel()

	lifecycle := buildConsoleProjectLifecycle(
		[]string{"importing", "building"},
		1,
		2,
		true,
		true,
		true,
	)

	if lifecycle.Label != "Updating" {
		t.Fatalf("expected mixed live/pending lifecycle to be Updating, got %q", lifecycle.Label)
	}
	if !lifecycle.Live {
		t.Fatal("expected updating lifecycle to stay live")
	}
	if lifecycle.SyncMode != "active" {
		t.Fatalf("expected active sync mode, got %q", lifecycle.SyncMode)
	}
	if lifecycle.Tone != "info" {
		t.Fatalf("expected info tone, got %q", lifecycle.Tone)
	}
}

func TestBuildConsoleProjectLifecycleKeepsBuildingForPendingOnly(t *testing.T) {
	t.Parallel()

	lifecycle := buildConsoleProjectLifecycle(
		[]string{"building"},
		1,
		1,
		true,
		false,
		true,
	)

	if lifecycle.Label != "Building" {
		t.Fatalf("expected pending-only lifecycle to stay Building, got %q", lifecycle.Label)
	}
}

func TestReadConsoleActiveReleaseOperationIgnoresPendingDeployForFailedApp(t *testing.T) {
	t.Parallel()

	operation := &model.Operation{
		ID:     "op_demo",
		Type:   model.OperationTypeDeploy,
		Status: model.OperationStatusRunning,
	}
	app := model.App{
		Status: model.AppStatus{
			Phase: "failed",
		},
	}

	if got := readConsoleActiveReleaseOperation(operation, app); got != nil {
		t.Fatalf("expected failed app to ignore active deploy operation, got %+v", got)
	}
}

func TestConsoleGallerySkipsLiveStatusOverlayByDefault(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Console Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "viewer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	if _, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: tenantSharedRuntimeID,
	}); err != nil {
		t.Fatalf("create app: %v", err)
	}

	managedStatusCalls := 0

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	server.newManagedAppStatusClient = func() (*managedAppStatusClient, error) {
		managedStatusCalls++
		return nil, errors.New("unexpected managed status lookup")
	}

	recorder := performJSONRequest(
		t,
		server,
		http.MethodGet,
		"/v1/console/gallery",
		apiKey,
		nil,
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	if managedStatusCalls != 0 {
		t.Fatalf("expected console gallery to skip live status overlay by default, got %d calls", managedStatusCalls)
	}
}

func TestConsoleGalleryCachesResourceOverlayBetweenRequests(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Console Cache Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "viewer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	if _, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: tenantSharedRuntimeID,
	}); err != nil {
		t.Fatalf("create app: %v", err)
	}

	clusterInventoryCalls := 0

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		clusterInventoryCalls++
		return nil, errors.New("unexpected cluster inventory lookup")
	}

	first := performJSONRequest(
		t,
		server,
		http.MethodGet,
		"/v1/console/gallery",
		apiKey,
		nil,
	)
	if first.Code != http.StatusOK {
		t.Fatalf("expected first status %d, got %d body=%s", http.StatusOK, first.Code, first.Body.String())
	}

	second := performJSONRequest(
		t,
		server,
		http.MethodGet,
		"/v1/console/gallery",
		apiKey,
		nil,
	)
	if second.Code != http.StatusOK {
		t.Fatalf("expected second status %d, got %d body=%s", http.StatusOK, second.Code, second.Body.String())
	}

	if clusterInventoryCalls != 1 {
		t.Fatalf("expected cached console gallery to collapse resource overlay fanout, got %d lookups", clusterInventoryCalls)
	}
}
