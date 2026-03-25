package api

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
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

func newManagedAppTestServer(t *testing.T, payload any) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Fatalf("encode payload: %v", err)
		}
	}))
}
