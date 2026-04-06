package controller

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestWaitForManagedAppRolloutFailsWhenManagedAppReportsError(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	deploymentName := runtime.RuntimeAppResourceName(app)
	managedAppName := runtime.ManagedAppResourceName(app)

	deployment := kubeDeployment{}
	deployment.Metadata.Name = deploymentName
	deployment.Metadata.Generation = 1
	deployment.Status.ObservedGeneration = 1
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1

	managedApp := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managedMetadata, _ := managedApp["metadata"].(map[string]any)
	if managedMetadata == nil {
		managedMetadata = map[string]any{}
		managedApp["metadata"] = managedMetadata
	}
	managedMetadata["generation"] = 1
	managedApp["status"] = map[string]any{
		"phase":              runtime.ManagedAppPhaseError,
		"message":            "pod demo-abc123 container demo failed: Error: exit_code=3",
		"observedGeneration": 1,
	}

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/apis/apps/v1/namespaces/" + namespace + "/deployments/" + deploymentName:
			if err := json.NewEncoder(w).Encode(deployment); err != nil {
				t.Fatalf("encode deployment: %v", err)
			}
		case managedAppAPIPath(namespace, managedAppName):
			if err := json.NewEncoder(w).Encode(managedApp); err != nil {
				t.Fatalf("encode managed app: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	svc := &Service{
		Config: config.ControllerConfig{
			ManagedAppRolloutTimeout: 2 * time.Second,
			PollInterval:             10 * time.Millisecond,
		},
		Logger: log.New(io.Discard, "", 0),
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client:      kubeServer.Client(),
				baseURL:     kubeServer.URL,
				bearerToken: "test",
				namespace:   namespace,
			}, nil
		},
	}

	err := svc.waitForManagedAppRollout(context.Background(), app, "")
	if err == nil {
		t.Fatal("expected rollout wait to fail")
	}
	if !strings.Contains(err.Error(), "exit_code=3") {
		t.Fatalf("expected managed app failure message in error, got %v", err)
	}
}
