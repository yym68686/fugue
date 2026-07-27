package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestPatchManagedAppStorageExpansionErrorStatusPreservesReadyDeployment(t *testing.T) {
	t.Parallel()

	const (
		namespace   = "fg-tenant-demo"
		managedName = "app-demo"
	)
	cause := errors.New("postgres PVC reports resize error")

	tests := []struct {
		name           string
		deploymentJSON string
		wantReady      int
	}{
		{
			name: "ready deployment",
			deploymentJSON: `{
				"metadata":{"name":"app-demo","generation":4},
				"status":{
					"observedGeneration":4,
					"replicas":1,
					"updatedReplicas":1,
					"readyReplicas":1,
					"availableReplicas":1,
					"conditions":[{"type":"Available","status":"True","reason":"MinimumReplicasAvailable"}]
				}
			}`,
			wantReady: 1,
		},
		{
			name: "over-reported deployment readiness is bounded",
			deploymentJSON: `{
				"metadata":{"name":"app-demo","generation":4},
				"status":{
					"observedGeneration":4,
					"replicas":1,
					"updatedReplicas":1,
					"readyReplicas":2,
					"availableReplicas":2
				}
			}`,
			wantReady: 1,
		},
		{
			name: "unready deployment",
			deploymentJSON: `{
				"metadata":{"name":"app-demo","generation":4},
				"status":{
					"observedGeneration":4,
					"replicas":1,
					"updatedReplicas":1,
					"readyReplicas":0,
					"availableReplicas":0,
					"unavailableReplicas":1
				}
			}`,
			wantReady: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var patched runtime.ManagedAppStatus
			transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch {
				case req.Method == http.MethodGet && req.URL.Path == deploymentAPIPath(namespace, managedName):
					return okJSONResponse(tt.deploymentJSON), nil
				case req.Method == http.MethodPatch && req.URL.Path == managedAppAPIPath(namespace, managedName)+"/status":
					var body struct {
						Status runtime.ManagedAppStatus `json:"status"`
					}
					if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
						t.Fatalf("decode status patch: %v", err)
					}
					patched = body.Status
					return okJSONResponse(`{}`), nil
				default:
					t.Fatalf("unexpected Kubernetes request %s %s", req.Method, req.URL.Path)
					return nil, nil
				}
			})
			client := &kubeClient{
				client:      &http.Client{Transport: transport},
				baseURL:     "http://kube.test",
				bearerToken: "test",
				namespace:   namespace,
			}
			app := model.App{
				ID:       "app_demo",
				TenantID: "tenant_demo",
				Name:     "demo",
				Spec: model.AppSpec{
					Replicas: 1,
				},
			}
			managed := runtime.ManagedAppObject{
				Metadata: runtime.ManagedAppMeta{
					Name:       managedName,
					Namespace:  namespace,
					Generation: 4,
				},
				Spec: runtime.ManagedAppSpec{
					AppID:    app.ID,
					TenantID: app.TenantID,
					Name:     app.Name,
					AppSpec:  app.Spec,
				},
				Status: runtime.ManagedAppStatus{
					CurrentReleaseKey:       "release-previous",
					CurrentReleaseStartedAt: "2026-07-27T12:00:00Z",
					CurrentReleaseReadyAt:   "2026-07-27T12:01:00Z",
				},
			}

			err := patchManagedAppStorageExpansionErrorStatus(
				context.Background(),
				client,
				namespace,
				managed,
				app,
				cause,
			)
			if !errors.Is(err, cause) {
				t.Fatalf("expected original cause, got %v", err)
			}
			if patched.Phase != runtime.ManagedAppPhaseError {
				t.Fatalf("expected error phase, got %q", patched.Phase)
			}
			if !strings.Contains(patched.Message, "resize error") {
				t.Fatalf("expected resize error message, got %q", patched.Message)
			}
			if patched.ReadyReplicas != tt.wantReady {
				t.Fatalf("expected ready replicas %d, got %d", tt.wantReady, patched.ReadyReplicas)
			}
			if patched.CurrentReleaseKey != "release-previous" ||
				patched.CurrentReleaseReadyAt != "2026-07-27T12:01:00Z" {
				t.Fatalf("expected current release metadata to be preserved, got %+v", patched)
			}
		})
	}
}

func TestPrepareManagedPostgresExpansionSkipsLegacyStorageClassWhenCapacityConverged(t *testing.T) {
	t.Parallel()

	const (
		namespace   = "fg-tenant-demo"
		clusterName = "demo-postgres"
		pvcName     = "demo-postgres-1"
	)
	storageClassRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet &&
			r.URL.Path == "/api/v1/namespaces/"+namespace+"/persistentvolumeclaims":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{{"metadata": map[string]any{"name": pvcName}}},
			})
		case r.Method == http.MethodGet &&
			r.URL.Path == "/api/v1/namespaces/"+namespace+"/persistentvolumeclaims/"+pvcName:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": pvcName},
				"spec": map[string]any{
					"storageClassName": "local-path",
					"resources": map[string]any{
						"requests": map[string]any{"storage": "1Gi"},
					},
				},
				"status": map[string]any{
					"capacity": map[string]any{"storage": "1Gi"},
				},
			})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/apis/storage.k8s.io/"):
			storageClassRequests++
			http.Error(w, "storage class should not be read for a converged PVC", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := &kubeClient{
		client:      server.Client(),
		baseURL:     server.URL,
		bearerToken: "test",
		namespace:   namespace,
	}
	err := (&Service{}).prepareManagedPostgresInPlaceStorageExpansion(
		context.Background(),
		client,
		namespace,
		clusterName,
		managedPostgresStorageTarget{
			StorageClassName: "fugue-postgres-rwo",
			StorageSize:      "1Gi",
		},
	)
	if err != nil {
		t.Fatalf("converged legacy PVC should not require storage-class validation: %v", err)
	}
	if storageClassRequests != 0 {
		t.Fatalf("expected no storage-class lookup for a converged PVC, got %d", storageClassRequests)
	}
}
