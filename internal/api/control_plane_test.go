package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestGetControlPlaneStatusReturnsCurrentDeployments(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/apps/v1/namespaces/fugue-system/deployments":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name": "fugue-fugue-api",
							"labels": map[string]any{
								"app.kubernetes.io/component": "api",
								"app.kubernetes.io/instance":  "fugue",
							},
						},
						"spec": map[string]any{
							"replicas": 2,
							"template": map[string]any{
								"spec": map[string]any{
									"containers": []map[string]any{
										{
											"name":  "api",
											"image": "ghcr.io/yym68686/fugue-api:6518ea4fd994ef90cb29c12f2e7a09b69751b158",
										},
									},
								},
							},
						},
						"status": map[string]any{
							"readyReplicas":     2,
							"updatedReplicas":   2,
							"availableReplicas": 2,
						},
					},
					{
						"metadata": map[string]any{
							"name": "fugue-fugue-controller",
							"labels": map[string]any{
								"app.kubernetes.io/component": "controller",
								"app.kubernetes.io/instance":  "fugue",
							},
						},
						"spec": map[string]any{
							"replicas": 2,
							"template": map[string]any{
								"spec": map[string]any{
									"containers": []map[string]any{
										{
											"name":  "controller",
											"image": "ghcr.io/yym68686/fugue-controller:6518ea4fd994ef90cb29c12f2e7a09b69751b158",
										},
									},
								},
							},
						},
						"status": map[string]any{
							"readyReplicas":     2,
							"updatedReplicas":   2,
							"availableReplicas": 2,
						},
					},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/fugue-system/pods":
			switch r.URL.Query().Get("labelSelector") {
			case "":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"items": []map[string]any{
						{
							"metadata": map[string]any{
								"namespace": "fugue-system",
								"name":      "fugue-shared-workspace-provisioner-abc",
								"labels": map[string]any{
									"app.kubernetes.io/component": "shared-workspace-provisioner",
									"app.kubernetes.io/instance":  "fugue",
								},
								"ownerReferences": []map[string]any{
									{"kind": "ReplicaSet", "name": "fugue-shared-workspace-provisioner"},
								},
							},
							"status": map[string]any{
								"phase":   "Pending",
								"reason":  "Unschedulable",
								"message": "0/3 nodes are available",
							},
						},
					},
				})
			case "app.kubernetes.io/component=api,app.kubernetes.io/instance=fugue":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"items": []map[string]any{
						{
							"metadata": map[string]any{
								"name":              "fugue-fugue-api-abc",
								"creationTimestamp": "2026-04-14T00:00:00Z",
							},
							"spec": map[string]any{
								"nodeName": "gcp2",
								"containers": []map[string]any{
									{
										"name":  "api",
										"image": "ghcr.io/yym68686/fugue-api:6518ea4fd994ef90cb29c12f2e7a09b69751b158",
									},
								},
							},
							"status": map[string]any{
								"phase":     "Running",
								"startTime": "2026-04-14T00:00:05Z",
								"containerStatuses": []map[string]any{
									{
										"name":  "api",
										"ready": true,
									},
								},
							},
						},
					},
				})
			case "app.kubernetes.io/component=controller,app.kubernetes.io/instance=fugue":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"items": []map[string]any{
						{
							"metadata": map[string]any{
								"name":              "fugue-fugue-controller-xyz",
								"creationTimestamp": "2026-04-14T00:00:00Z",
							},
							"spec": map[string]any{
								"nodeName": "gcp3",
								"containers": []map[string]any{
									{
										"name":  "controller",
										"image": "ghcr.io/yym68686/fugue-controller:6518ea4fd994ef90cb29c12f2e7a09b69751b158",
									},
								},
							},
							"status": map[string]any{
								"phase":     "Running",
								"startTime": "2026-04-14T00:00:06Z",
								"containerStatuses": []map[string]any{
									{
										"name":  "controller",
										"ready": true,
									},
								},
							},
						},
					},
				})
			default:
				t.Fatalf("unexpected pod selector %q", r.URL.Query().Get("labelSelector"))
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{
		ControlPlaneNamespace:       "fugue-system",
		ControlPlaneReleaseInstance: "fugue",
	})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/cluster/control-plane", nil)
	req.Header.Set("Authorization", "Bearer bootstrap-secret")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		ControlPlane model.ControlPlaneStatus `json:"control_plane"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.ControlPlane.Namespace != "fugue-system" {
		t.Fatalf("expected namespace fugue-system, got %q", response.ControlPlane.Namespace)
	}
	if response.ControlPlane.ReleaseInstance != "fugue" {
		t.Fatalf("expected release instance fugue, got %q", response.ControlPlane.ReleaseInstance)
	}
	if response.ControlPlane.Version != "6518ea4fd994ef90cb29c12f2e7a09b69751b158" {
		t.Fatalf("expected control plane version tag, got %q", response.ControlPlane.Version)
	}
	if response.ControlPlane.LiveVersion != "6518ea4fd994ef90cb29c12f2e7a09b69751b158" {
		t.Fatalf("expected live control plane version tag, got %q", response.ControlPlane.LiveVersion)
	}
	if response.ControlPlane.Status != controlPlaneStatusReady {
		t.Fatalf("expected control plane status %q, got %q", controlPlaneStatusReady, response.ControlPlane.Status)
	}
	if len(response.ControlPlane.Components) != 2 {
		t.Fatalf("expected 2 components, got %d", len(response.ControlPlane.Components))
	}
	if len(response.ControlPlane.Components[0].ObservedImageTags) != 1 {
		t.Fatalf("expected api observed image tags, got %+v", response.ControlPlane.Components[0].ObservedImageTags)
	}
	if len(response.ControlPlane.Components[1].ObservedPods) != 1 {
		t.Fatalf("expected controller observed pods, got %+v", response.ControlPlane.Components[1].ObservedPods)
	}
	if response.ControlPlane.Components[0].Status != controlPlaneStatusReady {
		t.Fatalf("expected api status %q, got %q", controlPlaneStatusReady, response.ControlPlane.Components[0].Status)
	}
	if response.ControlPlane.Components[1].Status != controlPlaneStatusReady {
		t.Fatalf("expected controller status %q, got %q", controlPlaneStatusReady, response.ControlPlane.Components[1].Status)
	}
	if len(response.ControlPlane.Warnings) != 1 || response.ControlPlane.Warnings[0].Name != "fugue-shared-workspace-provisioner" {
		t.Fatalf("expected non-core pending pod warning, got %+v", response.ControlPlane.Warnings)
	}
}

func TestGetControlPlaneStatusRequiresPlatformAdmin(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Control Plane Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiSecret, err := stateStore.CreateAPIKey(tenant.ID, "tenant-admin", []string{"project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{
		ControlPlaneNamespace: "fugue-system",
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/cluster/control-plane", nil)
	req.Header.Set("Authorization", "Bearer "+apiSecret)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusForbidden, recorder.Code, recorder.Body.String())
	}
}
