package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestGetControlPlaneStatusIncludesDeployWorkflowRunWhenConfigured(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/apps/v1/namespaces/fugue-system/deployments":
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
							"replicas": 1,
							"template": map[string]any{
								"spec": map[string]any{
									"containers": []map[string]any{{"name": "api", "image": "ghcr.io/acme/fugue-api:deadbeef"}},
								},
							},
						},
						"status": map[string]any{
							"readyReplicas":     1,
							"updatedReplicas":   1,
							"availableReplicas": 1,
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
							"replicas": 1,
							"template": map[string]any{
								"spec": map[string]any{
									"containers": []map[string]any{{"name": "controller", "image": "ghcr.io/acme/fugue-controller:deadbeef"}},
								},
							},
						},
						"status": map[string]any{
							"readyReplicas":     1,
							"updatedReplicas":   1,
							"availableReplicas": 1,
						},
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	githubServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if want := "/repos/acme/fugue/actions/workflows/deploy-control-plane.yml/runs"; r.URL.Path != want {
			t.Fatalf("unexpected github api path %q", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"workflow_runs": []map[string]any{
				{
					"status":      "completed",
					"conclusion":  "success",
					"run_number":  42,
					"event":       "push",
					"head_branch": "main",
					"head_sha":    "deadbeef",
					"html_url":    "https://github.com/acme/fugue/actions/runs/42",
					"created_at":  "2026-04-14T00:00:00Z",
					"updated_at":  "2026-04-14T00:10:00Z",
				},
			},
		})
	}))
	defer githubServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{
		ControlPlaneNamespace:        "fugue-system",
		ControlPlaneReleaseInstance:  "fugue",
		ControlPlaneGitHubRepository: "acme/fugue",
		ControlPlaneGitHubWorkflow:   "deploy-control-plane.yml",
		ControlPlaneGitHubAPIURL:     githubServer.URL,
	})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}
	server.controlPlaneHTTPClient = githubServer.Client()

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/cluster/control-plane", "bootstrap-secret", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		ControlPlane model.ControlPlaneStatus `json:"control_plane"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.ControlPlane.DeployWorkflow == nil {
		t.Fatalf("expected deploy workflow to be populated, got %+v", response.ControlPlane)
	}
	if response.ControlPlane.DeployWorkflow.RunNumber != 42 || response.ControlPlane.DeployWorkflow.HeadSHA != "deadbeef" {
		t.Fatalf("unexpected deploy workflow: %+v", response.ControlPlane.DeployWorkflow)
	}
}

func TestListClusterPodsReturnsSystemPods(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/pods":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"namespace": "kube-system",
							"name":      "coredns-abc",
							"ownerReferences": []map[string]any{
								{"kind": "ReplicaSet", "name": "coredns-85f7d9b4"},
							},
						},
						"spec": map[string]any{
							"nodeName": "gcp1",
							"containers": []map[string]any{
								{"name": "coredns", "image": "coredns/coredns:v1.11.1"},
							},
						},
						"status": map[string]any{
							"phase":    "Running",
							"qosClass": "Burstable",
							"containerStatuses": []map[string]any{
								{
									"name":         "coredns",
									"image":        "coredns/coredns:v1.11.1",
									"ready":        true,
									"restartCount": 1,
									"state": map[string]any{
										"running": map[string]any{},
									},
								},
							},
						},
					},
					{
						"metadata": map[string]any{
							"namespace": "kube-system",
							"name":      "coredns-failed",
						},
						"spec": map[string]any{
							"nodeName": "gcp2",
						},
						"status": map[string]any{
							"phase": "Failed",
						},
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/cluster/pods", "bootstrap-secret", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		ClusterPods []model.ClusterPod `json:"cluster_pods"`
	}
	mustDecodeJSON(t, recorder, &response)
	if len(response.ClusterPods) != 1 {
		t.Fatalf("expected one non-terminated pod, got %+v", response.ClusterPods)
	}
	if response.ClusterPods[0].Namespace != "kube-system" || response.ClusterPods[0].NodeName != "gcp1" {
		t.Fatalf("unexpected cluster pod: %+v", response.ClusterPods[0])
	}
	if response.ClusterPods[0].Owner == nil || response.ClusterPods[0].Owner.Name != "coredns-85f7d9b4" {
		t.Fatalf("expected owner reference, got %+v", response.ClusterPods[0].Owner)
	}
}

func TestGetClusterWorkloadReturnsNodeSelectorAndPods(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/apis/apps/v1/namespaces/kube-system/deployments/coredns":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]any{
					"namespace": "kube-system",
					"name":      "coredns",
					"labels": map[string]any{
						"k8s-app": "kube-dns",
					},
				},
				"spec": map[string]any{
					"replicas": 2,
					"selector": map[string]any{
						"matchLabels": map[string]any{"k8s-app": "kube-dns"},
					},
					"template": map[string]any{
						"spec": map[string]any{
							"nodeSelector": map[string]any{"kubernetes.io/os": "linux"},
							"containers": []map[string]any{
								{"name": "coredns", "image": "coredns/coredns:v1.11.1"},
							},
						},
					},
				},
				"status": map[string]any{
					"replicas":          2,
					"readyReplicas":     2,
					"updatedReplicas":   2,
					"availableReplicas": 2,
				},
			})
		case r.URL.Path == "/api/v1/namespaces/kube-system/pods":
			if got := r.URL.Query().Get("labelSelector"); !strings.Contains(got, "k8s-app=kube-dns") {
				t.Fatalf("expected coredns label selector, got %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"namespace": "kube-system",
							"name":      "coredns-abc",
						},
						"spec": map[string]any{
							"nodeName": "gcp1",
							"containers": []map[string]any{
								{"name": "coredns", "image": "coredns/coredns:v1.11.1"},
							},
						},
						"status": map[string]any{
							"phase": "Running",
							"conditions": []map[string]any{
								{"type": "Ready", "status": "True"},
							},
							"containerStatuses": []map[string]any{
								{
									"name":         "coredns",
									"image":        "coredns/coredns:v1.11.1",
									"ready":        true,
									"restartCount": 0,
									"state": map[string]any{
										"running": map[string]any{},
									},
								},
							},
						},
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/cluster/workloads/kube-system/deployment/coredns", "bootstrap-secret", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Workload model.ClusterWorkloadDetail `json:"workload"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Workload.NodeSelector["kubernetes.io/os"] != "linux" {
		t.Fatalf("expected node selector to be preserved, got %+v", response.Workload.NodeSelector)
	}
	if response.Workload.Selector == "" || len(response.Workload.Pods) != 1 {
		t.Fatalf("expected selector and pods, got %+v", response.Workload)
	}
}

func TestExecClusterPodUsesExecRunner(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	runner := &fakeFilesystemExecRunner{
		outputs: [][]byte{[]byte("10.43.0.10\n")},
	}
	server.filesystemExecRunner = runner

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/cluster/exec", "bootstrap-secret", map[string]any{
		"namespace": "kube-system",
		"pod":       "coredns-abc",
		"container": "coredns",
		"command":   []string{"cat", "/etc/resolv.conf"},
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Output string `json:"output"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Output != "10.43.0.10\n" {
		t.Fatalf("unexpected exec output %q", response.Output)
	}
	if len(runner.calls) != 1 || runner.calls[0].namespace != "kube-system" || runner.calls[0].podName != "coredns-abc" {
		t.Fatalf("unexpected exec runner calls: %+v", runner.calls)
	}
}
