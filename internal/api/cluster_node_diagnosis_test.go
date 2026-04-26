package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/store"
)

func TestGetClusterNodeDiagnosisCollectsHostEvidenceWithoutSSH(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/nodes":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name": "gcp1",
						},
						"status": map[string]any{
							"conditions": []map[string]string{
								{"type": "Ready", "status": "True"},
								{"type": "DiskPressure", "status": "True", "reason": "KubeletHasDiskPressure", "message": "node has disk pressure"},
							},
						},
					},
				},
			})
		case "/api/v1/pods":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{}})
		case "/api/v1/namespaces/fugue-system/pods":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"namespace": "fugue-system",
							"name":      "fugue-fugue-node-janitor-abc12",
						},
						"spec": map[string]any{
							"nodeName": "gcp1",
						},
						"status": map[string]any{
							"phase": "Running",
							"containerStatuses": []map[string]any{
								{
									"name":  "node-janitor",
									"ready": true,
									"state": map[string]any{
										"running": map[string]any{
											"startedAt": "2026-04-16T00:00:00Z",
										},
									},
								},
							},
						},
					},
				},
			})
		case "/api/v1/events":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name":      "gcp1.182739",
							"namespace": "tenant-123",
						},
						"involvedObject": map[string]any{
							"kind": "Node",
							"name": "gcp1",
						},
						"type":          "Warning",
						"reason":        "NodeHasDiskPressure",
						"message":       "Node gcp1 status is now: NodeHasDiskPressure",
						"lastTimestamp": "2026-04-16T00:00:00Z",
					},
				},
			})
		case "/api/v1/nodes/gcp1/proxy/stats/summary":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"node": map[string]any{
					"nodeName": "gcp1",
					"cpu":      map[string]any{"usageNanoCores": 1},
					"memory": map[string]any{
						"availableBytes":  1024,
						"usageBytes":      2048,
						"workingSetBytes": 1536,
					},
					"fs": map[string]any{
						"availableBytes": 1024,
						"capacityBytes":  4096,
						"usedBytes":      3072,
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{
		ControlPlaneNamespace: "fugue-system",
	})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}
	server.filesystemExecRunner = &fakeFilesystemExecRunner{
		outputs: [][]byte{
			[]byte("/dev/sda1\t10000000000\t9000000000\t1000000000\t90%\t/var/lib\n"),
			[]byte("7000000000\t/var/lib/containerd\n"),
			[]byte("2026-04-16T00:00:00Z eviction manager: attempting to reclaim ephemeral-storage\n"),
		},
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/cluster/nodes/gcp1/diagnosis", "bootstrap-secret", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Diagnosis clusterNodeDiagnosis `json:"diagnosis"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Diagnosis.JanitorPod != "fugue-fugue-node-janitor-abc12" {
		t.Fatalf("expected janitor pod, got %+v", response.Diagnosis)
	}
	if len(response.Diagnosis.Filesystems) != 1 || response.Diagnosis.Filesystems[0].MountPath != "/var/lib" {
		t.Fatalf("expected filesystem usage, got %+v", response.Diagnosis.Filesystems)
	}
	if len(response.Diagnosis.HotPaths) != 1 || response.Diagnosis.HotPaths[0].Path != "/var/lib/containerd" {
		t.Fatalf("expected hot path usage, got %+v", response.Diagnosis.HotPaths)
	}
	if len(response.Diagnosis.Journal) != 1 || !strings.Contains(response.Diagnosis.Journal[0].Message, "eviction manager") {
		t.Fatalf("expected kubelet journal evidence, got %+v", response.Diagnosis.Journal)
	}
	if response.Diagnosis.Metrics == nil || response.Diagnosis.Metrics.Status != "available" {
		t.Fatalf("expected metrics diagnosis, got %+v", response.Diagnosis.Metrics)
	}
	if len(response.Diagnosis.Events) != 1 || response.Diagnosis.Events[0].Reason != "NodeHasDiskPressure" {
		t.Fatalf("expected node event evidence, got %+v", response.Diagnosis.Events)
	}
}

func TestFindNodeJanitorPodIgnoresPendingPods(t *testing.T) {
	t.Parallel()

	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/namespaces/fugue-system/pods", "/api/v1/pods":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"namespace": "fugue-system",
							"name":      "fugue-fugue-node-janitor-pending",
						},
						"spec": map[string]any{
							"nodeName": "gcp1",
						},
						"status": map[string]any{
							"phase": "Pending",
							"containerStatuses": []map[string]any{
								{
									"name": "node-janitor",
									"state": map[string]any{
										"waiting": map[string]any{
											"reason": "ContainerCreating",
										},
									},
								},
							},
						},
					},
					{
						"metadata": map[string]any{
							"namespace": "fugue-system",
							"name":      "fugue-fugue-node-janitor-other",
						},
						"spec": map[string]any{
							"nodeName": "gcp2",
						},
						"status": map[string]any{
							"phase": "Running",
							"containerStatuses": []map[string]any{
								{
									"name": "node-janitor",
									"state": map[string]any{
										"running": map[string]any{
											"startedAt": "2026-04-16T00:00:00Z",
										},
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

	server := &Server{controlPlaneNamespace: "fugue-system"}
	client := &clusterNodeClient{
		client:      kubeServer.Client(),
		baseURL:     kubeServer.URL,
		bearerToken: "test-token",
	}

	namespace, podName, err := server.findNodeJanitorPod(t.Context(), client, "gcp1")
	if err == nil {
		t.Fatalf("expected pending janitor pod to be rejected, got namespace=%q pod=%q", namespace, podName)
	}
	if !strings.Contains(err.Error(), "is not ready") || !strings.Contains(err.Error(), "ContainerCreating") {
		t.Fatalf("expected readiness error to explain pending pod, got %v", err)
	}
}
