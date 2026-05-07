package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestAppPlatformEnvDriftReportsStalePlatformEnv(t *testing.T) {
	t.Parallel()

	app := model.App{
		Spec: model.AppSpec{
			Env: map[string]string{
				"ARGUS_KEEP": "current",
				"USER_ENV":   "ignored",
			},
		},
	}
	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "fg-demo",
			Name:      "demo",
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name: "demo",
						Env: []corev1.EnvVar{
							{Name: "ARGUS_KEEP", Value: "current"},
							{Name: "ARGUS_FUGUE_RUNTIME_IMAGE", Value: "old"},
							{Name: "FUGUE_TOKEN", Value: "injected"},
							{Name: "USER_ENV", Value: "live-hotfix"},
						},
					}},
				},
			},
		},
	}

	report := appPlatformEnvDrift(app, deployment)
	joinedEvidence := strings.Join(report.evidence, "\n")
	if !strings.Contains(joinedEvidence, "ARGUS_FUGUE_RUNTIME_IMAGE") {
		t.Fatalf("expected stale ARGUS env evidence, got %#v", report.evidence)
	}
	if strings.Contains(joinedEvidence, "FUGUE_TOKEN") || strings.Contains(joinedEvidence, "USER_ENV") {
		t.Fatalf("expected injected Fugue and user env to be ignored, got %#v", report.evidence)
	}
	if len(report.warnings) == 0 {
		t.Fatalf("expected drift warning, got %#v", report)
	}
}

func TestGetAppDiagnosisExplainsEvictionVolumeAffinityChain(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	namespace := runtime.NamespaceForTenant(app.TenantID)
	selector, containerName, err := runtimeLogTarget(app, "app")
	if err != nil {
		t.Fatalf("runtime log target: %v", err)
	}

	fake := newFakeAppLogsClient()
	evictedPod := fakePod("demo-7c9d89d4c6-old", "Failed", time.Date(2026, 4, 16, 0, 0, 0, 0, time.UTC), containerName)
	evictedPod.Metadata.Namespace = namespace
	evictedPod.Spec.NodeName = "gcp1"
	evictedPod.Status.Reason = "Evicted"
	evictedPod.Status.Message = "The node had condition: [DiskPressure]."

	pendingPod := fakePod("demo-7c9d89d4c6-new", "Pending", time.Date(2026, 4, 16, 0, 1, 0, 0, time.UTC), containerName)
	pendingPod.Metadata.Namespace = namespace
	pendingPod.Status.ContainerStatuses = []kubeContainerStatus{{
		Name:  containerName,
		Ready: false,
		State: kubeRuntimeState{
			Waiting: &kubeStateDetail{
				Reason:  "Pending",
				Message: "0/4 nodes are available: 1 node(s) had volume node affinity conflict.",
			},
		},
	}}
	fake.setPods(selector, []kubePodInfo{evictedPod, pendingPod})
	server.newLogsClient = func(namespace string) (appLogsClient, error) {
		return fake, nil
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
		case "/api/v1/namespaces/" + namespace + "/events":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name":      "demo-7c9d89d4c6-old.182739",
							"namespace": namespace,
						},
						"involvedObject": map[string]any{
							"kind":      "Pod",
							"name":      "demo-7c9d89d4c6-old",
							"namespace": namespace,
						},
						"type":          "Warning",
						"reason":        "Evicted",
						"message":       "The node had condition: [DiskPressure].",
						"lastTimestamp": "2026-04-16T00:00:00Z",
					},
					{
						"metadata": map[string]any{
							"name":      "demo-7c9d89d4c6-new.182740",
							"namespace": namespace,
						},
						"involvedObject": map[string]any{
							"kind":      "Pod",
							"name":      "demo-7c9d89d4c6-new",
							"namespace": namespace,
						},
						"type":          "Warning",
						"reason":        "FailedScheduling",
						"message":       "0/4 nodes are available: 1 node(s) had volume node affinity conflict.",
						"lastTimestamp": "2026-04-16T00:01:00Z",
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/diagnosis", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Diagnosis appDiagnosis `json:"diagnosis"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Diagnosis.Category != "evicted-disk-pressure-volume-affinity" {
		t.Fatalf("expected eviction chain diagnosis, got %+v", response.Diagnosis)
	}
	if response.Diagnosis.ImplicatedNode != "gcp1" || response.Diagnosis.ImplicatedPod != "demo-7c9d89d4c6-old" {
		t.Fatalf("unexpected implicated workload %+v", response.Diagnosis)
	}
	if !strings.Contains(response.Diagnosis.Summary, "blocked by volume node affinity") {
		t.Fatalf("expected summary to explain volume affinity, got %+v", response.Diagnosis)
	}
	joinedEvidence := strings.Join(response.Diagnosis.Evidence, "\n")
	for _, want := range []string{
		"DiskPressure=True",
		"volume node affinity conflict",
	} {
		if !strings.Contains(joinedEvidence, want) {
			t.Fatalf("expected evidence to contain %q, got %+v", want, response.Diagnosis.Evidence)
		}
	}
}

func TestGetAppDiagnosisCountsOnlyActivePodsWhenReadyReplicaExists(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	selector, containerName, err := runtimeLogTarget(app, "app")
	if err != nil {
		t.Fatalf("runtime log target: %v", err)
	}

	fake := newFakeAppLogsClient()
	evictedPod := fakePod("demo-old", "Failed", time.Date(2026, 4, 16, 0, 0, 0, 0, time.UTC), containerName)
	evictedPod.Status.Reason = "Evicted"
	evictedPod.Status.Message = "The node had condition: [DiskPressure]."

	readyPod := fakePod("demo-new", "Running", time.Date(2026, 4, 16, 0, 1, 0, 0, time.UTC), containerName)
	readyPod.Status.ContainerStatuses = []kubeContainerStatus{{
		Name:  containerName,
		Image: "ghcr.io/example/demo:latest",
		Ready: true,
		State: kubeRuntimeState{
			Running: &struct{}{},
		},
	}}

	fake.setPods(selector, []kubePodInfo{evictedPod, readyPod})
	server.newLogsClient = func(namespace string) (appLogsClient, error) {
		return fake, nil
	}
	server.appRequestHTTPClient = &http.Client{
		Transport: diagnosticRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				Status:     "404 Not Found",
				StatusCode: http.StatusNotFound,
				Body:       http.NoBody,
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}),
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/diagnosis", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Diagnosis appDiagnosis `json:"diagnosis"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Diagnosis.Category != "available" {
		t.Fatalf("expected available diagnosis, got %+v", response.Diagnosis)
	}
	if response.Diagnosis.LivePods != 1 || response.Diagnosis.ReadyPods != 1 {
		t.Fatalf("expected 1/1 active ready pods, got live=%d ready=%d", response.Diagnosis.LivePods, response.Diagnosis.ReadyPods)
	}
	if response.Diagnosis.Summary != "1/1 runtime pods are ready" {
		t.Fatalf("expected active-only readiness summary, got %q", response.Diagnosis.Summary)
	}
}

func TestGetAppDiagnosisDetectsReadyPodHTTPTimeout(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	selector, containerName, err := runtimeLogTarget(app, "app")
	if err != nil {
		t.Fatalf("runtime log target: %v", err)
	}

	fake := newFakeAppLogsClient()
	readyPod := fakePod("demo-ready", "Running", time.Date(2026, 4, 16, 0, 1, 0, 0, time.UTC), containerName)
	readyPod.Status.ContainerStatuses = []kubeContainerStatus{{
		Name:  containerName,
		Image: "ghcr.io/example/demo:latest",
		Ready: true,
		State: kubeRuntimeState{
			Running: &struct{}{},
		},
	}}

	fake.setPods(selector, []kubePodInfo{readyPod})
	server.newLogsClient = func(namespace string) (appLogsClient, error) {
		return fake, nil
	}

	probedPaths := []string{}
	server.appRequestHTTPClient = &http.Client{
		Transport: diagnosticRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			probedPaths = append(probedPaths, req.URL.Path)
			return nil, context.DeadlineExceeded
		}),
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/diagnosis", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Diagnosis appDiagnosis `json:"diagnosis"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Diagnosis.Category != "http-timeout" {
		t.Fatalf("expected http-timeout diagnosis, got %+v", response.Diagnosis)
	}
	if response.Diagnosis.LivePods != 1 || response.Diagnosis.ReadyPods != 1 {
		t.Fatalf("expected 1/1 active ready pods, got live=%d ready=%d", response.Diagnosis.LivePods, response.Diagnosis.ReadyPods)
	}
	if !strings.Contains(response.Diagnosis.Summary, "internal HTTP probe timed out") {
		t.Fatalf("expected timeout summary, got %q", response.Diagnosis.Summary)
	}
	if strings.Join(probedPaths, ",") != "/healthz,/" {
		t.Fatalf("expected /healthz and / probes, got %#v", probedPaths)
	}
	joinedEvidence := strings.Join(response.Diagnosis.Evidence, "\n")
	if !strings.Contains(joinedEvidence, "http probe GET /healthz failed") || !strings.Contains(joinedEvidence, "http probe GET / failed") {
		t.Fatalf("expected HTTP probe evidence, got %+v", response.Diagnosis.Evidence)
	}
}

func TestGetAppDiagnosisIncludesCrashLogSnippetForPodFailure(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	namespace := runtime.NamespaceForTenant(app.TenantID)
	selector, containerName, err := runtimeLogTarget(app, "app")
	if err != nil {
		t.Fatalf("runtime log target: %v", err)
	}

	fake := newFakeAppLogsClient()
	pod := fakePod("demo-crash", "Running", time.Date(2026, 4, 16, 0, 2, 0, 0, time.UTC), containerName)
	pod.Metadata.Namespace = namespace
	pod.Status.ContainerStatuses = []kubeContainerStatus{{
		Name:  containerName,
		Image: "ghcr.io/example/demo:latest",
		Ready: false,
		State: kubeRuntimeState{
			Waiting: &kubeStateDetail{
				Reason:  "CrashLoopBackOff",
				Message: "back-off restarting failed container",
			},
		},
		LastState: kubeRuntimeState{
			Terminated: &kubeStateDetail{
				Reason:   "Error",
				Message:  "app boot failed",
				ExitCode: 1,
			},
		},
	}}
	fake.setPods(selector, []kubePodInfo{pod})
	fake.setLogLines(namespace, "demo-crash", containerName, true,
		"INFO booting app",
		`ERROR: Error loading ASGI app. Attribute "app" not found in module "mecgod_cloud.app".`,
	)
	server.newLogsClient = func(namespace string) (appLogsClient, error) {
		return fake, nil
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/diagnosis", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Diagnosis appDiagnosis `json:"diagnosis"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Diagnosis.Category != "pod-failure" {
		t.Fatalf("expected pod-failure diagnosis, got %+v", response.Diagnosis)
	}
	joinedEvidence := strings.Join(response.Diagnosis.Evidence, "\n")
	if !strings.Contains(joinedEvidence, `Attribute "app" not found in module "mecgod_cloud.app"`) {
		t.Fatalf("expected crash log evidence, got %+v", response.Diagnosis.Evidence)
	}
}
