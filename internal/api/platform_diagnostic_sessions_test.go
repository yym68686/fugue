package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/livediagnostics"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

type fakePlatformDiagnosticBackend struct {
	namespace string
	created   batchv1.Job
	jobs      []batchv1.Job
	pods      []kubePodInfo
	nodes     map[string]corev1.Node
}

func (f *fakePlatformDiagnosticBackend) RunnerImage(context.Context) (string, error) {
	return "registry.example/fugue-release-guardian@sha256:" + strings.Repeat("a", 64), nil
}
func (f *fakePlatformDiagnosticBackend) SessionNamespace() string { return f.namespace }
func (f *fakePlatformDiagnosticBackend) CreateJob(_ context.Context, namespace string, job batchv1.Job) (batchv1.Job, error) {
	if namespace != f.namespace {
		return batchv1.Job{}, fmt.Errorf("unexpected namespace %q", namespace)
	}
	f.created = job
	return job, nil
}
func (f *fakePlatformDiagnosticBackend) GetJob(_ context.Context, _, name string) (batchv1.Job, error) {
	for _, job := range append(f.jobs, f.created) {
		if job.Name == name {
			return job, nil
		}
	}
	return batchv1.Job{}, fmt.Errorf("not found")
}
func (f *fakePlatformDiagnosticBackend) ListJobs(context.Context, string, string) ([]batchv1.Job, error) {
	return f.jobs, nil
}
func (f *fakePlatformDiagnosticBackend) DeleteJob(context.Context, string, string) error { return nil }
func (f *fakePlatformDiagnosticBackend) ListPods(context.Context, string, string) ([]kubePodInfo, error) {
	return f.pods, nil
}
func (f *fakePlatformDiagnosticBackend) GetNode(_ context.Context, name string) (corev1.Node, error) {
	node, ok := f.nodes[name]
	if !ok {
		return corev1.Node{}, fmt.Errorf("node not found")
	}
	return node, nil
}
func (f *fakePlatformDiagnosticBackend) ReadPodLogs(context.Context, string, string, string) (string, error) {
	return `{"schema":"fugue.diagnostic.process_snapshot.v1"}`, nil
}

func TestPlatformDiagnosticStartResolvesTrustedComponentIdentity(t *testing.T) {
	stateStore, server, _, _ := setupAppConfigTestServer(t, appObservabilityTestSpec())
	server.auth = auth.New(stateStore, "bootstrap-secret")
	server.controlPlaneNamespace = "fugue-system"
	server.controlPlaneReleaseInstance = "fugue"
	backend := &fakePlatformDiagnosticBackend{namespace: "fugue-system"}
	var pod kubePodInfo
	pod.Metadata.Name = "fugue-fugue-api-abc"
	pod.Metadata.UID = "pod-uid-1"
	pod.Metadata.Labels = map[string]string{"app.kubernetes.io/instance": "fugue", "app.kubernetes.io/component": "api"}
	pod.Spec.NodeName = "control-1"
	pod.Spec.Containers = append(pod.Spec.Containers, struct {
		Name  string `json:"name"`
		Image string `json:"image,omitempty"`
	}{Name: "api", Image: "registry.example/api@sha256:" + strings.Repeat("b", 64)})
	pod.Status.Phase = "Running"
	pod.Status.ContainerStatuses = []kubeContainerStatus{{Name: "api", Ready: true, ContainerID: "containerd://abcdef1234567890", ImageID: "registry.example/api@sha256:" + strings.Repeat("b", 64)}}
	backend.pods = []kubePodInfo{pod}
	server.diagnosticSessionBackend = backend

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/admin/diagnostics/sessions", "bootstrap-secret", map[string]any{
		"target": map[string]any{"type": "platform_component", "component": "api"},
		"kind":   "memory-profile", "duration_seconds": 300, "sample_interval_milliseconds": 500,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	if backend.created.Labels[livediagnostics.TargetTypeLabel] != string(livediagnostics.TargetPlatformComponent) || backend.created.Annotations[livediagnostics.TargetPodUIDAnnotation] != "pod-uid-1" || backend.created.Annotations[livediagnostics.TargetImageAnnotation] == "" {
		t.Fatalf("target runtime facts were not frozen: labels=%+v annotations=%+v", backend.created.Labels, backend.created.Annotations)
	}
	args := strings.Join(backend.created.Spec.Template.Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "--kind memory-profile") || !strings.Contains(args, "--sample-interval-ms 500") {
		t.Fatalf("unexpected diagnostic args %q", args)
	}
}

func TestPlatformDiagnosticStartRejectsTenantAndArbitraryTargets(t *testing.T) {
	stateStore, server, tenantKey, _ := setupAppConfigTestServer(t, appObservabilityTestSpec())
	server.auth = auth.New(stateStore, "bootstrap-secret")
	server.controlPlaneNamespace = "fugue-system"
	server.controlPlaneReleaseInstance = "fugue"
	server.diagnosticSessionBackend = &fakePlatformDiagnosticBackend{namespace: "fugue-system"}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/admin/diagnostics/sessions", tenantKey, map[string]any{
		"target": map[string]any{"type": "platform_component", "component": "api"},
	})
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected tenant request to be forbidden, got %d", recorder.Code)
	}
	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/admin/diagnostics/sessions", "bootstrap-secret", map[string]any{
		"target": map[string]any{"type": "platform_component", "component": "api", "namespace": "tenant-runtime"},
	})
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), "limited") {
		t.Fatalf("expected namespace rejection, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/admin/diagnostics/sessions", "bootstrap-secret", map[string]any{
		"target": map[string]any{"type": "node_process", "node": "node-1", "process_name": "bash"},
	})
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), "allowlist") {
		t.Fatalf("expected process allowlist rejection, got %d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestResolveNodeProcessDiagnosticTargetRequiresReadyNode(t *testing.T) {
	ready := corev1.Node{}
	ready.Name = "node-1"
	ready.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	backend := &fakePlatformDiagnosticBackend{nodes: map[string]corev1.Node{"node-1": ready}}
	target, err := resolveNodeProcessDiagnosticTarget(context.Background(), backend, platformDiagnosticTargetRequest{Node: "node-1", ProcessName: "fugue-agent"})
	if err != nil || target.Type != livediagnostics.TargetNodeProcess {
		t.Fatalf("target=%+v err=%v", target, err)
	}
	ready.Status.Conditions[0].Status = corev1.ConditionFalse
	backend.nodes["node-1"] = ready
	if _, err := resolveNodeProcessDiagnosticTarget(context.Background(), backend, platformDiagnosticTargetRequest{Node: "node-1", ProcessName: "fugue-agent"}); err == nil {
		t.Fatal("expected NotReady target to be rejected")
	}
}
