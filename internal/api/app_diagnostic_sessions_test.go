package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

type fakeDiagnosticSessionBackend struct {
	namespace string
	created   batchv1.Job
}

func (f *fakeDiagnosticSessionBackend) RunnerImage(context.Context) (string, error) {
	return "registry.example/fugue-api@sha256:" + strings.Repeat("a", 64), nil
}

func (f *fakeDiagnosticSessionBackend) SessionNamespace() string { return f.namespace }

func (f *fakeDiagnosticSessionBackend) CreateJob(_ context.Context, namespace string, job batchv1.Job) (batchv1.Job, error) {
	if namespace != f.namespace {
		return batchv1.Job{}, fmt.Errorf("unexpected namespace %q", namespace)
	}
	f.created = job
	return job, nil
}

func (f *fakeDiagnosticSessionBackend) GetJob(context.Context, string, string) (batchv1.Job, error) {
	return batchv1.Job{}, fmt.Errorf("not implemented")
}

func (f *fakeDiagnosticSessionBackend) ListJobs(context.Context, string, string) ([]batchv1.Job, error) {
	return []batchv1.Job{}, nil
}

func (f *fakeDiagnosticSessionBackend) DeleteJob(context.Context, string, string) error {
	return fmt.Errorf("not implemented")
}

func (f *fakeDiagnosticSessionBackend) ListPods(context.Context, string, string) ([]kubePodInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeDiagnosticSessionBackend) GetNode(context.Context, string) (corev1.Node, error) {
	return corev1.Node{}, fmt.Errorf("not implemented")
}

func (f *fakeDiagnosticSessionBackend) ReadPodLogs(context.Context, string, string, string) (string, error) {
	return "", fmt.Errorf("not implemented")
}

func TestNormalizeDiagnosticStartRequestUsesBoundedDefaults(t *testing.T) {
	req := diagnosticStartRequest{}
	if err := normalizeDiagnosticStartRequest(&req); err != nil {
		t.Fatal(err)
	}
	if req.Kind != "cpu-profile" || req.DurationSeconds != diagnosticDefaultDuration || req.FrequencyHz != diagnosticDefaultFrequency {
		t.Fatalf("unexpected defaults: %+v", req)
	}
	for _, req := range []diagnosticStartRequest{
		{Kind: "shell", DurationSeconds: 10, FrequencyHz: 10},
		{Kind: "cpu-profile", DurationSeconds: diagnosticMaxDuration + 1, FrequencyHz: 10},
		{Kind: "cpu-profile", DurationSeconds: 10, FrequencyHz: diagnosticMaxFrequency + 1},
	} {
		if err := normalizeDiagnosticStartRequest(&req); err == nil {
			t.Fatalf("expected request to be rejected: %+v", req)
		}
	}
}

func TestBuildDiagnosticJobIsBoundedAndDoesNotMutateTargetWorkload(t *testing.T) {
	app := model.App{ID: "app-123"}
	target := diagnosticTarget{
		Namespace:   "tenant-runtime",
		PodName:     "demo-7fcb6d9cc9-a1b2c",
		PodUID:      "pod-uid-1",
		Container:   "app",
		ContainerID: "containerd://abcdef",
		NodeName:    "runtime-node-1",
	}
	req := diagnosticStartRequest{Kind: "cpu-profile", DurationSeconds: 60, FrequencyHz: 19}
	job, err := buildDiagnosticJob(app, "diagnostic-123", "fugue-system", target, req, "registry.example/fugue-api@sha256:"+strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}

	if job.Namespace != "fugue-system" || job.Namespace == target.Namespace {
		t.Fatalf("diagnostic job escaped the control-plane namespace: %q", job.Namespace)
	}
	if job.Annotations[diagnosticTargetNamespaceAnnotation] != target.Namespace {
		t.Fatalf("target namespace was not retained as audit metadata: %+v", job.Annotations)
	}
	if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != 105 || job.Spec.TTLSecondsAfterFinished == nil {
		t.Fatalf("diagnostic job is missing lifecycle bounds: %+v", job.Spec)
	}
	pod := job.Spec.Template.Spec
	if !pod.HostPID || pod.NodeName != target.NodeName || pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Fatalf("unexpected diagnostic pod isolation: %+v", pod)
	}
	if len(pod.Containers) != 1 {
		t.Fatalf("expected one fixed diagnostic agent container, got %d", len(pod.Containers))
	}
	container := pod.Containers[0]
	if container.Command[0] != diagnosticAgentBinary || strings.Contains(strings.Join(container.Args, " "), "sh -c") {
		t.Fatalf("diagnostic command is not allowlisted: command=%q args=%q", container.Command, container.Args)
	}
	if container.SecurityContext == nil || container.SecurityContext.Privileged == nil || *container.SecurityContext.Privileged || container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatalf("diagnostic security context changed: %+v", container.SecurityContext)
	}
	if container.SecurityContext.AllowPrivilegeEscalation == nil || *container.SecurityContext.AllowPrivilegeEscalation || len(container.SecurityContext.Capabilities.Drop) != 1 || len(container.SecurityContext.Capabilities.Add) != 4 {
		t.Fatalf("diagnostic capabilities are not narrowly bounded: %+v", container.SecurityContext)
	}
	if got := fmt.Sprint(container.SecurityContext.Capabilities.Add); got != "[PERFMON SYS_PTRACE SYS_ADMIN SYSLOG]" {
		t.Fatalf("unexpected diagnostic capabilities %s", got)
	}
	if got := container.Resources.Limits.Cpu().MilliValue(); got != 250 {
		t.Fatalf("expected 250m CPU limit, got %dm", got)
	}
	if got := container.Resources.Limits.Memory().Value(); got != 256<<20 {
		t.Fatalf("expected 256Mi memory limit, got %d", got)
	}
	if len(pod.Volumes) != 3 || len(container.VolumeMounts) != 3 {
		t.Fatalf("expected only host proc, host cgroup, and scratch mounts: volumes=%+v mounts=%+v", pod.Volumes, container.VolumeMounts)
	}
}

func TestDiagnosticStartRequiresPlatformAdministrator(t *testing.T) {
	_, server, tenantKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/diagnostics/sessions", tenantKey, map[string]any{
		"kind": "cpu-profile",
	})
	if recorder.Code != http.StatusForbidden || !strings.Contains(recorder.Body.String(), "platform administrator") {
		t.Fatalf("expected platform-admin rejection, got %d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestDiagnosticStartCreatesControlPlaneJobForReadyAppContainer(t *testing.T) {
	stateStore, server, _, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	server.auth = auth.New(stateStore, "bootstrap-secret")
	backend := &fakeDiagnosticSessionBackend{namespace: "fugue-system"}
	server.diagnosticSessionBackend = backend

	selector, container, err := runtimeLogTarget(app, "app")
	if err != nil {
		t.Fatal(err)
	}
	logs := newFakeAppLogsClient()
	var pod kubePodInfo
	pod.Metadata.Name = "demo-7fcb6d9cc9-a1b2c"
	pod.Metadata.UID = "pod-uid-1"
	pod.Spec.NodeName = "runtime-node-1"
	pod.Status.Phase = "Running"
	pod.Status.ContainerStatuses = []kubeContainerStatus{{Name: container, ContainerID: "containerd://abcdef1234567890", Ready: true}}
	logs.setPods(selector, []kubePodInfo{pod})
	server.newLogsClient = func(string) (appLogsClient, error) { return logs, nil }

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/diagnostics/sessions", "bootstrap-secret", map[string]any{
		"kind": "cpu-profile", "duration_seconds": 15, "frequency_hz": 17,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	if backend.created.Namespace != "fugue-system" || backend.created.Annotations[diagnosticTargetNamespaceAnnotation] == "fugue-system" {
		t.Fatalf("diagnostic execution escaped namespace boundary: %+v", backend.created.ObjectMeta)
	}
	args := strings.Join(backend.created.Spec.Template.Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "containerd://abcdef1234567890") || !strings.Contains(args, "15") || !strings.Contains(args, "17") {
		t.Fatalf("unexpected agent args %q", args)
	}
}

func TestDecodeDiagnosticReportIsStrictlyBounded(t *testing.T) {
	report, err := decodeDiagnosticReport("runtime prefix\n{\"schema\":\"fugue.diagnostic.cpu_profile.v1\",\"samples\":12}\n")
	if err != nil {
		t.Fatal(err)
	}
	value, ok := report.(map[string]any)
	if !ok || value["samples"] != float64(12) {
		t.Fatalf("unexpected report: %#v", report)
	}
	if _, err := decodeDiagnosticReport(strings.Repeat("x", diagnosticMaxReportBytes+1)); err == nil {
		t.Fatal("expected oversized report to be rejected")
	}
	if _, err := decodeDiagnosticReport(fmt.Sprintf("not-json-%d", diagnosticMaxReportBytes)); err == nil {
		t.Fatal("expected invalid report to be rejected")
	}
}

func TestCountActiveDiagnosticJobsIgnoresCompletedJobs(t *testing.T) {
	target := diagnosticTarget{Namespace: "tenant-runtime", PodName: "app-1", PodUID: "pod-uid-1", Container: "app", ContainerID: "containerd://abcdef1234567890", NodeName: "node-1"}
	image := "registry.example/fugue-api@sha256:" + strings.Repeat("a", 64)
	request := diagnosticStartRequest{Kind: "cpu-profile", DurationSeconds: 60, FrequencyHz: 19}
	active, err := buildDiagnosticJob(model.App{ID: "app-a"}, "diagnostic-active", "fugue-system", target, request, image)
	if err != nil {
		t.Fatal(err)
	}
	other, err := buildDiagnosticJob(model.App{ID: "app-b"}, "diagnostic-other", "fugue-system", target, request, image)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := buildDiagnosticJob(model.App{ID: "app-a"}, "diagnostic-completed", "fugue-system", target, request, image)
	if err != nil {
		t.Fatal(err)
	}
	completed.Status.Succeeded = 1
	global, app := countActiveDiagnosticJobs([]batchv1.Job{active, other, completed}, "app-a")
	if global != 2 || app != 1 {
		t.Fatalf("unexpected active counts global=%d app=%d", global, app)
	}
}
