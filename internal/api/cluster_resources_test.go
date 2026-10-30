package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestClusterResourceReportAdminAPIRecommendationsAndMetricsFailure(t *testing.T) {
	s := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := s.CreateTenant("Resource Tenant")
	project, _ := s.CreateProject(tenant.ID, "services", "")
	raiseManagedTestCap(t, s, tenant.ID)
	app, err := s.CreateApp(tenant.ID, project.ID, "web", "", model.AppSpec{Image: "registry.example/web:v1", RuntimeID: "runtime_managed_shared", Replicas: 1, Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 128}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordResourceUsageSamples(rightSizingUsageSamples(tenant.ID, model.ClusterNodeWorkloadKindApp, app.ID, []rightSizingUsageValue{{cpuMilli: 20, memoryMiB: 64}, {cpuMilli: 20, memoryMiB: 64}}), time.Time{}); err != nil {
		t.Fatal(err)
	}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "app-a", Name: "web-1", Labels: map[string]string{runtimepkg.FugueLabelAppID: app.ID}}, Spec: corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{
		{Name: "web", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")}}},
		{Name: "workspace", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("25m"), corev1.ResourceMemory: resource.MustParse("32Mi")}}},
	}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/nodes/node-a":
			json.NewEncoder(w).Encode(corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}})
		case "/api/v1/pods":
			if r.URL.Query().Get("fieldSelector") != "spec.nodeName=node-a" {
				t.Errorf("missing node selector: %s", r.URL.RawQuery)
			}
			json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{pod}})
		default:
			http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
		}
	}))
	defer kube.Close()
	server := NewServer(s, auth.New(s, "bootstrap-secret"), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{client: kube.Client(), baseURL: kube.URL}, nil
	}
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/cluster/resources?node=node-a&min_samples=2", "bootstrap-secret", nil)
	if recorder.Code != 200 {
		t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
	var report model.ClusterResourceReport
	mustDecodeJSON(t, recorder, &report)
	if len(report.Services) != 1 {
		t.Fatalf("unexpected services: %+v", report)
	}
	row := report.Services[0]
	if row.CPURequestMilliCores != 125 || row.CPURecommendedMilliCores == nil || *row.CPURecommendedMilliCores != 45 || row.CPUCapacityMilliCores == nil || *row.CPUCapacityMilliCores != 55 || row.CPUUsageMilliCores != nil || len(report.Warnings) == 0 {
		t.Fatalf("live requests and request/capacity recommendations must remain distinct: %+v", row)
	}
	_, key, err := s.CreateAPIKey(tenant.ID, "viewer", []string{"app.read"})
	if err != nil {
		t.Fatal(err)
	}
	if got := performJSONRequest(t, server, http.MethodGet, "/v1/cluster/resources?node=node-a", key, nil); got.Code != http.StatusForbidden {
		t.Fatalf("tenant must not read node-wide report: %d", got.Code)
	}
	if got := performJSONRequest(t, server, http.MethodGet, "/v1/cluster/resources?node=node-a&window_hours=169", "bootstrap-secret", nil); got.Code != http.StatusBadRequest {
		t.Fatalf("expected invalid window rejected: %d", got.Code)
	}
}

func TestBuildClusterResourceReportAggregatesNodeWorkloadsAndKeepsMissingUsageUnknown(t *testing.T) {
	now := time.Now().UTC()
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "app-a", Name: "web-1", Labels: map[string]string{runtimepkg.FugueLabelAppID: "app_a"}}, Spec: corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "web", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi")}}}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "app-a", Name: "web-2", Labels: map[string]string{runtimepkg.FugueLabelAppID: "app_a"}}, Spec: corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "web", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi")}}}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "app-a", Name: "web-elsewhere", Labels: map[string]string{runtimepkg.FugueLabelAppID: "app_a"}}, Spec: corev1.PodSpec{NodeName: "node-b", Containers: []corev1.Container{{Name: "web"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}},
	}
	metrics := []clusterPodMetrics{{Metadata: metav1.ObjectMeta{Namespace: "app-a", Name: "web-1"}, Timestamp: metav1.NewTime(now.Add(-time.Minute)), Containers: []struct {
		Name  string              `json:"name"`
		Usage corev1.ResourceList `json:"usage"`
	}{{Name: "web", Usage: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("25m"), corev1.ResourceMemory: resource.MustParse("20Mi")}}}}}
	apps := []model.App{{ID: "app_a", TenantID: "tenant_a", Name: "web", Spec: model.AppSpec{Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 64}}}}
	report, _ := buildClusterResourceReport("node-a", pods, metrics, apps, nil, now)
	if report.TotalPods != 2 || report.TotalServices != 1 || report.CPURequestMilliCores != 200 || report.MemoryRequestBytes != 128*1024*1024 {
		t.Fatalf("unexpected node totals: %+v", report)
	}
	row := report.Services[0]
	if row.PodCount != 2 || row.MeasuredPods != 1 || row.CPUUsageMilliCores != nil || row.MemoryUsageBytes != nil {
		t.Fatalf("incomplete service metrics must be unknown: %+v", row)
	}
}

func TestClusterPodResourceRequestsPreserveSidecarsAndInitPeakWhenReplacingMainContainer(t *testing.T) {
	restartAlways := corev1.ContainerRestartPolicyAlways
	pod := corev1.Pod{Spec: corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: "main", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi")}}},
			{Name: "sidecar", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("20m"), corev1.ResourceMemory: resource.MustParse("16Mi")}}},
		},
		InitContainers: []corev1.Container{
			{Name: "bootstrap", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("300m"), corev1.ResourceMemory: resource.MustParse("90Mi")}}},
			{Name: "log", RestartPolicy: &restartAlways, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("8Mi")}}},
		},
		Overhead: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("5m"), corev1.ResourceMemory: resource.MustParse("4Mi")},
	}}
	requests := clusterPodResourceRequests(pod, &model.ResourceSpec{CPUMilliCores: 50, MemoryMebibytes: 32}, true, true)
	if requests.cpuMilliCores != 305 || requests.memoryBytes != 94*1024*1024 {
		t.Fatalf("unexpected request with init peak and retained sidecars: %+v", requests)
	}
	pod.Spec.InitContainers = pod.Spec.InitContainers[1:]
	requests = clusterPodResourceRequests(pod, &model.ResourceSpec{CPUMilliCores: 50, MemoryMebibytes: 32}, true, true)
	if requests.cpuMilliCores != 85 || requests.memoryBytes != 60*1024*1024 {
		t.Fatalf("regular and restartable sidecar requests must survive replacement: %+v", requests)
	}
}

func TestClusterResourceMetricsFallbackRejectsStaleOrIncompleteContainers(t *testing.T) {
	now := time.Now().UTC()
	nano, memory := uint64(12500000), uint64(16*1024*1024)
	summary := &kubeNodeSummary{Pods: []kubeNodeSummaryPod{{PodRef: kubeNodeSummaryPodRef{Namespace: "app", Name: "web"}, Containers: []kubeNodeSummaryContainer{{Name: "main", CPU: kubeNodeSummaryCPU{Time: now.Format(time.RFC3339Nano), UsageNanoCores: &nano}, Memory: kubeNodeSummaryMem{Time: now.Format(time.RFC3339Nano), WorkingSetBytes: &memory}}}}}}
	metrics := clusterResourceMetricsFromSummary(summary)
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "web"}, Spec: corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "main"}}}}
	report, _ := buildClusterResourceReport("node-a", []corev1.Pod{pod}, metrics, nil, nil, now)
	if report.Services[0].CPUUsageMilliCores == nil || *report.Services[0].CPUUsageMilliCores != 12.5 || *report.Services[0].MemoryUsageBytes != int64(memory) {
		t.Fatalf("fallback should preserve CPU precision and working set: %+v", report)
	}
	report, _ = buildClusterResourceReport("node-a", []corev1.Pod{pod}, metrics, nil, nil, now.Add(6*time.Minute))
	if report.Services[0].CPUUsageMilliCores != nil {
		t.Fatal("stale fallback statistics must be unknown")
	}
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "workspace"})
	report, _ = buildClusterResourceReport("node-a", []corev1.Pod{pod}, metrics, nil, nil, now)
	if report.Services[0].CPUUsageMilliCores != nil {
		t.Fatal("missing sidecar metrics must be unknown")
	}
}
