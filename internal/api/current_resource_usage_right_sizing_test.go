package api

import (
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestBuildResourceUsageSamplesUsesBusiestRunningMainContainerReplica(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:        "app_a",
		TenantID:  "tenant_a",
		ProjectID: "project_a",
		Name:      "demo",
		Spec:      model.AppSpec{Replicas: 2},
	}
	snapshots := []clusterNodeSnapshot{
		{
			pods: []clusterNodePod{rightSizingTestPod("demo-a", "Running", app.ID, "demo")},
			summary: &kubeNodeSummary{Pods: []kubeNodeSummaryPod{
				rightSizingTestPodUsage("demo-a", 100, 256, 80, 240, 1),
			}},
		},
		{
			pods: []clusterNodePod{
				rightSizingTestPod("demo-b", "Running", app.ID, "demo"),
				// A rolling update may temporarily create more Pods than desired.
				rightSizingTestPod("demo-rollout", "Running", app.ID, "demo"),
				rightSizingTestPod("demo-pending", "Pending", app.ID, "demo"),
			},
			summary: &kubeNodeSummary{Pods: []kubeNodeSummaryPod{
				rightSizingTestPodUsage("demo-b", 220, 384, 180, 320, 2),
				rightSizingTestPodUsage("demo-rollout", 220, 384, 180, 320, 2),
				rightSizingTestPodUsage("demo-pending", 900, 768, 850, 700, 4),
			}},
		},
	}

	overlay := buildCurrentResourceUsageOverlay(snapshots, []model.App{app}, nil)
	assertResourceUsage(t, currentResourceUsagePointer(overlay.apps[app.ID]), 1440, 1792*1024*1024, 9*1024*1024)

	samples := buildResourceUsageSamples(time.Unix(100, 0).UTC(), []model.App{app}, nil, overlay)
	if len(samples) != 1 {
		t.Fatalf("expected one app sample, got %#v", samples)
	}
	sample := samples[0]
	if sample.TargetKind != rightSizingSampleTargetKindAppV1 {
		t.Fatalf("expected versioned per-replica target kind, got %q", sample.TargetKind)
	}
	if sample.CPUMilliCores == nil || *sample.CPUMilliCores != 180 {
		t.Fatalf("expected busiest main-container CPU 180m, got %#v", sample.CPUMilliCores)
	}
	if sample.MemoryBytes == nil || *sample.MemoryBytes != 320*1024*1024 {
		t.Fatalf("expected busiest main-container memory 320MiB, got %#v", sample.MemoryBytes)
	}
	if sample.EphemeralStorageBytes == nil || *sample.EphemeralStorageBytes != 2*1024*1024 {
		t.Fatalf("expected busiest running Pod ephemeral storage 2MiB, got %#v", sample.EphemeralStorageBytes)
	}
}

func TestBuildResourceUsageSamplesUsesPerReplicaBackingServiceUsage(t *testing.T) {
	t.Parallel()

	service := model.BackingService{
		ID:        "service_a",
		TenantID:  "tenant_a",
		ProjectID: "project_a",
		Name:      "database",
		Type:      model.BackingServiceTypePostgres,
	}
	first := rightSizingTestServicePod("postgres-a", service.ID, "postgres")
	second := rightSizingTestServicePod("postgres-b", service.ID, "postgres")
	snapshots := []clusterNodeSnapshot{{
		pods: []clusterNodePod{first, second},
		summary: &kubeNodeSummary{Pods: []kubeNodeSummaryPod{
			rightSizingTestPodUsage("postgres-a", 250, 512, 250, 512, 1),
			rightSizingTestPodUsage("postgres-b", 300, 640, 300, 640, 1),
		}},
	}}

	overlay := buildCurrentResourceUsageOverlay(snapshots, nil, []model.BackingService{service})
	samples := buildResourceUsageSamples(time.Unix(100, 0).UTC(), nil, []model.BackingService{service}, overlay)
	if len(samples) != 1 {
		t.Fatalf("expected one backing-service sample, got %#v", samples)
	}
	if samples[0].TargetKind != rightSizingSampleTargetKindBackingServiceV1 {
		t.Fatalf("expected versioned backing-service target kind, got %q", samples[0].TargetKind)
	}
	if samples[0].CPUMilliCores == nil || *samples[0].CPUMilliCores != 300 {
		t.Fatalf("expected per-instance CPU 300m, got %#v", samples[0].CPUMilliCores)
	}
	if samples[0].MemoryBytes == nil || *samples[0].MemoryBytes != 640*1024*1024 {
		t.Fatalf("expected per-instance memory 640MiB, got %#v", samples[0].MemoryBytes)
	}
}

func TestBuildResourceUsageSamplesSkipsPodsWithoutMainContainerMetrics(t *testing.T) {
	t.Parallel()

	app := model.App{ID: "app_a", TenantID: "tenant_a", Name: "demo"}
	pod := rightSizingTestPod("demo-a", "Running", app.ID, "demo")
	missingMain := rightSizingTestPodUsage("demo-a", 100, 256, 80, 240, 1)
	missingMain.Containers = missingMain.Containers[:1]
	overlay := buildCurrentResourceUsageOverlay([]clusterNodeSnapshot{{
		pods:    []clusterNodePod{pod},
		summary: &kubeNodeSummary{Pods: []kubeNodeSummaryPod{missingMain}},
	}}, []model.App{app}, nil)

	if got := buildResourceUsageSamples(time.Unix(100, 0).UTC(), []model.App{app}, nil, overlay); len(got) != 0 {
		t.Fatalf("expected missing main-container metrics to fail closed, got %#v", got)
	}
	assertResourceUsage(t, currentResourceUsagePointer(overlay.apps[app.ID]), 100, 256*1024*1024, 1024*1024)
}

func rightSizingTestPod(name, phase, appID, mainContainerName string) clusterNodePod {
	var pod clusterNodePod
	pod.Metadata.Name = name
	pod.Metadata.Namespace = "apps"
	pod.Metadata.Labels = map[string]string{runtime.FugueLabelAppID: appID}
	pod.Spec.Containers = []clusterNodeContainer{{Name: mainContainerName}}
	pod.Status.Phase = phase
	return pod
}

func rightSizingTestServicePod(name, serviceID, mainContainerName string) clusterNodePod {
	var pod clusterNodePod
	pod.Metadata.Name = name
	pod.Metadata.Namespace = "apps"
	pod.Metadata.Labels = map[string]string{runtime.FugueLabelBackingServiceID: serviceID}
	pod.Spec.Containers = []clusterNodeContainer{{Name: mainContainerName}}
	pod.Status.Phase = "Running"
	return pod
}

func rightSizingTestPodUsage(name string, podCPUMilli, podMemoryMiB, mainCPUMilli, mainMemoryMiB, ephemeralMiB uint64) kubeNodeSummaryPod {
	return kubeNodeSummaryPod{
		PodRef:           kubeNodeSummaryPodRef{Name: name, Namespace: "apps"},
		CPU:              kubeNodeSummaryCPU{UsageNanoCores: uint64PointerForRightSizingTest(podCPUMilli * 1_000_000)},
		Memory:           kubeNodeSummaryMem{WorkingSetBytes: uint64PointerForRightSizingTest(podMemoryMiB * 1024 * 1024)},
		EphemeralStorage: kubeNodeSummaryFS{UsedBytes: uint64PointerForRightSizingTest(ephemeralMiB * 1024 * 1024)},
		Containers: []kubeNodeSummaryContainer{
			{
				Name:   "fugue-drain-agent",
				CPU:    kubeNodeSummaryCPU{UsageNanoCores: uint64PointerForRightSizingTest((podCPUMilli - mainCPUMilli) * 1_000_000)},
				Memory: kubeNodeSummaryMem{WorkingSetBytes: uint64PointerForRightSizingTest((podMemoryMiB - mainMemoryMiB) * 1024 * 1024)},
			},
			{
				Name:   mainContainerNameForRightSizingTest(name),
				CPU:    kubeNodeSummaryCPU{UsageNanoCores: uint64PointerForRightSizingTest(mainCPUMilli * 1_000_000)},
				Memory: kubeNodeSummaryMem{WorkingSetBytes: uint64PointerForRightSizingTest(mainMemoryMiB * 1024 * 1024)},
			},
		},
	}
}

func mainContainerNameForRightSizingTest(podName string) string {
	if len(podName) >= len("postgres") && podName[:len("postgres")] == "postgres" {
		return "postgres"
	}
	return "demo"
}

func uint64PointerForRightSizingTest(value uint64) *uint64 {
	return &value
}
