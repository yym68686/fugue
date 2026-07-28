package controller

import (
	"strings"
	"testing"

	"fugue/internal/config"
	"fugue/internal/runtime"
)

func TestPlanManagedPostgresResizeStagesUsesSafeDirectionOrder(t *testing.T) {
	current := kubeResourceRequirements{
		Requests: map[string]string{"cpu": "100m", "memory": "1024Mi"},
		Limits:   map[string]string{"cpu": "200m", "memory": "1536Mi"},
	}
	target := kubeResourceRequirements{
		Requests: map[string]string{"cpu": "300m", "memory": "768Mi"},
		Limits:   map[string]string{"cpu": "500m", "memory": "1024Mi"},
	}

	stages, err := planManagedPostgresResizeStages(current, target)
	if err != nil {
		t.Fatalf("plan resize: %v", err)
	}
	wantNames := []string{
		managedPostgresResizeStageLimitUpscale,
		managedPostgresResizeStageRequestUpscale,
		managedPostgresResizeStageRequestDownscale,
		managedPostgresResizeStageLimitDownscale,
	}
	if len(stages) != len(wantNames) {
		t.Fatalf("expected %d stages, got %+v", len(wantNames), stages)
	}
	wantChanges := []string{"limits.cpu", "requests.cpu", "requests.memory", "limits.memory"}
	for index, wantName := range wantNames {
		if stages[index].Name != wantName {
			t.Fatalf("stage %d name: got %q want %q", index, stages[index].Name, wantName)
		}
		if len(stages[index].ChangedResources) != 1 || stages[index].ChangedResources[0] != wantChanges[index] {
			t.Fatalf("stage %d changes: got %+v want %q", index, stages[index].ChangedResources, wantChanges[index])
		}
		if err := validateCompleteManagedPostgresResizeEnvelope(stages[index].Resources); err != nil {
			t.Fatalf("stage %d is not independently valid: %v", index, err)
		}
	}
	if !managedPostgresResizeResourcesEqual(stages[len(stages)-1].Resources, target) {
		t.Fatalf("final stage did not reach target: %+v", stages[len(stages)-1])
	}
}

func TestPlanManagedPostgresResizeStagesRejectsIncompleteEnvelope(t *testing.T) {
	current := kubeResourceRequirements{
		Requests: map[string]string{"cpu": "100m", "memory": "1024Mi"},
		Limits:   map[string]string{"memory": "1536Mi"},
	}
	target := kubeResourceRequirements{
		Requests: map[string]string{"cpu": "200m", "memory": "1024Mi"},
		Limits:   map[string]string{"cpu": "300m", "memory": "1536Mi"},
	}
	if _, err := planManagedPostgresResizeStages(current, target); err == nil || !strings.Contains(err.Error(), "limits.cpu") {
		t.Fatalf("expected missing CPU limit to fail closed, got %v", err)
	}
}

func TestManagedPostgresResizeDirectionGatesAreIndependent(t *testing.T) {
	gates := config.ManagedPostgresInPlaceResizeConfig{Enabled: true, CPURequestUpscaleEnabled: true}
	if allowed, reason := managedPostgresResizeDirectionGate("requests.cpu", true, gates); !allowed || reason != "enabled" {
		t.Fatalf("expected CPU request upscale enabled, got allowed=%t reason=%q", allowed, reason)
	}
	if allowed, reason := managedPostgresResizeDirectionGate("requests.cpu", false, gates); allowed || reason != "requests_cpu_downscale_disabled" {
		t.Fatalf("expected independent CPU request downscale gate, got allowed=%t reason=%q", allowed, reason)
	}
	if allowed, reason := managedPostgresResizeDirectionGate("limits.memory", true, gates); allowed || reason != "limits_memory_upscale_disabled" {
		t.Fatalf("expected memory limit upscale closed, got allowed=%t reason=%q", allowed, reason)
	}
	gates.Enabled = false
	if allowed, reason := managedPostgresResizeDirectionGate("requests.cpu", true, gates); allowed || reason != "global_resize_disabled" {
		t.Fatalf("expected global gate to dominate, got allowed=%t reason=%q", allowed, reason)
	}
}

func TestManagedPostgresResizeInvariantBaselineRejectsIdentityDrift(t *testing.T) {
	cluster, observation := managedPostgresResizeInvariantFixture(t)
	baseline, err := captureManagedPostgresResizeInvariantBaseline("tenant-demo", "demo-postgres", cluster, observation)
	if err != nil {
		t.Fatalf("capture baseline: %v", err)
	}
	if err := validateManagedPostgresResizeInvariantBaseline(baseline, cluster, observation); err != nil {
		t.Fatalf("validate unchanged baseline: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*kubeCloudNativePGCluster, *managedPostgresResizeObservation)
		want   string
	}{
		{name: "pod uid", mutate: func(_ *kubeCloudNativePGCluster, observation *managedPostgresResizeObservation) {
			observation.PodUID = "replacement-pod-uid"
		}, want: "Pod UID"},
		{name: "restart", mutate: func(_ *kubeCloudNativePGCluster, observation *managedPostgresResizeObservation) {
			observation.RestartCount++
		}, want: "restart count"},
		{name: "primary", mutate: func(cluster *kubeCloudNativePGCluster, _ *managedPostgresResizeObservation) {
			cluster.Status.CurrentPrimary = "demo-postgres-2"
		}, want: "current primary"},
		{name: "cluster generation", mutate: func(cluster *kubeCloudNativePGCluster, _ *managedPostgresResizeObservation) {
			cluster.Metadata.Generation++
		}, want: "cluster generation"},
		{name: "terminating", mutate: func(_ *kubeCloudNativePGCluster, observation *managedPostgresResizeObservation) {
			observation.DeletionTimestamp = "2026-07-28T00:00:00Z"
		}, want: "terminating"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidateCluster := cluster
			candidateObservation := observation
			test.mutate(&candidateCluster, &candidateObservation)
			if err := validateManagedPostgresResizeInvariantBaseline(baseline, candidateCluster, candidateObservation); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %q invariant failure, got %v", test.want, err)
			}
		})
	}
}

func managedPostgresResizeInvariantFixture(t *testing.T) (kubeCloudNativePGCluster, managedPostgresResizeObservation) {
	t.Helper()
	controller := true
	var cluster kubeCloudNativePGCluster
	cluster.Metadata.Name = "demo-postgres"
	cluster.Metadata.UID = "cluster-uid"
	cluster.Metadata.ResourceVersion = "100"
	cluster.Metadata.Generation = 7
	cluster.Spec.Instances = 1
	cluster.Status.ReadyInstances = 1
	cluster.Status.CurrentPrimary = "demo-postgres-1"
	cluster.Status.TargetPrimary = "demo-postgres-1"
	cluster.Status.Conditions = []runtime.ManagedAppCondition{{Type: "Ready", Status: "True"}}

	observation := managedPostgresResizeObservation{
		Namespace:          "tenant-demo",
		PodName:            "demo-postgres-1",
		PodUID:             "pod-uid",
		ResourceVersion:    "200",
		Generation:         3,
		ObservedGeneration: 3,
		Labels: map[string]string{
			cloudNativePGClusterLabel:      "demo-postgres",
			cloudNativePGLegacyRoleLabel:   "primary",
			cloudNativePGInstanceRoleLabel: "primary",
		},
		OwnerReferences: []kubeResizeOwnerReference{{
			APIVersion: "postgresql.cnpg.io/v1",
			Kind:       "Cluster",
			Name:       "demo-postgres",
			UID:        "cluster-uid",
			Controller: &controller,
		}},
		NodeName:           "node-a",
		Phase:              "Running",
		PodReady:           true,
		ContainerName:      managedPostgresMainContainerName,
		ContainerReady:     true,
		RestartCount:       4,
		ContainerStartedAt: "2026-07-28T00:00:00Z",
		DesiredResources: kubeResourceRequirements{
			Requests: map[string]string{"cpu": "100m", "memory": "1024Mi"},
			Limits:   map[string]string{"cpu": "200m", "memory": "1536Mi"},
		},
	}
	actual := cloneKubeResourceRequirements(observation.DesiredResources)
	observation.ActualResources = &actual
	return cluster, observation
}
