package controller

import "testing"

func resizeObservationFixture() managedPostgresResizeObservation {
	return managedPostgresResizeObservation{
		Namespace:       "tenant-a",
		PodName:         "database-1",
		PodUID:          "pod-uid",
		ResourceVersion: "42",
		Phase:           "Running",
		PodReady:        true,
		ContainerName:   managedPostgresMainContainerName,
		ContainerReady:  true,
		RestartCount:    3,
		DesiredResources: kubeResourceRequirements{
			Requests: map[string]string{"cpu": "100m", "memory": "512Mi", "ephemeral-storage": "2Gi"},
			Limits:   map[string]string{"cpu": "500m", "memory": "1Gi"},
		},
		ResizePolicy: []kubeResizePolicy{
			{ResourceName: "cpu", RestartPolicy: "NotRequired"},
			{ResourceName: "memory", RestartPolicy: "NotRequired"},
		},
	}
}

func TestAssessManagedPostgresResizeAllowsRequestOnlyUpscale(t *testing.T) {
	observation := resizeObservationFixture()
	baseline := observation.RestartCount
	assessment := assessManagedPostgresResize(observation, kubeResourceRequirements{
		Requests: map[string]string{"cpu": "150m", "memory": "640Mi"},
	}, managedPostgresResizeSafetyOptions{BaselineRestartCount: &baseline})
	if assessment.State != managedPostgresResizeStateReady {
		t.Fatalf("expected safe request upscale, got %+v", assessment)
	}
	if len(assessment.IncreaseResources) != 2 || len(assessment.DecreaseResources) != 0 {
		t.Fatalf("unexpected resize direction: %+v", assessment)
	}
}

func TestAssessManagedPostgresResizeBlocksRequestDownscaleByDefault(t *testing.T) {
	observation := resizeObservationFixture()
	assessment := assessManagedPostgresResize(observation, kubeResourceRequirements{
		Requests: map[string]string{"cpu": "75m"},
	}, managedPostgresResizeSafetyOptions{})
	if assessment.State != managedPostgresResizeStateBlocked || assessment.Reason != "request_downscale_disabled" {
		t.Fatalf("expected request downscale to be blocked, got %+v", assessment)
	}
}

func TestAssessManagedPostgresResizeBlocksLimitChangeUntilEnabled(t *testing.T) {
	observation := resizeObservationFixture()
	assessment := assessManagedPostgresResize(observation, kubeResourceRequirements{
		Limits: map[string]string{"cpu": "800m"},
	}, managedPostgresResizeSafetyOptions{})
	if assessment.State != managedPostgresResizeStateBlocked || assessment.Reason != "limit_resize_disabled" {
		t.Fatalf("expected limit change to be blocked, got %+v", assessment)
	}
	assessment = assessManagedPostgresResize(observation, kubeResourceRequirements{
		Limits: map[string]string{"cpu": "800m"},
	}, managedPostgresResizeSafetyOptions{AllowLimitChanges: true})
	if assessment.State != managedPostgresResizeStateReady {
		t.Fatalf("expected explicitly enabled limit change, got %+v", assessment)
	}
}

func TestAssessManagedPostgresResizeRejectsRestartAndPendingStates(t *testing.T) {
	observation := resizeObservationFixture()
	baseline := observation.RestartCount - 1
	assessment := assessManagedPostgresResize(observation, kubeResourceRequirements{
		Requests: map[string]string{"cpu": "150m"},
	}, managedPostgresResizeSafetyOptions{BaselineRestartCount: &baseline})
	if assessment.Reason != "restart_detected" {
		t.Fatalf("expected restart guard, got %+v", assessment)
	}

	observation = resizeObservationFixture()
	observation.Conditions = []managedPostgresResizeCondition{{
		Type: "PodResizePending", Status: "True", Reason: "Deferred", Message: "waiting for node capacity",
	}}
	assessment = assessManagedPostgresResize(observation, kubeResourceRequirements{
		Requests: map[string]string{"cpu": "150m"},
	}, managedPostgresResizeSafetyOptions{})
	if assessment.State != managedPostgresResizeStateDeferred || assessment.Reason != "kubernetes_resize_deferred" {
		t.Fatalf("expected pending resize state, got %+v", assessment)
	}

	observation.Conditions = []managedPostgresResizeCondition{{
		Type: "PodResizePending", Status: "True", Reason: "Infeasible", Message: "requested memory exceeds node capacity",
	}}
	assessment = assessManagedPostgresResize(observation, kubeResourceRequirements{
		Requests: map[string]string{"cpu": "150m"},
	}, managedPostgresResizeSafetyOptions{})
	if assessment.State != managedPostgresResizeStateInfeasible || assessment.Reason != "kubernetes_resize_infeasible" {
		t.Fatalf("expected infeasible resize state, got %+v", assessment)
	}
}

func TestAssessManagedPostgresResizeRejectsRestartRequiredPolicy(t *testing.T) {
	observation := resizeObservationFixture()
	observation.ResizePolicy = []kubeResizePolicy{{ResourceName: "cpu", RestartPolicy: "RestartContainer"}}
	assessment := assessManagedPostgresResize(observation, kubeResourceRequirements{
		Requests: map[string]string{"cpu": "150m"},
	}, managedPostgresResizeSafetyOptions{})
	if assessment.State != managedPostgresResizeStateBlocked || assessment.Reason != "restart_policy" {
		t.Fatalf("expected restart policy guard, got %+v", assessment)
	}
}

func TestAssessManagedPostgresResizeReportsNoop(t *testing.T) {
	observation := resizeObservationFixture()
	assessment := assessManagedPostgresResize(observation, kubeResourceRequirements{
		Requests: map[string]string{"cpu": "100m"},
	}, managedPostgresResizeSafetyOptions{})
	if assessment.State != managedPostgresResizeStateNoop || assessment.Reason != "already_current" {
		t.Fatalf("expected no-op resize, got %+v", assessment)
	}
}

func TestMergePodResizeResourcesPreservesUnownedDimensions(t *testing.T) {
	merged, err := mergePodResizeResources(
		kubeResourceRequirements{
			Requests: map[string]string{"cpu": "100m", "memory": "512Mi", "ephemeral-storage": "2Gi"},
			Limits:   map[string]string{"cpu": "500m", "memory": "1Gi"},
		},
		kubeResourceRequirements{Requests: map[string]string{"cpu": "150m"}},
	)
	if err != nil {
		t.Fatalf("merge resize resources: %v", err)
	}
	if merged.Requests["cpu"] != "150m" || merged.Requests["memory"] != "512Mi" || merged.Requests["ephemeral-storage"] != "2Gi" {
		t.Fatalf("unowned request dimensions were not preserved: %+v", merged)
	}
	if merged.Limits["cpu"] != "500m" || merged.Limits["memory"] != "1Gi" {
		t.Fatalf("unowned limits were not preserved: %+v", merged)
	}
}
