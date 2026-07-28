package controller

import "testing"

func resizeObservationFixture() managedPostgresResizeObservation {
	observation := managedPostgresResizeObservation{
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
	observation.Containers = []kubeResizeContainerSpec{{
		Name:      managedPostgresMainContainerName,
		Resources: cloneKubeResourceRequirements(observation.DesiredResources),
	}}
	return observation
}

func TestAssessManagedPostgresResizeBlocksDownscaleBelowInitContainerFloor(t *testing.T) {
	observation := resizeObservationFixture()
	observation.DesiredResources.Requests["cpu"] = "500m"
	observation.Containers[0].Resources.Requests["cpu"] = "500m"
	observation.InitContainers = []kubeResizeContainerSpec{{
		Name: "bootstrap-controller",
		Resources: kubeResourceRequirements{Requests: map[string]string{
			"cpu": "500m", "memory": "512Mi",
		}},
	}}

	assessment := assessManagedPostgresResize(observation, kubeResourceRequirements{
		Requests: map[string]string{"cpu": "100m"},
	}, managedPostgresResizeSafetyOptions{AllowRequestDownscale: true})
	if assessment.State != managedPostgresResizeStateBlocked || assessment.Reason != "ineffective_request_downscale" {
		t.Fatalf("expected ineffective downscale to be blocked, got %+v", assessment)
	}
}

func TestAssessManagedPostgresResizeAllowsDownscaleThatReleasesEffectiveRequest(t *testing.T) {
	observation := resizeObservationFixture()
	observation.DesiredResources.Requests["cpu"] = "500m"
	observation.Containers[0].Resources.Requests["cpu"] = "500m"
	observation.InitContainers = []kubeResizeContainerSpec{{
		Name: "bootstrap-controller",
		Resources: kubeResourceRequirements{Requests: map[string]string{
			"cpu": "100m", "memory": "512Mi",
		}},
	}}

	assessment := assessManagedPostgresResize(observation, kubeResourceRequirements{
		Requests: map[string]string{"cpu": "150m"},
	}, managedPostgresResizeSafetyOptions{AllowRequestDownscale: true})
	if assessment.State != managedPostgresResizeStateReady {
		t.Fatalf("expected effective downscale to pass, got %+v", assessment)
	}
}

func TestManagedPostgresEffectivePodRequestsMatchesRestartableInitSemantics(t *testing.T) {
	always := "Always"
	observation := resizeObservationFixture()
	observation.Containers = append(observation.Containers, kubeResizeContainerSpec{
		Name: "metrics",
		Resources: kubeResourceRequirements{Requests: map[string]string{
			"cpu": "50m", "memory": "64Mi",
		}},
	})
	observation.InitContainers = []kubeResizeContainerSpec{
		{
			Name: "sidecar-init", RestartPolicy: &always,
			Resources: kubeResourceRequirements{Requests: map[string]string{"cpu": "25m", "memory": "32Mi"}},
		},
		{
			Name:      "bootstrap",
			Resources: kubeResourceRequirements{Requests: map[string]string{"cpu": "300m", "memory": "768Mi"}},
		},
	}

	requests, err := managedPostgresEffectivePodRequests(observation, observation.DesiredResources)
	if err != nil {
		t.Fatalf("calculate effective requests: %v", err)
	}
	cpu := requests["cpu"]
	if got := cpu.MilliValue(); got != 325 {
		t.Fatalf("expected restartable init CPU accumulation of 325m, got %dm", got)
	}
	memory := requests["memory"]
	if got := memory.Value(); got != 800*1024*1024 {
		t.Fatalf("expected restartable init memory accumulation of 800Mi, got %d", got)
	}
}

func TestAssessManagedPostgresResizeBlocksPodLevelRequestDownscale(t *testing.T) {
	observation := resizeObservationFixture()
	observation.PodResources = &kubeResourceRequirements{Requests: map[string]string{"cpu": "200m"}}
	assessment := assessManagedPostgresResize(observation, kubeResourceRequirements{
		Requests: map[string]string{"cpu": "75m"},
	}, managedPostgresResizeSafetyOptions{AllowRequestDownscale: true})
	if assessment.State != managedPostgresResizeStateBlocked || assessment.Reason != "pod_level_request_floor" {
		t.Fatalf("expected Pod-level request floor to block downscale, got %+v", assessment)
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
