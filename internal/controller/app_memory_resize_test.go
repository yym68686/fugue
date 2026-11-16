package controller

import (
	"encoding/json"
	"testing"

	"fugue/internal/model"
)

func TestAppMemoryResizeRejectsUnsafeEnvelopeAndChecksActualAllocation(t *testing.T) {
	for _, scenario := range []string{"ready", "init-floor", "restart-policy", "qos", "missing-actual", "old-actual", "old-allocation"} {
		t.Run(scenario, func(t *testing.T) {
			pod := appResizeTestPod(t)
			ob, err := observeAppRequestResizePod(pod, "service")
			if err != nil {
				t.Fatal(err)
			}
			target := appRequestResizeTarget{"memory", 96}
			switch scenario {
			case "init-floor":
				ob.InitContainers = []kubeResizeContainerSpec{{Name: "initializer", Resources: kubeResourceRequirements{Requests: map[string]string{"memory": "128Mi"}}}}
			case "restart-policy":
				ob.ResizePolicy = []kubeResizePolicy{{ResourceName: "memory", RestartPolicy: "RestartContainer"}}
			case "qos":
				ob.DesiredResources.Limits["cpu"] = "100m"
				ob.DesiredResources.Limits["memory"] = "128Mi"
				ob.Containers[0].Resources = ob.DesiredResources
			case "missing-actual":
				ob.ActualResources = nil
			}
			err = assessAppRequestResize(ob, target, false)
			unsafe := scenario == "init-floor" || scenario == "restart-policy" || scenario == "qos" || scenario == "missing-actual"
			if (err != nil) != unsafe {
				t.Fatalf("unexpected safety gate: %v", err)
			}
			if unsafe {
				return
			}
			if appRequestResizeConverged(ob, target) {
				t.Fatal("old request counted as success")
			}
			ob.DesiredResources = cloneKubeResourceRequirements(ob.DesiredResources)
			ob.DesiredResources.Requests["memory"] = "96Mi"
			actual := cloneKubeResourceRequirements(ob.DesiredResources)
			if scenario == "old-actual" {
				actual.Requests["memory"] = "128Mi"
			}
			ob.ActualResources = &actual
			ob.AllocatedResources = map[string]string{"memory": "96Mi"}
			if scenario == "old-allocation" {
				ob.AllocatedResources["memory"] = "128Mi"
			}
			if appRequestResizeConverged(ob, target) != (scenario == "ready") {
				t.Fatal("success did not require actual allocated resources")
			}
		})
	}
}

func TestAppMemoryResizeReadinessUsesVerifiedRequestAndImmutableTemplate(t *testing.T) {
	for _, scenario := range []string{"converged", "both-markers", "old-spec", "old-actual", "limit-drift", "wrong-marker", "missing-marker"} {
		t.Run(scenario, func(t *testing.T) {
			marker := int64(128)
			app := model.App{Name: "service", Spec: model.AppSpec{Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 96}, MemoryRequestTemplateMebibytes: &marker}}
			resizePod := appResizeTestPod(t)
			deployment := kubeDeployment{}
			deployment.Spec.Template.Spec.Containers = []kubeContainerSpec{{Name: "service", Resources: cloneKubeResourceRequirements(resizePod.Spec.Containers[0].Resources)}}
			resizePod.Spec.Containers[0].Resources.Requests["memory"] = "96Mi"
			resizePod.Status.ContainerStatuses[0].Resources.Requests["memory"] = "96Mi"
			resizePod.Status.ContainerStatuses[0].AllocatedResources["memory"] = "96Mi"
			switch scenario {
			case "both-markers":
				cpu := int64(100)
				app.Spec.CPURequestTemplateMilliCores = &cpu
				app.Spec.Resources.CPUMilliCores = 75
				resizePod.Spec.Containers[0].Resources.Requests["cpu"] = "75m"
				resizePod.Status.ContainerStatuses[0].Resources.Requests["cpu"] = "75m"
				resizePod.Status.ContainerStatuses[0].AllocatedResources["cpu"] = "75m"
			case "old-spec":
				resizePod.Spec.Containers[0].Resources.Requests["memory"] = "128Mi"
			case "old-actual":
				resizePod.Status.ContainerStatuses[0].Resources.Requests["memory"] = "128Mi"
			case "limit-drift":
				resizePod.Spec.Containers[0].Resources.Limits["memory"] = "512Mi"
			case "wrong-marker":
				marker = 256
			case "missing-marker":
				app.Spec.MemoryRequestTemplateMebibytes = nil
			}
			raw, err := json.Marshal(resizePod)
			if err != nil {
				t.Fatal(err)
			}
			var pod kubePod
			if err := json.Unmarshal(raw, &pod); err != nil {
				t.Fatal(err)
			}
			if podMatchesManagedAppDeploymentTemplate(pod, deployment, app) != (scenario == "converged" || scenario == "both-markers") {
				t.Fatal("readiness mismatched verified memory/template")
			}
		})
	}
}
