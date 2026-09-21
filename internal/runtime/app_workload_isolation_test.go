package runtime

import (
	"reflect"
	"testing"

	"fugue/internal/model"
)

func TestAppServicesSelectOnlyTheirWorkload(t *testing.T) {
	app := model.App{ID: "app_demo", TenantID: "tenant_demo", ProjectID: "project_demo", Name: "demo", Source: &model.AppSource{ComposeService: "web"}, Spec: model.AppSpec{Image: "example.invalid/app:v1", Ports: []int{8080}, Replicas: 1}}
	options := []RenderOptions{{}, {Revision: AppRevisionRenderOptions{Role: AppRevisionRoleCandidate, ReleaseID: "release-a", Suffix: "candidate-a"}}, {Revision: AppRevisionRenderOptions{Role: AppRevisionRolePrevious, ReleaseID: "release-b", Suffix: "previous-b"}}}
	labels := make([]map[string]string, len(options))
	objects := make([][]map[string]any, len(options))
	for i, option := range options {
		objects[i] = buildAppObjectsWithPlacementsAndOptions(app, SchedulingConstraints{}, nil, option)
		deployment := firstObjectByKind(t, objects[i], "Deployment")
		spec := deployment["spec"].(map[string]any)
		labels[i] = spec["template"].(map[string]any)["metadata"].(map[string]any)["labels"].(map[string]string)
		selector := spec["selector"].(map[string]any)["matchLabels"].(map[string]string)
		if selector[FugueLabelAppWorkload] != "" {
			t.Fatal("changed the immutable Deployment selector", selector)
		}
		if !reflect.DeepEqual(selector, mergeStringMaps(appLabels(app), appRevisionLabels(option))) {
			t.Fatal("legacy Deployment selector changed")
		}
		key := managedDeploymentRuntimeKey(deployment)
		delete(labels[i], FugueLabelAppWorkload)
		if managedDeploymentRuntimeKey(deployment) != key {
			t.Fatal("isolation metadata changed executable release identity")
		}
		labels[i][FugueLabelAppWorkload] = RuntimeAppResourceNameWithOptions(app, option)
	}
	for i, group := range objects {
		for _, object := range group {
			if object["kind"] != "Service" {
				continue
			}
			name := object["metadata"].(map[string]any)["name"].(string)
			selector := object["spec"].(map[string]any)["selector"].(map[string]string)
			want := 0
			if name == RuntimeAppServiceNameWithOptions(app, options[i]) {
				want = i
			}
			for j, podLabels := range labels {
				matches := true
				for k, v := range selector {
					matches = matches && podLabels[k] == v
				}
				if matches != (j == want) {
					t.Fatalf("Service %s matched workload %d, want only %d", name, j, want)
				}
			}
		}
	}
}
