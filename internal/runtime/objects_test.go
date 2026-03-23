package runtime

import (
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestBuildAppObjectsIncludesStatefulResources(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "uni-api-demo",
		Spec: model.AppSpec{
			Image:     "registry.fugue.pro/fugue-apps/uni-api:git-abc123",
			Ports:     []int{8000},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Env: map[string]string{
				"DB_TYPE": "postgres",
			},
			Files: []model.AppFile{
				{
					Path:    "/home/api.yaml",
					Content: "providers: []",
					Secret:  true,
					Mode:    0o600,
				},
			},
			Postgres: &model.AppPostgresSpec{
				Database:    "uniapi",
				User:        "root",
				Password:    "secret",
				ServiceName: "uni-api-demo-postgres",
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{
		NodeSelector: map[string]string{
			RuntimeIDLabelKey: "runtime_demo",
		},
	})

	if len(objects) != 7 {
		t.Fatalf("expected 7 objects, got %d", len(objects))
	}
	if kind, _ := objects[1]["kind"].(string); kind != "Secret" {
		t.Fatalf("expected app files secret, got %#v", objects[1]["kind"])
	}
	if kind, _ := objects[3]["kind"].(string); kind != "Service" {
		t.Fatalf("expected postgres service, got %#v", objects[3]["kind"])
	}
	pgDeployment := objects[4]
	spec := pgDeployment["spec"].(map[string]any)
	template := spec["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)
	volumes := podSpec["volumes"].([]map[string]any)
	hostPath := volumes[0]["hostPath"].(map[string]any)
	if !strings.Contains(hostPath["path"].(string), "/tenant-data/fg-tenant-demo/uni-api-demo/postgres") {
		t.Fatalf("unexpected postgres host path: %s", hostPath["path"])
	}
	appDeployment := objects[5]
	appTemplate := appDeployment["spec"].(map[string]any)["template"].(map[string]any)
	appPodSpec := appTemplate["spec"].(map[string]any)
	if _, ok := appPodSpec["initContainers"]; !ok {
		t.Fatalf("expected wait-postgres init container")
	}
	containers := appPodSpec["containers"].([]map[string]any)
	volumeMounts := containers[0]["volumeMounts"].([]map[string]any)
	if volumeMounts[0]["mountPath"] != "/home/api.yaml" {
		t.Fatalf("unexpected mount path: %#v", volumeMounts[0]["mountPath"])
	}
}
