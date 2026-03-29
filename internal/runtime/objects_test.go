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

func TestBuildAppDeploymentTemplateAnnotationsTrackFilesAndRestart(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Files: []model.AppFile{
				{
					Path:    "/home/api.yaml",
					Content: "providers: []",
					Secret:  true,
					Mode:    0o600,
				},
			},
			RestartToken: "restart_1",
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	deployment := objects[2]
	template := deployment["spec"].(map[string]any)["template"].(map[string]any)
	annotations := template["metadata"].(map[string]any)["annotations"].(map[string]string)
	initialChecksum := annotations["fugue.pro/files-checksum"]
	if initialChecksum == "" {
		t.Fatal("expected file checksum annotation")
	}
	if annotations["fugue.pro/restart-token"] != "restart_1" {
		t.Fatalf("unexpected restart token annotation: %#v", annotations["fugue.pro/restart-token"])
	}

	app.Spec.Files[0].Content = "providers:\n  - gemini"
	updatedObjects := buildAppObjects(app, SchedulingConstraints{})
	updatedDeployment := updatedObjects[2]
	updatedTemplate := updatedDeployment["spec"].(map[string]any)["template"].(map[string]any)
	updatedAnnotations := updatedTemplate["metadata"].(map[string]any)["annotations"].(map[string]string)
	if updatedAnnotations["fugue.pro/files-checksum"] == initialChecksum {
		t.Fatal("expected file checksum annotation to change when file content changes")
	}
}

func TestBuildAppDeploymentUsesRollingUpdateAndReadinessProbe(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	deployment := objects[1]
	spec := deployment["spec"].(map[string]any)
	strategy := spec["strategy"].(map[string]any)
	if strategy["type"] != "RollingUpdate" {
		t.Fatalf("expected RollingUpdate strategy, got %#v", strategy["type"])
	}
	rollingUpdate := strategy["rollingUpdate"].(map[string]any)
	if rollingUpdate["maxUnavailable"] != 0 {
		t.Fatalf("expected maxUnavailable=0, got %#v", rollingUpdate["maxUnavailable"])
	}
	if rollingUpdate["maxSurge"] != 1 {
		t.Fatalf("expected maxSurge=1, got %#v", rollingUpdate["maxSurge"])
	}

	template := spec["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)
	containers := podSpec["containers"].([]map[string]any)
	readinessProbe := containers[0]["readinessProbe"].(map[string]any)
	tcpSocket := readinessProbe["tcpSocket"].(map[string]any)
	if tcpSocket["port"] != 8080 {
		t.Fatalf("expected readiness probe port 8080, got %#v", tcpSocket["port"])
	}
}

func TestBuildAppObjectsIncludesPersistentWorkspaceSidecar(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Workspace: &model.AppWorkspaceSpec{
				ResetToken: "workspace-reset-1",
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	deployment := objects[1]
	template := deployment["spec"].(map[string]any)["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)

	volumes := podSpec["volumes"].([]map[string]any)
	if len(volumes) != 1 {
		t.Fatalf("expected one workspace volume, got %d", len(volumes))
	}
	hostPath := volumes[0]["hostPath"].(map[string]any)
	if hostPath["path"] != "/var/lib/fugue/tenant-data/fg-tenant-demo/apps/app_demo/workspace" {
		t.Fatalf("unexpected workspace host path: %s", hostPath["path"])
	}

	containers := podSpec["containers"].([]map[string]any)
	if len(containers) != 2 {
		t.Fatalf("expected app container and workspace sidecar, got %d containers", len(containers))
	}
	if containers[1]["name"] != AppWorkspaceContainerName {
		t.Fatalf("expected workspace sidecar %q, got %#v", AppWorkspaceContainerName, containers[1]["name"])
	}
	workspaceMounts := containers[0]["volumeMounts"].([]map[string]any)
	if workspaceMounts[0]["mountPath"] != "/workspace" {
		t.Fatalf("unexpected workspace mount path: %#v", workspaceMounts[0]["mountPath"])
	}

	initContainers := podSpec["initContainers"].([]map[string]any)
	if len(initContainers) != 1 {
		t.Fatalf("expected one workspace init container, got %d", len(initContainers))
	}
	command := initContainers[0]["command"].([]string)
	if got := command[len(command)-1]; got != "workspace-reset-1" {
		t.Fatalf("expected workspace reset token in init container command, got %q", got)
	}
}

func TestBuildAppObjectsUsesBackingServicesWithoutDuplicatingLegacyInlinePostgres(t *testing.T) {
	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "uni-api-demo",
		Spec: model.AppSpec{
			Image:     "registry.fugue.pro/fugue-apps/uni-api:git-abc123",
			Ports:     []int{8000},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Env: map[string]string{
				"DB_HOST": "override-db.internal",
				"APP_ENV": "prod",
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
				Database:    "legacy",
				User:        "legacy",
				Password:    "legacy-secret",
				ServiceName: "legacy-postgres",
			},
		},
		BackingServices: []model.BackingService{
			{
				ID:          "service_demo",
				TenantID:    "tenant_demo",
				ProjectID:   "project_demo",
				OwnerAppID:  "app_demo",
				Name:        "uni-api-demo",
				Type:        model.BackingServiceTypePostgres,
				Provisioner: model.BackingServiceProvisionerManaged,
				Status:      model.BackingServiceStatusActive,
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						Database:    "uniapi",
						User:        "root",
						Password:    "secret",
						ServiceName: "uni-api-demo-postgres",
					},
				},
			},
		},
		Bindings: []model.ServiceBinding{
			{
				ID:        "binding_demo",
				TenantID:  "tenant_demo",
				AppID:     "app_demo",
				ServiceID: "service_demo",
				Alias:     "postgres",
				Env: map[string]string{
					"DB_TYPE":     "postgres",
					"DB_HOST":     "uni-api-demo-postgres",
					"DB_PORT":     "5432",
					"DB_USER":     "root",
					"DB_PASSWORD": "secret",
					"DB_NAME":     "uniapi",
				},
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	if len(objects) != 7 {
		t.Fatalf("expected 7 objects, got %d", len(objects))
	}

	appDeployment := objects[5]
	appLabels := appDeployment["metadata"].(map[string]any)["labels"].(map[string]string)
	if appLabels[FugueLabelAppID] != "app_demo" {
		t.Fatalf("expected app id label %q, got %#v", "app_demo", appLabels[FugueLabelAppID])
	}
	if appLabels[FugueLabelTenantID] != "tenant_demo" {
		t.Fatalf("expected tenant id label %q, got %#v", "tenant_demo", appLabels[FugueLabelTenantID])
	}
	if appLabels[FugueLabelProjectID] != "project_demo" {
		t.Fatalf("expected project id label %q, got %#v", "project_demo", appLabels[FugueLabelProjectID])
	}
	appTemplate := appDeployment["spec"].(map[string]any)["template"].(map[string]any)
	appPodSpec := appTemplate["spec"].(map[string]any)
	envObjects := appPodSpec["containers"].([]map[string]any)[0]["env"].([]map[string]any)
	if got := envValue(envObjects, "DB_HOST"); got != "override-db.internal" {
		t.Fatalf("expected DB_HOST override from app env, got %q", got)
	}
	if got := envValue(envObjects, "DB_USER"); got != "root" {
		t.Fatalf("expected DB_USER from binding env, got %q", got)
	}
	if got := envValue(envObjects, "APP_ENV"); got != "prod" {
		t.Fatalf("expected APP_ENV=prod, got %q", got)
	}

	postgresService := objects[3]
	if got := postgresService["metadata"].(map[string]any)["name"]; got != "uni-api-demo-postgres" {
		t.Fatalf("expected managed backing service resource name, got %#v", got)
	}
	postgresLabels := postgresService["metadata"].(map[string]any)["labels"].(map[string]string)
	if postgresLabels[FugueLabelBackingServiceID] != "service_demo" {
		t.Fatalf("expected backing service id label %q, got %#v", "service_demo", postgresLabels[FugueLabelBackingServiceID])
	}
	if postgresLabels[FugueLabelOwnerAppID] != "app_demo" {
		t.Fatalf("expected owner app id label %q, got %#v", "app_demo", postgresLabels[FugueLabelOwnerAppID])
	}
	if postgresLabels[FugueLabelBackingServiceType] != model.BackingServiceTypePostgres {
		t.Fatalf("expected backing service type label %q, got %#v", model.BackingServiceTypePostgres, postgresLabels[FugueLabelBackingServiceType])
	}
}

func TestBuildManagedPostgresObjectsUseStableSelectors(t *testing.T) {
	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "uni-api-demo",
		Spec: model.AppSpec{
			Image:     "registry.fugue.pro/fugue-apps/uni-api:git-abc123",
			Ports:     []int{8000},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
		BackingServices: []model.BackingService{
			{
				ID:          "service_demo",
				TenantID:    "tenant_demo",
				ProjectID:   "project_demo",
				OwnerAppID:  "app_demo",
				Name:        "uni-api-demo",
				Type:        model.BackingServiceTypePostgres,
				Provisioner: model.BackingServiceProvisionerManaged,
				Status:      model.BackingServiceStatusActive,
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						Database:    "uniapi",
						User:        "root",
						Password:    "secret",
						ServiceName: "uni-api-demo-postgres",
					},
				},
			},
		},
		Bindings: []model.ServiceBinding{
			{
				ID:        "binding_demo",
				TenantID:  "tenant_demo",
				AppID:     "app_demo",
				ServiceID: "service_demo",
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	postgresService := objects[2]
	serviceSelector := postgresService["spec"].(map[string]any)["selector"].(map[string]string)
	expectedSelector := map[string]string{
		FugueLabelName:      "uni-api-demo-postgres",
		FugueLabelComponent: "postgres",
		FugueLabelManagedBy: FugueLabelManagedByValue,
	}
	if len(serviceSelector) != len(expectedSelector) {
		t.Fatalf("expected postgres service selector %v, got %v", expectedSelector, serviceSelector)
	}
	for key, value := range expectedSelector {
		if got := serviceSelector[key]; got != value {
			t.Fatalf("expected postgres service selector %s=%q, got %#v", key, value, got)
		}
	}
	if _, exists := serviceSelector[FugueLabelBackingServiceID]; exists {
		t.Fatalf("expected postgres service selector to omit %s, got %v", FugueLabelBackingServiceID, serviceSelector)
	}
	if _, exists := serviceSelector[FugueLabelOwnerAppID]; exists {
		t.Fatalf("expected postgres service selector to omit %s, got %v", FugueLabelOwnerAppID, serviceSelector)
	}

	postgresDeployment := objects[3]
	deploymentSelector := postgresDeployment["spec"].(map[string]any)["selector"].(map[string]any)["matchLabels"].(map[string]string)
	if len(deploymentSelector) != len(expectedSelector) {
		t.Fatalf("expected postgres deployment selector %v, got %v", expectedSelector, deploymentSelector)
	}
	for key, value := range expectedSelector {
		if got := deploymentSelector[key]; got != value {
			t.Fatalf("expected postgres deployment selector %s=%q, got %#v", key, value, got)
		}
	}
	if _, exists := deploymentSelector[FugueLabelBackingServiceID]; exists {
		t.Fatalf("expected postgres deployment selector to omit %s, got %v", FugueLabelBackingServiceID, deploymentSelector)
	}
	if _, exists := deploymentSelector[FugueLabelOwnerAppID]; exists {
		t.Fatalf("expected postgres deployment selector to omit %s, got %v", FugueLabelOwnerAppID, deploymentSelector)
	}

	metadataLabels := postgresDeployment["metadata"].(map[string]any)["labels"].(map[string]string)
	if got := metadataLabels[FugueLabelBackingServiceID]; got != "service_demo" {
		t.Fatalf("expected postgres metadata label %s=%q, got %#v", FugueLabelBackingServiceID, "service_demo", got)
	}
	if got := metadataLabels[FugueLabelOwnerAppID]; got != "app_demo" {
		t.Fatalf("expected postgres metadata label %s=%q, got %#v", FugueLabelOwnerAppID, "app_demo", got)
	}
}

func envValue(envObjects []map[string]any, name string) string {
	for _, entry := range envObjects {
		if entry["name"] == name {
			if value, ok := entry["value"].(string); ok {
				return value
			}
		}
	}
	return ""
}
