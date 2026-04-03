package runtime

import (
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
	if kind, _ := objects[2]["kind"].(string); kind != "Secret" {
		t.Fatalf("expected postgres secret, got %#v", objects[2]["kind"])
	}
	if kind, _ := objects[3]["kind"].(string); kind != "Service" {
		t.Fatalf("expected postgres alias service, got %#v", objects[3]["kind"])
	}
	postgresAliasService := objects[3]
	postgresAliasSpec := postgresAliasService["spec"].(map[string]any)
	if got := postgresAliasSpec["type"]; got != "ExternalName" {
		t.Fatalf("expected postgres alias service type ExternalName, got %#v", got)
	}
	if got := postgresAliasSpec["externalName"]; got != "uni-api-demo-postgres-rw.fg-tenant-demo.svc.cluster.local" {
		t.Fatalf("expected postgres alias external name, got %#v", got)
	}
	if kind, _ := objects[4]["kind"].(string); kind != CloudNativePGClusterKind {
		t.Fatalf("expected postgres cluster, got %#v", objects[4]["kind"])
	}
	clusterSpec := objects[4]["spec"].(map[string]any)
	if got := clusterSpec["instances"]; got != defaultPostgresInstances {
		t.Fatalf("expected postgres instances %d, got %#v", defaultPostgresInstances, got)
	}
	if _, ok := clusterSpec["maxSyncReplicas"]; ok {
		t.Fatalf("expected single-instance postgres to omit maxSyncReplicas, got %#v", clusterSpec["maxSyncReplicas"])
	}
	storage := clusterSpec["storage"].(map[string]any)
	if got := storage["size"]; got != defaultPostgresStorage {
		t.Fatalf("expected postgres storage %q, got %#v", defaultPostgresStorage, got)
	}
	initdb := clusterSpec["bootstrap"].(map[string]any)["initdb"].(map[string]any)
	if got := initdb["database"]; got != "uniapi" {
		t.Fatalf("expected initdb database %q, got %#v", "uniapi", got)
	}
	appDeployment := objects[5]
	appTemplate := appDeployment["spec"].(map[string]any)["template"].(map[string]any)
	appPodSpec := appTemplate["spec"].(map[string]any)
	initContainers, ok := appPodSpec["initContainers"].([]map[string]any)
	if !ok {
		t.Fatalf("expected wait-postgres init container")
	}
	containers := appPodSpec["containers"].([]map[string]any)
	envObjects := containers[0]["env"].([]map[string]any)
	if got := envValue(envObjects, "DB_HOST"); got != "uni-api-demo-postgres-rw" {
		t.Fatalf("expected inline postgres DB_HOST to use rw service, got %q", got)
	}
	command := initContainers[0]["command"].([]string)
	if got := command[2]; got != "until nc -z uni-api-demo-postgres-rw 5432; do sleep 2; done" {
		t.Fatalf("expected wait-postgres init container to target rw service, got %q", got)
	}
	volumeMounts := containers[0]["volumeMounts"].([]map[string]any)
	if volumeMounts[0]["mountPath"] != "/home/api.yaml" {
		t.Fatalf("unexpected mount path: %#v", volumeMounts[0]["mountPath"])
	}
}

func TestNormalizeRuntimePostgresSpecDefaultsToAppScopedUser(t *testing.T) {
	spec := normalizeRuntimePostgresSpec("", "fugue-web", model.AppPostgresSpec{})
	if spec.User != "fugue_web" {
		t.Fatalf("expected app-scoped user fugue_web, got %q", spec.User)
	}
	if spec.Instances != 1 {
		t.Fatalf("expected default postgres instances 1, got %d", spec.Instances)
	}
	if spec.SynchronousReplicas != 0 {
		t.Fatalf("expected default synchronous replicas 0 for single-instance postgres, got %d", spec.SynchronousReplicas)
	}
}

func TestNormalizeRuntimePostgresSpecKeepsLegacyPostgresUserForStoragePath(t *testing.T) {
	spec := normalizeRuntimePostgresSpec("", "fugue-web", model.AppPostgresSpec{
		StoragePath: "/var/lib/postgres",
	})
	if spec.User != "postgres" {
		t.Fatalf("expected legacy postgres user, got %q", spec.User)
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
	deployment := objects[3]
	template := deployment["spec"].(map[string]any)["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)

	volumes := podSpec["volumes"].([]map[string]any)
	if len(volumes) != 1 {
		t.Fatalf("expected one workspace volume, got %d", len(volumes))
	}
	claim := volumes[0]["persistentVolumeClaim"].(map[string]any)
	if got := claim["claimName"]; got != WorkspacePVCName(app) {
		t.Fatalf("unexpected workspace pvc claim: %#v", got)
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

	workspacePVC := objects[1]
	if got := workspacePVC["kind"]; got != "PersistentVolumeClaim" {
		t.Fatalf("expected workspace pvc, got %#v", got)
	}
	requests := workspacePVC["spec"].(map[string]any)["resources"].(map[string]any)["requests"].(map[string]any)
	if got := requests["storage"]; got != defaultWorkspaceStorage {
		t.Fatalf("expected workspace storage %q, got %#v", defaultWorkspaceStorage, got)
	}

	replicationDestination := objects[2]
	if got := replicationDestination["kind"]; got != VolSyncReplicationDestinationKind {
		t.Fatalf("expected workspace replication destination, got %#v", got)
	}
	destinationSpec := replicationDestination["spec"].(map[string]any)
	destinationRsyncTLS := destinationSpec["rsyncTLS"].(map[string]any)
	if got := destinationRsyncTLS["copyMethod"]; got != "Direct" {
		t.Fatalf("expected workspace replication destination copyMethod %q, got %#v", "Direct", got)
	}

	strategy := deployment["spec"].(map[string]any)["strategy"].(map[string]any)
	if got := strategy["type"]; got != "Recreate" {
		t.Fatalf("expected workspace deployment strategy Recreate, got %#v", got)
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

	postgresCluster := objects[4]
	if got := postgresCluster["metadata"].(map[string]any)["name"]; got != "uni-api-demo-postgres" {
		t.Fatalf("expected managed backing service resource name, got %#v", got)
	}
	postgresLabels := postgresCluster["metadata"].(map[string]any)["labels"].(map[string]string)
	if postgresLabels[FugueLabelBackingServiceID] != "service_demo" {
		t.Fatalf("expected backing service id label %q, got %#v", "service_demo", postgresLabels[FugueLabelBackingServiceID])
	}
	if postgresLabels[FugueLabelOwnerAppID] != "app_demo" {
		t.Fatalf("expected owner app id label %q, got %#v", "app_demo", postgresLabels[FugueLabelOwnerAppID])
	}
	if postgresLabels[FugueLabelBackingServiceType] != model.BackingServiceTypePostgres {
		t.Fatalf("expected backing service type label %q, got %#v", model.BackingServiceTypePostgres, postgresLabels[FugueLabelBackingServiceType])
	}

	clusterSpec := postgresCluster["spec"].(map[string]any)
	storage := clusterSpec["storage"].(map[string]any)
	if got := storage["size"]; got != defaultPostgresStorage {
		t.Fatalf("expected postgres storage %q, got %#v", defaultPostgresStorage, got)
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
	postgresSecret := objects[1]
	secretLabels := postgresSecret["metadata"].(map[string]any)["labels"].(map[string]string)
	if got := secretLabels[FugueLabelBackingServiceID]; got != "service_demo" {
		t.Fatalf("expected postgres secret label %s=%q, got %#v", FugueLabelBackingServiceID, "service_demo", got)
	}
	if got := secretLabels[FugueLabelOwnerAppID]; got != "app_demo" {
		t.Fatalf("expected postgres secret label %s=%q, got %#v", FugueLabelOwnerAppID, "app_demo", got)
	}

	postgresAliasService := objects[2]
	if got := postgresAliasService["kind"]; got != "Service" {
		t.Fatalf("expected postgres service alias, got %#v", got)
	}
	aliasSpec := postgresAliasService["spec"].(map[string]any)
	if got := aliasSpec["externalName"]; got != "uni-api-demo-postgres-rw.fg-tenant-demo.svc.cluster.local" {
		t.Fatalf("expected postgres alias external name, got %#v", got)
	}

	postgresCluster := objects[3]
	if got := postgresCluster["kind"]; got != CloudNativePGClusterKind {
		t.Fatalf("expected postgres cluster, got %#v", got)
	}
	metadataLabels := postgresCluster["metadata"].(map[string]any)["labels"].(map[string]string)
	if got := metadataLabels[FugueLabelBackingServiceID]; got != "service_demo" {
		t.Fatalf("expected postgres metadata label %s=%q, got %#v", FugueLabelBackingServiceID, "service_demo", got)
	}
	if got := metadataLabels[FugueLabelOwnerAppID]; got != "app_demo" {
		t.Fatalf("expected postgres metadata label %s=%q, got %#v", FugueLabelOwnerAppID, "app_demo", got)
	}

	clusterSpec := postgresCluster["spec"].(map[string]any)
	if got := clusterSpec["instances"]; got != defaultPostgresInstances {
		t.Fatalf("expected postgres instances %d, got %#v", defaultPostgresInstances, got)
	}
	if _, ok := clusterSpec["maxSyncReplicas"]; ok {
		t.Fatalf("expected single-instance postgres to omit maxSyncReplicas, got %#v", clusterSpec["maxSyncReplicas"])
	}
}

func TestBuildAppObjectsKeepsLegacyManagedPostgresResourcesWhenStoragePathPresent(t *testing.T) {
	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "legacy-postgres-demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
		BackingServices: []model.BackingService{
			{
				ID:          "service_demo",
				TenantID:    "tenant_demo",
				ProjectID:   "project_demo",
				OwnerAppID:  "app_demo",
				Name:        "legacy-postgres-demo",
				Type:        model.BackingServiceTypePostgres,
				Provisioner: model.BackingServiceProvisionerManaged,
				Status:      model.BackingServiceStatusActive,
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						Image:       "postgres:16-alpine",
						Database:    "demo",
						User:        "postgres",
						Password:    "secret",
						ServiceName: "legacy-postgres-demo-postgres",
						StoragePath: "/var/lib/fugue/tenant-data/fg-tenant-demo/legacy-postgres-demo/postgres",
						StorageSize: "2Gi",
						Instances:   1,
						Resources:   &model.ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 512},
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
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	if len(objects) != 8 {
		t.Fatalf("expected 8 objects, got %d", len(objects))
	}

	aliasService := objects[2]
	if got := aliasService["kind"]; got != "Service" {
		t.Fatalf("expected alias service, got %#v", got)
	}
	aliasSpec := aliasService["spec"].(map[string]any)
	if got := aliasSpec["type"]; got != "ExternalName" {
		t.Fatalf("expected alias service type ExternalName, got %#v", got)
	}
	if got := aliasSpec["externalName"]; got != "legacy-postgres-demo-postgres-rw.fg-tenant-demo.svc.cluster.local" {
		t.Fatalf("expected alias external name, got %#v", got)
	}

	rwService := objects[3]
	if got := rwService["kind"]; got != "Service" {
		t.Fatalf("expected read-write service, got %#v", got)
	}
	rwMetadata := rwService["metadata"].(map[string]any)
	if got := rwMetadata["name"]; got != "legacy-postgres-demo-postgres-rw" {
		t.Fatalf("expected read-write service name, got %#v", got)
	}
	rwSpec := rwService["spec"].(map[string]any)
	selector := rwSpec["selector"].(map[string]string)
	if got := selector[FugueLabelName]; got != "legacy-postgres-demo-postgres" {
		t.Fatalf("expected selector label %s=%q, got %#v", FugueLabelName, "legacy-postgres-demo-postgres", got)
	}

	postgresPVC := objects[4]
	if got := postgresPVC["kind"]; got != "PersistentVolumeClaim" {
		t.Fatalf("expected postgres pvc, got %#v", got)
	}
	pvcSpec := postgresPVC["spec"].(map[string]any)
	requests := pvcSpec["resources"].(map[string]any)["requests"].(map[string]any)
	if got := requests["storage"]; got != "2Gi" {
		t.Fatalf("expected legacy postgres pvc storage %q, got %#v", "2Gi", got)
	}

	postgresDeployment := objects[5]
	if got := postgresDeployment["kind"]; got != "Deployment" {
		t.Fatalf("expected legacy postgres deployment, got %#v", got)
	}
	if got := postgresDeployment["metadata"].(map[string]any)["name"]; got != "legacy-postgres-demo-postgres" {
		t.Fatalf("expected legacy postgres deployment name, got %#v", got)
	}
}

func TestManagedBackingServiceDeploymentsUseLegacyDeploymentWhenStoragePathPresent(t *testing.T) {
	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "legacy-postgres-demo",
		BackingServices: []model.BackingService{
			{
				ID:          "service_demo",
				TenantID:    "tenant_demo",
				ProjectID:   "project_demo",
				OwnerAppID:  "app_demo",
				Name:        "legacy-postgres-demo",
				Type:        model.BackingServiceTypePostgres,
				Provisioner: model.BackingServiceProvisionerManaged,
				Status:      model.BackingServiceStatusActive,
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						Image:       "postgres:16-alpine",
						Database:    "demo",
						User:        "postgres",
						Password:    "secret",
						ServiceName: "legacy-postgres-demo-postgres",
						StoragePath: "/var/lib/fugue/tenant-data/fg-tenant-demo/legacy-postgres-demo/postgres",
						Instances:   1,
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

	deployments := ManagedBackingServiceDeployments(app, SchedulingConstraints{})
	if len(deployments) != 1 {
		t.Fatalf("expected one managed backing service deployment, got %d", len(deployments))
	}
	if deployments[0].ResourceKind != "Deployment" {
		t.Fatalf("expected legacy deployment resource kind, got %q", deployments[0].ResourceKind)
	}
	if deployments[0].ResourceName != "legacy-postgres-demo-postgres" {
		t.Fatalf("unexpected resource name %q", deployments[0].ResourceName)
	}
	if deployments[0].RuntimeKey == "" {
		t.Fatal("expected runtime key for legacy deployment")
	}
}

func TestBuildWorkspaceReplicationSourceObject(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_demo",
			Workspace: &model.AppWorkspaceSpec{},
		},
	}

	object := BuildWorkspaceReplicationSourceObject(app, nil, "tls://destination.default.svc:8000", "workspace-rsync-key")
	if got := object["kind"]; got != VolSyncReplicationSourceKind {
		t.Fatalf("expected workspace replication source, got %#v", got)
	}
	spec := object["spec"].(map[string]any)
	if got := spec["sourcePVC"]; got != WorkspacePVCName(app) {
		t.Fatalf("expected source pvc %q, got %#v", WorkspacePVCName(app), got)
	}
	rsyncTLS := spec["rsyncTLS"].(map[string]any)
	if got := rsyncTLS["address"]; got != "tls://destination.default.svc:8000" {
		t.Fatalf("expected rsync address, got %#v", got)
	}
	if got := rsyncTLS["keySecret"]; got != "workspace-rsync-key" {
		t.Fatalf("expected rsync key secret, got %#v", got)
	}
	if got := rsyncTLS["copyMethod"]; got != "Direct" {
		t.Fatalf("expected rsync copyMethod %q, got %#v", "Direct", got)
	}
}

func TestMergedRuntimeEnvRepairsLegacyManagedPostgresBindingHost(t *testing.T) {
	app := model.App{
		Spec: model.AppSpec{
			Env: map[string]string{
				"APP_ENV": "prod",
			},
		},
		BackingServices: []model.BackingService{
			{
				ID:   "service_demo",
				Type: model.BackingServiceTypePostgres,
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						Database:    "demo",
						User:        "root",
						Password:    "secret",
						ServiceName: "demo-postgres",
					},
				},
			},
		},
		Bindings: []model.ServiceBinding{
			{
				ServiceID: "service_demo",
				Env: map[string]string{
					"DB_TYPE":     "postgres",
					"DB_HOST":     "demo-postgres",
					"DB_PORT":     "5432",
					"DB_USER":     "legacy",
					"DB_PASSWORD": "legacy-secret",
					"DB_NAME":     "legacy",
					"KEEP":        "custom",
				},
			},
		},
	}

	env := mergedRuntimeEnv(app)
	if got := env["DB_HOST"]; got != "demo-postgres-rw" {
		t.Fatalf("expected runtime env DB_HOST to be repaired to rw service, got %q", got)
	}
	if got := env["DB_USER"]; got != "root" {
		t.Fatalf("expected runtime env DB_USER to follow backing service spec, got %q", got)
	}
	if got := env["DB_NAME"]; got != "demo" {
		t.Fatalf("expected runtime env DB_NAME to follow backing service spec, got %q", got)
	}
	if got := env["KEEP"]; got != "custom" {
		t.Fatalf("expected non-postgres binding env to be preserved, got %q", got)
	}
	if got := env["APP_ENV"]; got != "prod" {
		t.Fatalf("expected app env override to remain present, got %q", got)
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
