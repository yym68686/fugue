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
				Resources: &model.ResourceSpec{
					CPUMilliCores:   500,
					MemoryMebibytes: 1024,
				},
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
	if got := clusterSpec["minSyncReplicas"]; got != 0 {
		t.Fatalf("expected single-instance postgres to set minSyncReplicas 0, got %#v", got)
	}
	if got := clusterSpec["maxSyncReplicas"]; got != 0 {
		t.Fatalf("expected single-instance postgres to set maxSyncReplicas 0, got %#v", got)
	}
	storage := clusterSpec["storage"].(map[string]any)
	if got := storage["size"]; got != defaultPostgresStorage {
		t.Fatalf("expected postgres storage %q, got %#v", defaultPostgresStorage, got)
	}
	clusterResources, ok := clusterSpec["resources"].(map[string]any)
	if !ok {
		t.Fatalf("expected postgres resources, got %#v", clusterSpec["resources"])
	}
	clusterRequests := resourceStringValues(t, clusterResources["requests"])
	if got := clusterRequests["cpu"]; got != "500m" {
		t.Fatalf("expected postgres cpu request 500m, got %#v", got)
	}
	if got := clusterRequests["memory"]; got != "1024Mi" {
		t.Fatalf("expected postgres memory request 1024Mi, got %#v", got)
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
	if len(initContainers) != 1 {
		t.Fatalf("expected only wait-postgres init container, got %d", len(initContainers))
	}
	containers := appPodSpec["containers"].([]map[string]any)
	envObjects := containers[0]["env"].([]map[string]any)
	if got := envValue(envObjects, "DB_HOST"); got != "uni-api-demo-postgres-rw" {
		t.Fatalf("expected inline postgres DB_HOST to use rw service, got %q", got)
	}
	if got := initContainers[0]["name"]; got != "wait-postgres" {
		t.Fatalf("expected wait-postgres init container, got %#v", got)
	}
	command := initContainers[0]["command"].([]string)
	if got := command[2]; got != "until nc -z uni-api-demo-postgres-rw 5432; do sleep 1; done" {
		t.Fatalf("expected wait-postgres init container to target rw service, got %q", got)
	}
	assertHelperResources(t, initContainers[0]["resources"])
	volumeMounts, ok := containers[0]["volumeMounts"].([]map[string]any)
	if !ok {
		t.Fatalf("expected declarative app files to be mounted into the app container")
	}
	if len(volumeMounts) != 1 {
		t.Fatalf("expected one app file volume mount, got %d", len(volumeMounts))
	}
	if got := volumeMounts[0]["name"]; got != appFilesVolumeName {
		t.Fatalf("expected app file volume mount to use %q, got %#v", appFilesVolumeName, got)
	}
	if got := volumeMounts[0]["mountPath"]; got != "/home/api.yaml" {
		t.Fatalf("expected app file mount path %q, got %#v", "/home/api.yaml", got)
	}
	if got := volumeMounts[0]["subPath"]; got != "file-0" {
		t.Fatalf("expected app file subPath %q, got %#v", "file-0", got)
	}
	if got := volumeMounts[0]["readOnly"]; got != true {
		t.Fatalf("expected app file mount to be readOnly, got %#v", got)
	}
}

func TestBuildAppObjectsUseAppIDScopedRuntimeNames(t *testing.T) {
	app := model.App{
		ID:       "app_demo_123",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Files: []model.AppFile{
				{
					Path:    "/app/config.yaml",
					Content: "demo: true",
				},
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	if len(objects) != 4 {
		t.Fatalf("expected 4 objects, got %d", len(objects))
	}

	appFilesSecret := objects[1]
	if got := appFilesSecret["metadata"].(map[string]any)["name"]; got != "app-demo-123-files" {
		t.Fatalf("expected app files secret name %q, got %#v", "app-demo-123-files", got)
	}

	appDeployment := objects[2]
	deploymentMetadata := appDeployment["metadata"].(map[string]any)
	if got := deploymentMetadata["name"]; got != "app-demo-123" {
		t.Fatalf("expected deployment name %q, got %#v", "app-demo-123", got)
	}
	deploymentLabels := deploymentMetadata["labels"].(map[string]string)
	if got := deploymentLabels[FugueLabelName]; got != "demo" {
		t.Fatalf("expected human-readable app label %q, got %#v", "demo", got)
	}
	containers := appDeployment["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]map[string]any)
	if got := containers[0]["name"]; got != "demo" {
		t.Fatalf("expected app container name %q, got %#v", "demo", got)
	}

	appService := objects[3]
	if got := appService["metadata"].(map[string]any)["name"]; got != "app-demo-123" {
		t.Fatalf("expected service name %q, got %#v", "app-demo-123", got)
	}
}

func TestBuildAppObjectsExplicitlyClearsVolumeFieldsWhenNoMountsRemain(t *testing.T) {
	app := model.App{
		ID:       "app_demo_123",
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
	if len(objects) < 2 {
		t.Fatalf("expected deployment object, got %d objects", len(objects))
	}

	appDeployment := objects[1]
	appPodSpec := appDeployment["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	volumes, ok := appPodSpec["volumes"].([]map[string]any)
	if !ok {
		t.Fatalf("expected empty volumes list to be present, got %#v", appPodSpec["volumes"])
	}
	if len(volumes) != 0 {
		t.Fatalf("expected no volumes, got %#v", volumes)
	}

	containers := appPodSpec["containers"].([]map[string]any)
	volumeMounts, ok := containers[0]["volumeMounts"].([]map[string]any)
	if !ok {
		t.Fatalf("expected empty volumeMounts list to be present, got %#v", containers[0]["volumeMounts"])
	}
	if len(volumeMounts) != 0 {
		t.Fatalf("expected no volume mounts, got %#v", volumeMounts)
	}
}

func TestBuildAppObjectsIncludeComposeServiceAlias(t *testing.T) {
	app := model.App{
		ID:        "app_demo_123",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo_123456",
		Name:      "demo-api",
		Source: &model.AppSource{
			ComposeService: "api",
		},
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	if len(objects) != 5 {
		t.Fatalf("expected 5 objects, got %d", len(objects))
	}

	aliasService := objects[3]
	if got := aliasService["kind"]; got != "Service" {
		t.Fatalf("expected compose alias service, got %#v", got)
	}
	if got := aliasService["metadata"].(map[string]any)["name"]; got != ComposeServiceAliasName(app.ProjectID, "api") {
		t.Fatalf("expected compose alias service name %q, got %#v", ComposeServiceAliasName(app.ProjectID, "api"), got)
	}
	aliasSpec := aliasService["spec"].(map[string]any)
	if got := aliasSpec["type"]; got != "ExternalName" {
		t.Fatalf("expected compose alias service type ExternalName, got %#v", got)
	}
	if got := aliasSpec["externalName"]; got != "app-demo-123.fg-tenant-demo.svc.cluster.local" {
		t.Fatalf("expected compose alias external name, got %#v", got)
	}

	legacyAliasService := objects[4]
	if got := legacyAliasService["kind"]; got != "Service" {
		t.Fatalf("expected legacy compose app-name alias service, got %#v", got)
	}
	if got := legacyAliasService["metadata"].(map[string]any)["name"]; got != "demo-api" {
		t.Fatalf("expected legacy app-name alias service name %q, got %#v", "demo-api", got)
	}
	legacyAliasSpec := legacyAliasService["spec"].(map[string]any)
	if got := legacyAliasSpec["type"]; got != "ExternalName" {
		t.Fatalf("expected legacy app-name alias service type ExternalName, got %#v", got)
	}
	if got := legacyAliasSpec["externalName"]; got != "app-demo-123.fg-tenant-demo.svc.cluster.local" {
		t.Fatalf("expected legacy app-name alias external name, got %#v", got)
	}
}

func TestNormalizeRuntimePostgresSpecDefaultsToAppScopedUser(t *testing.T) {
	spec := normalizeRuntimePostgresSpec("fugue-web", model.AppPostgresSpec{})
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

func TestNormalizeRuntimePostgresSpecAllowsAsyncMultiInstance(t *testing.T) {
	spec := normalizeRuntimePostgresSpec("fugue-web", model.AppPostgresSpec{
		Instances:           2,
		SynchronousReplicas: 0,
	})
	if spec.Instances != 2 {
		t.Fatalf("expected two postgres instances, got %d", spec.Instances)
	}
	if spec.SynchronousReplicas != 0 {
		t.Fatalf("expected async multi-instance postgres to keep sync replicas 0, got %d", spec.SynchronousReplicas)
	}

	cluster := buildPostgresClusterObject("tenant-demo", "demo-secret", "demo-postgres", nil, spec, nil)
	clusterSpec := cluster["spec"].(map[string]any)
	if got := clusterSpec["minSyncReplicas"]; got != 0 {
		t.Fatalf("expected minSyncReplicas 0, got %#v", got)
	}
	if got := clusterSpec["maxSyncReplicas"]; got != 0 {
		t.Fatalf("expected maxSyncReplicas 0, got %#v", got)
	}
}

func TestNormalizeRuntimePostgresSpecStripsOfficialPostgresImage(t *testing.T) {
	spec := normalizeRuntimePostgresSpec("fugue-web", model.AppPostgresSpec{
		Image: "postgres:16-alpine",
	})
	if spec.Image != "" {
		t.Fatalf("expected official postgres image to be stripped, got %q", spec.Image)
	}
}

func TestBuildPostgresClusterUsesSingleRuntimePlacement(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_primary",
			Postgres: &model.AppPostgresSpec{
				Database:  "demo",
				User:      "demo_user",
				Password:  "secret",
				RuntimeID: "runtime_primary",
			},
		},
	}

	objects := buildAppObjectsWithPlacements(app, SchedulingConstraints{}, map[string][]SchedulingConstraints{
		"demo-postgres": {
			{
				NodeSelector: map[string]string{
					RuntimeIDLabelKey: "runtime_primary",
					TenantIDLabelKey:  "tenant_demo",
				},
				Tolerations: []Toleration{
					{
						Key:      TenantTaintKey,
						Operator: "Equal",
						Value:    "tenant_demo",
						Effect:   "NoSchedule",
					},
				},
			},
		},
	})

	clusterSpec := objects[3]["spec"].(map[string]any)
	affinity, ok := clusterSpec["affinity"].(map[string]any)
	if !ok {
		t.Fatalf("expected postgres affinity, got %#v", clusterSpec["affinity"])
	}
	nodeAffinity := affinity["nodeAffinity"].(map[string]any)
	required := nodeAffinity["requiredDuringSchedulingIgnoredDuringExecution"].(map[string]any)
	terms := required["nodeSelectorTerms"].([]map[string]any)
	if len(terms) != 1 {
		t.Fatalf("expected one node selector term, got %d", len(terms))
	}
	expressions := terms[0]["matchExpressions"].([]map[string]any)
	if len(expressions) != 2 {
		t.Fatalf("expected two node selector expressions, got %d", len(expressions))
	}
	tolerations := affinity["tolerations"].([]map[string]any)
	if len(tolerations) != 1 {
		t.Fatalf("expected one postgres toleration, got %d", len(tolerations))
	}
	if _, ok := affinity["enablePodAntiAffinity"]; ok {
		t.Fatalf("expected single-runtime postgres to omit pod anti-affinity, got %#v", affinity["enablePodAntiAffinity"])
	}
}

func TestBuildPostgresClusterUsesFailoverPlacementsAndAntiAffinity(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_primary",
			Postgres: &model.AppPostgresSpec{
				Database:                "demo",
				User:                    "demo_user",
				Password:                "secret",
				RuntimeID:               "runtime_primary",
				FailoverTargetRuntimeID: "runtime_failover",
				Instances:               2,
				SynchronousReplicas:     1,
			},
		},
	}

	objects := buildAppObjectsWithPlacements(app, SchedulingConstraints{}, map[string][]SchedulingConstraints{
		"demo-postgres": {
			{
				NodeSelector: map[string]string{
					RuntimeIDLabelKey: "runtime_primary",
					TenantIDLabelKey:  "tenant_demo",
				},
				Tolerations: []Toleration{
					{
						Key:      TenantTaintKey,
						Operator: "Equal",
						Value:    "tenant_demo",
						Effect:   "NoSchedule",
					},
				},
			},
			{
				NodeSelector: map[string]string{
					RuntimeIDLabelKey: "runtime_failover",
					TenantIDLabelKey:  "tenant_demo",
				},
				Tolerations: []Toleration{
					{
						Key:      TenantTaintKey,
						Operator: "Equal",
						Value:    "tenant_demo",
						Effect:   "NoSchedule",
					},
				},
			},
		},
	})

	clusterSpec := objects[3]["spec"].(map[string]any)
	if got := clusterSpec["maxSyncReplicas"]; got != 1 {
		t.Fatalf("expected maxSyncReplicas=1, got %#v", got)
	}
	affinity := clusterSpec["affinity"].(map[string]any)
	if got := affinity["enablePodAntiAffinity"]; got != true {
		t.Fatalf("expected pod anti-affinity enabled, got %#v", got)
	}
	if got := affinity["podAntiAffinityType"]; got != "required" {
		t.Fatalf("expected required pod anti-affinity type, got %#v", got)
	}
	nodeAffinity := affinity["nodeAffinity"].(map[string]any)
	required := nodeAffinity["requiredDuringSchedulingIgnoredDuringExecution"].(map[string]any)
	terms := required["nodeSelectorTerms"].([]map[string]any)
	if len(terms) != 2 {
		t.Fatalf("expected two node selector terms, got %d", len(terms))
	}
	tolerations := affinity["tolerations"].([]map[string]any)
	if len(tolerations) != 1 {
		t.Fatalf("expected deduplicated postgres tolerations, got %d", len(tolerations))
	}
	metadata := objects[3]["metadata"].(map[string]any)
	if _, ok := metadata["annotations"]; ok {
		t.Fatalf("expected failover cluster without pending rebalance to omit annotations, got %#v", metadata["annotations"])
	}
}

func TestBuildPostgresClusterPendingRebalanceDisablesPodSpecReconciliation(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_primary",
			Postgres: &model.AppPostgresSpec{
				Database:                         "demo",
				User:                             "demo_user",
				Password:                         "secret",
				RuntimeID:                        "runtime_primary",
				FailoverTargetRuntimeID:          "runtime_failover",
				Instances:                        2,
				SynchronousReplicas:              1,
				PrimaryPlacementPendingRebalance: true,
			},
		},
	}

	objects := buildAppObjectsWithPlacements(app, SchedulingConstraints{}, map[string][]SchedulingConstraints{
		"demo-postgres": {
			{
				NodeSelector: map[string]string{
					RuntimeIDLabelKey: "runtime_primary",
					TenantIDLabelKey:  "tenant_demo",
				},
				Tolerations: []Toleration{
					{
						Key:      TenantTaintKey,
						Operator: "Equal",
						Value:    "tenant_demo",
						Effect:   "NoSchedule",
					},
				},
			},
			{
				NodeSelector: map[string]string{
					RuntimeIDLabelKey: "runtime_failover",
					TenantIDLabelKey:  "tenant_demo",
				},
				Tolerations: []Toleration{
					{
						Key:      TenantTaintKey,
						Operator: "Equal",
						Value:    "tenant_demo",
						Effect:   "NoSchedule",
					},
				},
			},
		},
	})

	metadata := objects[3]["metadata"].(map[string]any)
	annotations, ok := metadata["annotations"].(map[string]string)
	if !ok {
		t.Fatalf("expected postgres cluster annotations, got %#v", metadata["annotations"])
	}
	if got := annotations[CloudNativePGReconcilePodSpecAnno]; got != CloudNativePGReconcilePodSpecHold {
		t.Fatalf("expected %s=%q, got %#v", CloudNativePGReconcilePodSpecAnno, CloudNativePGReconcilePodSpecHold, got)
	}
}

func TestBuildSingleInstancePostgresClusterPendingRebalanceDisablesPodSpecReconciliation(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_primary",
			Postgres: &model.AppPostgresSpec{
				Database:                         "demo",
				User:                             "demo_user",
				Password:                         "secret",
				RuntimeID:                        "runtime_primary",
				Instances:                        1,
				PrimaryPlacementPendingRebalance: true,
			},
		},
	}

	objects := buildAppObjectsWithPlacements(app, SchedulingConstraints{}, map[string][]SchedulingConstraints{
		"demo-postgres": {
			{
				NodeSelector: map[string]string{
					RuntimeIDLabelKey: "runtime_primary",
					TenantIDLabelKey:  "tenant_demo",
				},
				Tolerations: []Toleration{
					{
						Key:      TenantTaintKey,
						Operator: "Equal",
						Value:    "tenant_demo",
						Effect:   "NoSchedule",
					},
				},
			},
		},
	})

	metadata := objects[3]["metadata"].(map[string]any)
	annotations, ok := metadata["annotations"].(map[string]string)
	if !ok {
		t.Fatalf("expected postgres cluster annotations, got %#v", metadata["annotations"])
	}
	if got := annotations[CloudNativePGReconcilePodSpecAnno]; got != CloudNativePGReconcilePodSpecHold {
		t.Fatalf("expected %s=%q, got %#v", CloudNativePGReconcilePodSpecAnno, CloudNativePGReconcilePodSpecHold, got)
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

func TestBuildAppDeploymentAnnotatesReleaseKey(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v1",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	deployment := objects[1]
	metadata := deployment["metadata"].(map[string]any)
	annotations, ok := metadata["annotations"].(map[string]string)
	if !ok {
		t.Fatalf("expected deployment annotations, got %#v", metadata["annotations"])
	}
	releaseKey := annotations[FugueAnnotationReleaseKey]
	if releaseKey == "" {
		t.Fatal("expected deployment release key annotation")
	}
	if expected := ManagedAppReleaseKey(app, SchedulingConstraints{}); releaseKey != expected {
		t.Fatalf("expected release key %q, got %q", expected, releaseKey)
	}

	app.Spec.Image = "ghcr.io/example/demo:v2"
	updatedDeployment := buildAppObjects(app, SchedulingConstraints{})[1]
	updatedMetadata := updatedDeployment["metadata"].(map[string]any)
	updatedAnnotations := updatedMetadata["annotations"].(map[string]string)
	if updatedAnnotations[FugueAnnotationReleaseKey] == releaseKey {
		t.Fatal("expected release key to change when deployment template changes")
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
	if readinessProbe["initialDelaySeconds"] != 0 {
		t.Fatalf("expected readiness probe to start immediately, got initialDelaySeconds=%#v", readinessProbe["initialDelaySeconds"])
	}
	if readinessProbe["periodSeconds"] != 1 {
		t.Fatalf("expected readiness probe period 1s, got %#v", readinessProbe["periodSeconds"])
	}
	if got := containers[0]["imagePullPolicy"]; got != "Always" {
		t.Fatalf("expected tagged images to use imagePullPolicy Always, got %#v", got)
	}
}

func TestBuildAppDeploymentUsesIfNotPresentForDigestPinnedImages(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	deployment := objects[1]
	spec := deployment["spec"].(map[string]any)
	template := spec["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)
	containers := podSpec["containers"].([]map[string]any)
	if got := containers[0]["imagePullPolicy"]; got != "IfNotPresent" {
		t.Fatalf("expected digest-pinned images to use imagePullPolicy IfNotPresent, got %#v", got)
	}
}

func TestBuildAppDeploymentUsesIfNotPresentForFugueManagedImmutableTags(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "registry.pull.example/fugue-apps/example-runtime:git-0123456789ab",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	deployment := objects[1]
	spec := deployment["spec"].(map[string]any)
	template := spec["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)
	containers := podSpec["containers"].([]map[string]any)
	if got := containers[0]["imagePullPolicy"]; got != "IfNotPresent" {
		t.Fatalf("expected fugue-managed immutable tags to use imagePullPolicy IfNotPresent, got %#v", got)
	}
}

func TestBuildAppObjectsSkipsServiceForBackgroundApps(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "worker",
		Spec: model.AppSpec{
			Image:       "ghcr.io/example/worker:latest",
			NetworkMode: model.AppNetworkModeBackground,
			Replicas:    1,
			RuntimeID:   "runtime_demo",
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	if len(objects) != 2 {
		t.Fatalf("expected namespace and deployment only, got %d objects", len(objects))
	}
	if kind, _ := objects[1]["kind"].(string); kind != "Deployment" {
		t.Fatalf("expected deployment object, got %#v", objects[1]["kind"])
	}

	deployment := objects[1]
	spec := deployment["spec"].(map[string]any)
	template := spec["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)
	containers := podSpec["containers"].([]map[string]any)
	if _, ok := containers[0]["readinessProbe"]; ok {
		t.Fatalf("expected background app to omit readiness probe, got %#v", containers[0]["readinessProbe"])
	}
}

func TestBuildAppObjectsKeepsInternalServiceWithoutPublicRoute(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "worker",
		Spec: model.AppSpec{
			Image:       "ghcr.io/example/worker:latest",
			NetworkMode: model.AppNetworkModeInternal,
			Ports:       []int{7777},
			Replicas:    1,
			RuntimeID:   "runtime_demo",
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	if len(objects) != 3 {
		t.Fatalf("expected namespace, deployment, and service, got %d objects", len(objects))
	}
	if kind, _ := objects[2]["kind"].(string); kind != "Service" {
		t.Fatalf("expected service object, got %#v", objects[2]["kind"])
	}

	deployment := objects[1]
	spec := deployment["spec"].(map[string]any)
	template := spec["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)
	containers := podSpec["containers"].([]map[string]any)
	if _, ok := containers[0]["readinessProbe"]; !ok {
		t.Fatalf("expected internal app to keep readiness probe, got %#v", containers[0])
	}

	service := objects[2]
	serviceSpec := service["spec"].(map[string]any)
	ports := serviceSpec["ports"].([]map[string]any)
	if len(ports) != 1 || ports[0]["port"] != 7777 {
		t.Fatalf("expected internal service port 7777, got %#v", ports)
	}
}

func TestBuildAppDeploymentOmitsResourcesWhenUnset(t *testing.T) {
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
	template := spec["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)
	containers := podSpec["containers"].([]map[string]any)
	if _, ok := containers[0]["resources"]; ok {
		t.Fatalf("expected app container resources to be omitted when unset, got %#v", containers[0]["resources"])
	}
}

func TestBuildAppDeploymentIncludesExplicitResources(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			Resources: &model.ResourceSpec{MemoryMebibytes: 1536},
			RuntimeID: "runtime_demo",
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	deployment := objects[1]
	spec := deployment["spec"].(map[string]any)
	template := spec["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)
	containers := podSpec["containers"].([]map[string]any)
	resources, ok := containers[0]["resources"].(map[string]any)
	if !ok {
		t.Fatalf("expected explicit app resources, got %#v", containers[0]["resources"])
	}
	requests, ok := resources["requests"].(map[string]string)
	if !ok {
		t.Fatalf("expected resource requests map, got %#v", resources["requests"])
	}
	limits, ok := resources["limits"].(map[string]string)
	if !ok {
		t.Fatalf("expected resource limits map, got %#v", resources["limits"])
	}
	if got := requests["memory"]; got != "1536Mi" {
		t.Fatalf("expected memory request 1536Mi, got %#v", got)
	}
	if got := limits["memory"]; got != "1536Mi" {
		t.Fatalf("expected memory limit 1536Mi, got %#v", got)
	}
	if _, ok := requests["cpu"]; ok {
		t.Fatalf("expected cpu request to remain unset, got %#v", requests["cpu"])
	}
	if _, ok := limits["cpu"]; ok {
		t.Fatalf("expected cpu limit to remain unset, got %#v", limits["cpu"])
	}
}

func TestBuildAppDeploymentAppliesResourceLimitsByWorkloadClass(t *testing.T) {
	t.Parallel()

	serviceResources := runtimeAppResourceRequirements(model.AppSpec{
		WorkloadClass: model.WorkloadClassService,
		Resources: &model.ResourceSpec{
			CPUMilliCores:        100,
			MemoryMebibytes:      256,
			MemoryLimitMebibytes: 512,
		},
	})
	serviceRequests := resourceStringValues(t, serviceResources["requests"])
	serviceLimits := resourceStringValues(t, serviceResources["limits"])
	if got := serviceRequests["cpu"]; got != "100m" {
		t.Fatalf("expected service cpu request 100m, got %#v", got)
	}
	if _, ok := serviceLimits["cpu"]; ok {
		t.Fatalf("expected service cpu limit to remain unset, got %#v", serviceLimits["cpu"])
	}
	if got := serviceLimits["memory"]; got != "512Mi" {
		t.Fatalf("expected explicit service memory limit 512Mi, got %#v", got)
	}

	criticalResources := runtimeAppResourceRequirements(model.AppSpec{
		WorkloadClass: model.WorkloadClassCritical,
		Resources: &model.ResourceSpec{
			CPUMilliCores:   250,
			MemoryMebibytes: 512,
		},
	})
	criticalLimits := resourceStringValues(t, criticalResources["limits"])
	if got := criticalLimits["cpu"]; got != "250m" {
		t.Fatalf("expected critical cpu limit to match request, got %#v", got)
	}
	if got := criticalLimits["memory"]; got != "512Mi" {
		t.Fatalf("expected critical memory limit to match request, got %#v", got)
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
	deployment := firstObjectByKind(t, objects, "Deployment")
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
	assertHelperResources(t, containers[1]["resources"])
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
	assertHelperResources(t, initContainers[0]["resources"])

	workspacePVC := objects[1]
	if got := workspacePVC["kind"]; got != "PersistentVolumeClaim" {
		t.Fatalf("expected workspace pvc, got %#v", got)
	}
	requests := workspacePVC["spec"].(map[string]any)["resources"].(map[string]any)["requests"].(map[string]any)
	if got := requests["storage"]; got != defaultWorkspaceStorage {
		t.Fatalf("expected workspace storage %q, got %#v", defaultWorkspaceStorage, got)
	}

	for _, object := range objects {
		if got := object["kind"]; got == VolSyncReplicationDestinationKind {
			t.Fatalf("expected workspace replication destination to be opt-in, got %#v", object)
		}
	}

	strategy := deployment["spec"].(map[string]any)["strategy"].(map[string]any)
	if got := strategy["type"]; got != "Recreate" {
		t.Fatalf("expected workspace deployment strategy Recreate, got %#v", got)
	}
}

func TestBuildAppObjectsIncludesWorkspaceReplicationWhenEnabled(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Workspace: &model.AppWorkspaceSpec{},
			VolumeReplication: &model.AppVolumeReplicationSpec{
				Mode: model.AppVolumeReplicationModeScheduled,
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	replicationDestination := firstObjectByKind(t, objects, VolSyncReplicationDestinationKind)
	destinationSpec := replicationDestination["spec"].(map[string]any)
	destinationRsyncTLS := destinationSpec["rsyncTLS"].(map[string]any)
	if got := destinationRsyncTLS["copyMethod"]; got != "Direct" {
		t.Fatalf("expected workspace replication destination copyMethod %q, got %#v", "Direct", got)
	}
}

func TestBuildAppObjectsIncludesPersistentStorageMounts(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			PersistentStorage: &model.AppPersistentStorageSpec{
				ResetToken: "storage-reset-1",
				Mounts: []model.AppPersistentStorageMount{
					{
						Kind:        model.AppPersistentStorageMountKindFile,
						Path:        "/home/api.yaml",
						SeedContent: "providers: []\n",
					},
					{
						Kind: model.AppPersistentStorageMountKindDirectory,
						Path: "/home/data",
					},
				},
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	deployment := firstObjectByKind(t, objects, "Deployment")
	template := deployment["spec"].(map[string]any)["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)

	volumes := podSpec["volumes"].([]map[string]any)
	if len(volumes) != 1 {
		t.Fatalf("expected one persistent storage volume, got %d", len(volumes))
	}
	claim := volumes[0]["persistentVolumeClaim"].(map[string]any)
	if got := claim["claimName"]; got != WorkspacePVCName(app) {
		t.Fatalf("unexpected persistent storage pvc claim: %#v", got)
	}

	containers := podSpec["containers"].([]map[string]any)
	if len(containers) != 2 {
		t.Fatalf("expected app container and storage sidecar, got %d containers", len(containers))
	}
	assertHelperResources(t, containers[1]["resources"])
	appMounts := containers[0]["volumeMounts"].([]map[string]any)
	if len(appMounts) != 2 {
		t.Fatalf("expected two persistent storage mounts, got %+v", appMounts)
	}
	if appMounts[0]["mountPath"] != "/home/api.yaml" {
		t.Fatalf("unexpected file mount path: %#v", appMounts[0]["mountPath"])
	}
	if got := appMounts[0]["subPath"]; !strings.HasPrefix(got.(string), "mounts/mount-") {
		t.Fatalf("expected file mount subPath to target persistent storage mount, got %#v", got)
	}
	if appMounts[1]["mountPath"] != "/home/data" {
		t.Fatalf("unexpected directory mount path: %#v", appMounts[1]["mountPath"])
	}

	initContainers := podSpec["initContainers"].([]map[string]any)
	if len(initContainers) != 1 {
		t.Fatalf("expected one persistent storage init container, got %d", len(initContainers))
	}
	command := initContainers[0]["command"].([]string)
	if got := command[len(command)-2]; got != "storage-reset-1" {
		t.Fatalf("expected persistent storage reset token in init container command, got %q", got)
	}
	if got := command[len(command)-1]; !strings.Contains(got, "file\tmount-") {
		t.Fatalf("expected persistent storage mount plan to include file mount metadata, got %q", got)
	}
	assertHelperResources(t, initContainers[0]["resources"])

	persistentPVC := objects[1]
	if got := persistentPVC["kind"]; got != "PersistentVolumeClaim" {
		t.Fatalf("expected persistent storage pvc, got %#v", got)
	}
	requests := persistentPVC["spec"].(map[string]any)["resources"].(map[string]any)["requests"].(map[string]any)
	if got := requests["storage"]; got != defaultWorkspaceStorage {
		t.Fatalf("expected persistent storage size %q, got %#v", defaultWorkspaceStorage, got)
	}
	for _, object := range objects {
		if got := object["kind"]; got == VolSyncReplicationDestinationKind {
			t.Fatalf("expected persistent storage replication destination to be opt-in, got %#v", object)
		}
	}

	strategy := deployment["spec"].(map[string]any)["strategy"].(map[string]any)
	if got := strategy["type"]; got != "Recreate" {
		t.Fatalf("expected persistent storage deployment strategy Recreate, got %#v", got)
	}
}

func TestBuildAppObjectsUsesPersistentStorageClaimName(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Replicas:  1,
			RuntimeID: "runtime_demo",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:      model.AppPersistentStorageModeMovableRWO,
				ClaimName: "app-demo-workspace-mv-op123",
				Mounts: []model.AppPersistentStorageMount{
					{
						Kind: model.AppPersistentStorageMountKindDirectory,
						Path: "/workspace",
					},
				},
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	deployment := firstObjectByKind(t, objects, "Deployment")
	template := deployment["spec"].(map[string]any)["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)
	volumes := podSpec["volumes"].([]map[string]any)
	claim := volumes[0]["persistentVolumeClaim"].(map[string]any)
	if got := claim["claimName"]; got != "app-demo-workspace-mv-op123" {
		t.Fatalf("expected generated movable RWO claim name, got %#v", got)
	}
	pvc := firstObjectByKind(t, objects, "PersistentVolumeClaim")
	metadata := pvc["metadata"].(map[string]any)
	if got := metadata["name"]; got != "app-demo-workspace-mv-op123" {
		t.Fatalf("expected persistent storage PVC object name to match claim name, got %#v", got)
	}
}

func TestBuildManagedAppChildObjectsIncludesSharedProjectRWXPersistentStorage(t *testing.T) {
	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:          model.AppPersistentStorageModeSharedProjectRWX,
				StorageSize:   "20Gi",
				SharedSubPath: "sessions/session-123",
				Mounts: []model.AppPersistentStorageMount{
					{
						Kind: model.AppPersistentStorageMountKindDirectory,
						Path: "/workspace",
					},
				},
			},
		},
	}
	managed := ManagedAppObject{
		APIVersion: ManagedAppAPIVersion,
		Kind:       ManagedAppKind,
		Metadata: ManagedAppMeta{
			Name:      "demo",
			Namespace: NamespaceForTenant(app.TenantID),
			UID:       "uid-demo",
		},
	}

	objects := BuildManagedAppChildObjects(app, SchedulingConstraints{}, ManagedAppOwnerReference(managed))
	if len(objects) != 3 {
		t.Fatalf("expected shared pvc, deployment, and service objects, got %d: %#v", len(objects), objects)
	}

	sharedPVC := objects[0]
	if got := sharedPVC["kind"]; got != "PersistentVolumeClaim" {
		t.Fatalf("expected project shared pvc, got %#v", got)
	}
	metadata := sharedPVC["metadata"].(map[string]any)
	if got := metadata["name"]; got != ProjectSharedWorkspacePVCName(app) {
		t.Fatalf("unexpected project shared pvc name: %#v", got)
	}
	if _, ok := metadata["ownerReferences"]; ok {
		t.Fatalf("project shared pvc must not be owned by one app: %#v", metadata["ownerReferences"])
	}
	labels := metadata["labels"].(map[string]string)
	if got := labels[FugueLabelComponent]; got != projectSharedStorageComponent {
		t.Fatalf("expected shared storage component label, got %#v", got)
	}
	pvcSpec := sharedPVC["spec"].(map[string]any)
	accessModes := pvcSpec["accessModes"].([]string)
	if len(accessModes) != 1 || accessModes[0] != "ReadWriteMany" {
		t.Fatalf("expected RWX access mode, got %#v", accessModes)
	}

	deployment := objects[1]
	template := deployment["spec"].(map[string]any)["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)
	volumes := podSpec["volumes"].([]map[string]any)
	claim := volumes[0]["persistentVolumeClaim"].(map[string]any)
	if got := claim["claimName"]; got != ProjectSharedWorkspacePVCName(app) {
		t.Fatalf("expected deployment to mount project shared pvc, got %#v", got)
	}
	appMounts := podSpec["containers"].([]map[string]any)[0]["volumeMounts"].([]map[string]any)
	if got := appMounts[0]["mountPath"]; got != "/workspace" {
		t.Fatalf("expected workspace mount path, got %#v", got)
	}
	if got := appMounts[0]["subPath"]; got != "sessions/session-123" {
		t.Fatalf("expected app mount to use direct session subdir, got %#v", got)
	}
	if containers := podSpec["containers"].([]map[string]any); len(containers) != 1 {
		t.Fatalf("expected direct shared project mount to omit storage sidecar, got %d containers", len(containers))
	}
	if _, ok := podSpec["initContainers"]; ok {
		t.Fatalf("expected direct shared project mount to omit init containers, got %#v", podSpec["initContainers"])
	}

	for _, obj := range objects[1:] {
		objMetadata := obj["metadata"].(map[string]any)
		if _, ok := objMetadata["ownerReferences"]; !ok {
			t.Fatalf("expected app-owned object %s to have owner reference", objMetadata["name"])
		}
	}
}

func TestBuildManagedAppChildObjectsIncludesMovableRWOPersistentStorage(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Replicas:  1,
			RuntimeID: "runtime_demo",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:             model.AppPersistentStorageModeMovableRWO,
				StorageSize:      "20Gi",
				StorageClassName: "fast-rwo",
				Mounts: []model.AppPersistentStorageMount{
					{
						Kind: model.AppPersistentStorageMountKindDirectory,
						Path: "/workspace",
					},
				},
			},
		},
	}
	managed := ManagedAppObject{
		APIVersion: ManagedAppAPIVersion,
		Kind:       ManagedAppKind,
		Metadata: ManagedAppMeta{
			Name:      "demo",
			Namespace: NamespaceForTenant(app.TenantID),
			UID:       "uid-demo",
		},
	}

	objects := BuildManagedAppChildObjects(app, SchedulingConstraints{}, ManagedAppOwnerReference(managed))
	pvc := firstObjectByKind(t, objects, "PersistentVolumeClaim")
	metadata := pvc["metadata"].(map[string]any)
	if got := metadata["name"]; got != WorkspacePVCName(app) {
		t.Fatalf("expected app-scoped pvc name, got %#v", got)
	}
	pvcSpec := pvc["spec"].(map[string]any)
	accessModes := pvcSpec["accessModes"].([]string)
	if len(accessModes) != 1 || accessModes[0] != "ReadWriteOnce" {
		t.Fatalf("expected RWO access mode, got %#v", accessModes)
	}
	if got := pvcSpec["storageClassName"]; got != "fast-rwo" {
		t.Fatalf("expected storage class fast-rwo, got %#v", got)
	}
}

func TestBuildManagedAppChildObjectsKeepsSharedProjectRWXInitForComplexPersistentStorage(t *testing.T) {
	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:          model.AppPersistentStorageModeSharedProjectRWX,
				StorageSize:   "20Gi",
				SharedSubPath: "sessions/session-123",
				Mounts: []model.AppPersistentStorageMount{
					{
						Kind:        model.AppPersistentStorageMountKindFile,
						Path:        "/home/api.yaml",
						SeedContent: "providers: []\n",
					},
					{
						Kind: model.AppPersistentStorageMountKindDirectory,
						Path: "/workspace",
					},
				},
			},
		},
	}
	managed := ManagedAppObject{
		APIVersion: ManagedAppAPIVersion,
		Kind:       ManagedAppKind,
		Metadata: ManagedAppMeta{
			Name:      "demo",
			Namespace: NamespaceForTenant(app.TenantID),
			UID:       "uid-demo",
		},
	}

	objects := BuildManagedAppChildObjects(app, SchedulingConstraints{}, ManagedAppOwnerReference(managed))
	deployment := objects[1]
	template := deployment["spec"].(map[string]any)["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)

	containers := podSpec["containers"].([]map[string]any)
	if len(containers) != 2 {
		t.Fatalf("expected app container and storage sidecar, got %d containers", len(containers))
	}
	appMounts := containers[0]["volumeMounts"].([]map[string]any)
	if got := appMounts[0]["subPath"]; !strings.HasPrefix(got.(string), "sessions/session-123/mounts/mount-") {
		t.Fatalf("expected complex mount to use initialized mount subdir, got %#v", got)
	}
	initContainers := podSpec["initContainers"].([]map[string]any)
	if len(initContainers) != 1 {
		t.Fatalf("expected one persistent storage init container, got %d", len(initContainers))
	}
	command := initContainers[0]["command"].([]string)
	if got := command[len(command)-3]; got != "/fugue-persistent-storage/sessions/session-123" {
		t.Fatalf("expected init container to prepare session subdir, got %q", got)
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
	if got := clusterSpec["minSyncReplicas"]; got != 0 {
		t.Fatalf("expected single-instance postgres to set minSyncReplicas 0, got %#v", got)
	}
	if got := clusterSpec["maxSyncReplicas"]; got != 0 {
		t.Fatalf("expected single-instance postgres to set maxSyncReplicas 0, got %#v", got)
	}
}

func TestManagedBackingServiceDeploymentsUseCNPGCluster(t *testing.T) {
	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "managed-postgres-demo",
		BackingServices: []model.BackingService{
			{
				ID:          "service_demo",
				TenantID:    "tenant_demo",
				ProjectID:   "project_demo",
				OwnerAppID:  "app_demo",
				Name:        "managed-postgres-demo",
				Type:        model.BackingServiceTypePostgres,
				Provisioner: model.BackingServiceProvisionerManaged,
				Status:      model.BackingServiceStatusActive,
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						Image:       "postgres:16-alpine",
						Database:    "demo",
						User:        "managed_postgres_demo",
						Password:    "secret",
						ServiceName: "managed-postgres-demo-postgres",
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
	if deployments[0].ResourceKind != CloudNativePGClusterKind {
		t.Fatalf("expected CNPG cluster resource kind, got %q", deployments[0].ResourceKind)
	}
	if deployments[0].ResourceName != "managed-postgres-demo-postgres" {
		t.Fatalf("unexpected resource name %q", deployments[0].ResourceName)
	}
	if deployments[0].RuntimeKey == "" {
		t.Fatal("expected runtime key for CNPG cluster")
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
			VolumeReplication: &model.AppVolumeReplicationSpec{
				Mode:     model.AppVolumeReplicationModeScheduled,
				Schedule: "*/10 * * * *",
			},
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
	trigger := spec["trigger"].(map[string]any)
	if got := trigger["schedule"]; got != "*/10 * * * *" {
		t.Fatalf("expected replication source schedule, got %#v", got)
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

func TestBuildWorkspaceReplicationSourceObjectSupportsManualMode(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_demo",
			Workspace: &model.AppWorkspaceSpec{},
			VolumeReplication: &model.AppVolumeReplicationSpec{
				Mode: model.AppVolumeReplicationModeManual,
			},
		},
	}

	object := BuildWorkspaceReplicationSourceObject(app, nil, "tls://destination.default.svc:8000", "workspace-rsync-key")
	spec := object["spec"].(map[string]any)
	trigger := spec["trigger"].(map[string]any)
	if got := trigger["manual"]; got != "bootstrap" {
		t.Fatalf("expected manual replication trigger, got %#v", got)
	}
	if _, ok := trigger["schedule"]; ok {
		t.Fatalf("expected manual replication source to omit schedule, got %#v", trigger)
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

func firstObjectByKind(t *testing.T, objects []map[string]any, kind string) map[string]any {
	t.Helper()
	for _, object := range objects {
		if got, _ := object["kind"].(string); got == kind {
			return object
		}
	}
	t.Fatalf("expected object kind %q in %#v", kind, objects)
	return nil
}

func assertHelperResources(t *testing.T, value any) {
	t.Helper()

	resources, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("expected helper resources object, got %#v", value)
	}
	requests := resourceStringValues(t, resources["requests"])
	limits := resourceStringValues(t, resources["limits"])
	if got := requests["cpu"]; got != defaultHelperCPURequest {
		t.Fatalf("expected helper cpu request %q, got %#v", defaultHelperCPURequest, got)
	}
	if got := requests["memory"]; got != defaultHelperMemoryRequest {
		t.Fatalf("expected helper memory request %q, got %#v", defaultHelperMemoryRequest, got)
	}
	if got := requests["ephemeral-storage"]; got != defaultHelperEphemeralRequest {
		t.Fatalf("expected helper ephemeral request %q, got %#v", defaultHelperEphemeralRequest, got)
	}
	if got := limits["cpu"]; got != defaultHelperCPULimit {
		t.Fatalf("expected helper cpu limit %q, got %#v", defaultHelperCPULimit, got)
	}
	if got := limits["memory"]; got != defaultHelperMemoryLimit {
		t.Fatalf("expected helper memory limit %q, got %#v", defaultHelperMemoryLimit, got)
	}
	if got := limits["ephemeral-storage"]; got != defaultHelperEphemeralLimit {
		t.Fatalf("expected helper ephemeral limit %q, got %#v", defaultHelperEphemeralLimit, got)
	}
}

func resourceStringValues(t *testing.T, value any) map[string]string {
	t.Helper()
	values, ok := value.(map[string]string)
	if !ok {
		t.Fatalf("expected resource values map, got %#v", value)
	}
	return values
}
