package runtime

import (
	"path"
	"reflect"
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

	if len(objects) != 9 {
		t.Fatalf("expected 9 objects, got %d", len(objects))
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
	if _, ok := postgresAliasSpec["externalName"]; ok {
		t.Fatalf("postgres alias service should not use externalName, got %#v", postgresAliasSpec["externalName"])
	}
	if selector, ok := postgresAliasSpec["selector"].(map[string]string); !ok {
		t.Fatalf("expected postgres alias selector, got %#v", postgresAliasSpec["selector"])
	} else if got := selector["cnpg.io/cluster"]; got != "uni-api-demo-postgres" {
		t.Fatalf("expected postgres alias selector cluster %q, got %#v", "uni-api-demo-postgres", got)
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
	clusterLimits := resourceStringValues(t, clusterResources["limits"])
	if got := clusterLimits["memory"]; got != "1536Mi" {
		t.Fatalf("expected postgres memory limit with headroom 1536Mi, got %#v", got)
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
	if len(initContainers) < 1 {
		t.Fatalf("expected wait-postgres init container, got %d", len(initContainers))
	}
	containers := appPodSpec["containers"].([]map[string]any)
	envObjects := containers[0]["env"].([]map[string]any)
	if got := envValue(envObjects, "DB_HOST"); got != "uni-api-demo-postgres" {
		t.Fatalf("expected inline postgres DB_HOST to use Fugue-managed primary service, got %q", got)
	}
	waitPostgres := initContainers[len(initContainers)-1]
	if got := waitPostgres["name"]; got != "wait-postgres" {
		t.Fatalf("expected wait-postgres init container, got %#v", got)
	}
	command := waitPostgres["command"].([]string)
	expectedWaitCommand := `host="uni-api-demo-postgres"; env_host="${UNI_API_DEMO_POSTGRES_SERVICE_HOST:-}"; if [ -n "$env_host" ]; then host="$env_host"; fi; until nc -z "$host" 5432; do sleep 1; done`
	if got := command[2]; got != expectedWaitCommand {
		t.Fatalf("expected wait-postgres init container to prefer service ClusterIP env, got %q", got)
	}
	assertHelperResources(t, waitPostgres["resources"])
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
	if len(objects) != 6 {
		t.Fatalf("expected 6 objects, got %d", len(objects))
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

func TestBuildAppObjectsUseDNS1035ServiceAliases(t *testing.T) {
	app := model.App{
		ID:        "app_1780126966_e0a8316d46cd",
		TenantID:  "tenant_1774720388_5c9ca0db3d42",
		ProjectID: "project_1780126966_0502785f2fc8",
		Name:      "001-fugue-oiuhu89",
		Source: &model.AppSource{
			ComposeService: "logseq-sync",
		},
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/logseq-sync:latest",
			Ports:     []int{8787},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	if len(objects) != 7 {
		t.Fatalf("expected namespace + deployment + service + compose alias + legacy alias + drain RBAC, got %d", len(objects))
	}

	appService := objects[2]
	if got := appService["metadata"].(map[string]any)["name"]; got != "app-1780126966-e0a8316d46cd" {
		t.Fatalf("expected primary service name to remain app-id scoped, got %#v", got)
	}

	legacyAlias := objects[4]
	if got := legacyAlias["kind"]; got != "Service" {
		t.Fatalf("expected legacy alias service, got %#v", got)
	}
	if got := legacyAlias["metadata"].(map[string]any)["name"]; got != "app-001-fugue-oiuhu89" {
		t.Fatalf("expected DNS-1035 legacy service alias, got %#v", got)
	}
}

func TestBuildAppObjectsUseDNS1035PostgresServiceNames(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "001-demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Postgres: &model.AppPostgresSpec{
				Password: "secret",
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	if len(objects) != 8 {
		t.Fatalf("expected namespace + postgres secret/service/cluster + app deployment/service + drain RBAC, got %d", len(objects))
	}
	postgresService := objects[2]
	if got := postgresService["metadata"].(map[string]any)["name"]; got != "postgres-001-demo-postgres" {
		t.Fatalf("expected DNS-1035 postgres service name, got %#v", got)
	}
	appDeployment := objects[4]
	containers := appDeployment["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]map[string]any)
	if got := envValue(containers[0]["env"].([]map[string]any), "DB_HOST"); got != "postgres-001-demo-postgres" {
		t.Fatalf("expected app env to point at DNS-1035 postgres service, got %q", got)
	}
	appService := objects[5]
	if got := appService["metadata"].(map[string]any)["name"]; got != "app-001-demo" {
		t.Fatalf("expected numeric app-name fallback service to be DNS-1035, got %#v", got)
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
	if len(objects) != 7 {
		t.Fatalf("expected 7 objects, got %d", len(objects))
	}

	aliasService := objects[3]
	if got := aliasService["kind"]; got != "Service" {
		t.Fatalf("expected compose alias service, got %#v", got)
	}
	if got := aliasService["metadata"].(map[string]any)["name"]; got != ComposeServiceAliasName(app.ProjectID, "api") {
		t.Fatalf("expected compose alias service name %q, got %#v", ComposeServiceAliasName(app.ProjectID, "api"), got)
	}
	aliasSpec := aliasService["spec"].(map[string]any)
	if _, ok := aliasSpec["externalName"]; ok {
		t.Fatalf("compose alias service should not use externalName, got %#v", aliasSpec["externalName"])
	}
	if selector, ok := aliasSpec["selector"].(map[string]string); !ok {
		t.Fatalf("expected compose alias selector, got %#v", aliasSpec["selector"])
	} else if got := selector[FugueLabelAppID]; got != app.ID {
		t.Fatalf("expected compose alias selector app id %q, got %#v", app.ID, got)
	}

	legacyAliasService := objects[4]
	if got := legacyAliasService["kind"]; got != "Service" {
		t.Fatalf("expected legacy compose app-name alias service, got %#v", got)
	}
	if got := legacyAliasService["metadata"].(map[string]any)["name"]; got != "demo-api" {
		t.Fatalf("expected legacy app-name alias service name %q, got %#v", "demo-api", got)
	}
	legacyAliasSpec := legacyAliasService["spec"].(map[string]any)
	if _, ok := legacyAliasSpec["externalName"]; ok {
		t.Fatalf("legacy app-name alias service should not use externalName, got %#v", legacyAliasSpec["externalName"])
	}
	if selector, ok := legacyAliasSpec["selector"].(map[string]string); !ok {
		t.Fatalf("expected legacy app-name alias selector, got %#v", legacyAliasSpec["selector"])
	} else if got := selector[FugueLabelAppID]; got != app.ID {
		t.Fatalf("expected legacy app-name alias selector app id %q, got %#v", app.ID, got)
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

func TestBuildPostgresClusterManagesApplicationRolePassword(t *testing.T) {
	spec := normalizeRuntimePostgresSpec("fugue-web", model.AppPostgresSpec{
		Database: "fugue",
		User:     "fugue_web",
		Password: "secret",
	})

	cluster := buildPostgresClusterObject("tenant-demo", "demo-secret", "demo-postgres", nil, spec, nil)
	clusterSpec := cluster["spec"].(map[string]any)
	managed := clusterSpec["managed"].(map[string]any)
	roles := managed["roles"].([]map[string]any)
	if len(roles) != 1 {
		t.Fatalf("expected one managed role, got %#v", roles)
	}
	role := roles[0]
	if got := role["name"]; got != "fugue_web" {
		t.Fatalf("expected managed role name fugue_web, got %#v", got)
	}
	if got := role["login"]; got != true {
		t.Fatalf("expected managed role login true, got %#v", got)
	}
	passwordSecret := role["passwordSecret"].(map[string]any)
	if got := passwordSecret["name"]; got != "demo-secret" {
		t.Fatalf("expected managed role password secret demo-secret, got %#v", got)
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
	if got := clusterSpec["minSyncReplicas"]; got != 0 {
		t.Fatalf("expected failover postgres to fail open with minSyncReplicas=0, got %#v", got)
	}
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
	template := deployment["spec"].(map[string]any)["template"].(map[string]any)
	templateMetadata := template["metadata"].(map[string]any)
	templateAnnotations, ok := templateMetadata["annotations"].(map[string]string)
	if !ok {
		t.Fatalf("expected template annotations, got %#v", templateMetadata["annotations"])
	}
	if got := templateAnnotations[FugueAnnotationReleaseKey]; got != releaseKey {
		t.Fatalf("expected template release key %q, got %q", releaseKey, got)
	}
	if got := managedDeploymentRuntimeKey(deployment); got != releaseKey {
		t.Fatalf("expected release key annotation not to change runtime key, got %q want %q", got, releaseKey)
	}

	app.Spec.Image = "ghcr.io/example/demo:v2"
	updatedDeployment := buildAppObjects(app, SchedulingConstraints{})[1]
	updatedMetadata := updatedDeployment["metadata"].(map[string]any)
	updatedAnnotations := updatedMetadata["annotations"].(map[string]string)
	if updatedAnnotations[FugueAnnotationReleaseKey] == releaseKey {
		t.Fatal("expected release key to change when deployment template changes")
	}
}

func TestBuildAppRevisionObjectsKeepDefaultRenderCompatible(t *testing.T) {
	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v1",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	defaultObjects := buildAppObjectsWithPlacementsAndOptions(app, SchedulingConstraints{}, nil, defaultRenderOptions())
	options := defaultRenderOptions()
	options.Revision = AppRevisionRenderOptions{Role: AppRevisionRoleDefault}
	revisionObjects := buildAppObjectsWithPlacementsAndOptions(app, SchedulingConstraints{}, nil, options)

	if !reflect.DeepEqual(defaultObjects, revisionObjects) {
		t.Fatalf("expected default revision render to match existing render")
	}
}

func TestBuildAppRevisionObjectsUseIndependentDeploymentAndService(t *testing.T) {
	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v1",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}
	options := defaultRenderOptions()
	options.Revision = AppRevisionRenderOptions{Role: AppRevisionRoleCandidate, ReleaseID: "apprel_candidate"}

	objects := buildAppObjectsWithPlacementsAndOptions(app, SchedulingConstraints{}, nil, options)
	deployment := firstObjectByKind(t, objects, "Deployment")
	service := firstObjectByKind(t, objects, "Service")
	deploymentMetadata := deployment["metadata"].(map[string]any)
	serviceMetadata := service["metadata"].(map[string]any)
	if got := deploymentMetadata["name"]; got != "app-demo-candidate" {
		t.Fatalf("expected candidate deployment name, got %#v", got)
	}
	if got := serviceMetadata["name"]; got != "app-demo-candidate" {
		t.Fatalf("expected candidate service name, got %#v", got)
	}
	deploymentLabels := deploymentMetadata["labels"].(map[string]string)
	if got := deploymentLabels[FugueLabelAppReleaseRole]; got != AppRevisionRoleCandidate {
		t.Fatalf("expected candidate release role label, got %#v", got)
	}
	if got := deploymentLabels[FugueLabelAppReleaseID]; got != "apprel_candidate" {
		t.Fatalf("expected release id label, got %#v", got)
	}
	selector := deployment["spec"].(map[string]any)["selector"].(map[string]any)["matchLabels"].(map[string]string)
	if got := selector[FugueLabelAppReleaseRole]; got != AppRevisionRoleCandidate {
		t.Fatalf("expected candidate selector label, got %#v", got)
	}
	serviceSelector := service["spec"].(map[string]any)["selector"].(map[string]string)
	if got := serviceSelector[FugueLabelAppReleaseID]; got != "apprel_candidate" {
		t.Fatalf("expected service selector release id, got %#v", got)
	}
}

func TestManagedPostgresFailoverPlacementDoesNotChangeAppPodTemplate(t *testing.T) {
	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_app",
		},
		BackingServices: []model.BackingService{
			{
				ID:          "service_demo",
				TenantID:    "tenant_demo",
				ProjectID:   "project_demo",
				OwnerAppID:  "app_demo",
				Name:        "demo",
				Type:        model.BackingServiceTypePostgres,
				Provisioner: model.BackingServiceProvisionerManaged,
				Status:      model.BackingServiceStatusActive,
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						Database:    "demo",
						User:        "demo",
						Password:    "secret",
						ServiceName: "demo-postgres",
						RuntimeID:   "runtime_app",
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
				Alias:     "postgres",
			},
		},
	}
	baseDeployment := buildAppObjects(app, SchedulingConstraints{})[4]
	baseTemplate := baseDeployment["spec"].(map[string]any)["template"]

	staged := app
	staged.BackingServices[0].Spec.Postgres.Instances = 2
	staged.BackingServices[0].Spec.Postgres.SynchronousReplicas = 1
	staged.BackingServices[0].Spec.Postgres.FailoverTargetRuntimeID = "runtime_target"
	stagedDeployment := buildAppObjects(staged, SchedulingConstraints{})[4]
	stagedTemplate := stagedDeployment["spec"].(map[string]any)["template"]

	if !reflect.DeepEqual(baseTemplate, stagedTemplate) {
		t.Fatalf("expected database failover placement to leave app pod template unchanged\nbase=%#v\nstaged=%#v", baseTemplate, stagedTemplate)
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
	if spec["progressDeadlineSeconds"] != appProgressDeadlineSeconds {
		t.Fatalf("expected progressDeadlineSeconds=%d, got %#v", appProgressDeadlineSeconds, spec["progressDeadlineSeconds"])
	}
	if spec["minReadySeconds"] != DefaultStrictDrainConfig().MinReadySeconds {
		t.Fatalf("expected strict drain minReadySeconds=%d, got %#v", DefaultStrictDrainConfig().MinReadySeconds, spec["minReadySeconds"])
	}
	rollingUpdate := strategy["rollingUpdate"].(map[string]any)
	if rollingUpdate["maxUnavailable"] != 0 {
		t.Fatalf("expected maxUnavailable=0, got %#v", rollingUpdate["maxUnavailable"])
	}
	if rollingUpdate["maxSurge"] != 1 {
		t.Fatalf("expected maxSurge=1, got %#v", rollingUpdate["maxSurge"])
	}
	metadata := deployment["metadata"].(map[string]any)
	annotations := metadata["annotations"].(map[string]string)
	if annotations["fugue.io/rollout-mode"] != "rolling-update" {
		t.Fatalf("expected stateless app rollout-mode rolling-update, got %#v", annotations["fugue.io/rollout-mode"])
	}
	if annotations["fugue.io/downtime-class"] != "online-required" {
		t.Fatalf("expected stateless app downtime-class online-required, got %#v", annotations["fugue.io/downtime-class"])
	}

	template := spec["template"].(map[string]any)
	templateAnnotations := template["metadata"].(map[string]any)["annotations"].(map[string]string)
	if templateAnnotations["fugue.io/rollout-mode"] != "rolling-update" {
		t.Fatalf("expected stateless app template rollout-mode rolling-update, got %#v", templateAnnotations["fugue.io/rollout-mode"])
	}
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
	lifecycle := containers[0]["lifecycle"].(map[string]any)
	preStop := lifecycle["preStop"].(map[string]any)
	httpGet := preStop["httpGet"].(map[string]any)
	if got := httpGet["path"]; got != "/drain/prestop" {
		t.Fatalf("expected connection-aware preStop hook path, got %#v", got)
	}
	if got := httpGet["port"]; got != DefaultStrictDrainConfig().AgentPort {
		t.Fatalf("expected connection-aware preStop hook port %d, got %#v", DefaultStrictDrainConfig().AgentPort, got)
	}
	if got := podSpec["terminationGracePeriodSeconds"]; got != DefaultStrictDrainConfig().TerminationGraceMinSeconds() {
		t.Fatalf("expected steady-state stateless app terminationGracePeriodSeconds=%d, got %#v", DefaultStrictDrainConfig().TerminationGraceMinSeconds(), got)
	}
	if got := podSpec["shareProcessNamespace"]; got != true {
		t.Fatalf("expected connection-aware strict drain to enable shareProcessNamespace, got %#v", got)
	}
	initContainers := podSpec["initContainers"].([]map[string]any)
	if len(initContainers) == 0 || initContainers[0]["name"] != "fugue-drain-agent" {
		t.Fatalf("expected connection-aware drain-agent native sidecar, got %#v", initContainers)
	}
	if got := initContainers[0]["restartPolicy"]; got != "Always" {
		t.Fatalf("expected drain-agent native sidecar restartPolicy Always, got %#v", got)
	}
	if got := annotations["fugue.io/drain-mode"]; got != StrictDrainModeConnectionAware {
		t.Fatalf("expected steady-state stateless app connection-aware drain annotation, got %#v", got)
	}
}

func TestBuildAppDeploymentDoesNotAddStrictDrainForDowntimeRequiredDurableSteadyState(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Workspace: &model.AppWorkspaceSpec{},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	var deployment map[string]any
	for _, object := range objects {
		if object["kind"] == "Deployment" {
			deployment = object
			break
		}
	}
	if deployment == nil {
		t.Fatal("expected Deployment object")
	}
	annotations := deployment["metadata"].(map[string]any)["annotations"].(map[string]string)
	if annotations["fugue.io/downtime-class"] != "downtime-required" {
		t.Fatalf("expected durable steady-state app downtime-required, got %#v", annotations["fugue.io/downtime-class"])
	}
	spec := deployment["spec"].(map[string]any)
	template := spec["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)
	containers := podSpec["containers"].([]map[string]any)
	if _, ok := containers[0]["lifecycle"]; ok {
		t.Fatalf("expected downtime-required durable steady-state app not to get strict drain lifecycle")
	}
	if _, ok := podSpec["terminationGracePeriodSeconds"]; ok {
		t.Fatalf("expected downtime-required durable steady-state app not to set terminationGracePeriodSeconds")
	}
	if got := spec["minReadySeconds"]; got != appServiceMinReadySeconds {
		t.Fatalf("expected default minReadySeconds=%d for durable steady-state app, got %#v", appServiceMinReadySeconds, got)
	}
	if got := annotations["fugue.io/drain-mode"]; got != "" {
		t.Fatalf("expected no strict drain annotation for durable steady-state app, got %#v", got)
	}
}

func TestBuildAppDeploymentAddsStrictDrainForOnlineImageUpdate(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:                         "ghcr.io/example/demo:latest",
			Ports:                         []int{8080},
			Replicas:                      1,
			RuntimeID:                     "runtime_demo",
			RolloutIntent:                 model.AppRolloutIntentOnlineImageUpdate,
			TerminationGracePeriodSeconds: 30,
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	deployment := objects[1]
	annotations := deployment["metadata"].(map[string]any)["annotations"].(map[string]string)
	if got := annotations["fugue.io/drain-mode"]; got != StrictDrainModeConnectionAware {
		t.Fatalf("expected strict drain mode annotation, got %#v", got)
	}
	if got := annotations["fugue.io/drain-timeout-seconds"]; got != "600" {
		t.Fatalf("expected drain timeout annotation 600, got %#v", got)
	}
	spec := deployment["spec"].(map[string]any)
	if got := spec["minReadySeconds"]; got != DefaultStrictDrainConfig().MinReadySeconds {
		t.Fatalf("expected strict drain minReadySeconds=%d, got %#v", DefaultStrictDrainConfig().MinReadySeconds, got)
	}
	template := spec["template"].(map[string]any)
	templateAnnotations := template["metadata"].(map[string]any)["annotations"].(map[string]string)
	if got := templateAnnotations["fugue.io/drain-mode"]; got != StrictDrainModeConnectionAware {
		t.Fatalf("expected template strict drain mode annotation, got %#v", got)
	}
	podSpec := template["spec"].(map[string]any)
	if got := podSpec["terminationGracePeriodSeconds"]; got != int64(630) {
		t.Fatalf("expected terminationGracePeriodSeconds=630 for strict drain, got %#v", got)
	}
	containers := podSpec["containers"].([]map[string]any)
	lifecycle := containers[0]["lifecycle"].(map[string]any)
	preStop := lifecycle["preStop"].(map[string]any)
	httpGet := preStop["httpGet"].(map[string]any)
	if got := httpGet["path"]; got != "/drain/prestop" {
		t.Fatalf("expected preStop httpGet path /drain/prestop, got %#v", got)
	}
}

func TestBuildAppDeploymentUsesFixedSleepFallbackWhenNativeSidecarDisabled(t *testing.T) {
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
	options := defaultRenderOptions()
	options.StrictDrain.Mode = StrictDrainModeConnectionAware
	options.StrictDrain.NativeSidecarEnabled = false

	deployment := firstObjectByKind(t, buildAppObjectsWithPlacementsAndOptions(app, SchedulingConstraints{}, nil, options), "Deployment")
	spec := deployment["spec"].(map[string]any)
	podSpec := spec["template"].(map[string]any)["spec"].(map[string]any)
	containers := podSpec["containers"].([]map[string]any)
	preStop := containers[0]["lifecycle"].(map[string]any)["preStop"].(map[string]any)
	sleep := preStop["sleep"].(map[string]any)
	if got := sleep["seconds"]; got != DefaultStrictDrainConfig().DrainTimeoutSeconds() {
		t.Fatalf("expected fixed-sleep fallback seconds=%d, got %#v", DefaultStrictDrainConfig().DrainTimeoutSeconds(), got)
	}
	if _, ok := podSpec["initContainers"]; ok {
		t.Fatalf("expected fixed-sleep fallback not to inject drain-agent, got %#v", podSpec["initContainers"])
	}
	if _, ok := podSpec["shareProcessNamespace"]; ok {
		t.Fatalf("expected fixed-sleep fallback not to enable shareProcessNamespace, got %#v", podSpec["shareProcessNamespace"])
	}
	annotations := deployment["metadata"].(map[string]any)["annotations"].(map[string]string)
	if got := annotations["fugue.io/drain-mode"]; got != StrictDrainModeFixedSleep {
		t.Fatalf("expected fixed-sleep drain mode annotation, got %#v", got)
	}
}

func TestBuildAppDeploymentDoesNotInjectConnectionAwareDrainWithoutServicePort(t *testing.T) {
	app := model.App{
		TenantID: "tenant_demo",
		Name:     "worker",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/worker:latest",
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	deployment := firstObjectByKind(t, buildAppObjects(app, SchedulingConstraints{}), "Deployment")
	spec := deployment["spec"].(map[string]any)
	if _, ok := spec["minReadySeconds"]; ok {
		t.Fatalf("expected app without cluster service to omit minReadySeconds, got %#v", spec["minReadySeconds"])
	}
	podSpec := spec["template"].(map[string]any)["spec"].(map[string]any)
	containers := podSpec["containers"].([]map[string]any)
	if _, ok := containers[0]["lifecycle"]; ok {
		t.Fatalf("expected app without cluster service not to get drain lifecycle")
	}
	if _, ok := podSpec["initContainers"]; ok {
		t.Fatalf("expected app without cluster service not to get drain-agent")
	}
}

func TestBuildAppObjectsAddsDrainAgentEventRecorderRBAC(t *testing.T) {
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
	role := firstObjectByKind(t, objects, "Role")
	if got := role["apiVersion"]; got != KubernetesRBACAPIVersion {
		t.Fatalf("expected drain-agent event recorder Role apiVersion %s, got %#v", KubernetesRBACAPIVersion, got)
	}
	roleMeta := role["metadata"].(map[string]any)
	roleName := drainAgentEventRecorderRoleName(app)
	if roleMeta["name"] != roleName || roleMeta["namespace"] != NamespaceForTenant(app.TenantID) {
		t.Fatalf("unexpected drain-agent event recorder Role metadata %#v", roleMeta)
	}
	rules := role["rules"].([]map[string]any)
	if len(rules) != 2 {
		t.Fatalf("expected core and events.k8s.io event rules, got %#v", rules)
	}
	roleBinding := firstObjectByKind(t, objects, "RoleBinding")
	roleRef := roleBinding["roleRef"].(map[string]any)
	if roleRef["apiGroup"] != "rbac.authorization.k8s.io" || roleRef["kind"] != "Role" || roleRef["name"] != roleName {
		t.Fatalf("unexpected drain-agent event recorder RoleBinding roleRef %#v", roleRef)
	}
	subjects := roleBinding["subjects"].([]map[string]any)
	if len(subjects) != 1 || subjects[0]["kind"] != "ServiceAccount" || subjects[0]["name"] != "default" {
		t.Fatalf("unexpected drain-agent event recorder RoleBinding subjects %#v", subjects)
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
	if _, ok := spec["minReadySeconds"]; ok {
		t.Fatalf("expected background app to omit minReadySeconds, got %#v", spec["minReadySeconds"])
	}
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
	if len(objects) != 5 {
		t.Fatalf("expected namespace, deployment, service, and drain RBAC, got %d objects", len(objects))
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

func TestBuildAppObjectsRendersSSHPortWithoutChangingHTTPReadiness(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "web-agent",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/agent:ssh",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Env: map[string]string{
				"APP_WORKSPACE":      "/workspace",
				"ARGUS_OPENAI_TOKEN": "argus-openai-v1.demo",
			},
			SSH: &model.AppSSHSpec{
				Enabled:        true,
				TargetPort:     22,
				AuthorizedKeys: []string{"ssh-ed25519 AQIDBAUGBwg= laptop"},
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	secret := firstObjectByKind(t, objects, "Secret")
	stringData := secret["stringData"].(map[string]string)
	if got := stringData["authorized_keys"]; got != "ssh-ed25519 AQIDBAUGBwg= laptop\n" {
		t.Fatalf("expected authorized_keys secret content, got %q", got)
	}

	deployment := firstObjectByKind(t, objects, "Deployment")
	spec := deployment["spec"].(map[string]any)
	template := spec["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)
	container := podSpec["containers"].([]map[string]any)[0]
	envObjects := container["env"].([]map[string]any)
	if got := envValue(envObjects, "FUGUE_SSH_USER"); got != model.DefaultAppSSHUser {
		t.Fatalf("expected SSH user env %q, got %q", model.DefaultAppSSHUser, got)
	}
	if got := envValue(envObjects, "FUGUE_SSH_AUTHORIZED_KEYS"); got != model.DefaultAppSSHAuthorizedKeysPath {
		t.Fatalf("expected authorized keys env %q, got %q", model.DefaultAppSSHAuthorizedKeysPath, got)
	}
	if got := envValue(envObjects, "FUGUE_SSH_SESSION_ENV_CONFIG"); got != AppSSHSessionEnvConfigPath {
		t.Fatalf("expected SSH session env config path %q, got %q", AppSSHSessionEnvConfigPath, got)
	}
	readinessProbe := container["readinessProbe"].(map[string]any)
	tcpSocket := readinessProbe["tcpSocket"].(map[string]any)
	if tcpSocket["port"] != 8080 {
		t.Fatalf("expected HTTP readiness to stay on port 8080, got %#v", tcpSocket["port"])
	}
	containerPorts := container["ports"].([]map[string]any)
	if !containerPortsContain(containerPorts, 8080) || !containerPortsContain(containerPorts, 22) {
		t.Fatalf("expected container ports 8080 and 22, got %#v", containerPorts)
	}
	volumeMounts := container["volumeMounts"].([]map[string]any)
	if got := volumeMounts[0]["mountPath"]; got != model.DefaultAppSSHAuthorizedKeysPath {
		t.Fatalf("expected authorized_keys mount path, got %#v", got)
	}
	if !volumeMountsContainPath(volumeMounts, path.Dir(AppSSHSessionEnvConfigPath)) {
		t.Fatalf("expected SSH session env config volume mount, got %#v", volumeMounts)
	}
	volumes := podSpec["volumes"].([]map[string]any)
	if !volumesContainSecret(volumes, appSSHSessionEnvVolumeName, appSSHSessionEnvSecretName(RuntimeAppResourceName(app))) {
		t.Fatalf("expected SSH session env secret volume, got %#v", volumes)
	}

	sessionEnvSecret := objectByName(t, objects, appSSHSessionEnvSecretName(RuntimeAppResourceName(app)))
	sessionEnvStringData := sessionEnvSecret["stringData"].(map[string]string)
	sessionEnvConfig := sessionEnvStringData[appSSHSessionEnvConfigKey]
	if !strings.HasPrefix(sessionEnvConfig, "SetEnv ") || !strings.HasSuffix(sessionEnvConfig, "\n") {
		t.Fatalf("expected SSH session env config to be a SetEnv line, got %q", sessionEnvConfig)
	}
	for _, want := range []string{
		"APP_WORKSPACE=/workspace",
		"ARGUS_OPENAI_TOKEN=argus-openai-v1.demo",
		"FUGUE_SSH_SESSION_ENV_CONFIG=" + AppSSHSessionEnvConfigPath,
	} {
		if !strings.Contains(sessionEnvConfig, want) {
			t.Fatalf("expected SSH session env config to contain %q, got %q", want, sessionEnvConfig)
		}
	}

	service := firstObjectByKind(t, objects, "Service")
	servicePorts := service["spec"].(map[string]any)["ports"].([]map[string]any)
	if !servicePortsContain(servicePorts, 8080) || !servicePortsContain(servicePorts, 22) {
		t.Fatalf("expected service ports 8080 and 22, got %#v", servicePorts)
	}
}

func TestBuildAppObjectsSSHKeyChecksumRollsTemplate(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "web-agent",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/agent:ssh",
			Replicas:  1,
			RuntimeID: "runtime_demo",
			SSH: &model.AppSSHSpec{
				Enabled:        true,
				TargetPort:     22,
				AuthorizedKeys: []string{"ssh-ed25519 AQIDBAUGBwg= laptop"},
			},
		},
	}
	objects := buildAppObjects(app, SchedulingConstraints{})
	deployment := firstObjectByKind(t, objects, "Deployment")
	template := deployment["spec"].(map[string]any)["template"].(map[string]any)
	annotations := template["metadata"].(map[string]any)["annotations"].(map[string]string)
	initialChecksum := annotations["fugue.pro/ssh-authorized-keys-checksum"]
	if initialChecksum == "" {
		t.Fatal("expected ssh authorized keys checksum annotation")
	}

	app.Spec.SSH.AuthorizedKeys = []string{"ssh-ed25519 CQoLDA0ODxA= workstation"}
	updatedObjects := buildAppObjects(app, SchedulingConstraints{})
	updatedDeployment := firstObjectByKind(t, updatedObjects, "Deployment")
	updatedTemplate := updatedDeployment["spec"].(map[string]any)["template"].(map[string]any)
	updatedAnnotations := updatedTemplate["metadata"].(map[string]any)["annotations"].(map[string]string)
	if updatedAnnotations["fugue.pro/ssh-authorized-keys-checksum"] == initialChecksum {
		t.Fatal("expected ssh authorized keys checksum to change when key content changes")
	}
}

func TestBuildAppObjectsRendersServiceForBackgroundSSHApp(t *testing.T) {
	app := model.App{
		ID:       "app_worker",
		TenantID: "tenant_demo",
		Name:     "worker",
		Spec: model.AppSpec{
			Image:       "ghcr.io/example/worker:ssh",
			NetworkMode: model.AppNetworkModeBackground,
			Replicas:    1,
			RuntimeID:   "runtime_demo",
			SSH: &model.AppSSHSpec{
				Enabled:        true,
				TargetPort:     22,
				AuthorizedKeys: []string{"ssh-ed25519 AQIDBAUGBwg= laptop"},
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	service := firstObjectByKind(t, objects, "Service")
	servicePorts := service["spec"].(map[string]any)["ports"].([]map[string]any)
	if len(servicePorts) != 1 || servicePorts[0]["port"] != 22 {
		t.Fatalf("expected SSH-only service port 22, got %#v", servicePorts)
	}
	deployment := firstObjectByKind(t, objects, "Deployment")
	container := deployment["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]map[string]any)[0]
	if _, ok := container["readinessProbe"]; ok {
		t.Fatalf("expected background SSH app to omit HTTP readiness, got %#v", container["readinessProbe"])
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
	metadata := deployment["metadata"].(map[string]any)
	annotations := metadata["annotations"].(map[string]string)
	if annotations["fugue.io/rollout-mode"] != "isolated-singleton" {
		t.Fatalf("expected workspace rollout-mode isolated-singleton, got %#v", annotations["fugue.io/rollout-mode"])
	}
	if annotations["fugue.io/downtime-class"] != "downtime-required" {
		t.Fatalf("expected workspace downtime-class downtime-required, got %#v", annotations["fugue.io/downtime-class"])
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
	if !strings.Contains(command[2], "chmod a+rwX") {
		t.Fatalf("expected persistent storage init script to make directory mounts writable, got %q", command[2])
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
	metadata := deployment["metadata"].(map[string]any)
	annotations := metadata["annotations"].(map[string]string)
	if annotations["fugue.io/rollout-reason"] != "single-writer-storage" {
		t.Fatalf("expected persistent storage rollout reason single-writer-storage, got %#v", annotations["fugue.io/rollout-reason"])
	}
}

func TestBuildAppObjectsUsesRecreateForOnlinePersistentStorageRestart(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:         "ghcr.io/example/demo:latest",
			Ports:         []int{8080},
			Replicas:      1,
			RuntimeID:     "runtime_demo",
			RolloutIntent: model.AppRolloutIntentOnlineRestart,
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:             model.AppPersistentStorageModeMovableRWO,
				StorageClassName: model.AppStorageClassFugueWorkspaceRWO,
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
	spec := deployment["spec"].(map[string]any)
	strategy := spec["strategy"].(map[string]any)
	if got := strategy["type"]; got != "Recreate" {
		t.Fatalf("expected single-writer storage restart to use Recreate, got %#v", got)
	}

	metadata := deployment["metadata"].(map[string]any)
	annotations := metadata["annotations"].(map[string]string)
	if annotations["fugue.io/rollout-mode"] != "isolated-singleton" {
		t.Fatalf("expected deployment rollout mode isolated-singleton, got %#v", annotations["fugue.io/rollout-mode"])
	}
	if annotations["fugue.io/downtime-class"] != "downtime-required" {
		t.Fatalf("expected deployment downtime class downtime-required, got %#v", annotations["fugue.io/downtime-class"])
	}
	if annotations["fugue.io/rollout-reason"] != "single-writer-storage" {
		t.Fatalf("expected deployment rollout reason single-writer-storage, got %#v", annotations["fugue.io/rollout-reason"])
	}

	templateAnnotations := spec["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]string)
	if templateAnnotations["fugue.io/rollout-mode"] != "isolated-singleton" {
		t.Fatalf("expected durable template rollout mode to remain isolated-singleton, got %#v", templateAnnotations["fugue.io/rollout-mode"])
	}
	if templateAnnotations["fugue.io/downtime-class"] != "downtime-required" {
		t.Fatalf("expected durable template downtime class to remain downtime-required, got %#v", templateAnnotations["fugue.io/downtime-class"])
	}
}

func TestBuildAppObjectsUsesRecreateForOnlinePersistentStorageResourceUpdate(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:         "ghcr.io/example/demo:latest",
			Ports:         []int{8080},
			Replicas:      1,
			RuntimeID:     "runtime_demo",
			RolloutIntent: model.AppRolloutIntentOnlineResourceUpdate,
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:             model.AppPersistentStorageModeMovableRWO,
				StorageClassName: model.AppStorageClassFugueWorkspaceRWO,
				Mounts: []model.AppPersistentStorageMount{
					{
						Kind:        model.AppPersistentStorageMountKindFile,
						Path:        "/home/api.yaml",
						SeedContent: "providers: []\n",
					},
				},
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	deployment := firstObjectByKind(t, objects, "Deployment")
	spec := deployment["spec"].(map[string]any)
	strategy := spec["strategy"].(map[string]any)
	if got := strategy["type"]; got != "Recreate" {
		t.Fatalf("expected single-writer storage resource update to use Recreate, got %#v", got)
	}

	metadata := deployment["metadata"].(map[string]any)
	annotations := metadata["annotations"].(map[string]string)
	if annotations["fugue.io/rollout-mode"] != "isolated-singleton" {
		t.Fatalf("expected deployment rollout mode isolated-singleton, got %#v", annotations["fugue.io/rollout-mode"])
	}
	if annotations["fugue.io/downtime-class"] != "downtime-required" {
		t.Fatalf("expected deployment downtime class downtime-required, got %#v", annotations["fugue.io/downtime-class"])
	}
	if annotations["fugue.io/rollout-reason"] != "single-writer-storage" {
		t.Fatalf("expected deployment rollout reason single-writer-storage, got %#v", annotations["fugue.io/rollout-reason"])
	}

	templateAnnotations := spec["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]string)
	if templateAnnotations["fugue.io/rollout-mode"] != "isolated-singleton" {
		t.Fatalf("expected durable template rollout mode to remain isolated-singleton, got %#v", templateAnnotations["fugue.io/rollout-mode"])
	}
	if templateAnnotations["fugue.io/downtime-class"] != "downtime-required" {
		t.Fatalf("expected durable template downtime class to remain downtime-required, got %#v", templateAnnotations["fugue.io/downtime-class"])
	}
}

func TestBuildAppObjectsUsesRecreateForOnlinePersistentStorageImageUpdate(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:         "ghcr.io/example/demo:v2",
			Ports:         []int{8080},
			Replicas:      1,
			RuntimeID:     "runtime_demo",
			RolloutIntent: model.AppRolloutIntentOnlineImageUpdate,
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:             model.AppPersistentStorageModeMovableRWO,
				StorageClassName: model.AppStorageClassFugueWorkspaceRWO,
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

	deployment := firstObjectByKind(t, buildAppObjects(app, SchedulingConstraints{}), "Deployment")
	spec := deployment["spec"].(map[string]any)
	strategy := spec["strategy"].(map[string]any)
	if got := strategy["type"]; got != "Recreate" {
		t.Fatalf("expected single-writer storage image update to use Recreate, got %#v", got)
	}

	annotations := deployment["metadata"].(map[string]any)["annotations"].(map[string]string)
	if annotations["fugue.io/rollout-mode"] != "isolated-singleton" {
		t.Fatalf("expected deployment rollout mode isolated-singleton, got %#v", annotations["fugue.io/rollout-mode"])
	}
	if annotations["fugue.io/downtime-class"] != "downtime-required" {
		t.Fatalf("expected deployment downtime class downtime-required, got %#v", annotations["fugue.io/downtime-class"])
	}
	if annotations["fugue.io/rollout-reason"] != "single-writer-storage" {
		t.Fatalf("expected single-writer-storage rollout reason, got %#v", annotations["fugue.io/rollout-reason"])
	}

	templateAnnotations := spec["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]string)
	if templateAnnotations["fugue.io/rollout-mode"] != "isolated-singleton" {
		t.Fatalf("expected durable template rollout mode to remain isolated-singleton, got %#v", templateAnnotations["fugue.io/rollout-mode"])
	}
}

func TestBuildAppObjectsUsesRollingUpdateForOnlineLocalRWOPersistentStorageImageUpdate(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:         "ghcr.io/example/demo:v2",
			Ports:         []int{8080},
			Replicas:      1,
			RuntimeID:     "runtime_demo",
			RolloutIntent: model.AppRolloutIntentOnlineImageUpdate,
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:             model.AppPersistentStorageModeMovableRWO,
				StorageClassName: model.AppStorageClassFugueLocalRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindFile, Path: "/home/api.yaml"},
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/home/data"},
				},
			},
		},
	}

	deployment := firstObjectByKind(t, buildAppObjects(app, SchedulingConstraints{}), "Deployment")
	spec := deployment["spec"].(map[string]any)
	strategy := spec["strategy"].(map[string]any)
	if got := strategy["type"]; got != "RollingUpdate" {
		t.Fatalf("expected local RWO online image update to use RollingUpdate, got %#v", got)
	}
	rolling := strategy["rollingUpdate"].(map[string]any)
	if rolling["maxUnavailable"] != 0 || rolling["maxSurge"] != 1 {
		t.Fatalf("expected zero-downtime rolling update parameters, got %#v", rolling)
	}

	annotations := deployment["metadata"].(map[string]any)["annotations"].(map[string]string)
	if annotations["fugue.io/rollout-mode"] != "rolling-restart" {
		t.Fatalf("expected deployment rollout mode rolling-restart, got %#v", annotations["fugue.io/rollout-mode"])
	}
	if annotations["fugue.io/downtime-class"] != "online-required" {
		t.Fatalf("expected deployment downtime class online-required, got %#v", annotations["fugue.io/downtime-class"])
	}
	if annotations["fugue.io/rollout-reason"] != "image-only" {
		t.Fatalf("expected image-only rollout reason, got %#v", annotations["fugue.io/rollout-reason"])
	}
	if annotations["fugue.io/drain-mode"] == "" {
		t.Fatalf("expected local RWO online update to include strict drain annotations, got %#v", annotations)
	}
	podSpec := spec["template"].(map[string]any)["spec"].(map[string]any)
	containers := podSpec["containers"].([]map[string]any)
	if _, ok := containers[0]["lifecycle"]; !ok {
		t.Fatalf("expected local RWO online update to use strict drain lifecycle")
	}
}

func TestBuildAppObjectsUsesRecreateForOnlinePersistentStorageLifecycleUpdate(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:                         "ghcr.io/example/demo:latest",
			Ports:                         []int{8080},
			Replicas:                      1,
			RuntimeID:                     "runtime_demo",
			RolloutIntent:                 model.AppRolloutIntentOnlineLifecycleUpdate,
			TerminationGracePeriodSeconds: 2100,
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:             model.AppPersistentStorageModeMovableRWO,
				StorageClassName: model.AppStorageClassFugueWorkspaceRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"},
				},
			},
		},
	}

	deployment := firstObjectByKind(t, buildAppObjects(app, SchedulingConstraints{}), "Deployment")
	spec := deployment["spec"].(map[string]any)
	strategy := spec["strategy"].(map[string]any)
	if got := strategy["type"]; got != "Recreate" {
		t.Fatalf("expected single-writer storage lifecycle update to use Recreate, got %#v", got)
	}
	annotations := deployment["metadata"].(map[string]any)["annotations"].(map[string]string)
	if got := annotations["fugue.io/rollout-reason"]; got != "single-writer-storage" {
		t.Fatalf("expected single-writer-storage rollout reason, got %#v", got)
	}
	podSpec := spec["template"].(map[string]any)["spec"].(map[string]any)
	if got := podSpec["terminationGracePeriodSeconds"]; got != int64(2100) {
		t.Fatalf("expected terminationGracePeriodSeconds=2100, got %#v", got)
	}
	containers := podSpec["containers"].([]map[string]any)
	if _, ok := containers[0]["lifecycle"]; ok {
		t.Fatalf("expected single-writer storage lifecycle update not to use strict drain lifecycle")
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
	if len(objects) != 9 {
		t.Fatalf("expected 9 objects, got %d", len(objects))
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
	if got := secretLabels[CloudNativePGReloadLabel]; got != "true" {
		t.Fatalf("expected postgres secret label %s=%q, got %#v", CloudNativePGReloadLabel, "true", got)
	}

	postgresAliasService := objects[2]
	if got := postgresAliasService["kind"]; got != "Service" {
		t.Fatalf("expected postgres service alias, got %#v", got)
	}
	aliasSpec := postgresAliasService["spec"].(map[string]any)
	if _, ok := aliasSpec["externalName"]; ok {
		t.Fatalf("postgres alias service should not use externalName, got %#v", aliasSpec["externalName"])
	}
	if selector, ok := aliasSpec["selector"].(map[string]string); !ok {
		t.Fatalf("expected postgres alias selector, got %#v", aliasSpec["selector"])
	} else if got := selector["cnpg.io/cluster"]; got != "uni-api-demo-postgres" {
		t.Fatalf("expected postgres alias selector cluster %q, got %#v", "uni-api-demo-postgres", got)
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
	if got := env["DB_HOST"]; got != "demo-postgres" {
		t.Fatalf("expected runtime env DB_HOST to be repaired to Fugue-managed primary service, got %q", got)
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

func TestBuildAppObjectsIncludesRestrictedNetworkPolicy(t *testing.T) {
	app := model.App{
		ID:        "app_session",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "argus-session-demo",
		Spec: model.AppSpec{
			Image:     "registry.fugue.pro/fugue-apps/argus-runtime:latest",
			Ports:     []int{7777},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			NetworkPolicy: &model.AppNetworkPolicySpec{
				Egress: &model.AppNetworkPolicyDirectionSpec{
					Mode:                model.AppNetworkPolicyModeRestricted,
					AllowDNS:            true,
					AllowPublicInternet: true,
					AllowApps: []model.AppNetworkPolicyAppPeer{
						{AppID: "app_gateway", Ports: []int{8080}},
					},
				},
				Ingress: &model.AppNetworkPolicyDirectionSpec{
					Mode: model.AppNetworkPolicyModeRestricted,
					AllowApps: []model.AppNetworkPolicyAppPeer{
						{AppID: "app_gateway", Ports: []int{7777}},
					},
				},
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	networkPolicy := firstObjectByKind(t, objects, "NetworkPolicy")
	if got := networkPolicy["apiVersion"]; got != KubernetesNetworkPolicyAPIVersion {
		t.Fatalf("expected NetworkPolicy api version %q, got %#v", KubernetesNetworkPolicyAPIVersion, got)
	}
	spec := networkPolicy["spec"].(map[string]any)
	podSelector := spec["podSelector"].(map[string]any)["matchLabels"].(map[string]string)
	if got := podSelector[FugueLabelAppID]; got != "app_session" {
		t.Fatalf("expected policy to select session app id, got %#v", podSelector)
	}

	egress := spec["egress"].([]map[string]any)
	dnsRule := networkPolicyRuleByPodSelector(t, egress, map[string]string{"k8s-app": "kube-dns"})
	dnsTarget := networkPolicyTargetWithPodSelector(t, dnsRule, map[string]string{"k8s-app": "kube-dns"})
	dnsNamespaceSelector := dnsTarget["namespaceSelector"].(map[string]any)["matchLabels"].(map[string]string)
	if got := dnsNamespaceSelector["kubernetes.io/metadata.name"]; got != "kube-system" {
		t.Fatalf("expected DNS egress to target kube-system, got %#v", dnsNamespaceSelector)
	}
	dnsPorts := dnsRule["ports"].([]map[string]any)
	if len(dnsPorts) != 2 || dnsPorts[0]["port"] != 53 || dnsPorts[1]["port"] != 53 {
		t.Fatalf("expected DNS egress ports 53/TCP and 53/UDP, got %#v", dnsPorts)
	}
	_ = networkPolicyIPBlock(t, egress, "10.43.0.10/32")
	if !networkPolicyHasPortOnlyDNSRule(egress) {
		t.Fatalf("expected port-only DNS fallback rule in %#v", egress)
	}

	publicIPv4 := networkPolicyIPBlock(t, egress, "0.0.0.0/0")
	publicIPv4Except := publicIPv4["except"].([]string)
	for _, cidr := range []string{"10.0.0.0/8", "169.254.0.0/16", "198.18.0.0/15", "224.0.0.0/4"} {
		if !stringSliceContains(publicIPv4Except, cidr) {
			t.Fatalf("expected IPv4 public internet exception %q in %#v", cidr, publicIPv4Except)
		}
	}
	publicIPv6 := networkPolicyIPBlock(t, egress, "::/0")
	publicIPv6Except := publicIPv6["except"].([]string)
	for _, cidr := range []string{"::1/128", "fc00::/7", "fe80::/10"} {
		if !stringSliceContains(publicIPv6Except, cidr) {
			t.Fatalf("expected IPv6 public internet exception %q in %#v", cidr, publicIPv6Except)
		}
	}

	gatewayEgress := networkPolicyRuleByPodSelector(t, egress, map[string]string{FugueLabelAppID: "app_gateway"})
	to := gatewayEgress["to"].([]map[string]any)
	gatewaySelector := to[0]["podSelector"].(map[string]any)["matchLabels"].(map[string]string)
	if got := gatewaySelector[FugueLabelAppID]; got != "app_gateway" {
		t.Fatalf("expected gateway app selector, got %#v", gatewaySelector)
	}
	egressPorts := gatewayEgress["ports"].([]map[string]any)
	if got := egressPorts[0]["port"]; got != 8080 {
		t.Fatalf("expected gateway egress port 8080, got %#v", got)
	}

	ingress := spec["ingress"].([]map[string]any)
	from := ingress[0]["from"].([]map[string]any)
	ingressSelector := from[0]["podSelector"].(map[string]any)["matchLabels"].(map[string]string)
	if got := ingressSelector[FugueLabelAppID]; got != "app_gateway" {
		t.Fatalf("expected gateway ingress selector, got %#v", ingressSelector)
	}
	ingressPorts := ingress[0]["ports"].([]map[string]any)
	if got := ingressPorts[0]["port"]; got != 7777 {
		t.Fatalf("expected session ingress port 7777, got %#v", got)
	}
}

func TestBuildAppObjectsNetworkPolicyAllowsSSHFront(t *testing.T) {
	app := model.App{
		ID:       "app_session",
		TenantID: "tenant_demo",
		Name:     "session",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/session:ssh",
			Replicas:  1,
			RuntimeID: "runtime_demo",
			SSH: &model.AppSSHSpec{
				Enabled:        true,
				TargetPort:     2222,
				AuthorizedKeys: []string{"ssh-ed25519 AQIDBAUGBwg= laptop"},
			},
			NetworkPolicy: &model.AppNetworkPolicySpec{
				Ingress: &model.AppNetworkPolicyDirectionSpec{
					Mode: model.AppNetworkPolicyModeRestricted,
				},
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	networkPolicy := firstObjectByKind(t, objects, "NetworkPolicy")
	spec := networkPolicy["spec"].(map[string]any)
	ingress := spec["ingress"].([]map[string]any)
	sshRule := networkPolicyIngressRuleByPodSelector(t, ingress, map[string]string{
		"app.kubernetes.io/component": "fugue-ssh-front",
	})
	ports := sshRule["ports"].([]map[string]any)
	if len(ports) != 1 || ports[0]["port"] != 2222 {
		t.Fatalf("expected ssh-front ingress port 2222, got %#v", ports)
	}
	from := sshRule["from"].([]map[string]any)
	hasHostNetworkSource := false
	for _, target := range from {
		ipBlock, ok := target["ipBlock"].(map[string]any)
		if ok && ipBlock["cidr"] == "0.0.0.0/0" {
			hasHostNetworkSource = true
			break
		}
	}
	if !hasHostNetworkSource {
		t.Fatalf("expected ssh-front ingress to allow hostNetwork source ipBlock, got %#v", from)
	}
}

func TestBuildAppObjectsNetworkPolicyAllowsBackingPostgres(t *testing.T) {
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
			Postgres: &model.AppPostgresSpec{
				Database:    "uniapi",
				User:        "root",
				Password:    "secret",
				ServiceName: "uni-api-demo-postgres",
			},
			NetworkPolicy: &model.AppNetworkPolicySpec{
				Egress: &model.AppNetworkPolicyDirectionSpec{
					Mode:                 model.AppNetworkPolicyModeRestricted,
					AllowBackingServices: true,
				},
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	networkPolicy := firstObjectByKind(t, objects, "NetworkPolicy")
	spec := networkPolicy["spec"].(map[string]any)
	egress := spec["egress"].([]map[string]any)
	postgresEgress := networkPolicyRuleByPodSelector(t, egress, map[string]string{
		"cnpg.io/cluster":      "uni-api-demo-postgres",
		"cnpg.io/instanceRole": "primary",
	})
	postgresPorts := postgresEgress["ports"].([]map[string]any)
	if len(postgresPorts) != 1 || postgresPorts[0]["port"] != 5432 {
		t.Fatalf("expected backing postgres egress port 5432, got %#v", postgresPorts)
	}
}

func TestBuildAppObjectsNetworkPolicyAllowsInjectedAppObservabilityEndpoint(t *testing.T) {
	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "observed-demo",
		Spec: model.AppSpec{
			Image:     "registry.fugue.pro/fugue-apps/observed:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Env: map[string]string{
				"FUGUE_OBSERVABILITY_ENDPOINT": "http://fugue-fugue-telemetry-agent.fugue-system.svc.cluster.local:7834",
			},
			NetworkPolicy: &model.AppNetworkPolicySpec{
				Egress: &model.AppNetworkPolicyDirectionSpec{
					Mode: model.AppNetworkPolicyModeRestricted,
				},
			},
		},
	}

	objects := buildAppObjects(app, SchedulingConstraints{})
	networkPolicy := firstObjectByKind(t, objects, "NetworkPolicy")
	spec := networkPolicy["spec"].(map[string]any)
	egress := spec["egress"].([]map[string]any)

	telemetryEgress := networkPolicyRuleByPodSelector(t, egress, map[string]string{
		"app.kubernetes.io/component": "telemetry-agent",
	})
	telemetryTarget := networkPolicyTargetWithPodSelector(t, telemetryEgress, map[string]string{
		"app.kubernetes.io/component": "telemetry-agent",
	})
	namespaceSelector := telemetryTarget["namespaceSelector"].(map[string]any)["matchLabels"].(map[string]string)
	if got := namespaceSelector["kubernetes.io/metadata.name"]; got != "fugue-system" {
		t.Fatalf("expected telemetry egress to target fugue-system, got %#v", namespaceSelector)
	}
	telemetryPorts := telemetryEgress["ports"].([]map[string]any)
	if len(telemetryPorts) != 1 || telemetryPorts[0]["port"] != 7834 {
		t.Fatalf("expected telemetry egress port 7834, got %#v", telemetryPorts)
	}

	for _, cidr := range []string{"10.43.0.0/16", "10.96.0.0/12", "172.20.0.0/16"} {
		serviceCIDRRule := networkPolicyRuleByIPBlock(t, egress, cidr)
		serviceCIDRPorts := serviceCIDRRule["ports"].([]map[string]any)
		if len(serviceCIDRPorts) != 1 || serviceCIDRPorts[0]["port"] != 7834 {
			t.Fatalf("expected telemetry service CIDR %s to allow port 7834, got %#v", cidr, serviceCIDRPorts)
		}
	}
}

func networkPolicyRuleByPodSelector(t *testing.T, rules []map[string]any, selector map[string]string) map[string]any {
	t.Helper()
	for _, rule := range rules {
		if networkPolicyTargetWithPodSelectorMaybe(rule, selector) != nil {
			return rule
		}
	}
	t.Fatalf("expected network policy rule with pod selector %#v in %#v", selector, rules)
	return nil
}

func networkPolicyIngressRuleByPodSelector(t *testing.T, rules []map[string]any, selector map[string]string) map[string]any {
	t.Helper()
	for _, rule := range rules {
		targets, ok := rule["from"].([]map[string]any)
		if !ok {
			continue
		}
		for _, target := range targets {
			podSelector, ok := target["podSelector"].(map[string]any)
			if !ok {
				continue
			}
			matchLabels, ok := podSelector["matchLabels"].(map[string]string)
			if !ok {
				continue
			}
			matched := true
			for key, value := range selector {
				if matchLabels[key] != value {
					matched = false
					break
				}
			}
			if matched {
				return rule
			}
		}
	}
	t.Fatalf("expected network policy ingress rule with pod selector %#v in %#v", selector, rules)
	return nil
}

func networkPolicyTargetWithPodSelector(t *testing.T, rule map[string]any, selector map[string]string) map[string]any {
	t.Helper()
	target := networkPolicyTargetWithPodSelectorMaybe(rule, selector)
	if target == nil {
		t.Fatalf("expected network policy target with pod selector %#v in %#v", selector, rule)
	}
	return target
}

func networkPolicyTargetWithPodSelectorMaybe(rule map[string]any, selector map[string]string) map[string]any {
	targets, ok := rule["to"].([]map[string]any)
	if !ok {
		return nil
	}
	for _, target := range targets {
		podSelector, ok := target["podSelector"].(map[string]any)
		if !ok {
			continue
		}
		matchLabels, ok := podSelector["matchLabels"].(map[string]string)
		if !ok {
			continue
		}
		matched := true
		for key, value := range selector {
			if matchLabels[key] != value {
				matched = false
				break
			}
		}
		if matched {
			return target
		}
	}
	return nil
}

func networkPolicyIPBlock(t *testing.T, rules []map[string]any, cidr string) map[string]any {
	t.Helper()
	rule := networkPolicyRuleByIPBlock(t, rules, cidr)
	targets := rule["to"].([]map[string]any)
	for _, target := range targets {
		ipBlock, ok := target["ipBlock"].(map[string]any)
		if !ok {
			continue
		}
		if ipBlock["cidr"] == cidr {
			return ipBlock
		}
	}
	t.Fatalf("expected network policy ipBlock cidr %q in %#v", cidr, rules)
	return nil
}

func networkPolicyRuleByIPBlock(t *testing.T, rules []map[string]any, cidr string) map[string]any {
	t.Helper()
	for _, rule := range rules {
		targets, ok := rule["to"].([]map[string]any)
		if !ok {
			continue
		}
		for _, target := range targets {
			ipBlock, ok := target["ipBlock"].(map[string]any)
			if !ok {
				continue
			}
			if ipBlock["cidr"] == cidr {
				return rule
			}
		}
	}
	t.Fatalf("expected network policy rule with ipBlock cidr %q in %#v", cidr, rules)
	return nil
}

func networkPolicyHasPortOnlyDNSRule(rules []map[string]any) bool {
	for _, rule := range rules {
		if _, hasTo := rule["to"]; hasTo {
			continue
		}
		ports, ok := rule["ports"].([]map[string]any)
		if !ok || len(ports) != 2 {
			continue
		}
		if ports[0]["port"] == 53 && ports[1]["port"] == 53 {
			return true
		}
	}
	return false
}

func stringSliceContains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func containerPortsContain(ports []map[string]any, want int) bool {
	for _, port := range ports {
		if port["containerPort"] == want {
			return true
		}
	}
	return false
}

func servicePortsContain(ports []map[string]any, want int) bool {
	for _, port := range ports {
		if port["port"] == want {
			return true
		}
	}
	return false
}

func volumeMountsContainPath(volumeMounts []map[string]any, want string) bool {
	for _, volumeMount := range volumeMounts {
		if volumeMount["mountPath"] == want {
			return true
		}
	}
	return false
}

func volumesContainSecret(volumes []map[string]any, volumeName string, secretName string) bool {
	for _, volume := range volumes {
		if volume["name"] != volumeName {
			continue
		}
		secret, ok := volume["secret"].(map[string]any)
		if !ok {
			return false
		}
		return secret["secretName"] == secretName
	}
	return false
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

func objectByName(t *testing.T, objects []map[string]any, name string) map[string]any {
	t.Helper()
	for _, object := range objects {
		metadata, ok := object["metadata"].(map[string]any)
		if !ok {
			continue
		}
		if metadata["name"] == name {
			return object
		}
	}
	t.Fatalf("expected object named %q in %#v", name, objects)
	return nil
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
	if got, ok := limits["ephemeral-storage"]; ok {
		t.Fatalf("expected helper ephemeral limit to be unset, got %#v", got)
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
