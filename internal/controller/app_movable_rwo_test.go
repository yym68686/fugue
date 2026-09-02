package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func TestBuildMovableRWOCopyPlanConvertsDirectSharedProjectMount(t *testing.T) {
	current := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_a",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:             model.AppPersistentStorageModeSharedProjectRWX,
				StorageClassName: "fugue-rwx",
				SharedSubPath:    "sessions/demo",
				Mounts: []model.AppPersistentStorageMount{
					{
						Kind: model.AppPersistentStorageMountKindDirectory,
						Path: "/workspace",
					},
				},
			},
		},
	}
	desired := current
	desired.Spec.RuntimeID = "runtime_b"
	desired.Spec.PersistentStorage = &model.AppPersistentStorageSpec{
		Mode:             model.AppPersistentStorageModeMovableRWO,
		StorageClassName: "fugue-local-rwo",
		Mounts: []model.AppPersistentStorageMount{
			{
				Kind: model.AppPersistentStorageMountKindDirectory,
				Path: "/workspace",
			},
		},
	}

	svc := &Service{}
	plan, prepared, changed, err := svc.buildMovableRWOCopyPlan(context.Background(), model.Operation{Type: model.OperationTypeDeploy, ID: "op_test"}, current, desired)
	if err != nil {
		t.Fatalf("build copy plan: %v", err)
	}
	if changed {
		t.Fatal("shared-project conversion should not need a generated claim name")
	}
	if plan == nil {
		t.Fatal("expected copy plan")
	}
	if got := plan.sourceMountSubPath; got != "sessions/demo" {
		t.Fatalf("expected source shared subpath, got %q", got)
	}
	if got := plan.targetCopyPath; got == "" || got == "." {
		t.Fatalf("expected direct shared content to copy into target mount subpath, got %q", got)
	}
	if !plan.sourceSharedProject {
		t.Fatal("expected shared-project source copy plan")
	}
	if got := prepared.Spec.PersistentStorage.SharedSubPath; got != "" {
		t.Fatalf("expected movable RWO target spec to clear shared subpath, got %q", got)
	}
}

func TestBuildMovableRWOCopyPlanClearsStaleSharedSubPath(t *testing.T) {
	current := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_a",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:             model.AppPersistentStorageModeSharedProjectRWX,
				StorageClassName: "fugue-rwx",
				SharedSubPath:    "sessions/demo",
				Mounts: []model.AppPersistentStorageMount{
					{
						Kind: model.AppPersistentStorageMountKindDirectory,
						Path: "/workspace",
					},
				},
			},
		},
	}
	desired := current
	desired.Spec.PersistentStorage = &model.AppPersistentStorageSpec{
		Mode:             model.AppPersistentStorageModeMovableRWO,
		StorageClassName: "fugue-local-rwo",
		SharedSubPath:    "sessions/demo",
		Mounts: []model.AppPersistentStorageMount{
			{
				Kind: model.AppPersistentStorageMountKindDirectory,
				Path: "/workspace",
			},
		},
	}

	svc := &Service{}
	_, prepared, changed, err := svc.buildMovableRWOCopyPlan(context.Background(), model.Operation{Type: model.OperationTypeDeploy, ID: "op_test"}, current, desired)
	if err != nil {
		t.Fatalf("build copy plan: %v", err)
	}
	if !changed {
		t.Fatal("expected stale shared subpath cleanup to mark desired spec changed")
	}
	if got := prepared.Spec.PersistentStorage.SharedSubPath; got != "" {
		t.Fatalf("expected stale shared subpath to be cleared, got %q", got)
	}
}

func TestDesiredPersistentStorageClaimNameUsesWorkspacePVCWhenClaimNameEmpty(t *testing.T) {
	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
	}
	if got, want := desiredPersistentStorageClaimName(app, model.AppPersistentStorageSpec{}), runtimepkg.WorkspacePVCName(app); got != want {
		t.Fatalf("expected empty claim name to use workspace PVC %q, got %q", want, got)
	}
}

func TestDesiredPersistentStorageClaimNameMatchesRuntimeForLongExplicitClaim(t *testing.T) {
	app := model.App{ID: "app_" + strings.Repeat("a", 28)}
	storage := model.AppPersistentStorageSpec{
		Mode:      model.AppPersistentStorageModeMovableRWO,
		ClaimName: "app-" + strings.Repeat("workspace-", 8) + "claim",
	}

	got := desiredPersistentStorageClaimName(app, storage)
	want := runtimepkg.PersistentStoragePVCName(app, storage)
	if got != want {
		t.Fatalf("planned claim %q does not match rendered claim %q", got, want)
	}
	if len(got) != runtimepkg.PersistentStorageClaimNameMaxLength {
		t.Fatalf("expected canonical claim length %d, got %d (%q)", runtimepkg.PersistentStorageClaimNameMaxLength, len(got), got)
	}
}

func TestMovableRWOTargetClaimNamePreservesSuffixWithinRuntimeLimit(t *testing.T) {
	app := model.App{ID: "app_" + strings.Repeat("b", 32)}
	operationID := "op_aaaaaaaa1234567890ab"

	claimName := movableRWOTargetClaimName(app, operationID)
	if len(claimName) > runtimepkg.PersistentStorageClaimNameMaxLength {
		t.Fatalf("target claim exceeds runtime limit: len=%d claim=%q", len(claimName), claimName)
	}
	if !strings.HasSuffix(claimName, "-mv-1234567890ab") {
		t.Fatalf("target claim lost operation suffix: %q", claimName)
	}
	storage := model.AppPersistentStorageSpec{
		Mode:      model.AppPersistentStorageModeMovableRWO,
		ClaimName: claimName,
	}
	if rendered := runtimepkg.PersistentStoragePVCName(app, storage); rendered != claimName {
		t.Fatalf("copy target %q does not match rendered claim %q", claimName, rendered)
	}
}

func TestBuildMovableRWOCopyPlanGeneratedClaimMatchesRenderedWorkload(t *testing.T) {
	t.Parallel()

	kubeServer := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(kubeServer.Close)
	svc := &Service{
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client:      kubeServer.Client(),
				baseURL:     kubeServer.URL,
				bearerToken: "test",
				namespace:   namespace,
			}, nil
		},
	}
	current := model.App{
		ID:       "app_" + strings.Repeat("c", 32),
		TenantID: "tenant_demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_source",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:             model.AppPersistentStorageModeMovableRWO,
				StorageClassName: model.AppStorageClassFugueLocalRWO,
			},
		},
	}
	desired := current
	desired.Spec.RuntimeID = "runtime_target"
	desired.Spec.PersistentStorage = &model.AppPersistentStorageSpec{
		Mode:             model.AppPersistentStorageModeMovableRWO,
		StorageClassName: model.AppStorageClassFugueLocalRWO,
	}
	op := model.Operation{
		ID:              "op_aaaaaaaa1234567890ab",
		Type:            model.OperationTypeMigrate,
		SourceRuntimeID: current.Spec.RuntimeID,
		TargetRuntimeID: desired.Spec.RuntimeID,
	}

	plan, prepared, changed, err := svc.buildMovableRWOCopyPlan(context.Background(), op, current, desired)
	if err != nil {
		t.Fatalf("build movable RWO copy plan: %v", err)
	}
	if !changed {
		t.Fatal("expected migration to generate a fresh target claim")
	}
	if plan == nil {
		t.Fatal("expected movable RWO copy plan")
	}
	want := runtimepkg.PersistentStoragePVCName(prepared, *prepared.Spec.PersistentStorage)
	if plan.targetClaimName != want {
		t.Fatalf("copy target %q does not match rendered workload claim %q", plan.targetClaimName, want)
	}
	if len(plan.targetClaimName) > runtimepkg.PersistentStorageClaimNameMaxLength {
		t.Fatalf("generated claim exceeds runtime limit: %q", plan.targetClaimName)
	}
	if !strings.HasSuffix(plan.targetClaimName, "-mv-1234567890ab") {
		t.Fatalf("generated claim lost operation suffix: %q", plan.targetClaimName)
	}
}

func TestBuildMovableRWOCopyPodMountsSharedSourceAndTarget(t *testing.T) {
	pod := buildMovableRWOCopyPod("tenant-a", "copy", map[string]string{"fugue.pro/volume-migration": "demo"}, movableRWOCopyPlan{
		sourceClaimName:     "project-shared",
		sourceMountSubPath:  "sessions/demo",
		sourceCopyPath:      ".",
		sourceSharedProject: true,
		targetClaimName:     "app-workspace",
		targetCopyPath:      "mounts/mount-demo",
	}, runtimepkg.SchedulingConstraints{})

	spec := pod["spec"].(map[string]any)
	containers := spec["containers"].([]map[string]any)
	mounts := containers[0]["volumeMounts"].([]map[string]any)
	if got := mounts[0]["subPath"]; got != "sessions/demo" {
		t.Fatalf("expected source subPath, got %#v", got)
	}
	volumes := spec["volumes"].([]map[string]any)
	sourcePVC := volumes[0]["persistentVolumeClaim"].(map[string]any)
	if got := sourcePVC["claimName"]; got != "project-shared" {
		t.Fatalf("expected shared source claim, got %#v", got)
	}
	targetPVC := volumes[1]["persistentVolumeClaim"].(map[string]any)
	if got := targetPVC["claimName"]; got != "app-workspace" {
		t.Fatalf("expected target claim, got %#v", got)
	}
}

func TestBuildMovableRWOSourcePodRetriesReceiverConnection(t *testing.T) {
	pod := buildMovableRWOSourcePod("tenant-a", "source", map[string]string{"fugue.pro/volume-migration": "demo"}, movableRWOCopyPlan{
		sourceClaimName: "app-workspace",
		sourceCopyPath:  ".",
	}, "10.42.6.64", runtimepkg.SchedulingConstraints{})

	spec := pod["spec"].(map[string]any)
	containers := spec["containers"].([]map[string]any)
	command := containers[0]["command"].([]string)
	script := command[2]
	for _, want := range []string{
		`while [ "$attempt" -le 30 ]; do`,
		`tar -cpf - -C "$source" . | nc "$target" 8730`,
		`waiting for movable RWO receiver`,
		`did not become reachable`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("expected source pod script to contain %q, got:\n%s", want, script)
		}
	}
	if got := command[len(command)-1]; got != "10.42.6.64" {
		t.Fatalf("expected target address arg, got %q", got)
	}
}

func TestMovableRWOMigrationServiceNameStaysShort(t *testing.T) {
	app := model.App{ID: "app_demo", Name: "demo"}
	names := movableRWOMigrationResourceNames(app, "app-1780920656-d995f31e1f88-workspace-mv-c0c22b6ed2c6")
	if got := len(names.service); got > 40 {
		t.Fatalf("expected compact service name, got length %d name %q", got, names.service)
	}
	if !strings.HasPrefix(names.service, "fugue-rwo-svc-") {
		t.Fatalf("expected fugue-rwo service prefix, got %q", names.service)
	}
}

func TestMovableRWOPodFailureMessageIncludesContainerTermination(t *testing.T) {
	pod := kubePod{}
	pod.Status.ContainerStatuses = []kubeContainerStatus{
		{
			Name: "sender",
			State: kubeRuntimeState{
				Terminated: &kubeStateDetail{
					Reason:   "Error",
					ExitCode: 1,
				},
			},
		},
	}

	message := movableRWOPodFailureMessage(pod)
	for _, want := range []string{"sender terminated", "exit=1", "reason=Error"} {
		if !strings.Contains(message, want) {
			t.Fatalf("expected failure message to contain %q, got %q", want, message)
		}
	}
}

func TestSchedulingForPodNodePinsToNFSNode(t *testing.T) {
	pod := kubePod{}
	pod.Spec.NodeName = "gcp1"
	pod.Spec.Tolerations = []runtimepkg.Toleration{
		{Key: "node-role.kubernetes.io/control-plane", Operator: "Exists", Effect: "NoSchedule"},
	}

	scheduling, ok := schedulingForPodNode(pod)
	if !ok {
		t.Fatal("expected scheduling for pod node")
	}
	if got := scheduling.NodeSelector[kubeHostnameLabelKey]; got != "gcp1" {
		t.Fatalf("expected node selector to pin gcp1, got %q", got)
	}
	if len(scheduling.Tolerations) != 1 || scheduling.Tolerations[0].Key != "node-role.kubernetes.io/control-plane" {
		t.Fatalf("expected NFS pod toleration to be preserved, got %#v", scheduling.Tolerations)
	}
}

func TestMovableRWONeedsFreshClaimWhenMigratingRuntime(t *testing.T) {
	current := model.App{
		ID: "app_demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_a",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode: model.AppPersistentStorageModeMovableRWO,
			},
		},
	}
	desired := current
	desired.Spec.RuntimeID = "runtime_b"

	if !movableRWONeedsFreshClaim(model.Operation{
		Type:            model.OperationTypeMigrate,
		SourceRuntimeID: "runtime_a",
		TargetRuntimeID: "runtime_b",
	}, current, desired) {
		t.Fatal("expected runtime migration to allocate a fresh target claim")
	}
}

func TestMigrateDesiredSpecPreservesManagedPostgresRuntime(t *testing.T) {
	current := model.App{
		ID:   "app_demo",
		Name: "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_a",
			Image:     "ghcr.io/example/demo:new",
			Env:       map[string]string{"RIGHTSIZED": "current"},
			Resources: &model.ResourceSpec{CPUMilliCores: 65},
			Postgres: &model.AppPostgresSpec{
				Database:  "demo",
				User:      "demo",
				RuntimeID: "runtime_db_source",
			},
		},
	}
	desired := current.Spec
	desired.RuntimeID = "runtime_b"
	desired.Image = "ghcr.io/example/demo:stale"
	desired.Env = map[string]string{"RIGHTSIZED": "stale"}
	desired.Resources = &model.ResourceSpec{CPUMilliCores: 85}
	desired.Postgres = &model.AppPostgresSpec{
		Database:  "demo",
		User:      "demo",
		RuntimeID: "runtime_b",
	}

	prepared := migrateDesiredSpecForManagedOperation(current, desired)
	if got := prepared.RuntimeID; got != "runtime_b" {
		t.Fatalf("expected app runtime to move to runtime_b, got %q", got)
	}
	if prepared.Postgres == nil {
		t.Fatal("expected managed postgres spec to be preserved")
	}
	if got := prepared.Postgres.RuntimeID; got != "runtime_db_source" {
		t.Fatalf("expected managed postgres runtime to stay on source until database switchover, got %q", got)
	}
	if got := prepared.Image; got != "ghcr.io/example/demo:new" {
		t.Fatalf("expected latest image to survive queued migration, got %q", got)
	}
	if got := prepared.Env["RIGHTSIZED"]; got != "current" {
		t.Fatalf("expected latest environment to survive queued migration, got %q", got)
	}
	if prepared.Resources == nil || prepared.Resources.CPUMilliCores != 65 {
		t.Fatalf("expected latest right-sized resources to survive queued migration, got %#v", prepared.Resources)
	}
}
