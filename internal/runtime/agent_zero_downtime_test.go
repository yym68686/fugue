package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"fugue/internal/config"
	"fugue/internal/model"
)

func TestAgentLiveGuardAllowsReadyStrategyOnlyUpgradeFromRecreate(t *testing.T) {
	current := agentZeroDowntimeTestApp(model.AppStorageClassFugueWorkspaceRWO)
	live := agentTestLiveDeployment(t, current, Renderer{})
	liveSpec := live["spec"].(map[string]any)
	liveSpec["strategy"] = map[string]any{"type": "Recreate"}
	svc := agentTestLiveGuardService(t, live, true)

	prepared, err := svc.prepareAgentTaskRollout(context.Background(), model.OperationTypeDeploy, current, current)
	if err != nil {
		t.Fatalf("ready same-template Recreate to RollingUpdate transition must be allowed: %v", err)
	}
	if !agentDeploymentStrategyIsSafe(agentRenderedDeployment(prepared, svc.Renderer)) {
		t.Fatal("expected the prepared external deployment to use a safe RollingUpdate")
	}
}

func TestAgentLiveGuardRefusesUnknownReleaseIdentity(t *testing.T) {
	current := agentZeroDowntimeTestApp("")
	current.Spec.PersistentStorage = nil
	live := agentTestLiveDeployment(t, current, Renderer{})
	metadata := live["metadata"].(map[string]any)
	annotations := metadata["annotations"].(map[string]string)
	annotations[FugueAnnotationReleaseKey] = "unknown-live-release"
	svc := agentTestLiveGuardService(t, live, true)
	desired := current
	desired.Spec.Env = map[string]string{"MODE": "new"}

	_, err := svc.prepareAgentTaskRollout(context.Background(), model.OperationTypeDeploy, current, desired)
	if err == nil || !strings.Contains(err.Error(), "matches neither current nor desired") {
		t.Fatalf("unknown live identity must fail closed, got %v", err)
	}
}

func TestAgentLiveGuardRefusesPreviouslyServingMissingDeployment(t *testing.T) {
	current := agentZeroDowntimeTestApp("")
	current.Spec.PersistentStorage = nil
	svc := agentTestLiveGuardService(t, nil, false)

	_, err := svc.prepareAgentTaskRollout(context.Background(), model.OperationTypeDeploy, current, current)
	if err == nil || !strings.Contains(err.Error(), "live deployment is missing") {
		t.Fatalf("a missing deployment for a previously serving app must fail closed, got %v", err)
	}
}

func TestAgentLiveGuardRefusesWorkspaceRWOReplacementBeforeApply(t *testing.T) {
	current := agentZeroDowntimeTestApp(model.AppStorageClassFugueWorkspaceRWO)
	live := agentTestLiveDeployment(t, current, Renderer{})
	svc := agentTestLiveGuardService(t, live, true)
	desired := current
	desired.Spec.Env = map[string]string{"MODE": "new"}

	_, err := svc.prepareAgentTaskRollout(context.Background(), model.OperationTypeDeploy, current, desired)
	if err == nil || !strings.Contains(err.Error(), "does not support same-node online dual mount") {
		t.Fatalf("workspace RWO replacement must fail closed, got %v", err)
	}
}

func TestAgentZeroDowntimeRolloutRefusesServingWorkspaceRWOReplacement(t *testing.T) {
	current := agentZeroDowntimeTestApp(model.AppStorageClassFugueWorkspaceRWO)
	desired := current
	desired.Spec.Env = map[string]string{"MODE": "new"}

	_, err := agentZeroDowntimeRollout(model.OperationTypeDeploy, current, desired)
	if err == nil || !strings.Contains(err.Error(), "does not support same-node online dual mount") {
		t.Fatalf("expected serving workspace RWO rollout to fail closed, got %v", err)
	}
}

func TestAgentZeroDowntimeRolloutPreparesServingStatelessReplacement(t *testing.T) {
	current := agentZeroDowntimeTestApp("")
	current.Spec.PersistentStorage = nil
	desired := current
	desired.Spec.Env = map[string]string{"MODE": "new"}

	prepared, err := agentZeroDowntimeRollout(model.OperationTypeDeploy, current, desired)
	if err != nil {
		t.Fatalf("expected stateless online rollout to be accepted: %v", err)
	}
	if prepared.Spec.RolloutIntent != model.AppRolloutIntentOnlineRestart {
		t.Fatalf("expected transient online restart intent, got %q", prepared.Spec.RolloutIntent)
	}
	deployment := agentRenderedDeployment(prepared, Renderer{})
	if got := agentDeploymentStrategy(deployment); got != "RollingUpdate" {
		t.Fatalf("expected RollingUpdate strategy, got %q", got)
	}
}

func TestAgentZeroDowntimeRolloutAllowsInitialWorkspaceDeployment(t *testing.T) {
	current := agentZeroDowntimeTestApp(model.AppStorageClassFugueWorkspaceRWO)
	current.Status = model.AppStatus{Phase: "deploying"}
	desired := current

	if _, err := agentZeroDowntimeRollout(model.OperationTypeDeploy, current, desired); err != nil {
		t.Fatalf("initial deployment must not be blocked by an absent live service: %v", err)
	}
}

func TestAgentZeroDowntimeRolloutRefusesInitialUnsupportedMultiReplicaStorage(t *testing.T) {
	current := agentZeroDowntimeTestApp(model.AppStorageClassFugueWorkspaceRWO)
	current.Status = model.AppStatus{Phase: "deploying"}
	desired := current
	desired.Spec.Replicas = 2

	if _, err := agentZeroDowntimeRollout(model.OperationTypeDeploy, current, desired); err == nil {
		t.Fatal("initial unsupported multi-replica storage must fail closed")
	}
}

func TestAgentZeroDowntimeRolloutRefusesUnsupportedScaleUp(t *testing.T) {
	current := agentZeroDowntimeTestApp(model.AppStorageClassFugueWorkspaceRWO)
	desired := current
	desired.Spec.Replicas = 2

	_, err := agentZeroDowntimeRollout(model.OperationTypeScale, current, desired)
	if err == nil || !strings.Contains(err.Error(), "does not support same-node online dual mount") {
		t.Fatalf("expected unsupported storage scale-up to fail closed, got %v", err)
	}
}

func TestAgentZeroDowntimeRolloutAllowsExplicitScaleToZero(t *testing.T) {
	current := agentZeroDowntimeTestApp(model.AppStorageClassFugueWorkspaceRWO)
	desired := current
	desired.Spec.Replicas = 0

	if _, err := agentZeroDowntimeRollout(model.OperationTypeScale, current, desired); err != nil {
		t.Fatalf("explicit scale-to-zero maintenance action must remain available: %v", err)
	}
}

func TestAgentZeroDowntimeRolloutRefusesServingLocalRWOWithoutNodeProof(t *testing.T) {
	current := agentZeroDowntimeTestApp(model.AppStorageClassFugueLocalRWO)
	desired := current
	desired.Spec.Env = map[string]string{"MODE": "new"}

	if _, err := agentZeroDowntimeRollout(model.OperationTypeDeploy, current, desired); err == nil {
		t.Fatal("external agent must not claim a local-RWO rollout safe without same-node proof")
	}
}

func TestAgentZeroDowntimeRolloutTreatsDeployedPhaseAsLiveEvidence(t *testing.T) {
	current := agentZeroDowntimeTestApp(model.AppStorageClassFugueWorkspaceRWO)
	current.Status.CurrentReplicas = 0
	current.Status.CurrentRuntimeID = ""
	current.Status.Phase = "deployed"
	desired := current
	desired.Spec.Env = map[string]string{"MODE": "new"}

	if _, err := agentZeroDowntimeRollout(model.OperationTypeDeploy, current, desired); err == nil {
		t.Fatal("stale status projections for a deployed app must fail closed")
	}
}

func TestAgentZeroDowntimeRolloutAllowsInitialFailureRecovery(t *testing.T) {
	current := agentZeroDowntimeTestApp(model.AppStorageClassFugueWorkspaceRWO)
	current.Status = model.AppStatus{Phase: "failed"}
	desired := current
	desired.Spec.Image = "ghcr.io/example/demo:fixed"

	if _, err := agentZeroDowntimeRollout(model.OperationTypeDeploy, current, desired); err != nil {
		t.Fatalf("initial failed deployment must remain repairable: %v", err)
	}
}

func agentZeroDowntimeTestApp(storageClass string) model.App {
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
			Env:       map[string]string{"MODE": "old"},
		},
		Status: model.AppStatus{
			CurrentReplicas:  1,
			CurrentRuntimeID: "runtime_demo",
		},
	}
	if storageClass != "" {
		app.Spec.PersistentStorage = &model.AppPersistentStorageSpec{
			Mode:             model.AppPersistentStorageModeMovableRWO,
			StorageClassName: storageClass,
			Mounts: []model.AppPersistentStorageMount{
				{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"},
			},
		}
	}
	return app
}

func agentTestLiveDeployment(t *testing.T, app model.App, renderer Renderer) map[string]any {
	t.Helper()
	deployment := agentRenderedDeployment(app, renderer)
	metadata := deployment["metadata"].(map[string]any)
	metadata["generation"] = 1
	spec := deployment["spec"].(map[string]any)
	spec["replicas"] = app.Spec.Replicas
	deployment["status"] = map[string]any{
		"observedGeneration":  1,
		"replicas":            app.Spec.Replicas,
		"updatedReplicas":     app.Spec.Replicas,
		"readyReplicas":       app.Spec.Replicas,
		"availableReplicas":   app.Spec.Replicas,
		"unavailableReplicas": 0,
	}
	return deployment
}

func agentTestLiveGuardService(t *testing.T, deployment map[string]any, found bool) *AgentService {
	t.Helper()
	return &AgentService{
		Config:   config.AgentConfig{ApplyWithKubectl: true},
		Renderer: Renderer{},
		CommandRunner: func(_ context.Context, name string, args ...string) ([]byte, error) {
			if name != "kubectl" {
				t.Fatalf("unexpected command %s", name)
			}
			command := strings.Join(args, " ")
			switch {
			case strings.Contains(command, "get deployment"):
				if !found {
					return nil, nil
				}
				data, err := json.Marshal(deployment)
				if err != nil {
					t.Fatalf("marshal live deployment: %v", err)
				}
				return data, nil
			case strings.Contains(command, "get endpoints"):
				return []byte(`{"subsets":[{"addresses":[{"ip":"10.0.0.1"}]}]}`), nil
			default:
				t.Fatalf("unexpected kubectl command %s", command)
				return nil, nil
			}
		},
	}
}
