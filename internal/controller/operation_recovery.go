package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

// The controller's existing leader lease owns recovery. Claiming and registering
// a normal worker share a mutex with recovery, so a live worker can never be
// mistaken for an orphan between its DB claim and goroutine launch.
func (s *Service) recoverCompletedManagedDeploys(ctx context.Context) error {
	if !s.Config.KubectlApply {
		return nil
	}
	operations, err := s.Store.ListActiveOperations()
	if err != nil {
		return err
	}
	for _, op := range operations {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if op.Type == model.OperationTypeAppCPUResize && op.Status == model.OperationStatusRunning && op.ExecutionMode == model.ExecutionModeManaged {
			s.operationClaimMu.Lock()
			owner := new(int)
			_, owned := s.runningManagedOperations.LoadOrStore(op.ID, owner)
			s.operationClaimMu.Unlock()
			if !owned {
				call, cancel := context.WithTimeout(ctx, 20*time.Second)
				app, err := s.Store.GetApp(op.AppID)
				if err == nil {
					err = s.executeAppCPUResize(call, op, app)
				}
				if err != nil && !errors.Is(err, errAppCPUResizeUnsettled) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					_, err = s.Store.FailManagedOperationGuarded(op, err.Error())
				}
				cancel()
				s.runningManagedOperations.CompareAndDelete(op.ID, owner)
				if err != nil && s.Logger != nil {
					s.Logger.Printf("app CPU resize recovery %s remains pending: %v", op.ID, err)
				}
			}
			continue
		}
		if op.Status != model.OperationStatusRunning || op.ExecutionMode != model.ExecutionModeManaged || op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil || op.ConfigBaseSpec == nil {
			continue
		}
		s.operationClaimMu.Lock()
		owner := new(int)
		_, owned := s.runningManagedOperations.LoadOrStore(op.ID, owner)
		s.operationClaimMu.Unlock()
		if owned {
			continue
		}
		call, cancel := context.WithTimeout(ctx, 15*time.Second)
		recovered, err := s.recoverCompletedManagedDeploy(call, op)
		cancel()
		s.runningManagedOperations.CompareAndDelete(op.ID, owner)
		if err != nil {
			if errors.Is(err, store.ErrConflict) || errors.Is(err, store.ErrNotFound) {
				continue
			}
			return fmt.Errorf("observe interrupted deployment %s: %w", op.ID, err)
		}
		if recovered && s.Logger != nil {
			s.Logger.Printf("operation %s recovered_completed from verified runtime; no rollout replayed", op.ID)
		}
	}
	return nil
}

func (s *Service) recoverCompletedManagedDeploy(ctx context.Context, op model.Operation) (bool, error) {
	current, err := s.Store.GetApp(op.AppID)
	if err != nil {
		return false, err
	}
	// Revision/canary promotion and stateful migrations have additional evidence
	// gates; they resume through their established recovery mechanisms.
	if model.AppSafeZeroDowntimeRolloutEnabled(current.Spec) || model.AppSafeZeroDowntimeRolloutEnabled(*op.DesiredSpec) ||
		op.DesiredSpec.Postgres != nil || op.DesiredSpec.PersistentStorage != nil || op.DesiredSpec.Workspace != nil || op.DesiredSpec.Replicas <= 0 {
		return false, nil
	}
	merged, err := store.RebasedManagedDeploy(op, current)
	if err != nil {
		return false, err
	}
	desired := current
	desired.Spec = *merged.DesiredSpec
	source := model.AppBuildSource(current)
	if merged.DesiredSource != nil {
		source = model.CloneAppSource(merged.DesiredSource)
	}
	origin := model.AppOriginSource(current)
	if merged.DesiredOriginSource != nil {
		origin = model.CloneAppSource(merged.DesiredOriginSource)
	}
	model.SetAppSourceState(&desired, origin, source)
	desired, _ = s.normalizeManagedAppRuntimeImageRefs(desired)
	desired, err = store.OverlayDesiredManagedPostgres(desired)
	if err != nil {
		return false, err
	}
	client, err := s.kubeClient()
	if err != nil {
		return false, err
	}
	managed, found, err := client.getManagedApp(ctx, runtime.NamespaceForTenant(desired.TenantID), runtime.ManagedAppResourceName(desired))
	if err != nil || !found {
		return false, err
	}
	base, err := s.managedSchedulingConstraints(desired.Spec.RuntimeID)
	if err != nil {
		return false, err
	}
	scheduling := managed.Spec.Scheduling
	if !desiredStringMapSubset(scheduling.NodeSelector, base.NodeSelector) {
		return false, nil
	}
	desired = s.appWithResolvedLaunchOverride(ctx, desired)
	desired.Spec.RolloutIntent = rolloutIntentForManagedOperation(merged, current, desired)
	completionSpec := cloneControllerAppSpec(merged.DesiredSpec)
	completionSpec.Image = desired.Spec.Image
	desired = s.Renderer.PrepareApp(desired)
	ready, err := s.observeRecoveredDeployment(ctx, desired, scheduling)
	if err != nil || !ready {
		return false, err
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	completed, err := s.Store.CompleteManagedOperationGuarded(op, &current, op.ManifestPath, "deployment completion recovered from verified runtime; no rollout replayed", completionSpec, model.AppBuildSource(desired), model.AppOriginSource(desired))
	if err != nil {
		return false, err
	}
	s.completeReleaseAttemptForOperation(completed, desired, "deployment completion recovered from verified runtime")
	s.logOperationAppEvent("completed", "info", completed, desired, "deployment completion recovered from verified runtime", map[string]any{"recovered_without_rollout": true})
	return true, nil
}

// Read runtime facts only. In particular, never call the ordinary apply/rollback
// paths or repair a workload to make it match the stored observation.
func (s *Service) observeRecoveredDeployment(ctx context.Context, app model.App, scheduling runtime.SchedulingConstraints) (bool, error) {
	client, err := s.kubeClient()
	if err != nil {
		return false, err
	}
	namespace, name := runtime.NamespaceForTenant(app.TenantID), runtime.RuntimeAppResourceName(app)
	managed, found, err := client.getManagedApp(ctx, namespace, runtime.ManagedAppResourceName(app))
	if err != nil || !found {
		return false, err
	}
	if managed.Spec.AppID != app.ID || managed.Spec.TenantID != app.TenantID || strings.TrimSpace(managed.Metadata.DeletionTimestamp) != "" ||
		runtime.ManagedAppSpecHash(managed.Spec) != expectedManagedAppSpecHash(app, scheduling) {
		return false, nil
	}
	deployment, found, err := client.getDeployment(ctx, namespace, name)
	if err != nil {
		return false, err
	}
	expected, hasExpected := s.expectedManagedAppDeployment(app, scheduling)
	if !hasExpected {
		return false, nil
	}
	ready, _, err := deploymentRolloutReady(deployment, found, app.Spec.Replicas, name, s.expectedManagedAppReleaseKey(app, scheduling), app.Spec.Image)
	if err != nil || !ready {
		return false, err
	}
	if ready, _ = deploymentSchedulingReadyForRollout(deployment, app.Spec.Replicas, scheduling); !ready {
		return false, nil
	}
	if ready, _ = deploymentRolloutPolicyReady(deployment, found, expected); !ready {
		return false, nil
	}
	if ready, _, err = deploymentReadyPodsMatchTemplate(ctx, client, namespace, app, deployment, app.Spec.Replicas); err != nil || !ready {
		return false, err
	}
	if len(app.BackingServices) > 0 {
		placements, e := s.managedPostgresPlacements(ctx, app)
		if e != nil {
			return false, e
		}
		backing := runtime.ManagedBackingServiceDeploymentsWithPlacements(app, scheduling, placements)
		if len(backing) > 0 {
			if ready, _, _, err = s.managedBackingServicesRolloutReady(ctx, client, namespace, app, backing); err != nil || !ready {
				return false, err
			}
		}
	}
	// Recheck the immutable spec and Kubernetes UID/version after observations.
	// A config rollout or concurrent replacement invalidates this evidence.
	latest, found, err := client.getManagedApp(ctx, namespace, runtime.ManagedAppResourceName(app))
	if err != nil || !found {
		return false, err
	}
	if !reflect.DeepEqual(latest.Spec, managed.Spec) || latest.Metadata.UID != managed.Metadata.UID || latest.Metadata.Generation != managed.Metadata.Generation {
		return false, nil
	}
	return true, nil
}

func operationExecutionKey(op model.Operation) string {
	if op.StartedAt == nil {
		return op.ID + ":unstarted"
	}
	return op.StartedAt.UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano)
}
