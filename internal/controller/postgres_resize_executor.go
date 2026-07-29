package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"

	"github.com/jackc/pgx/v5"
)

const managedPostgresResizeEvidencePayloadVersion = 1

var errManagedPostgresResizeUnsettled = errors.New("managed postgres resize mutation is not terminally settled")

type managedPostgresResizeSnapshot struct {
	Cluster     kubeCloudNativePGCluster
	Observation managedPostgresResizeObservation
}

type managedPostgresResizeExecutionEvent struct {
	EvidenceType     string
	State            string
	Reason           string
	Message          string
	StageName        string
	StageIndex       int
	ChangedResources []string
	Resources        kubeResourceRequirements
	Snapshot         *managedPostgresResizeSnapshot
}

type managedPostgresResizeExecutionHooks struct {
	InspectCapability func(context.Context) (managedPostgresResizeCapability, error)
	Observe           func(context.Context) (managedPostgresResizeSnapshot, error)
	Patch             func(context.Context, managedPostgresResizeObservation, kubeResourceRequirements) (managedPostgresResizeObservation, error)
	Probe             func(context.Context, managedPostgresResizeSnapshot) error
	EnsureOwned       func() error
	Report            func(managedPostgresResizeExecutionEvent) error
	Wait              func(context.Context, time.Duration) error
}

// executeManagedDatabaseResizeOperation is the only controller path allowed to
// mutate managed PostgreSQL Pod resources. It never renders or patches the
// CNPG Cluster, never deletes or evicts a Pod, and persists RuntimeResources
// only after the exact Pod identity, database probe, and target resources have
// all converged.
func (s *Service) executeManagedDatabaseResizeOperation(
	ctx context.Context,
	op model.Operation,
	app model.App,
) error {
	if !s.Config.KubectlApply {
		return s.blockManagedPostgresResizeOperation(op, app, "kubernetes_apply_disabled", "managed postgres resize requires kubernetes apply mode")
	}
	if !s.Config.ManagedPostgresInPlaceResize.Enabled {
		return s.blockManagedPostgresResizeOperation(op, app, "global_resize_disabled", "managed postgres in-place resize is disabled")
	}
	if op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil || op.DesiredSpec.Postgres.RuntimeResources == nil {
		return s.blockManagedPostgresResizeOperation(op, app, "missing_runtime_target", "managed postgres resize operation is missing a complete runtime resource target")
	}

	target, err := store.ManagedPostgresOperationTargetForApp(app, op.ServiceID)
	if err != nil {
		return fmt.Errorf("resolve managed postgres resize target for app %s: %w", app.ID, err)
	}
	if target == nil || target.Service == nil || !target.AppOwned || strings.TrimSpace(target.ServiceID) == "" {
		return s.blockManagedPostgresResizeOperation(op, app, "unowned_database", "managed postgres resize requires one app-owned backing service")
	}
	if target.Postgres.Suspended {
		return s.blockManagedPostgresResizeOperation(op, app, "database_suspended", "managed postgres resize requires an active database")
	}

	targetResources, err := managedPostgresKubeResources(*op.DesiredSpec.Postgres.RuntimeResources)
	if err != nil {
		return s.blockManagedPostgresResizeOperation(op, app, "invalid_runtime_target", err.Error())
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	clusterName := strings.TrimSpace(target.Postgres.ServiceName)
	if namespace == "" || clusterName == "" {
		return s.blockManagedPostgresResizeOperation(op, app, "missing_runtime_identity", "managed postgres resize requires an exact namespace and CNPG Cluster name")
	}

	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes managed postgres resize client: %w", err)
	}
	hooks := managedPostgresResizeExecutionHooks{
		InspectCapability: func(callCtx context.Context) (managedPostgresResizeCapability, error) {
			return client.inspectManagedPostgresResizeCapability(callCtx, namespace)
		},
		Observe: func(callCtx context.Context) (managedPostgresResizeSnapshot, error) {
			return observeManagedPostgresResizeSnapshot(callCtx, client, namespace, clusterName)
		},
		Patch: func(
			callCtx context.Context,
			observation managedPostgresResizeObservation,
			resources kubeResourceRequirements,
		) (managedPostgresResizeObservation, error) {
			pod, patchErr := client.patchPodContainerResources(
				callCtx,
				namespace,
				observation.PodName,
				observation.PodUID,
				observation.ResourceVersion,
				observation.ContainerName,
				observation.DesiredResources,
				resources,
			)
			if patchErr != nil {
				return managedPostgresResizeObservation{}, patchErr
			}
			return observeManagedPostgresResize(pod, managedPostgresMainContainerName)
		},
		Probe: func(callCtx context.Context, snapshot managedPostgresResizeSnapshot) error {
			return probeManagedPostgresResizeDatabase(callCtx, client, namespace, target.Postgres, snapshot)
		},
		EnsureOwned: func() error {
			return s.ensureManagedPostgresLifecycleOperationOwned(op)
		},
		Report: func(event managedPostgresResizeExecutionEvent) error {
			return s.reportManagedPostgresResizeExecutionEvent(op, app, namespace, clusterName, event)
		},
		Wait: waitManagedPostgresResizePollInterval,
	}

	timeout := s.Config.ManagedAppRolloutTimeout
	if timeout <= 0 {
		timeout = config.DefaultManagedAppRolloutTimeout
	}
	interval := s.Config.PollInterval
	if interval <= 0 || interval > 2*time.Second {
		interval = 2 * time.Second
	}
	finalSnapshot, err := executeManagedPostgresResizeStages(
		ctx,
		targetResources,
		s.Config.ManagedPostgresInPlaceResize,
		timeout,
		interval,
		hooks,
	)
	if err != nil {
		return err
	}
	if err := hooks.EnsureOwned(); err != nil {
		// executeManagedPostgresResizeStages may already have accepted one or
		// more /resize mutations. Do not route a late ownership/read failure
		// through the generic requeue/fail paths after that boundary.
		return fmt.Errorf(
			"%w: verify managed postgres resize operation ownership before completion: %v",
			errManagedPostgresResizeUnsettled,
			err,
		)
	}
	if err := hooks.Probe(ctx, finalSnapshot); err != nil {
		return fmt.Errorf("%w: %v", errManagedPostgresResizeUnsettled, managedPostgresResizeExecutionFailure(
			hooks,
			managedPostgresResizeExecutionEvent{
				EvidenceType: model.OperationEvidenceTypePostgresResizeFailed,
				State:        managedPostgresResizeStateFailed,
				Reason:       "final_database_probe_failed",
				Message:      err.Error(),
				Resources:    targetResources,
				Snapshot:     &finalSnapshot,
			},
			err,
		))
	}

	message := fmt.Sprintf("managed postgres service %s resized in place", strings.TrimSpace(target.ServiceID))
	completed, err := s.Store.CompleteManagedOperation(op.ID, "", message)
	if err != nil {
		return fmt.Errorf(
			"%w: persist managed postgres runtime resources after verified resize %s: %v",
			errManagedPostgresResizeUnsettled,
			op.ID,
			err,
		)
	}
	s.logOperationAppEvent("completed", "info", completed, app, message, map[string]any{
		"elapsed_ms": operationElapsedMilliseconds(completed, time.Now().UTC()),
		"service_id": strings.TrimSpace(target.ServiceID),
		"pod_uid":    finalSnapshot.Observation.PodUID,
	})
	if s.Logger != nil {
		s.Logger.Printf(
			"operation %s completed managed postgres in-place resize service=%s pod=%s uid=%s",
			op.ID,
			strings.TrimSpace(target.ServiceID),
			finalSnapshot.Observation.PodName,
			finalSnapshot.Observation.PodUID,
		)
	}
	return nil
}

func (s *Service) blockManagedPostgresResizeOperation(
	op model.Operation,
	app model.App,
	reason, message string,
) error {
	executionErr := fmt.Errorf("managed postgres resize blocked: %s", strings.TrimSpace(message))
	if reportErr := s.reportManagedPostgresResizeExecutionEvent(
		op,
		app,
		"",
		"",
		managedPostgresResizeExecutionEvent{
			EvidenceType: model.OperationEvidenceTypePostgresResizeBlocked,
			State:        managedPostgresResizeStateBlocked,
			Reason:       reason,
			Message:      message,
		},
	); reportErr != nil {
		return errors.Join(executionErr, reportErr)
	}
	return executionErr
}

func executeManagedPostgresResizeStages(
	ctx context.Context,
	target kubeResourceRequirements,
	gates config.ManagedPostgresInPlaceResizeConfig,
	timeout, interval time.Duration,
	hooks managedPostgresResizeExecutionHooks,
) (finalSnapshot managedPostgresResizeSnapshot, executionErr error) {
	mutationAttempted := false
	defer func() {
		if executionErr == nil || !mutationAttempted ||
			errors.Is(executionErr, errOperationNoLongerActive) ||
			errors.Is(executionErr, errManagedPostgresResizeUnsettled) {
			return
		}
		executionErr = fmt.Errorf("%w: %v", errManagedPostgresResizeUnsettled, executionErr)
	}()
	if hooks.InspectCapability == nil || hooks.Observe == nil || hooks.Patch == nil ||
		hooks.Probe == nil || hooks.EnsureOwned == nil || hooks.Report == nil || hooks.Wait == nil {
		return managedPostgresResizeSnapshot{}, fmt.Errorf("managed postgres resize execution hooks are incomplete")
	}
	if timeout <= 0 {
		timeout = config.DefaultManagedAppRolloutTimeout
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	executionCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := hooks.EnsureOwned(); err != nil {
		return managedPostgresResizeSnapshot{}, err
	}
	capability, err := hooks.InspectCapability(executionCtx)
	if err != nil {
		return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
			hooks,
			managedPostgresResizeExecutionEvent{
				EvidenceType: model.OperationEvidenceTypePostgresResizeBlocked,
				State:        managedPostgresResizeStateBlocked,
				Reason:       "capability_check_failed",
				Message:      err.Error(),
			},
			err,
		)
	}
	if !capability.Available() {
		capabilityErr := fmt.Errorf("managed postgres resize capability unavailable: %s", strings.TrimSpace(capability.Message))
		return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
			hooks,
			managedPostgresResizeExecutionEvent{
				EvidenceType: model.OperationEvidenceTypePostgresResizeBlocked,
				State:        managedPostgresResizeStateBlocked,
				Reason:       firstNonEmptyResizeString(capability.Reason, "capability_unavailable"),
				Message:      capabilityErr.Error(),
			},
			capabilityErr,
		)
	}

	initial, err := hooks.Observe(executionCtx)
	if err != nil {
		return managedPostgresResizeSnapshot{}, fmt.Errorf("observe managed postgres resize preflight: %w", err)
	}
	baseline, err := captureManagedPostgresResizeInvariantBaseline(
		initial.Observation.Namespace,
		initial.Cluster.Metadata.Name,
		initial.Cluster,
		initial.Observation,
	)
	if err != nil {
		return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
			hooks,
			managedPostgresResizeExecutionEvent{
				EvidenceType: model.OperationEvidenceTypePostgresResizeBlocked,
				State:        managedPostgresResizeStateBlocked,
				Reason:       "preflight_invariant_failed",
				Message:      err.Error(),
				Snapshot:     &initial,
			},
			err,
		)
	}
	if initial.Observation.ActualResources == nil {
		return managedPostgresResizeSnapshot{}, fmt.Errorf("managed postgres resize preflight has no actual container resources")
	}
	current := managedPostgresCPUAndMemoryEnvelope(*initial.Observation.ActualResources)
	desired := managedPostgresCPUAndMemoryEnvelope(initial.Observation.DesiredResources)
	if !managedPostgresResizeResourcesEqual(current, desired) {
		err := fmt.Errorf("managed postgres resize preflight found unconverged desired and actual resources")
		return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
			hooks,
			managedPostgresResizeExecutionEvent{
				EvidenceType: model.OperationEvidenceTypePostgresResizeBlocked,
				State:        managedPostgresResizeStateBlocked,
				Reason:       "preexisting_resource_drift",
				Message:      err.Error(),
				Resources:    current,
				Snapshot:     &initial,
			},
			err,
		)
	}
	stages, err := planManagedPostgresResizeStages(current, target)
	if err != nil {
		return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
			hooks,
			managedPostgresResizeExecutionEvent{
				EvidenceType: model.OperationEvidenceTypePostgresResizeBlocked,
				State:        managedPostgresResizeStateBlocked,
				Reason:       "invalid_resize_plan",
				Message:      err.Error(),
				Resources:    target,
				Snapshot:     &initial,
			},
			err,
		)
	}
	if err := validateManagedPostgresResizePlanPolicy(initial.Observation, current, stages, gates); err != nil {
		return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
			hooks,
			managedPostgresResizeExecutionEvent{
				EvidenceType: model.OperationEvidenceTypePostgresResizeBlocked,
				State:        managedPostgresResizeStateBlocked,
				Reason:       "resize_policy_blocked",
				Message:      err.Error(),
				Resources:    target,
				Snapshot:     &initial,
			},
			err,
		)
	}
	if err := hooks.Probe(executionCtx, initial); err != nil {
		return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
			hooks,
			managedPostgresResizeExecutionEvent{
				EvidenceType: model.OperationEvidenceTypePostgresResizeBlocked,
				State:        managedPostgresResizeStateBlocked,
				Reason:       "preflight_database_probe_failed",
				Message:      err.Error(),
				Resources:    current,
				Snapshot:     &initial,
			},
			err,
		)
	}
	if err := hooks.Report(managedPostgresResizeExecutionEvent{
		EvidenceType: model.OperationEvidenceTypePostgresResizePreflight,
		State:        managedPostgresResizeStateReady,
		Reason:       "preflight_passed",
		Message:      fmt.Sprintf("managed postgres resize preflight passed with %d stage(s)", len(stages)),
		Resources:    target,
		Snapshot:     &initial,
	}); err != nil {
		return managedPostgresResizeSnapshot{}, fmt.Errorf("record managed postgres resize preflight: %w", err)
	}
	if len(stages) == 0 {
		if err := hooks.Report(managedPostgresResizeExecutionEvent{
			EvidenceType: model.OperationEvidenceTypePostgresResizeVerified,
			State:        managedPostgresResizeStateNoop,
			Reason:       "already_current",
			Message:      "managed postgres resources already match the exact runtime target",
			Resources:    target,
			Snapshot:     &initial,
		}); err != nil {
			return managedPostgresResizeSnapshot{}, err
		}
		return initial, nil
	}

	cursor := current
	latest := initial
	for stageIndex, stage := range stages {
		if err := hooks.EnsureOwned(); err != nil {
			return managedPostgresResizeSnapshot{}, err
		}
		before, err := hooks.Observe(executionCtx)
		if err != nil {
			return managedPostgresResizeSnapshot{}, fmt.Errorf("observe managed postgres before resize stage %s: %w", stage.Name, err)
		}
		if err := validateManagedPostgresResizeInvariantBaseline(baseline, before.Cluster, before.Observation); err != nil {
			return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
				hooks,
				managedPostgresResizeExecutionEvent{
					EvidenceType: model.OperationEvidenceTypePostgresResizeFailed,
					State:        managedPostgresResizeStateFailed,
					Reason:       "invariant_changed_before_stage",
					Message:      err.Error(),
					StageName:    stage.Name,
					StageIndex:   stageIndex,
					Resources:    stage.Resources,
					Snapshot:     &before,
				},
				err,
			)
		}
		if before.Observation.ActualResources == nil ||
			!managedPostgresResizeResourcesEqual(managedPostgresCPUAndMemoryEnvelope(*before.Observation.ActualResources), cursor) ||
			!managedPostgresResizeResourcesEqual(managedPostgresCPUAndMemoryEnvelope(before.Observation.DesiredResources), cursor) {
			err := fmt.Errorf("managed postgres resources changed before stage %s", stage.Name)
			return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
				hooks,
				managedPostgresResizeExecutionEvent{
					EvidenceType: model.OperationEvidenceTypePostgresResizeFailed,
					State:        managedPostgresResizeStateFailed,
					Reason:       "resource_drift_before_stage",
					Message:      err.Error(),
					StageName:    stage.Name,
					StageIndex:   stageIndex,
					Resources:    stage.Resources,
					Snapshot:     &before,
				},
				err,
			)
		}
		if err := hooks.Report(managedPostgresResizeExecutionEvent{
			EvidenceType:     model.OperationEvidenceTypePostgresResizeApplying,
			State:            managedPostgresResizeStateReady,
			Reason:           "applying_stage",
			Message:          fmt.Sprintf("applying managed postgres resize stage %s", stage.Name),
			StageName:        stage.Name,
			StageIndex:       stageIndex,
			ChangedResources: stage.ChangedResources,
			Resources:        stage.Resources,
			Snapshot:         &before,
		}); err != nil {
			return managedPostgresResizeSnapshot{}, fmt.Errorf("record managed postgres resize stage %s intent: %w", stage.Name, err)
		}

		var acceptedObservation managedPostgresResizeObservation
		patchConflictReported := false
		for {
			acceptedObservation, err = hooks.Patch(executionCtx, before.Observation, stage.Resources)
			if err == nil {
				// A successful /resize response proves that the apiserver accepted
				// this mutation. Every later error, including controller context
				// cancellation, must retain the operation and its interlocks until
				// recovery proves the terminal Pod state.
				mutationAttempted = true
				break
			}
			if !errors.Is(err, errKubeConflict) {
				// Any non-409 transport failure is ambiguous: the apiserver may
				// have committed the resize before the response was lost.
				mutationAttempted = true
				return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
					hooks,
					managedPostgresResizeExecutionEvent{
						EvidenceType:     model.OperationEvidenceTypePostgresResizeFailed,
						State:            managedPostgresResizeStateFailed,
						Reason:           "resize_patch_failed",
						Message:          err.Error(),
						StageName:        stage.Name,
						StageIndex:       stageIndex,
						ChangedResources: stage.ChangedResources,
						Resources:        stage.Resources,
						Snapshot:         &before,
					},
					err,
				)
			}

			// Kubernetes rejects a resourceVersion precondition conflict before
			// applying the patch, so this one response is conclusively
			// non-mutating. Re-observe and retry only while the complete Pod and
			// database invariant remains identical; the operation timeout bounds
			// contention without guessing whether a different mutation occurred.
			if !patchConflictReported {
				if reportErr := hooks.Report(managedPostgresResizeExecutionEvent{
					EvidenceType:     model.OperationEvidenceTypePostgresResizeDeferred,
					State:            managedPostgresResizeStateDeferred,
					Reason:           "resource_version_conflict_retry",
					Message:          fmt.Sprintf("managed postgres resize stage %s will retry after a Kubernetes resourceVersion conflict", stage.Name),
					StageName:        stage.Name,
					StageIndex:       stageIndex,
					ChangedResources: stage.ChangedResources,
					Resources:        stage.Resources,
					Snapshot:         &before,
				}); reportErr != nil {
					return managedPostgresResizeSnapshot{}, fmt.Errorf("record managed postgres resize stage %s conflict: %w", stage.Name, reportErr)
				}
				patchConflictReported = true
			}
			if waitErr := hooks.Wait(executionCtx, interval); waitErr != nil {
				return managedPostgresResizeSnapshot{}, waitErr
			}
			if ownedErr := hooks.EnsureOwned(); ownedErr != nil {
				return managedPostgresResizeSnapshot{}, ownedErr
			}
			before, err = hooks.Observe(executionCtx)
			if err != nil {
				return managedPostgresResizeSnapshot{}, fmt.Errorf("re-observe managed postgres after resize stage %s conflict: %w", stage.Name, err)
			}
			if err := validateManagedPostgresResizeInvariantBaseline(baseline, before.Cluster, before.Observation); err != nil {
				return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
					hooks,
					managedPostgresResizeExecutionEvent{
						EvidenceType:     model.OperationEvidenceTypePostgresResizeBlocked,
						State:            managedPostgresResizeStateBlocked,
						Reason:           "invariant_changed_during_conflict_retry",
						Message:          err.Error(),
						StageName:        stage.Name,
						StageIndex:       stageIndex,
						ChangedResources: stage.ChangedResources,
						Resources:        stage.Resources,
						Snapshot:         &before,
					},
					err,
				)
			}
			if before.Observation.ActualResources == nil ||
				!managedPostgresResizeResourcesEqual(managedPostgresCPUAndMemoryEnvelope(*before.Observation.ActualResources), cursor) ||
				!managedPostgresResizeResourcesEqual(managedPostgresCPUAndMemoryEnvelope(before.Observation.DesiredResources), cursor) {
				resourceErr := fmt.Errorf("managed postgres resources changed during resize stage %s conflict retry", stage.Name)
				return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
					hooks,
					managedPostgresResizeExecutionEvent{
						EvidenceType:     model.OperationEvidenceTypePostgresResizeBlocked,
						State:            managedPostgresResizeStateBlocked,
						Reason:           "resource_drift_during_conflict_retry",
						Message:          resourceErr.Error(),
						StageName:        stage.Name,
						StageIndex:       stageIndex,
						ChangedResources: stage.ChangedResources,
						Resources:        stage.Resources,
						Snapshot:         &before,
					},
					resourceErr,
				)
			}
		}
		if !managedPostgresResizeResourcesEqual(
			managedPostgresCPUAndMemoryEnvelope(acceptedObservation.DesiredResources),
			stage.Resources,
		) {
			err := fmt.Errorf("managed postgres resize response did not preserve the exact stage target")
			return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
				hooks,
				managedPostgresResizeExecutionEvent{
					EvidenceType: model.OperationEvidenceTypePostgresResizeFailed,
					State:        managedPostgresResizeStateFailed,
					Reason:       "resize_response_target_drift",
					Message:      err.Error(),
					StageName:    stage.Name,
					StageIndex:   stageIndex,
					Resources:    stage.Resources,
					Snapshot:     &before,
				},
				err,
			)
		}
		if err := advanceManagedPostgresResizeInvariantBaseline(&baseline, before.Cluster, acceptedObservation); err != nil {
			return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
				hooks,
				managedPostgresResizeExecutionEvent{
					EvidenceType: model.OperationEvidenceTypePostgresResizeFailed,
					State:        managedPostgresResizeStateFailed,
					Reason:       "resize_response_invariant_failed",
					Message:      err.Error(),
					StageName:    stage.Name,
					StageIndex:   stageIndex,
					Resources:    stage.Resources,
					Snapshot:     &before,
				},
				err,
			)
		}
		accepted := managedPostgresResizeSnapshot{Cluster: before.Cluster, Observation: acceptedObservation}
		if err := hooks.Report(managedPostgresResizeExecutionEvent{
			EvidenceType:     model.OperationEvidenceTypePostgresResizeAccepted,
			State:            managedPostgresResizeStateInProgress,
			Reason:           "resize_patch_accepted",
			Message:          fmt.Sprintf("Kubernetes accepted managed postgres resize stage %s", stage.Name),
			StageName:        stage.Name,
			StageIndex:       stageIndex,
			ChangedResources: stage.ChangedResources,
			Resources:        stage.Resources,
			Snapshot:         &accepted,
		}); err != nil {
			return managedPostgresResizeSnapshot{}, fmt.Errorf("record accepted managed postgres resize stage %s: %w", stage.Name, err)
		}

		latest, err = waitForManagedPostgresResizeStage(
			executionCtx,
			baseline,
			stageIndex,
			stage,
			interval,
			hooks,
		)
		if err != nil {
			return managedPostgresResizeSnapshot{}, err
		}
		cursor = stage.Resources
	}

	if !managedPostgresResizeResourcesEqual(cursor, target) {
		return managedPostgresResizeSnapshot{}, fmt.Errorf("managed postgres resize execution did not reach the exact target")
	}
	return latest, nil
}

func waitForManagedPostgresResizeStage(
	ctx context.Context,
	baseline managedPostgresResizeInvariantBaseline,
	stageIndex int,
	stage managedPostgresResizePlanStage,
	interval time.Duration,
	hooks managedPostgresResizeExecutionHooks,
) (managedPostgresResizeSnapshot, error) {
	lastReportedState := ""
	lastReportedReason := ""
	for {
		if err := hooks.EnsureOwned(); err != nil {
			return managedPostgresResizeSnapshot{}, err
		}
		snapshot, err := hooks.Observe(ctx)
		if err != nil {
			return managedPostgresResizeSnapshot{}, fmt.Errorf("observe managed postgres resize stage %s: %w", stage.Name, err)
		}
		if err := validateManagedPostgresResizeInvariantDuringResize(baseline, snapshot.Cluster, snapshot.Observation); err != nil {
			return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
				hooks,
				managedPostgresResizeExecutionEvent{
					EvidenceType: model.OperationEvidenceTypePostgresResizeFailed,
					State:        managedPostgresResizeStateFailed,
					Reason:       "invariant_changed_during_stage",
					Message:      err.Error(),
					StageName:    stage.Name,
					StageIndex:   stageIndex,
					Resources:    stage.Resources,
					Snapshot:     &snapshot,
				},
				err,
			)
		}
		if !managedPostgresResizeResourcesEqual(
			managedPostgresCPUAndMemoryEnvelope(snapshot.Observation.DesiredResources),
			stage.Resources,
		) {
			err := fmt.Errorf("managed postgres desired resources drifted during stage %s", stage.Name)
			return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
				hooks,
				managedPostgresResizeExecutionEvent{
					EvidenceType: model.OperationEvidenceTypePostgresResizeFailed,
					State:        managedPostgresResizeStateFailed,
					Reason:       "desired_resources_drifted",
					Message:      err.Error(),
					StageName:    stage.Name,
					StageIndex:   stageIndex,
					Resources:    stage.Resources,
					Snapshot:     &snapshot,
				},
				err,
			)
		}

		state := managedPostgresResizeStateInProgress
		reason := "waiting_for_actual_resources"
		message := fmt.Sprintf("waiting for managed postgres resize stage %s actual resources", stage.Name)
		evidenceType := model.OperationEvidenceTypePostgresResizeInProgress
		if pending := resizePendingCondition(snapshot.Observation.Conditions); pending != nil {
			switch strings.ToLower(strings.TrimSpace(pending.Reason)) {
			case "deferred":
				state = managedPostgresResizeStateDeferred
				reason = "kubernetes_resize_deferred"
				evidenceType = model.OperationEvidenceTypePostgresResizeDeferred
			case "infeasible":
				state = managedPostgresResizeStateInfeasible
				reason = "kubernetes_resize_infeasible"
				evidenceType = model.OperationEvidenceTypePostgresResizeBlocked
			default:
				reason = "kubernetes_resize_in_progress"
			}
			if strings.TrimSpace(pending.Message) != "" {
				message = strings.TrimSpace(pending.Message)
			}
		}

		actualConverged := snapshot.Observation.ActualResources != nil &&
			managedPostgresResizeResourcesEqual(
				managedPostgresCPUAndMemoryEnvelope(*snapshot.Observation.ActualResources),
				stage.Resources,
			)
		if actualConverged && resizePendingCondition(snapshot.Observation.Conditions) == nil &&
			snapshot.Observation.ObservedGeneration >= snapshot.Observation.Generation {
			if err := validateManagedPostgresResizeInvariantBaseline(baseline, snapshot.Cluster, snapshot.Observation); err != nil {
				return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
					hooks,
					managedPostgresResizeExecutionEvent{
						EvidenceType: model.OperationEvidenceTypePostgresResizeFailed,
						State:        managedPostgresResizeStateFailed,
						Reason:       "terminal_invariant_failed",
						Message:      err.Error(),
						StageName:    stage.Name,
						StageIndex:   stageIndex,
						Resources:    stage.Resources,
						Snapshot:     &snapshot,
					},
					err,
				)
			}
			if err := hooks.Probe(ctx, snapshot); err != nil {
				return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
					hooks,
					managedPostgresResizeExecutionEvent{
						EvidenceType: model.OperationEvidenceTypePostgresResizeFailed,
						State:        managedPostgresResizeStateFailed,
						Reason:       "database_probe_failed",
						Message:      err.Error(),
						StageName:    stage.Name,
						StageIndex:   stageIndex,
						Resources:    stage.Resources,
						Snapshot:     &snapshot,
					},
					err,
				)
			}
			if err := hooks.Report(managedPostgresResizeExecutionEvent{
				EvidenceType:     model.OperationEvidenceTypePostgresResizeVerified,
				State:            managedPostgresResizeStateReady,
				Reason:           "stage_verified",
				Message:          fmt.Sprintf("managed postgres resize stage %s verified without restart", stage.Name),
				StageName:        stage.Name,
				StageIndex:       stageIndex,
				ChangedResources: stage.ChangedResources,
				Resources:        stage.Resources,
				Snapshot:         &snapshot,
			}); err != nil {
				return managedPostgresResizeSnapshot{}, err
			}
			return snapshot, nil
		}

		if state == managedPostgresResizeStateInfeasible {
			err := fmt.Errorf("managed postgres resize stage %s is infeasible: %s", stage.Name, message)
			return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
				hooks,
				managedPostgresResizeExecutionEvent{
					EvidenceType:     evidenceType,
					State:            state,
					Reason:           reason,
					Message:          err.Error(),
					StageName:        stage.Name,
					StageIndex:       stageIndex,
					ChangedResources: stage.ChangedResources,
					Resources:        stage.Resources,
					Snapshot:         &snapshot,
				},
				err,
			)
		}
		if state != lastReportedState || reason != lastReportedReason {
			if err := hooks.Report(managedPostgresResizeExecutionEvent{
				EvidenceType:     evidenceType,
				State:            state,
				Reason:           reason,
				Message:          message,
				StageName:        stage.Name,
				StageIndex:       stageIndex,
				ChangedResources: stage.ChangedResources,
				Resources:        stage.Resources,
				Snapshot:         &snapshot,
			}); err != nil {
				return managedPostgresResizeSnapshot{}, err
			}
			lastReportedState = state
			lastReportedReason = reason
		}
		if err := hooks.Wait(ctx, interval); err != nil {
			if errors.Is(err, context.Canceled) {
				return managedPostgresResizeSnapshot{}, err
			}
			waitErr := fmt.Errorf("wait for managed postgres resize stage %s: %w", stage.Name, err)
			return managedPostgresResizeSnapshot{}, managedPostgresResizeExecutionFailure(
				hooks,
				managedPostgresResizeExecutionEvent{
					EvidenceType:     model.OperationEvidenceTypePostgresResizeFailed,
					State:            state,
					Reason:           "resize_wait_ended",
					Message:          waitErr.Error(),
					StageName:        stage.Name,
					StageIndex:       stageIndex,
					ChangedResources: stage.ChangedResources,
					Resources:        stage.Resources,
					Snapshot:         &snapshot,
				},
				waitErr,
			)
		}
	}
}

func validateManagedPostgresResizePlanPolicy(
	observation managedPostgresResizeObservation,
	current kubeResourceRequirements,
	stages []managedPostgresResizePlanStage,
	gates config.ManagedPostgresInPlaceResizeConfig,
) error {
	cursor := current
	for _, stage := range stages {
		changed, increases, decreases := compareResizeResources(cursor, stage.Resources)
		if len(changed) == 0 {
			return fmt.Errorf("managed postgres resize stage %s does not change resources", stage.Name)
		}
		for _, resourceName := range changed {
			increase := containsResizeResource(increases, resourceName)
			if !increase && !containsResizeResource(decreases, resourceName) {
				return fmt.Errorf("managed postgres resize stage %s has unknown direction for %s", stage.Name, resourceName)
			}
			if allowed, reason := managedPostgresResizeDirectionGate(resourceName, increase, gates); !allowed {
				return fmt.Errorf("managed postgres resize stage %s blocked by %s", stage.Name, reason)
			}
		}
		simulated := observation
		simulated.DesiredResources = cloneKubeResourceRequirements(cursor)
		actual := cloneKubeResourceRequirements(cursor)
		simulated.ActualResources = &actual
		assessment := assessManagedPostgresResize(
			simulated,
			stage.Resources,
			managedPostgresResizeSafetyOptions{
				AllowRequestDownscale: hasRequestDownscale(decreases),
				AllowLimitChanges:     hasLimitChange(changed),
				BaselineRestartCount:  &observation.RestartCount,
			},
		)
		if assessment.State != managedPostgresResizeStateReady {
			return fmt.Errorf("managed postgres resize stage %s blocked by %s: %s", stage.Name, assessment.Reason, assessment.Message)
		}
		cursor = stage.Resources
	}
	return nil
}

func observeManagedPostgresResizeSnapshot(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName string,
) (managedPostgresResizeSnapshot, error) {
	cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, clusterName)
	if err != nil {
		return managedPostgresResizeSnapshot{}, fmt.Errorf("read cloudnativepg cluster %s/%s: %w", namespace, clusterName, err)
	}
	if !found {
		return managedPostgresResizeSnapshot{}, fmt.Errorf("cloudnativepg cluster %s/%s was not found", namespace, clusterName)
	}
	primaryPodName := strings.TrimSpace(cluster.Status.CurrentPrimary)
	if primaryPodName == "" {
		return managedPostgresResizeSnapshot{}, fmt.Errorf("cloudnativepg cluster %s/%s has no current primary", namespace, clusterName)
	}
	pod, found, err := client.getPodResizeState(ctx, namespace, primaryPodName)
	if err != nil {
		return managedPostgresResizeSnapshot{}, fmt.Errorf("read managed postgres primary Pod %s/%s: %w", namespace, primaryPodName, err)
	}
	if !found {
		return managedPostgresResizeSnapshot{}, fmt.Errorf("managed postgres primary Pod %s/%s was not found", namespace, primaryPodName)
	}
	observation, err := observeManagedPostgresResize(pod, managedPostgresMainContainerName)
	if err != nil {
		return managedPostgresResizeSnapshot{}, err
	}
	return managedPostgresResizeSnapshot{Cluster: cluster, Observation: observation}, nil
}

func managedPostgresKubeResources(resources model.ResourceSpec) (kubeResourceRequirements, error) {
	if resources.CPUMilliCores <= 0 || resources.MemoryMebibytes <= 0 ||
		resources.CPULimitMilliCores <= 0 || resources.MemoryLimitMebibytes <= 0 {
		return kubeResourceRequirements{}, fmt.Errorf("managed postgres resize requires explicit positive CPU and memory requests and limits")
	}
	target := kubeResourceRequirements{
		Requests: map[string]string{
			"cpu":    fmt.Sprintf("%dm", resources.CPUMilliCores),
			"memory": fmt.Sprintf("%dMi", resources.MemoryMebibytes),
		},
		Limits: map[string]string{
			"cpu":    fmt.Sprintf("%dm", resources.CPULimitMilliCores),
			"memory": fmt.Sprintf("%dMi", resources.MemoryLimitMebibytes),
		},
	}
	if err := validateCompleteManagedPostgresResizeEnvelope(target); err != nil {
		return kubeResourceRequirements{}, err
	}
	return target, nil
}

func probeManagedPostgresResizeDatabase(
	ctx context.Context,
	client *kubeClient,
	namespace string,
	postgres model.AppPostgresSpec,
	snapshot managedPostgresResizeSnapshot,
) error {
	if strings.TrimSpace(postgres.Database) == "" || strings.TrimSpace(postgres.User) == "" || postgres.Password == "" {
		return fmt.Errorf("managed postgres resize database credentials are incomplete")
	}
	podName := strings.TrimSpace(snapshot.Observation.PodName)
	podIP, found, err := client.getPodIP(ctx, namespace, podName)
	if err != nil {
		return fmt.Errorf("read managed postgres resize target Pod %s/%s IP: %w", namespace, podName, err)
	}
	if !found || podIP == "" {
		return fmt.Errorf("managed postgres resize target Pod %s/%s has no reachable IP", namespace, podName)
	}
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(probeCtx, managedPostgresResizeDatabaseURL(podIP, postgres))
	if err != nil {
		return fmt.Errorf("connect to managed postgres resize target Pod %s/%s: %w", namespace, podName, err)
	}
	defer closeManagedPostgresReplicationConnection(conn)

	var inRecovery bool
	var transactionReadOnly string
	if err := conn.QueryRow(probeCtx, `SELECT pg_is_in_recovery(), current_setting('transaction_read_only')`).Scan(&inRecovery, &transactionReadOnly); err != nil {
		return fmt.Errorf("read managed postgres resize target state from %s: %w", podName, err)
	}
	if inRecovery || !strings.EqualFold(strings.TrimSpace(transactionReadOnly), "off") {
		return fmt.Errorf("managed postgres resize target %s is not a writable primary", podName)
	}
	tx, err := conn.Begin(probeCtx)
	if err != nil {
		return fmt.Errorf("begin managed postgres resize write probe on %s: %w", podName, err)
	}
	rolledBack := false
	defer func() {
		if !rolledBack {
			rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer rollbackCancel()
			_ = tx.Rollback(rollbackCtx)
		}
	}()
	if _, err := tx.Exec(probeCtx, `CREATE TEMP TABLE fugue_in_place_resize_probe (value integer NOT NULL) ON COMMIT DROP`); err != nil {
		return fmt.Errorf("create managed postgres resize temporary write probe on %s: %w", podName, err)
	}
	if _, err := tx.Exec(probeCtx, `INSERT INTO fugue_in_place_resize_probe (value) VALUES (1)`); err != nil {
		return fmt.Errorf("write managed postgres resize temporary probe on %s: %w", podName, err)
	}
	var value int
	if err := tx.QueryRow(probeCtx, `SELECT value FROM fugue_in_place_resize_probe`).Scan(&value); err != nil {
		return fmt.Errorf("read managed postgres resize temporary probe on %s: %w", podName, err)
	}
	if value != 1 {
		return fmt.Errorf("managed postgres resize temporary probe on %s returned %d", podName, value)
	}
	if err := tx.Rollback(probeCtx); err != nil {
		return fmt.Errorf("rollback managed postgres resize temporary probe on %s: %w", podName, err)
	}
	rolledBack = true
	return nil
}

func managedPostgresResizeDatabaseURL(podIP string, postgres model.AppPostgresSpec) string {
	databaseURL := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(strings.TrimSpace(postgres.User), postgres.Password),
		Host:   net.JoinHostPort(strings.TrimSpace(podIP), "5432"),
		Path:   "/" + strings.TrimSpace(postgres.Database),
	}
	query := databaseURL.Query()
	query.Set("application_name", "fugue-controller-in-place-resize")
	query.Set("connect_timeout", "5")
	query.Set("sslmode", "disable")
	databaseURL.RawQuery = query.Encode()
	return databaseURL.String()
}

func waitManagedPostgresResizePollInterval(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func managedPostgresResizeExecutionFailure(
	hooks managedPostgresResizeExecutionHooks,
	event managedPostgresResizeExecutionEvent,
	executionErr error,
) error {
	if executionErr == nil {
		executionErr = errors.New(strings.TrimSpace(event.Message))
	}
	if errors.Is(executionErr, context.Canceled) {
		return executionErr
	}
	if hooks.Report != nil {
		if reportErr := hooks.Report(event); reportErr != nil {
			return errors.Join(executionErr, reportErr)
		}
	}
	return executionErr
}

func (s *Service) reportManagedPostgresResizeExecutionEvent(
	op model.Operation,
	app model.App,
	namespace, clusterName string,
	event managedPostgresResizeExecutionEvent,
) error {
	message := strings.TrimSpace(event.Message)
	if message == "" {
		message = strings.TrimSpace(event.State)
	}
	progress := "managed postgres resize"
	if stage := strings.TrimSpace(event.StageName); stage != "" {
		progress += " " + stage
	}
	if state := strings.TrimSpace(event.State); state != "" {
		progress += " " + state
	}
	if message != "" {
		progress += ": " + message
	}
	if _, err := s.Store.UpdateOperationProgress(op.ID, progress); err != nil {
		return err
	}

	stageIndex := event.StageIndex
	if strings.TrimSpace(event.StageName) == "" {
		stageIndex = -1
	}
	payload := map[string]any{
		"schema_version":    managedPostgresResizeEvidencePayloadVersion,
		"state":             strings.TrimSpace(event.State),
		"stage_index":       stageIndex,
		"stage_name":        strings.TrimSpace(event.StageName),
		"changed_resources": append([]string(nil), event.ChangedResources...),
		"resources": map[string]any{
			"requests": cloneKubeResourceStringMap(event.Resources.Requests),
			"limits":   cloneKubeResourceStringMap(event.Resources.Limits),
		},
	}
	evidence := model.OperationEvidence{
		TenantID:         app.TenantID,
		ProjectID:        app.ProjectID,
		AppID:            app.ID,
		OperationID:      op.ID,
		Type:             firstNonEmptyResizeString(event.EvidenceType, model.OperationEvidenceTypePostgresResizeInProgress),
		Source:           model.OperationEvidenceSourceKubernetesAPI,
		Severity:         managedPostgresResizeEvidenceSeverity(event.State),
		Confidence:       model.OperationEvidenceConfidenceConfirmed,
		SubjectKind:      "Pod",
		SubjectName:      strings.TrimSpace(clusterName),
		SubjectNamespace: strings.TrimSpace(namespace),
		Summary:          progress,
		Message:          message,
		Reason:           strings.TrimSpace(event.Reason),
		RedactionStatus:  model.OperationEvidenceRedactionNone,
		Payload:          payload,
		PayloadVersion:   managedPostgresResizeEvidencePayloadVersion,
	}
	if event.Snapshot != nil {
		observation := event.Snapshot.Observation
		evidence.SubjectName = observation.PodName
		evidence.SubjectNamespace = observation.Namespace
		evidence.SubjectUID = observation.PodUID
		evidence.PodName = observation.PodName
		evidence.NodeName = observation.NodeName
		evidence.ContainerName = observation.ContainerName
		payload["cluster_name"] = event.Snapshot.Cluster.Metadata.Name
		payload["cluster_uid"] = event.Snapshot.Cluster.Metadata.UID
		payload["cluster_generation"] = event.Snapshot.Cluster.Metadata.Generation
		payload["pod_uid"] = observation.PodUID
		payload["pod_generation"] = observation.Generation
		payload["pod_observed_generation"] = observation.ObservedGeneration
		payload["pod_resource_version"] = observation.ResourceVersion
		payload["restart_count"] = observation.RestartCount
		payload["container_started_at"] = observation.ContainerStartedAt
	}
	s.recordOperationEvidenceBestEffort(evidence)
	return nil
}

func managedPostgresResizeEvidenceSeverity(state string) string {
	switch strings.TrimSpace(state) {
	case managedPostgresResizeStateFailed, managedPostgresResizeStateInfeasible:
		return model.OperationEvidenceSeverityError
	case managedPostgresResizeStateBlocked, managedPostgresResizeStateDeferred:
		return model.OperationEvidenceSeverityWarning
	default:
		return model.OperationEvidenceSeverityInfo
	}
}

func firstNonEmptyResizeString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
