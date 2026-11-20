package controller

import (
	"context"
	"encoding/json"
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
	AcceptedBaseline  *managedPostgresResizeInvariantBaseline
	InspectCapability func(context.Context) (managedPostgresResizeCapability, error)
	Observe           func(context.Context) (managedPostgresResizeSnapshot, error)
	Patch             func(context.Context, managedPostgresResizeObservation, kubeResourceRequirements) (managedPostgresResizeObservation, error)
	Probe             func(context.Context, managedPostgresResizeSnapshot) error
	EnsureOwned       func() error
	BeforePatch       func(context.Context, managedPostgresResizeSnapshot, kubeResourceRequirements) error
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
) (executionErr error) {
	var acceptedBaseline *managedPostgresResizeInvariantBaseline
	var restoreIntent *managedPostgresResizeInvariantBaseline
	restoring := strings.HasPrefix(op.RequestedByID, postgresRestoreRuntimeActor+"/")
	var restoreIntentEvidence model.OperationEvidence
	// An accepted automatic mutation remains owned across process/leader loss.
	// A transient preflight/read failure on replay cannot prove it was undone.
	if restoring || strings.HasPrefix(op.RequestedByID, managedPostgresAutoMemoryActor+"/") {
		accepted, err := s.Store.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: op.TenantID, OperationID: op.ID, Types: []string{model.OperationEvidenceTypePostgresResizeAccepted}, Limit: 1})
		if err != nil {
			return fmt.Errorf("%w: read accepted automatic resize evidence: %v", errManagedPostgresResizeUnsettled, err)
		}
		if restoring && len(accepted) == 0 {
			intent, err := s.Store.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: op.TenantID, OperationID: op.ID, Types: []string{model.OperationEvidenceTypePostgresResizeApplying}, Limit: 1})
			if err != nil {
				return fmt.Errorf("%w: read runtime restoration intent: %v", errManagedPostgresResizeUnsettled, err)
			}
			if len(intent) > 0 {
				restoreIntentEvidence = intent[0]
			}
		}
		if len(accepted) > 0 || restoreIntentEvidence.ID != "" {
			defer func() {
				if executionErr != nil && !errors.Is(executionErr, errOperationNoLongerActive) && !errors.Is(executionErr, errManagedPostgresResizeUnsettled) {
					executionErr = fmt.Errorf("%w: %v", errManagedPostgresResizeUnsettled, executionErr)
				}
			}()
			if len(accepted) > 0 {
				baseline, err := managedPostgresResizeAcceptedBaseline(accepted[0])
				if err != nil {
					return err
				}
				acceptedBaseline = &baseline
			} else {
				baseline, err := managedPostgresResizeEvidenceBaseline(restoreIntentEvidence, model.OperationEvidenceTypePostgresResizeApplying)
				if err != nil {
					return err
				}
				restoreIntent = &baseline
			}
		}
	}
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
	if target == nil || target.Service == nil || strings.TrimSpace(target.ServiceID) == "" ||
		(target.Service.OwnerAppID != "" && !target.AppOwned) {
		return s.blockManagedPostgresResizeOperation(op, app, "unowned_database", "managed postgres resize requires one owned or exclusively bound backing service")
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
	gates := s.Config.ManagedPostgresInPlaceResize
	enrollment := managedPostgresCPUEnrollment(gates, target.ServiceID, namespace, clusterName)
	automaticCPU := strings.HasPrefix(op.RequestedByID, managedPostgresAutoCPUActor+"/")
	automaticMemory := strings.HasPrefix(op.RequestedByID, managedPostgresAutoMemoryActor+"/")
	if automaticMemory && (op.RequestedByType != model.ActorTypeSystem || !gates.AutoMemoryRequestResizeEnabled) {
		return s.blockManagedPostgresResizeOperation(op, app, "automatic_memory_policy_disabled", "automatic PostgreSQL memory request resize requires the enabled database policy and a system actor")
	}
	if automaticCPU &&
		(enrollment == nil || op.RequestedByID != managedPostgresCPUActor(*enrollment)) {
		return s.blockManagedPostgresResizeOperation(op, app, "automatic_cpu_enrollment_changed", "automatic PostgreSQL CPU request enrollment no longer matches the queued database identity")
	}
	if enrollment != nil {
		gates.CPURequestDownscaleEnabled = true
	}
	var restoreBootstrap kubeResourceRequirements
	if restoring {
		if op.RequestedByType != model.ActorTypeSystem || !gates.RecoveryEnabled || target.Postgres.RuntimeResources == nil ||
			*target.Postgres.RuntimeResources != *op.DesiredSpec.Postgres.RuntimeResources {
			return fmt.Errorf("PostgreSQL runtime restoration requires recovery and the exact persisted target")
		}
		snapshot, err := observeManagedPostgresResizeSnapshot(ctx, client, namespace, clusterName)
		if err != nil {
			return err
		}
		if !postgresRuntimeRestoreMatches(snapshot, op.RequestedByID) {
			return fmt.Errorf("PostgreSQL runtime restoration database or replacement Pod identity changed")
		}
		restoreBootstrap, _, err = postgresRuntimeRestoreBaseline(target.ServiceID, target.Postgres, snapshot)
		if err != nil {
			return err
		}
		prior, err := s.postgresRuntimeRestoreProof(app, target.ServiceID, target.Postgres, snapshot)
		if err != nil {
			return err
		}
		if op.RequestedByID != postgresRuntimeRestoreActor(snapshot, prior.ID) {
			return fmt.Errorf("PostgreSQL runtime restoration source resize changed")
		}
		if restoreIntent != nil {
			if !postgresRuntimeRestoreAtBootstrap(snapshot, restoreBootstrap) {
				if err := validatePostgresRestoreAcceptedIntent(restoreIntent, restoreIntentEvidence, snapshot, targetResources); err != nil {
					return err
				}
				acceptedBaseline = restoreIntent
			} else if err := validateManagedPostgresResizeInvariantBaseline(*restoreIntent, snapshot.Cluster, snapshot.Observation); err != nil {
				return err
			}
		}
		if acceptedBaseline == nil && !postgresRuntimeRestoreAtBootstrap(snapshot, restoreBootstrap) {
			return fmt.Errorf("PostgreSQL runtime restoration source no longer matches the bootstrap template")
		}
	}
	restoreApplying := acceptedBaseline != nil
	hooks := managedPostgresResizeExecutionHooks{
		AcceptedBaseline: acceptedBaseline,
		InspectCapability: func(callCtx context.Context) (managedPostgresResizeCapability, error) {
			return client.inspectManagedPostgresResizeCapability(callCtx, namespace)
		},
		Observe: func(callCtx context.Context) (managedPostgresResizeSnapshot, error) {
			snapshot, err := observeManagedPostgresResizeSnapshot(callCtx, client, namespace, clusterName)
			if err == nil && enrollment != nil && !managedPostgresCPUEnrollmentMatches(*enrollment, snapshot) {
				return managedPostgresResizeSnapshot{}, fmt.Errorf("PostgreSQL CPU request enrollment identity changed")
			}
			if err == nil && automaticCPU {
				err = validateManagedPostgresAutomaticCPUTarget(snapshot.Observation.DesiredResources, targetResources)
			}
			if err == nil && automaticMemory {
				err = validateManagedPostgresAutomaticMemoryTarget(op.RequestedByID, snapshot, targetResources)
				if err == nil {
					if _, readErr := managedPostgresLiveResourceSpec(snapshot.Observation.DesiredResources); readErr != nil {
						return managedPostgresResizeSnapshot{}, readErr
					}
					if op.CreatedAt.Before(time.Now().UTC().Add(-15*time.Minute)) && !resizeQuantitiesEqual(snapshot.Observation.DesiredResources.Requests["memory"], targetResources.Requests["memory"]) {
						return managedPostgresResizeSnapshot{}, fmt.Errorf("automatic PostgreSQL memory request recommendation expired before mutation")
					}
					node, found, nodeErr := client.getNode(callCtx, snapshot.Observation.NodeName)
					if nodeErr != nil {
						return managedPostgresResizeSnapshot{}, nodeErr
					}
					// Keep the downscale pressure guard active after our own patch,
					// when the desired memory request already equals the target.
					source := automaticPostgresMemorySource(op.RequestedByID)
					err = validateManagedPostgresAutoMemoryNode(node, found, op.DesiredSpec.Postgres.RuntimeResources.MemoryMebibytes < source)
				}
			}
			if err == nil && restoring {
				_, _, err = postgresRuntimeRestoreBaseline(target.ServiceID, target.Postgres, snapshot)
				if err == nil && !postgresRuntimeRestoreMatches(snapshot, op.RequestedByID) {
					err = fmt.Errorf("PostgreSQL runtime restoration database or replacement Pod identity changed")
				}
				if err == nil && !restoreApplying && !postgresRuntimeRestoreAtBootstrap(snapshot, restoreBootstrap) {
					err = fmt.Errorf("PostgreSQL runtime restoration source no longer matches the bootstrap template")
				}
			}
			return snapshot, err
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
			restoreApplying = true
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
	if restoring {
		hooks.BeforePatch = func(callCtx context.Context, snapshot managedPostgresResizeSnapshot, resources kubeResourceRequirements) error {
			if !managedPostgresResizeResourcesEqual(resources, targetResources) {
				return fmt.Errorf("PostgreSQL runtime restoration requires the exact target in one stage")
			}
			return s.postgresRuntimeRestoreRequestPolicy(callCtx, client, app, target.ServiceID, snapshot, restoreBootstrap, targetResources, gates)
		}
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
		gates,
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
	var completed model.Operation
	if restoring {
		completed, err = s.Store.CompleteManagedOperationGuarded(op, &app, "", message+" to restore the verified runtime target", nil, nil, nil)
	} else {
		completed, err = s.Store.CompleteManagedOperation(op.ID, "", message)
	}
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
	mutationAttempted := hooks.AcceptedBaseline != nil
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
	// A replay must not reject (or re-PATCH) its own accepted resize merely
	// because kubelet is still Deferred. Observe it under the same immutable
	// identity fence and retain the interlock until actual resources converge.
	if hooks.AcceptedBaseline != nil {
		if !gates.RecoveryEnabled || !managedPostgresResizeResourcesEqual(managedPostgresCPUAndMemoryEnvelope(initial.Observation.DesiredResources), target) {
			return managedPostgresResizeSnapshot{}, fmt.Errorf("managed postgres accepted resize requires recovery with the exact live desired target")
		}
		baseline := *hooks.AcceptedBaseline
		if err := validateManagedPostgresResizeInvariantDuringResize(baseline, initial.Cluster, initial.Observation); err != nil {
			return managedPostgresResizeSnapshot{}, err
		}
		if err := hooks.Probe(executionCtx, initial); err != nil {
			return managedPostgresResizeSnapshot{}, err
		}
		if err := hooks.Report(managedPostgresResizeExecutionEvent{EvidenceType: model.OperationEvidenceTypePostgresResizeInProgress, State: managedPostgresResizeStateInProgress, Reason: "recovering_accepted_resize", Message: "observing an already accepted resize without replaying its mutation", Resources: target, Snapshot: &initial}); err != nil {
			return managedPostgresResizeSnapshot{}, err
		}
		return waitForManagedPostgresResizeStage(executionCtx, baseline, 0, managedPostgresResizePlanStage{Name: "accepted_resize_recovery", Resources: target}, interval, hooks)
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
		var acceptedObservation managedPostgresResizeObservation
		patchConflictReported := false
		intentReported := false
		for {
			if hooks.BeforePatch != nil {
				if err := hooks.BeforePatch(executionCtx, before, stage.Resources); err != nil {
					return managedPostgresResizeSnapshot{}, err
				}
			}
			if !intentReported {
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
				intentReported = true
			}
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

// Restore the immutable fence from durable evidence, never from a fresh live
// baseline that could legitimize an unrelated replacement or configuration edit.
func managedPostgresResizeAcceptedBaseline(evidence model.OperationEvidence) (managedPostgresResizeInvariantBaseline, error) {
	return managedPostgresResizeEvidenceBaseline(evidence, model.OperationEvidenceTypePostgresResizeAccepted)
}

func managedPostgresResizeEvidenceBaseline(evidence model.OperationEvidence, evidenceType string) (managedPostgresResizeInvariantBaseline, error) {
	var payload struct {
		ClusterName        string `json:"cluster_name"`
		ClusterUID         string `json:"cluster_uid"`
		ClusterGeneration  int64  `json:"cluster_generation"`
		PodUID             string `json:"pod_uid"`
		PodGeneration      int64  `json:"pod_generation"`
		RestartCount       int    `json:"restart_count"`
		ContainerStartedAt string `json:"container_started_at"`
	}
	encoded, err := json.Marshal(evidence.Payload)
	if err != nil {
		return managedPostgresResizeInvariantBaseline{}, err
	}
	if err := json.Unmarshal(encoded, &payload); err != nil {
		return managedPostgresResizeInvariantBaseline{}, err
	}
	if evidence.Type != evidenceType || evidence.PayloadVersion != managedPostgresResizeEvidencePayloadVersion ||
		evidence.SubjectNamespace == "" || evidence.PodName == "" || evidence.NodeName == "" || evidence.ContainerName == "" ||
		payload.ClusterName == "" || payload.ClusterUID == "" || payload.ClusterGeneration <= 0 ||
		payload.PodUID == "" || payload.PodUID != evidence.SubjectUID || payload.PodGeneration <= 0 || payload.ContainerStartedAt == "" {
		return managedPostgresResizeInvariantBaseline{}, fmt.Errorf("accepted managed postgres resize evidence lacks an exact immutable identity")
	}
	return managedPostgresResizeInvariantBaseline{
		Namespace: evidence.SubjectNamespace, ClusterName: payload.ClusterName, ClusterUID: payload.ClusterUID,
		ClusterGeneration: payload.ClusterGeneration, PrimaryPodName: evidence.PodName, PodUID: payload.PodUID,
		PodGeneration: payload.PodGeneration, NodeName: evidence.NodeName, ContainerName: evidence.ContainerName,
		RestartCount: payload.RestartCount, ContainerStartedAt: payload.ContainerStartedAt,
	}, nil
}

// A lost PATCH response may leave only the durable pre-mutation intent.
// Promote it to an accepted baseline only for the one expected generation
// increment and exact target. The executor will observe without another PATCH.
func validatePostgresRestoreAcceptedIntent(baseline *managedPostgresResizeInvariantBaseline, intent model.OperationEvidence, snapshot managedPostgresResizeSnapshot, target kubeResourceRequirements) error {
	var payload struct {
		SystemID  string                   `json:"system_id"`
		Resources kubeResourceRequirements `json:"resources"`
	}
	raw, err := json.Marshal(intent.Payload)
	if err != nil || json.Unmarshal(raw, &payload) != nil ||
		payload.SystemID == "" || payload.SystemID != snapshot.Cluster.Status.SystemID ||
		!managedPostgresResizeResourcesEqual(payload.Resources, target) ||
		!managedPostgresResizeResourcesEqual(snapshot.Observation.DesiredResources, target) {
		return fmt.Errorf("PostgreSQL runtime restoration mutation outcome is unknown; live state does not prove the recorded intent")
	}
	return advanceManagedPostgresResizeInvariantBaseline(baseline, snapshot.Cluster, snapshot.Observation)
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
		resources.CPULimitMilliCores < 0 || resources.MemoryLimitMebibytes <= 0 {
		return kubeResourceRequirements{}, fmt.Errorf("managed postgres resize requires explicit positive requests and memory limit, with zero CPU limit preserving an existing unlimited allowance")
	}
	target := kubeResourceRequirements{
		Requests: map[string]string{
			"cpu":    fmt.Sprintf("%dm", resources.CPUMilliCores),
			"memory": fmt.Sprintf("%dMi", resources.MemoryMebibytes),
		},
		Limits: map[string]string{
			"memory": fmt.Sprintf("%dMi", resources.MemoryLimitMebibytes),
		},
	}
	if resources.CPULimitMilliCores > 0 {
		target.Limits["cpu"] = fmt.Sprintf("%dm", resources.CPULimitMilliCores)
	}
	if err := validateCurrentManagedPostgresResizeEnvelope(target); err != nil {
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
		payload["system_id"] = event.Snapshot.Cluster.Status.SystemID
		payload["cluster_generation"] = event.Snapshot.Cluster.Metadata.Generation
		payload["pod_uid"] = observation.PodUID
		payload["pod_generation"] = observation.Generation
		payload["pod_observed_generation"] = observation.ObservedGeneration
		payload["pod_resource_version"] = observation.ResourceVersion
		payload["restart_count"] = observation.RestartCount
		payload["container_started_at"] = observation.ContainerStartedAt
	}
	if strings.HasPrefix(op.RequestedByID, postgresRestoreRuntimeActor+"/") &&
		(evidence.Type == model.OperationEvidenceTypePostgresResizeApplying || evidence.Type == model.OperationEvidenceTypePostgresResizeAccepted || evidence.Type == model.OperationEvidenceTypePostgresResizeVerified) {
		_, err := s.Store.RecordOperationEvidence(evidence)
		return err
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
