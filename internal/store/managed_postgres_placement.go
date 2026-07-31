package store

import (
	"net"
	"strings"
	"time"

	"fugue/internal/model"
)

// ManagedPostgresPlacementWitness is the indivisible observation that binds a
// PostgreSQL primary Pod to both its node/runtime placement and the exact
// Fugue app/backing-service identity that owns the desired state mutation.
// PrimaryPod and PodIP are deliberately retained even though only NodeName and
// RuntimeID are persisted: callers must prove that all four physical identity
// fields came from the same observation before the store accepts a write.
type ManagedPostgresPlacementWitness struct {
	AppID       string
	TenantID    string
	ProjectID   string
	ServiceID   string
	ServiceName string
	RuntimeID   string
	NodeName    string
	PrimaryPod  string
	PodIP       string
}

// ManagedPostgresPlacementState is the complete placement-control slice of a
// managed PostgreSQL spec. Keeping the expected and desired slices explicit
// prevents a stale observer from overwriting unrelated database settings.
type ManagedPostgresPlacementState struct {
	RuntimeID                        string
	FailoverTargetRuntimeID          string
	PrimaryNodeName                  string
	Instances                        int
	SynchronousReplicas              int
	PrimaryPlacementPendingRebalance bool
}

type ManagedPostgresPlacementMutation struct {
	Witness  ManagedPostgresPlacementWitness
	Expected ManagedPostgresPlacementState
	Desired  ManagedPostgresPlacementState
}

func ManagedPostgresPlacementStateFromSpec(spec model.AppPostgresSpec) ManagedPostgresPlacementState {
	return normalizeManagedPostgresPlacementState(ManagedPostgresPlacementState{
		RuntimeID:                        spec.RuntimeID,
		FailoverTargetRuntimeID:          spec.FailoverTargetRuntimeID,
		PrimaryNodeName:                  spec.PrimaryNodeName,
		Instances:                        spec.Instances,
		SynchronousReplicas:              spec.SynchronousReplicas,
		PrimaryPlacementPendingRebalance: spec.PrimaryPlacementPendingRebalance,
	})
}

func normalizeManagedPostgresPlacementState(state ManagedPostgresPlacementState) ManagedPostgresPlacementState {
	state.RuntimeID = strings.TrimSpace(state.RuntimeID)
	state.FailoverTargetRuntimeID = strings.TrimSpace(state.FailoverTargetRuntimeID)
	state.PrimaryNodeName = strings.TrimSpace(state.PrimaryNodeName)
	return state
}

func normalizeManagedPostgresPlacementMutation(mutation ManagedPostgresPlacementMutation) (ManagedPostgresPlacementMutation, error) {
	witness := &mutation.Witness
	witness.AppID = strings.TrimSpace(witness.AppID)
	witness.TenantID = strings.TrimSpace(witness.TenantID)
	witness.ProjectID = strings.TrimSpace(witness.ProjectID)
	witness.ServiceID = strings.TrimSpace(witness.ServiceID)
	witness.ServiceName = model.NormalizePostgresServiceName(witness.ServiceName, "")
	witness.RuntimeID = strings.TrimSpace(witness.RuntimeID)
	witness.NodeName = strings.TrimSpace(witness.NodeName)
	witness.PrimaryPod = strings.TrimSpace(witness.PrimaryPod)
	witness.PodIP = strings.TrimSpace(witness.PodIP)
	mutation.Expected = normalizeManagedPostgresPlacementState(mutation.Expected)
	mutation.Desired = normalizeManagedPostgresPlacementState(mutation.Desired)

	if witness.AppID == "" || witness.TenantID == "" || witness.ProjectID == "" ||
		witness.ServiceName == "" || witness.RuntimeID == "" || witness.NodeName == "" ||
		witness.PrimaryPod == "" || witness.PodIP == "" || net.ParseIP(witness.PodIP) == nil ||
		mutation.Expected.RuntimeID == "" || mutation.Desired.RuntimeID == "" ||
		mutation.Desired.RuntimeID != witness.RuntimeID ||
		mutation.Desired.PrimaryNodeName != witness.NodeName ||
		mutation.Desired.Instances < 1 || mutation.Desired.SynchronousReplicas < 0 ||
		mutation.Desired.SynchronousReplicas >= mutation.Desired.Instances {
		return ManagedPostgresPlacementMutation{}, ErrInvalidInput
	}
	return mutation, nil
}

func managedPostgresPlacementStateEqual(left, right ManagedPostgresPlacementState) bool {
	left = normalizeManagedPostgresPlacementState(left)
	right = normalizeManagedPostgresPlacementState(right)
	return left == right
}

func managedPostgresIdlePlacementConsumesFailover(
	expected, desired ManagedPostgresPlacementState,
) (bool, error) {
	expected = normalizeManagedPostgresPlacementState(expected)
	desired = normalizeManagedPostgresPlacementState(desired)
	if expected.RuntimeID == desired.RuntimeID {
		if expected.FailoverTargetRuntimeID != desired.FailoverTargetRuntimeID ||
			expected.Instances != desired.Instances ||
			expected.SynchronousReplicas != desired.SynchronousReplicas ||
			expected.PrimaryPlacementPendingRebalance != desired.PrimaryPlacementPendingRebalance {
			return false, ErrConflict
		}
		return false, nil
	}
	if expected.FailoverTargetRuntimeID == "" ||
		expected.FailoverTargetRuntimeID != desired.RuntimeID ||
		desired.FailoverTargetRuntimeID != "" ||
		desired.Instances != 1 || desired.SynchronousReplicas != 0 ||
		desired.PrimaryPlacementPendingRebalance {
		return false, ErrConflict
	}
	return true, nil
}

func managedPostgresFailoverSourceUnavailableInState(state *model.State, runtimeID string) bool {
	runtimeIndex := findRuntime(state, strings.TrimSpace(runtimeID))
	if runtimeIndex < 0 {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(state.Runtimes[runtimeIndex].Status), model.RuntimeStatusOffline)
}

func applyManagedPostgresPlacementState(spec *model.AppPostgresSpec, state ManagedPostgresPlacementState) {
	if spec == nil {
		return
	}
	state = normalizeManagedPostgresPlacementState(state)
	spec.RuntimeID = state.RuntimeID
	spec.FailoverTargetRuntimeID = state.FailoverTargetRuntimeID
	spec.PrimaryNodeName = state.PrimaryNodeName
	spec.Instances = state.Instances
	spec.SynchronousReplicas = state.SynchronousReplicas
	spec.PrimaryPlacementPendingRebalance = state.PrimaryPlacementPendingRebalance
}

type managedPostgresPlacementPersistTarget struct {
	appIndex     int
	serviceIndex int
	postgres     model.AppPostgresSpec
}

func validateManagedPostgresPlacementIdentity(
	app model.App,
	target *ManagedPostgresOperationTarget,
	witness ManagedPostgresPlacementWitness,
	bindingCount int,
) error {
	if strings.TrimSpace(app.ID) != witness.AppID ||
		strings.TrimSpace(app.TenantID) != witness.TenantID ||
		strings.TrimSpace(app.ProjectID) != witness.ProjectID {
		return ErrNotFound
	}
	if target == nil || strings.TrimSpace(target.ServiceID) != witness.ServiceID {
		return ErrNotFound
	}
	if model.NormalizePostgresServiceName(target.Postgres.ServiceName, "") != witness.ServiceName {
		return ErrConflict
	}

	if witness.ServiceID == "" {
		if target.Service != nil || app.Spec.Postgres == nil {
			return ErrNotFound
		}
		return nil
	}
	if target.Service == nil || strings.TrimSpace(target.Service.ID) != witness.ServiceID ||
		strings.TrimSpace(target.Service.TenantID) != witness.TenantID ||
		strings.TrimSpace(target.Service.ProjectID) != witness.ProjectID ||
		!isManagedPostgresService(*target.Service) || target.Service.Spec.Postgres == nil ||
		isDeletedBackingService(*target.Service) {
		return ErrNotFound
	}
	if !appHasBindingToServiceID(app, witness.ServiceID) || bindingCount != 1 {
		return ErrConflict
	}
	bindingFound := false
	for _, binding := range app.Bindings {
		if strings.TrimSpace(binding.ServiceID) != witness.ServiceID {
			continue
		}
		if strings.TrimSpace(binding.AppID) != witness.AppID ||
			strings.TrimSpace(binding.TenantID) != witness.TenantID {
			return ErrNotFound
		}
		bindingFound = true
		break
	}
	if !bindingFound {
		return ErrNotFound
	}

	ownerAppID := strings.TrimSpace(target.Service.OwnerAppID)
	switch ownerAppID {
	case witness.AppID:
		return nil
	case "":
		return nil
	default:
		return ErrNotFound
	}
}

func managedPostgresPlacementTargetInState(
	state *model.State,
	witness ManagedPostgresPlacementWitness,
) (managedPostgresPlacementPersistTarget, error) {
	appIndex := findApp(state, witness.AppID)
	if appIndex < 0 || isDeletedApp(state.Apps[appIndex]) {
		return managedPostgresPlacementPersistTarget{}, ErrNotFound
	}
	app := state.Apps[appIndex]
	hydrateAppBackingServices(state, &app)
	target, err := ManagedPostgresOperationTargetForApp(app, witness.ServiceID)
	if err != nil {
		return managedPostgresPlacementPersistTarget{}, err
	}

	bindingCount := 0
	serviceIndex := -1
	if witness.ServiceID != "" {
		serviceIndex = findBackingService(state, witness.ServiceID)
		if serviceIndex < 0 {
			return managedPostgresPlacementPersistTarget{}, ErrNotFound
		}
		for _, binding := range state.ServiceBindings {
			if strings.TrimSpace(binding.ServiceID) == witness.ServiceID {
				bindingCount++
			}
		}
	}
	if err := validateManagedPostgresPlacementIdentity(app, target, witness, bindingCount); err != nil {
		return managedPostgresPlacementPersistTarget{}, err
	}

	var postgres *model.AppPostgresSpec
	if serviceIndex >= 0 {
		postgres = state.BackingServices[serviceIndex].Spec.Postgres
	} else {
		postgres = state.Apps[appIndex].Spec.Postgres
	}
	if postgres == nil {
		return managedPostgresPlacementPersistTarget{}, ErrNotFound
	}
	return managedPostgresPlacementPersistTarget{
		appIndex: appIndex, serviceIndex: serviceIndex, postgres: *model.CloneAppPostgresSpec(postgres),
	}, nil
}

func activeOperationsForManagedPostgresPlacementTarget(
	operations []model.Operation,
	appID, serviceID string,
) []model.Operation {
	if strings.TrimSpace(serviceID) != "" {
		return activeOperationsForLifecycleTarget(operations, appID, serviceID)
	}
	active := make([]model.Operation, 0, 1)
	for _, op := range operations {
		if strings.TrimSpace(op.AppID) == strings.TrimSpace(appID) && isActiveOperationStatus(op.Status) {
			active = append(active, op)
		}
	}
	sortActiveOperations(active)
	return active
}

func applyManagedPostgresPlacementStateInState(
	state *model.State,
	target managedPostgresPlacementPersistTarget,
	desired ManagedPostgresPlacementState,
	now time.Time,
) {
	postgres := *model.CloneAppPostgresSpec(&target.postgres)
	applyManagedPostgresPlacementState(&postgres, desired)
	if target.serviceIndex >= 0 {
		service := cloneBackingService(state.BackingServices[target.serviceIndex])
		if strings.TrimSpace(target.postgres.RuntimeID) != strings.TrimSpace(postgres.RuntimeID) {
			service.CurrentRuntimeStartedAt = nil
			service.CurrentRuntimeReadyAt = nil
		}
		service.Spec.Postgres = &postgres
		service.UpdatedAt = now
		state.BackingServices[target.serviceIndex] = service
	} else {
		state.Apps[target.appIndex].Spec.Postgres = &postgres
	}
	state.Apps[target.appIndex].UpdatedAt = now
}

// SyncObservedManagedPostgresPlacement persists an idle placement correction.
// The no-active-operation check and exact app/service/snapshot validation live
// in the same store critical section as the write, closing the controller-side
// check/use race for both app-owned and independently bound databases.
func (s *Store) SyncObservedManagedPostgresPlacement(mutation ManagedPostgresPlacementMutation) (model.App, error) {
	mutation, err := normalizeManagedPostgresPlacementMutation(mutation)
	if err != nil {
		return model.App{}, err
	}
	if s.usingDatabase() {
		return s.pgSyncObservedManagedPostgresPlacement(mutation)
	}

	var app model.App
	err = s.withLockedState(true, func(state *model.State) error {
		target, err := managedPostgresPlacementTargetInState(state, mutation.Witness)
		if err != nil {
			return err
		}
		if active := activeOperationsForManagedPostgresPlacementTarget(
			state.Operations, mutation.Witness.AppID, mutation.Witness.ServiceID,
		); len(active) != 0 {
			return ErrConflict
		}
		if !managedPostgresPlacementStateEqual(
			ManagedPostgresPlacementStateFromSpec(target.postgres), mutation.Expected,
		) {
			return ErrConflict
		}
		consumeFailover, err := managedPostgresIdlePlacementConsumesFailover(mutation.Expected, mutation.Desired)
		if err != nil {
			return err
		}
		if consumeFailover && !managedPostgresFailoverSourceUnavailableInState(state, mutation.Expected.RuntimeID) {
			return ErrConflict
		}

		applyManagedPostgresPlacementStateInState(state, target, mutation.Desired, time.Now().UTC())
		app = state.Apps[target.appIndex]
		normalizeAppStatusForRead(&app)
		hydrateAppBackingServices(state, &app)
		return nil
	})
	return app, err
}

func validateManagedPostgresSwitchoverPlacementOperation(
	op model.Operation,
	mutation ManagedPostgresPlacementMutation,
) error {
	if op.Type != model.OperationTypeDatabaseSwitchover || !operationCanTransitionToCompleted(op) {
		return ErrConflict
	}
	if strings.TrimSpace(op.AppID) != mutation.Witness.AppID ||
		strings.TrimSpace(op.TenantID) != mutation.Witness.TenantID ||
		(strings.TrimSpace(op.ServiceID) != "" && strings.TrimSpace(op.ServiceID) != mutation.Witness.ServiceID) {
		return ErrNotFound
	}
	sourceRuntimeID := strings.TrimSpace(op.SourceRuntimeID)
	targetRuntimeID := strings.TrimSpace(op.TargetRuntimeID)
	if sourceRuntimeID == "" || targetRuntimeID == "" || sourceRuntimeID == targetRuntimeID ||
		mutation.Expected.RuntimeID != sourceRuntimeID ||
		mutation.Desired.RuntimeID != targetRuntimeID ||
		mutation.Witness.RuntimeID != targetRuntimeID ||
		mutation.Desired.FailoverTargetRuntimeID != sourceRuntimeID {
		return ErrConflict
	}
	return nil
}

func completeManagedPostgresSwitchoverOperationModel(
	op *model.Operation,
	app *model.App,
	manifestPath, message string,
	now time.Time,
) error {
	completionSpec := cloneAppSpec(&app.Spec)
	if completionSpec == nil {
		return ErrInvalidInput
	}
	op.DesiredSpec = completionSpec
	op.Status = model.OperationStatusCompleted
	op.UpdatedAt = now
	op.CompletedAt = &now
	op.ManifestPath = manifestPath
	op.ResultMessage = strings.TrimSpace(message)
	if op.StartedAt == nil {
		op.StartedAt = &now
	}
	return applyOperationToAppModel(app, op)
}

// CompleteManagedPostgresSwitchoverWithPlacement is the only switchover
// completion path that may persist RuntimeID/FailoverTargetRuntimeID/
// PrimaryNodeName. It rechecks the sole active operation, exact target, stale
// snapshot, and SQL-ready placement witness in the same transaction as both
// the desired-state write and the Completed transition.
func (s *Store) CompleteManagedPostgresSwitchoverWithPlacement(
	id, manifestPath, message string,
	mutation ManagedPostgresPlacementMutation,
) (model.Operation, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.Operation{}, ErrInvalidInput
	}
	mutation, err := normalizeManagedPostgresPlacementMutation(mutation)
	if err != nil {
		return model.Operation{}, err
	}
	if s.usingDatabase() {
		return s.pgCompleteManagedPostgresSwitchoverWithPlacement(id, manifestPath, message, mutation)
	}

	var completed model.Operation
	err = s.withLockedState(true, func(state *model.State) error {
		opIndex := findOperation(state, id)
		if opIndex < 0 {
			return ErrNotFound
		}
		op := state.Operations[opIndex]
		if err := validateManagedPostgresSwitchoverPlacementOperation(op, mutation); err != nil {
			return err
		}
		target, err := managedPostgresPlacementTargetInState(state, mutation.Witness)
		if err != nil {
			return err
		}
		active := activeOperationsForManagedPostgresPlacementTarget(
			state.Operations, mutation.Witness.AppID, mutation.Witness.ServiceID,
		)
		if len(active) != 1 || strings.TrimSpace(active[0].ID) != id {
			return ErrConflict
		}
		if !managedPostgresPlacementStateEqual(
			ManagedPostgresPlacementStateFromSpec(target.postgres), mutation.Expected,
		) {
			return ErrConflict
		}

		now := time.Now().UTC()
		applyManagedPostgresPlacementStateInState(state, target, mutation.Desired, now)
		app := state.Apps[target.appIndex]
		if err := completeManagedPostgresSwitchoverOperationModel(&op, &app, manifestPath, message, now); err != nil {
			return err
		}
		state.Operations[opIndex] = op
		state.Apps[target.appIndex] = app
		if err := syncStableReleaseForCompletedDeployInState(state, app, op, now); err != nil {
			return err
		}
		updateAppImageTrackingDeployedInState(state, op, now)
		completed = op
		return nil
	})
	return completed, err
}
