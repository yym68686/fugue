package store

import (
	"strings"

	"fugue/internal/model"
)

func preparePostgresReplicaRetirement(app model.App, op *model.Operation) error {
	if op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil {
		return ErrInvalidInput
	}
	request := op.DesiredSpec.Postgres
	node := strings.TrimSpace(request.RetireReplicaNode)
	uid := strings.TrimSpace(request.RetireReplicaPodUID)
	if node == "" || len(node) > 253 || len(uid) > 253 {
		return ErrInvalidInput
	}
	target, err := ManagedPostgresOperationTargetForApp(app, op.ServiceID)
	if err != nil || target == nil || target.Service == nil {
		return ErrInvalidInput
	}
	pg := target.Postgres
	if pg.Suspended || pg.Instances != 2 || pg.PrimaryPlacementPendingRebalance || node == pg.PrimaryNodeName {
		return ErrConflict
	}
	if pg.RuntimeID == "" {
		return ErrInvalidInput
	}
	op.SourceRuntimeID, op.TargetRuntimeID = pg.RuntimeID, pg.RuntimeID
	op.DesiredReplicas, op.DesiredSource, op.DesiredOriginSource = nil, nil, nil
	// The operation cannot change an app's configuration or any other
	// PostgreSQL fields. The final placement is derived from live SQL proof.
	op.DesiredSpec = cloneAppSpec(&app.Spec)
	op.DesiredSpec.Postgres = &pg
	pg.RetireReplicaNode, pg.RetireReplicaPodUID = node, uid
	return nil
}

func validateManagedPostgresPlacementCompletion(op model.Operation, mutation ManagedPostgresPlacementMutation, retirement *model.Operation) error {
	if retirement == nil {
		return validateManagedPostgresSwitchoverPlacementOperation(op, mutation)
	}
	if err := validateOperationFinalization(op, model.App{}, &operationFinalizationGuard{Operation: *retirement}); err != nil {
		return err
	}
	witness := mutation.Witness
	if op.Type != model.OperationTypeDatabaseReplicaRetire || op.AppID != witness.AppID ||
		op.TenantID != witness.TenantID || op.ServiceID == "" || op.ServiceID != witness.ServiceID ||
		op.SourceRuntimeID != witness.RuntimeID || op.TargetRuntimeID != witness.RuntimeID ||
		mutation.Expected.RuntimeID != witness.RuntimeID || mutation.Desired.RuntimeID != witness.RuntimeID ||
		mutation.Expected.Instances != 2 ||
		mutation.Expected.PrimaryPlacementPendingRebalance || mutation.Desired.Instances != 1 ||
		mutation.Desired.SynchronousReplicas != 0 || mutation.Desired.PrimaryPlacementPendingRebalance ||
		mutation.Desired.FailoverTargetRuntimeID != "" || op.DesiredSpec == nil ||
		op.DesiredSpec.Postgres == nil || op.DesiredSpec.Postgres.RetireReplicaNode == "" ||
		op.DesiredSpec.Postgres.RetireReplicaNode == witness.NodeName {
		return ErrConflict
	}
	return nil
}
