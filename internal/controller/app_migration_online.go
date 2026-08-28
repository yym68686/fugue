package controller

import (
	"fmt"
	"strings"

	"fugue/internal/model"
)

// applyManagedMigrationOnlineRolloutIntent enables the normal Kubernetes
// RollingUpdate path only for a stateless handoff between runtimes that are
// both managed by this control plane. External-agent handoffs and any
// state-bearing app stay on the fail-closed migration path.
func (s *Service) applyManagedMigrationOnlineRolloutIntent(op model.Operation, currentApp model.App, desiredApp *model.App) error {
	if desiredApp == nil || !managedMigrateOperationIsStatelessRuntimeOnly(op, currentApp, *desiredApp) {
		return nil
	}
	if s == nil || s.Store == nil {
		return nil
	}

	sourceRuntimeID := strings.TrimSpace(op.SourceRuntimeID)
	if sourceRuntimeID == "" {
		sourceRuntimeID = strings.TrimSpace(currentApp.Spec.RuntimeID)
	}
	targetRuntimeID := strings.TrimSpace(op.TargetRuntimeID)
	if targetRuntimeID == "" {
		targetRuntimeID = strings.TrimSpace(desiredApp.Spec.RuntimeID)
	}
	if sourceRuntimeID == "" || targetRuntimeID == "" || sourceRuntimeID == targetRuntimeID {
		return nil
	}

	sourceRuntime, err := s.Store.GetRuntime(sourceRuntimeID)
	if err != nil {
		return fmt.Errorf("load source runtime %s for online migration: %w", sourceRuntimeID, err)
	}
	targetRuntime, err := s.Store.GetRuntime(targetRuntimeID)
	if err != nil {
		return fmt.Errorf("load target runtime %s for online migration: %w", targetRuntimeID, err)
	}
	if !managedClusterRuntimeForOnlineMigration(sourceRuntime) || !managedClusterRuntimeForOnlineMigration(targetRuntime) {
		return nil
	}

	desiredApp.Spec.RolloutIntent = model.AppRolloutIntentOnlineRestart
	return nil
}

func managedClusterRuntimeForOnlineMigration(runtimeObj model.Runtime) bool {
	switch strings.TrimSpace(runtimeObj.Type) {
	case model.RuntimeTypeManagedShared:
		return true
	case model.RuntimeTypeManagedOwned:
		return strings.TrimSpace(runtimeObj.ConnectionMode) == model.MachineConnectionModeCluster
	default:
		return false
	}
}
