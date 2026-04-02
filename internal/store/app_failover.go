package store

import (
	"strings"

	"fugue/internal/model"
)

func validateFailoverSpec(spec model.AppSpec) error {
	if spec.Failover == nil {
		return nil
	}
	targetRuntimeID := strings.TrimSpace(spec.Failover.TargetRuntimeID)
	if targetRuntimeID == "" {
		return ErrInvalidInput
	}
	if strings.TrimSpace(spec.RuntimeID) != "" && targetRuntimeID == strings.TrimSpace(spec.RuntimeID) {
		return ErrInvalidInput
	}
	return nil
}

func validateFailoverRuntimeState(state *model.State, tenantID string, spec model.AppSpec) error {
	if err := validateFailoverSpec(spec); err != nil {
		return err
	}
	if spec.Failover == nil {
		return nil
	}
	targetRuntimeID := strings.TrimSpace(spec.Failover.TargetRuntimeID)
	if !runtimeVisibleToTenant(state, targetRuntimeID, tenantID) {
		return ErrNotFound
	}
	runtimeIndex := findRuntime(state, targetRuntimeID)
	if runtimeIndex < 0 {
		return ErrNotFound
	}
	return validateFailoverTargetRuntimeType(state.Runtimes[runtimeIndex].Type)
}

func validateFailoverTargetRuntimeType(runtimeType string) error {
	switch strings.TrimSpace(runtimeType) {
	case model.RuntimeTypeManagedOwned, model.RuntimeTypeManagedShared:
		return nil
	default:
		return ErrInvalidInput
	}
}
