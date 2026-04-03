package store

import (
	"strings"

	"fugue/internal/model"
)

func derefPostgresSpec(spec *model.AppPostgresSpec) model.AppPostgresSpec {
	if spec == nil {
		return model.AppPostgresSpec{}
	}
	return *spec
}

func normalizedManagedPostgresRuntimeID(appRuntimeID string, spec model.AppPostgresSpec) string {
	if runtimeID := strings.TrimSpace(spec.RuntimeID); runtimeID != "" {
		return runtimeID
	}
	return strings.TrimSpace(appRuntimeID)
}

func managedPostgresReferencedRuntimeIDs(appRuntimeID string, spec model.AppPostgresSpec) []string {
	seen := make(map[string]struct{}, 2)
	ordered := make([]string, 0, 2)

	appendID := func(runtimeID string) {
		runtimeID = strings.TrimSpace(runtimeID)
		if runtimeID == "" {
			return
		}
		if _, ok := seen[runtimeID]; ok {
			return
		}
		seen[runtimeID] = struct{}{}
		ordered = append(ordered, runtimeID)
	}

	appendID(normalizedManagedPostgresRuntimeID(appRuntimeID, spec))
	appendID(spec.FailoverTargetRuntimeID)
	return ordered
}

func validateManagedPostgresRuntimeSpec(appRuntimeID string, spec model.AppPostgresSpec) error {
	runtimeID := normalizedManagedPostgresRuntimeID(appRuntimeID, spec)
	targetRuntimeID := strings.TrimSpace(spec.FailoverTargetRuntimeID)
	if targetRuntimeID != "" && runtimeID == "" {
		return ErrInvalidInput
	}
	if runtimeID != "" && targetRuntimeID != "" && runtimeID == targetRuntimeID {
		return ErrInvalidInput
	}
	return nil
}

func validateManagedPostgresRuntimeState(state *model.State, tenantID, appRuntimeID string, spec *model.AppPostgresSpec) error {
	if spec == nil {
		return nil
	}
	if err := validateManagedPostgresRuntimeSpec(appRuntimeID, *spec); err != nil {
		return err
	}

	for _, runtimeID := range managedPostgresReferencedRuntimeIDs(appRuntimeID, *spec) {
		if !runtimeVisibleToTenant(state, runtimeID, tenantID) {
			return ErrNotFound
		}
		runtimeIndex := findRuntime(state, runtimeID)
		if runtimeIndex < 0 {
			return ErrNotFound
		}
		if err := validateFailoverTargetRuntimeType(state.Runtimes[runtimeIndex].Type); err != nil {
			return err
		}
	}
	return nil
}
