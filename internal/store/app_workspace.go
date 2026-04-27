package store

import (
	"strings"

	"fugue/internal/model"
)

func validateWorkspaceSpecForRuntime(spec model.AppSpec, runtimeType string) error {
	if err := validateWorkspaceSpec(spec); err != nil {
		return err
	}
	if spec.Workspace == nil && spec.PersistentStorage == nil {
		return nil
	}
	if !model.RuntimeSupportsPersistentWorkspace(runtimeType) {
		return ErrInvalidInput
	}
	return nil
}

func validateWorkspaceSpec(spec model.AppSpec) error {
	if spec.Workspace != nil && spec.PersistentStorage != nil {
		return ErrInvalidInput
	}
	var err error
	switch {
	case spec.Workspace != nil:
		err = validateLegacyWorkspaceSpec(spec)
	case spec.PersistentStorage != nil:
		err = validatePersistentStorageSpec(spec)
	}
	if err != nil {
		return err
	}
	return validateVolumeReplicationSpec(spec)
}

func validateLegacyWorkspaceSpec(spec model.AppSpec) error {
	if spec.Replicas > 1 {
		return ErrInvalidInput
	}

	mountPath, err := model.NormalizeAppWorkspaceMountPath(spec.Workspace.MountPath)
	if err != nil {
		return ErrInvalidInput
	}
	if _, err := model.NormalizeAppWorkspaceStoragePath(spec.Workspace.StoragePath); err != nil {
		return ErrInvalidInput
	}

	internalPath := model.AppWorkspaceInternalPath(mountPath)
	for _, file := range spec.Files {
		filePath := strings.TrimSpace(file.Path)
		if filePath == "" {
			continue
		}
		if model.PathWithinBase(mountPath, filePath) || model.PathWithinBase(internalPath, filePath) {
			return ErrInvalidInput
		}
	}
	return nil
}

func validatePersistentStorageSpec(spec model.AppSpec) error {
	if spec.PersistentStorage == nil {
		return nil
	}
	if spec.Replicas > 1 {
		return ErrInvalidInput
	}
	if _, err := model.NormalizeAppPersistentStorageMode(spec.PersistentStorage.Mode); err != nil {
		return ErrInvalidInput
	}
	if _, err := model.NormalizeAppPersistentStoragePath(spec.PersistentStorage.StoragePath); err != nil {
		return ErrInvalidInput
	}
	if _, err := model.NormalizeAppPersistentStorageSharedSubPath(spec.PersistentStorage.SharedSubPath); err != nil {
		return ErrInvalidInput
	}
	if len(spec.PersistentStorage.Mounts) == 0 {
		return ErrInvalidInput
	}

	normalizedMounts := make([]model.AppPersistentStorageMount, 0, len(spec.PersistentStorage.Mounts))
	for _, mount := range spec.PersistentStorage.Mounts {
		kind, err := model.NormalizeAppPersistentStorageMountKind(mount.Kind)
		if err != nil {
			return ErrInvalidInput
		}
		pathValue, err := model.NormalizeAppPersistentStorageMountPath(kind, mount.Path)
		if err != nil {
			return ErrInvalidInput
		}
		if mount.Mode < 0 || mount.Mode > 0o777 {
			return ErrInvalidInput
		}

		normalized := mount
		normalized.Kind = kind
		normalized.Path = pathValue
		for _, existing := range normalizedMounts {
			if model.AppPersistentStorageMountPathConflict(existing, normalized) {
				return ErrInvalidInput
			}
		}
		normalizedMounts = append(normalizedMounts, normalized)
	}

	for _, file := range spec.Files {
		filePath := strings.TrimSpace(file.Path)
		if filePath == "" {
			continue
		}
		for _, mount := range normalizedMounts {
			if persistentStorageMountContainsPath(mount, filePath) {
				return ErrInvalidInput
			}
		}
	}
	return nil
}

func validateWorkspaceRuntimeState(state *model.State, runtimeID string, spec model.AppSpec) error {
	runtimeIndex := findRuntime(state, runtimeID)
	if runtimeIndex < 0 {
		return ErrNotFound
	}
	return validateWorkspaceSpecForRuntime(spec, state.Runtimes[runtimeIndex].Type)
}

func hasPersistentWorkspace(app model.App) bool {
	return app.Spec.Workspace != nil || app.Spec.PersistentStorage != nil
}

func validateVolumeReplicationSpec(spec model.AppSpec) error {
	if spec.VolumeReplication != nil {
		mode, err := model.NormalizeAppVolumeReplicationMode(spec.VolumeReplication.Mode)
		if err != nil {
			return ErrInvalidInput
		}
		if mode != model.AppVolumeReplicationModeDisabled && !model.AppSpecHasReplicableVolume(spec) {
			return ErrInvalidInput
		}
		if mode == model.AppVolumeReplicationModeManual && strings.TrimSpace(spec.VolumeReplication.Schedule) != "" {
			return ErrInvalidInput
		}
	}
	if model.AppSpecHasReplicableVolume(spec) && spec.Failover != nil && !model.AppSpecVolumeReplicationEnabled(spec) {
		return ErrInvalidInput
	}
	return nil
}

func validateFailoverVolumeReplication(app model.App) error {
	if model.AppSpecHasReplicableVolume(app.Spec) && !model.AppSpecVolumeReplicationEnabled(app.Spec) {
		return ErrInvalidInput
	}
	return nil
}

func persistentStorageMountContainsPath(mount model.AppPersistentStorageMount, targetPath string) bool {
	targetPath = strings.TrimSpace(targetPath)
	if targetPath == "" {
		return false
	}
	switch strings.TrimSpace(strings.ToLower(mount.Kind)) {
	case model.AppPersistentStorageMountKindFile:
		return mount.Path == targetPath
	case model.AppPersistentStorageMountKindDirectory:
		return model.PathWithinBase(mount.Path, targetPath)
	default:
		return false
	}
}
