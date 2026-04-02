package store

import (
	"strings"

	"fugue/internal/model"
)

func validateWorkspaceSpecForRuntime(spec model.AppSpec, runtimeType string) error {
	if err := validateWorkspaceSpec(spec); err != nil {
		return err
	}
	if spec.Workspace == nil {
		return nil
	}
	if runtimeType != model.RuntimeTypeManagedOwned {
		return ErrInvalidInput
	}
	return nil
}

func validateWorkspaceSpec(spec model.AppSpec) error {
	if spec.Workspace == nil {
		return nil
	}
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

func validateWorkspaceRuntimeState(state *model.State, runtimeID string, spec model.AppSpec) error {
	runtimeIndex := findRuntime(state, runtimeID)
	if runtimeIndex < 0 {
		return ErrNotFound
	}
	return validateWorkspaceSpecForRuntime(spec, state.Runtimes[runtimeIndex].Type)
}

func hasPersistentWorkspace(app model.App) bool {
	return app.Spec.Workspace != nil
}
