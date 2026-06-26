package api

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/store"
)

const (
	defaultImportedConfigPath                 = "/home/api.yaml"
	defaultImportedMovableRWOStorageClassName = "fugue-workspace-rwo"
	defaultManagedPostgresStorageClassName    = "fugue-postgres-rwo"
)

func normalizeBuildStrategy(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.AppBuildStrategyAuto:
		return model.AppBuildStrategyAuto
	case model.AppBuildStrategyStaticSite:
		return model.AppBuildStrategyStaticSite
	case model.AppBuildStrategyDockerfile:
		return model.AppBuildStrategyDockerfile
	case model.AppBuildStrategyBuildpacks:
		return model.AppBuildStrategyBuildpacks
	case model.AppBuildStrategyNixpacks:
		return model.AppBuildStrategyNixpacks
	default:
		return strings.TrimSpace(strings.ToLower(raw))
	}
}

func repoNameFromGitHubURL(repoURL string) string {
	repoURL = strings.TrimSpace(strings.TrimSuffix(repoURL, ".git"))
	parts := strings.Split(strings.Trim(repoURL, "/"), "/")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

func (s *Server) buildImportedAppSpec(buildStrategy, appName, imageRef, runtimeID string, replicas, servicePort int, configContent string, files []model.AppFile, persistentStorage *model.AppPersistentStorageSpec, postgres *model.AppPostgresSpec, suggestedEnv map[string]string) (model.AppSpec, error) {
	if replicas <= 0 {
		replicas = 1
	}
	if servicePort <= 0 {
		if strings.TrimSpace(imageRef) == "" {
			servicePort = 0
		} else {
			switch normalizeBuildStrategy(buildStrategy) {
			case model.AppBuildStrategyStaticSite:
				servicePort = 80
			case model.AppBuildStrategyBuildpacks:
				servicePort = 8080
			case model.AppBuildStrategyNixpacks:
				servicePort = 3000
			default:
				servicePort = 80
			}
		}
	}

	env, err := normalizeImportedEnv(suggestedEnv)
	if err != nil {
		return model.AppSpec{}, err
	}
	var ports []int
	if servicePort > 0 {
		ports = []int{servicePort}
	}
	appFiles, err := normalizeAppFiles(configContent, files)
	if err != nil {
		return model.AppSpec{}, err
	}
	normalizedPersistentStorage, err := s.normalizeImportedPersistentStorage(persistentStorage, appFiles)
	if err != nil {
		return model.AppSpec{}, err
	}
	var normalizedPostgres *model.AppPostgresSpec
	if postgres != nil {
		pgSpec, err := normalizeGenericPostgresSpec(appName, postgres)
		if err != nil {
			return model.AppSpec{}, err
		}
		s.applyManagedPostgresDefaults(&pgSpec)
		normalizedPostgres = &pgSpec
	}
	return model.AppSpec{
		Image:             imageRef,
		Env:               env,
		Ports:             ports,
		Replicas:          replicas,
		RuntimeID:         runtimeID,
		Files:             appFiles,
		PersistentStorage: normalizedPersistentStorage,
		Postgres:          normalizedPostgres,
	}, nil
}

func hasStartupCommand(value *string) bool {
	return value != nil && strings.TrimSpace(*value) != ""
}

func hasImportedPersistentStorage(value *model.AppPersistentStorageSpec) bool {
	return value != nil
}

func normalizeDefaultMovableRWOStorageClassName(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return defaultImportedMovableRWOStorageClassName
	}
	return value
}

func (s *Server) effectiveDefaultMovableRWOStorageClassName() string {
	if s == nil {
		return defaultImportedMovableRWOStorageClassName
	}
	return normalizeDefaultMovableRWOStorageClassName(s.movableRWOStorageClass)
}

func normalizeDefaultManagedPostgresStorageClassName(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return defaultManagedPostgresStorageClassName
	}
	return value
}

func (s *Server) effectiveDefaultManagedPostgresStorageClassName() string {
	if s == nil {
		return defaultManagedPostgresStorageClassName
	}
	return normalizeDefaultManagedPostgresStorageClassName(s.managedPostgresStorageClass)
}

func (s *Server) applyManagedPostgresDefaults(spec *model.AppPostgresSpec) {
	if spec == nil || strings.TrimSpace(spec.StorageClassName) != "" {
		return
	}
	spec.StorageClassName = s.effectiveDefaultManagedPostgresStorageClassName()
}

func (s *Server) applyManagedPostgresDefaultsToAppSpec(spec model.AppSpec) model.AppSpec {
	if spec.Postgres == nil {
		return spec
	}
	out := cloneAppSpec(spec)
	s.applyManagedPostgresDefaults(out.Postgres)
	return out
}

func (s *Server) applyManagedPostgresDefaultsForDeploy(app model.App, spec model.AppSpec) model.AppSpec {
	if spec.Postgres == nil {
		return spec
	}
	out := cloneAppSpec(spec)
	if strings.TrimSpace(out.Postgres.StorageClassName) != "" {
		return out
	}
	current := store.OwnedManagedPostgresSpec(app)
	if current != nil {
		out.Postgres.StorageClassName = strings.TrimSpace(current.StorageClassName)
		return out
	}
	s.applyManagedPostgresDefaults(out.Postgres)
	return out
}

func (s *Server) applyManagedPostgresDefaultsToBackingServiceSpec(spec model.BackingServiceSpec) model.BackingServiceSpec {
	if spec.Postgres == nil {
		return spec
	}
	out := cloneBackingServiceSpec(spec)
	s.applyManagedPostgresDefaults(out.Postgres)
	return out
}

func (s *Server) normalizeImportedPersistentStorage(storage *model.AppPersistentStorageSpec, files []model.AppFile) (*model.AppPersistentStorageSpec, error) {
	normalized, err := normalizeImportedPersistentStorage(storage, files)
	if err != nil || normalized == nil {
		return normalized, err
	}
	if storage != nil && strings.TrimSpace(storage.Mode) == "" {
		if strings.TrimSpace(normalized.StorageClassName) == "" {
			normalized.StorageClassName = s.effectiveDefaultMovableRWOStorageClassName()
		}
		if strings.TrimSpace(normalized.StorageClassName) != "" {
			normalized.Mode = model.AppPersistentStorageModeMovableRWO
		}
	}
	return normalized, nil
}

func normalizeStartupCommand(value *string) []string {
	if value == nil {
		return nil
	}

	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}

	return []string{"sh", "-lc", trimmed}
}

func applyStartupCommand(spec *model.AppSpec, value *string) {
	if spec == nil || value == nil {
		return
	}

	spec.Command = normalizeStartupCommand(value)
}

func startupCommandsEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}

	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}

	return true
}

func normalizeImportedEnv(in map[string]string) (map[string]string, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(in))
	for rawKey, value := range in {
		key := strings.TrimSpace(rawKey)
		if key == "" {
			return nil, fmt.Errorf("env contains empty key")
		}
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("duplicate env key %s", key)
		}
		out[key] = value
	}
	return out, nil
}

func mergeImportedEnv(base, override map[string]string) map[string]string {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	out := cloneStringMap(base)
	if out == nil {
		out = map[string]string{}
	}
	for key, value := range override {
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeAppFiles(configContent string, files []model.AppFile) ([]model.AppFile, error) {
	out := make([]model.AppFile, 0, len(files)+1)
	if strings.TrimSpace(configContent) != "" {
		out = append(out, model.AppFile{
			Path:    defaultImportedConfigPath,
			Content: configContent,
			Secret:  true,
			Mode:    0o600,
		})
	}
	out = append(out, files...)
	if len(out) == 0 {
		return nil, nil
	}
	return normalizeUploadedFiles(out)
}

func normalizeImportedPersistentStorage(storage *model.AppPersistentStorageSpec, files []model.AppFile) (*model.AppPersistentStorageSpec, error) {
	if storage == nil {
		return nil, nil
	}

	normalized := *storage
	mode, err := model.NormalizeAppPersistentStorageMode(storage.Mode)
	if err != nil {
		return nil, err
	}
	normalized.Mode = mode
	if storagePath, err := model.NormalizeAppPersistentStoragePath(storage.StoragePath); err != nil {
		return nil, err
	} else {
		normalized.StoragePath = storagePath
	}
	normalized.StorageSize = strings.TrimSpace(storage.StorageSize)
	normalized.StorageClassName = strings.TrimSpace(storage.StorageClassName)
	if sharedSubPath, err := model.NormalizeAppPersistentStorageSharedSubPath(storage.SharedSubPath); err != nil {
		return nil, err
	} else {
		normalized.SharedSubPath = sharedSubPath
	}
	normalized.ResetToken = strings.TrimSpace(storage.ResetToken)
	normalized.Mounts = nil

	for index, mount := range storage.Mounts {
		kind, err := model.NormalizeAppPersistentStorageMountKind(mount.Kind)
		if err != nil {
			return nil, fmt.Errorf("persistent_storage.mounts[%d].kind: %w", index, err)
		}
		pathValue, err := model.NormalizeAppPersistentStorageMountPath(kind, mount.Path)
		if err != nil {
			return nil, fmt.Errorf("persistent_storage.mounts[%d].path: %w", index, err)
		}
		if mount.Mode < 0 || mount.Mode > 0o777 {
			return nil, fmt.Errorf("persistent_storage.mounts[%d].mode must be between 0 and 0777", index)
		}
		normalizedMount := mount
		normalizedMount.Kind = kind
		normalizedMount.Path = pathValue
		if normalizedMount.Mode == 0 {
			switch normalizedMount.Kind {
			case model.AppPersistentStorageMountKindDirectory:
				normalizedMount.Mode = 0o755
			case model.AppPersistentStorageMountKindFile:
				if normalizedMount.Secret {
					normalizedMount.Mode = 0o600
				} else {
					normalizedMount.Mode = 0o644
				}
			}
		}
		for _, existing := range normalized.Mounts {
			if model.AppPersistentStorageMountPathConflict(existing, normalizedMount) {
				return nil, fmt.Errorf("persistent_storage.mounts contains overlapping path %s", normalizedMount.Path)
			}
		}
		for _, file := range files {
			if importedPersistentStorageConflictsWithFile(normalizedMount, file.Path) {
				return nil, fmt.Errorf("persistent_storage.mounts[%d].path overlaps file %s", index, file.Path)
			}
		}
		normalized.Mounts = append(normalized.Mounts, normalizedMount)
	}

	if len(normalized.Mounts) == 0 {
		return nil, nil
	}
	return &normalized, nil
}

func importedPersistentStorageConflictsWithFile(mount model.AppPersistentStorageMount, filePath string) bool {
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return false
	}
	switch mount.Kind {
	case model.AppPersistentStorageMountKindFile:
		return mount.Path == filePath
	case model.AppPersistentStorageMountKindDirectory:
		return model.PathWithinBase(mount.Path, filePath)
	default:
		return false
	}
}

func normalizeGenericPostgresSpec(appName string, override *model.AppPostgresSpec) (model.AppPostgresSpec, error) {
	spec := model.AppPostgresSpec{
		Database:    appName,
		ServiceName: appName + "-postgres",
	}
	applyManagedPostgresOverrides(&spec, override)
	spec.Image = model.NormalizeManagedPostgresImage(spec.Image)
	if spec.User == "" {
		spec.User = model.DefaultManagedPostgresUser(appName)
	}
	if err := model.ValidateManagedPostgresUser(appName, spec); err != nil {
		return model.AppPostgresSpec{}, err
	}
	if spec.Password == "" {
		password, err := randomHex(24)
		if err != nil {
			return model.AppPostgresSpec{}, fmt.Errorf("generate postgres password: %w", err)
		}
		spec.Password = password
	}
	return spec, nil
}

func applyManagedPostgresOverrides(spec *model.AppPostgresSpec, override *model.AppPostgresSpec) {
	if spec == nil || override == nil {
		return
	}
	if value := strings.TrimSpace(override.Image); value != "" {
		spec.Image = model.NormalizeManagedPostgresImage(value)
	}
	if value := strings.TrimSpace(override.Database); value != "" {
		spec.Database = value
	}
	if value := strings.TrimSpace(override.User); value != "" {
		spec.User = value
	}
	if value := strings.TrimSpace(override.Password); value != "" {
		spec.Password = value
	}
	if value := strings.TrimSpace(override.ServiceName); value != "" {
		spec.ServiceName = value
	}
	if value := strings.TrimSpace(override.RuntimeID); value != "" {
		spec.RuntimeID = value
	}
	if value := strings.TrimSpace(override.FailoverTargetRuntimeID); value != "" {
		spec.FailoverTargetRuntimeID = value
	}
	if value := strings.TrimSpace(override.PrimaryNodeName); value != "" {
		spec.PrimaryNodeName = value
	}
	if override.PrimaryPlacementPendingRebalance {
		spec.PrimaryPlacementPendingRebalance = true
	}
	if value := strings.TrimSpace(override.StorageSize); value != "" {
		spec.StorageSize = value
	}
	if value := strings.TrimSpace(override.StorageClassName); value != "" {
		spec.StorageClassName = value
	}
	if override.Instances > 0 {
		spec.Instances = override.Instances
	}
	if override.SynchronousReplicas > 0 {
		spec.SynchronousReplicas = override.SynchronousReplicas
	}
	if override.Resources != nil {
		spec.Resources = cloneResourceSpec(override.Resources)
	}
}

func randomHex(numBytes int) (string, error) {
	if numBytes <= 0 {
		numBytes = 16
	}
	buf := make([]byte, numBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
