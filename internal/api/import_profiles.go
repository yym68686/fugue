package api

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"fugue/internal/model"
)

const defaultImportedConfigPath = "/home/api.yaml"

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
	normalizedPersistentStorage, err := normalizeImportedPersistentStorage(persistentStorage, appFiles)
	if err != nil {
		return model.AppSpec{}, err
	}
	var normalizedPostgres *model.AppPostgresSpec
	if postgres != nil {
		pgSpec, err := normalizeGenericPostgresSpec(appName, postgres)
		if err != nil {
			return model.AppSpec{}, err
		}
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
	if storagePath, err := model.NormalizeAppPersistentStoragePath(storage.StoragePath); err != nil {
		return nil, err
	} else {
		normalized.StoragePath = storagePath
	}
	normalized.StorageSize = strings.TrimSpace(storage.StorageSize)
	normalized.StorageClassName = strings.TrimSpace(storage.StorageClassName)
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
	if override != nil {
		if strings.TrimSpace(override.Image) != "" {
			spec.Image = model.NormalizeManagedPostgresImage(override.Image)
		}
		if strings.TrimSpace(override.Database) != "" {
			spec.Database = strings.TrimSpace(override.Database)
		}
		if strings.TrimSpace(override.User) != "" {
			spec.User = strings.TrimSpace(override.User)
		}
		if strings.TrimSpace(override.Password) != "" {
			spec.Password = strings.TrimSpace(override.Password)
		}
		if strings.TrimSpace(override.ServiceName) != "" {
			spec.ServiceName = strings.TrimSpace(override.ServiceName)
		}
	}
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
