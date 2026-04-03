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

func (s *Server) buildImportedAppSpec(buildStrategy, appName, imageRef, runtimeID string, replicas, servicePort int, configContent string, files []model.AppFile, postgres *model.AppPostgresSpec, suggestedEnv map[string]string) (model.AppSpec, error) {
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
	var normalizedPostgres *model.AppPostgresSpec
	if postgres != nil {
		pgSpec, err := normalizeGenericPostgresSpec(appName, postgres)
		if err != nil {
			return model.AppSpec{}, err
		}
		normalizedPostgres = &pgSpec
	}
	return model.AppSpec{
		Image:     imageRef,
		Env:       env,
		Ports:     ports,
		Replicas:  replicas,
		RuntimeID: runtimeID,
		Files:     appFiles,
		Postgres:  normalizedPostgres,
	}, nil
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

func normalizeGenericPostgresSpec(appName string, override *model.AppPostgresSpec) (model.AppPostgresSpec, error) {
	spec := model.AppPostgresSpec{
		Image:       "postgres:17.6-alpine",
		Database:    appName,
		ServiceName: appName + "-postgres",
	}
	if override != nil {
		if strings.TrimSpace(override.Image) != "" {
			spec.Image = strings.TrimSpace(override.Image)
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
		if strings.TrimSpace(override.StoragePath) != "" {
			spec.StoragePath = strings.TrimSpace(override.StoragePath)
		}
	}
	if spec.User == "" {
		spec.User = model.DefaultManagedPostgresUser(appName, spec.StoragePath)
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
