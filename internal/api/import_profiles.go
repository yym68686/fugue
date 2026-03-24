package api

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"fugue/internal/model"
)

func resolveImportProfile(rawProfile, repoURL string, hasStatefulInputs bool) string {
	profile := strings.TrimSpace(strings.ToLower(rawProfile))
	if profile != "" {
		return profile
	}
	if hasStatefulInputs && strings.EqualFold(strings.TrimSpace(repoNameFromGitHubURL(repoURL)), "uni-api") {
		return model.AppImportProfileUniAPI
	}
	return ""
}

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

func (s *Server) buildImportedAppSpec(profile, buildStrategy, appName, imageRef, runtimeID string, replicas, servicePort int, configContent string, files []model.AppFile, postgres *model.AppPostgresSpec, suggestedEnv map[string]string) (model.AppSpec, error) {
	if replicas <= 0 {
		replicas = 1
	}
	if servicePort <= 0 {
		if strings.TrimSpace(imageRef) == "" && profile == "" {
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

	switch profile {
	case "":
		env := make(map[string]string, len(suggestedEnv))
		for key, value := range suggestedEnv {
			env[key] = value
		}
		if len(env) == 0 {
			env = nil
		}
		var ports []int
		if servicePort > 0 {
			ports = []int{servicePort}
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
			Postgres:  normalizedPostgres,
		}, nil
	case model.AppImportProfileUniAPI:
		if err := s.validateStatefulRuntime(runtimeID); err != nil {
			return model.AppSpec{}, err
		}
		appFiles, err := normalizeAppFiles(configContent, files)
		if err != nil {
			return model.AppSpec{}, err
		}
		pgSpec, err := normalizeUniAPIPostgresSpec(appName, postgres)
		if err != nil {
			return model.AppSpec{}, err
		}
		env := make(map[string]string, len(suggestedEnv))
		for key, value := range suggestedEnv {
			env[key] = value
		}
		if len(env) == 0 {
			env = nil
		}
		return model.AppSpec{
			Image:     imageRef,
			Ports:     []int{8000},
			Replicas:  replicas,
			RuntimeID: runtimeID,
			Env:       env,
			Files:     appFiles,
			Postgres:  &pgSpec,
		}, nil
	default:
		return model.AppSpec{}, fmt.Errorf("unsupported import profile %q", profile)
	}
}

func (s *Server) validateStatefulRuntime(runtimeID string) error {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return fmt.Errorf("stateful imports require runtime_id")
	}
	runtimeObj, err := s.store.GetRuntime(runtimeID)
	if err != nil {
		return fmt.Errorf("load runtime %s: %w", runtimeID, err)
	}
	if runtimeObj.Type == model.RuntimeTypeManagedShared {
		return fmt.Errorf("stateful imports currently require a dedicated runtime, not managed-shared")
	}
	return nil
}

func normalizeAppFiles(configContent string, files []model.AppFile) ([]model.AppFile, error) {
	configContent = strings.TrimSpace(configContent)
	out := make([]model.AppFile, 0, len(files)+1)
	if configContent != "" {
		out = append(out, model.AppFile{
			Path:    "/home/api.yaml",
			Content: configContent,
			Secret:  true,
			Mode:    0o600,
		})
	}
	out = append(out, files...)
	if len(out) == 0 {
		return nil, fmt.Errorf("stateful import requires config_content or files")
	}

	for index := range out {
		out[index].Path = strings.TrimSpace(out[index].Path)
		out[index].Content = strings.TrimSpace(out[index].Content)
		if out[index].Path == "" {
			if len(out) == 1 {
				out[index].Path = "/home/api.yaml"
			} else {
				return nil, fmt.Errorf("files[%d].path is required", index)
			}
		}
		if !strings.HasPrefix(out[index].Path, "/") {
			return nil, fmt.Errorf("files[%d].path must be absolute", index)
		}
		if out[index].Content == "" {
			return nil, fmt.Errorf("files[%d].content is required", index)
		}
		if out[index].Mode == 0 {
			if out[index].Secret {
				out[index].Mode = 0o600
			} else {
				out[index].Mode = 0o644
			}
		}
	}
	return out, nil
}

func normalizeUniAPIPostgresSpec(appName string, override *model.AppPostgresSpec) (model.AppPostgresSpec, error) {
	spec := model.AppPostgresSpec{
		Image:       "postgres:17.6-alpine",
		Database:    "uniapi",
		User:        "root",
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
	if spec.Password == "" {
		password, err := randomHex(24)
		if err != nil {
			return model.AppPostgresSpec{}, fmt.Errorf("generate postgres password: %w", err)
		}
		spec.Password = password
	}
	return spec, nil
}

func normalizeGenericPostgresSpec(appName string, override *model.AppPostgresSpec) (model.AppPostgresSpec, error) {
	spec := model.AppPostgresSpec{
		Image:       "postgres:17.6-alpine",
		Database:    appName,
		User:        "postgres",
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
