package sourceimport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"fugue/internal/model"

	"gopkg.in/yaml.v3"
)

var ErrFugueManifestNotFound = errors.New("fugue manifest not found")

var fugueManifestCandidates = []string{
	"fugue.yaml",
	"fugue.yml",
}

type GitHubFugueManifestInspectRequest struct {
	RepoURL       string
	RepoAuthToken string
	Branch        string
}

type GitHubFugueManifest struct {
	RepoOwner      string
	RepoName       string
	Branch         string
	CommitSHA      string
	DefaultAppName string
	ManifestPath   string
	PrimaryService string
	Services       []ComposeService
	Warnings       []string
}

type fugueManifestFile struct {
	Version        any                             `yaml:"version"`
	PrimaryService string                          `yaml:"primary_service"`
	Services       map[string]fugueManifestService `yaml:"services"`
}

type fugueManifestService struct {
	Type        string `yaml:"type"`
	Public      bool   `yaml:"public"`
	Image       string `yaml:"image"`
	Port        int    `yaml:"port"`
	Build       any    `yaml:"build"`
	Env         any    `yaml:"env"`
	Environment any    `yaml:"environment"`
	DependsOn   any    `yaml:"depends_on"`
	Database    string `yaml:"database"`
	User        string `yaml:"user"`
	Password    string `yaml:"password"`
	ServiceName string `yaml:"service_name"`
	StoragePath string `yaml:"storage_path"`
}

type fugueBuildSpec struct {
	Strategy        string `yaml:"strategy"`
	BuildStrategy   string `yaml:"build_strategy"`
	Context         string `yaml:"context"`
	SourceDir       string `yaml:"source_dir"`
	Dockerfile      string `yaml:"dockerfile"`
	DockerfilePath  string `yaml:"dockerfile_path"`
	BuildContextDir string `yaml:"build_context_dir"`
}

func (i *Importer) InspectGitHubFugueManifest(ctx context.Context, req GitHubFugueManifestInspectRequest) (GitHubFugueManifest, error) {
	repo, err := i.cloneGitHubRepo(ctx, req.RepoURL, req.RepoAuthToken, req.Branch, "github-fugue-manifest-inspect-*")
	if err != nil {
		return GitHubFugueManifest{}, err
	}
	defer releaseClonedRepo(repo)

	return inspectFugueManifestFromRepo(repo)
}

func inspectFugueManifestFromRepo(repo clonedGitHubRepo) (GitHubFugueManifest, error) {
	manifestPath, err := findFugueManifestFile(repo.RepoDir)
	if err != nil {
		return GitHubFugueManifest{}, err
	}

	data, err := os.ReadFile(filepath.Join(repo.RepoDir, filepath.FromSlash(manifestPath)))
	if err != nil {
		return GitHubFugueManifest{}, fmt.Errorf("read fugue manifest %q: %w", manifestPath, err)
	}

	var file fugueManifestFile
	if err := yaml.Unmarshal(data, &file); err != nil {
		return GitHubFugueManifest{}, fmt.Errorf("parse fugue manifest %q: %w", manifestPath, err)
	}
	if err := validateFugueManifestVersion(file.Version); err != nil {
		return GitHubFugueManifest{}, fmt.Errorf("invalid fugue manifest %q: %w", manifestPath, err)
	}
	if len(file.Services) == 0 {
		return GitHubFugueManifest{}, fmt.Errorf("fugue manifest %q does not define services", manifestPath)
	}

	vars := readComposeEnvDefaults(repo.RepoDir)
	serviceNames := make([]string, 0, len(file.Services))
	for name := range file.Services {
		serviceNames = append(serviceNames, name)
	}
	sort.Strings(serviceNames)

	manifest := GitHubFugueManifest{
		RepoOwner:      repo.RepoOwner,
		RepoName:       repo.RepoName,
		Branch:         repo.Branch,
		CommitSHA:      repo.CommitSHA,
		DefaultAppName: repo.DefaultAppName,
		ManifestPath:   manifestPath,
		Services:       make([]ComposeService, 0, len(serviceNames)),
	}

	seenNames := make(map[string]struct{}, len(serviceNames))
	for _, rawName := range serviceNames {
		service, err := resolveFugueManifestService(repo.RepoDir, rawName, file.Services[rawName], vars)
		if err != nil {
			return GitHubFugueManifest{}, err
		}
		if _, exists := seenNames[service.Name]; exists {
			return GitHubFugueManifest{}, fmt.Errorf("fugue manifest %q contains duplicate normalized service name %q", manifestPath, service.Name)
		}
		seenNames[service.Name] = struct{}{}
		manifest.Services = append(manifest.Services, service)
	}

	primaryService, err := resolveFugueManifestPrimaryService(file.PrimaryService, manifest.Services)
	if err != nil {
		return GitHubFugueManifest{}, fmt.Errorf("invalid fugue manifest %q: %w", manifestPath, err)
	}
	manifest.PrimaryService = primaryService
	return manifest, nil
}

func findFugueManifestFile(repoDir string) (string, error) {
	for _, candidate := range fugueManifestCandidates {
		if _, err := os.Stat(filepath.Join(repoDir, candidate)); err == nil {
			return candidate, nil
		}
	}
	return "", ErrFugueManifestNotFound
}

func validateFugueManifestVersion(raw any) error {
	version := strings.TrimSpace(stringifyComposeValue(raw))
	if version == "" || version == "1" {
		return nil
	}
	return fmt.Errorf("unsupported version %q", version)
}

func resolveFugueManifestService(repoDir, rawName string, raw fugueManifestService, vars map[string]string) (ComposeService, error) {
	service := ComposeService{
		Name:        model.Slugify(rawName),
		Image:       strings.TrimSpace(raw.Image),
		Environment: mergeFugueManifestEnvironment(raw.Env, raw.Environment, vars),
		DependsOn:   parseComposeDependsOn(raw.DependsOn),
	}
	if service.Name == "" {
		return ComposeService{}, fmt.Errorf("fugue service name %q is invalid", rawName)
	}

	kind, err := resolveFugueManifestServiceKind(raw.Type, service.Image, raw.Build != nil)
	if err != nil {
		return ComposeService{}, fmt.Errorf("resolve fugue service %q: %w", rawName, err)
	}

	switch kind {
	case ComposeServiceKindPostgres:
		if raw.Build != nil {
			return ComposeService{}, fmt.Errorf("fugue postgres service %q must not define build", rawName)
		}
		if raw.Public {
			return ComposeService{}, fmt.Errorf("fugue postgres service %q cannot be public", rawName)
		}
		if raw.Port > 0 && raw.Port != 5432 {
			return ComposeService{}, fmt.Errorf("fugue postgres service %q only supports port 5432", rawName)
		}
		service.Kind = ComposeServiceKindPostgres
		service.InternalPort = 5432
		postgresSpec := model.AppPostgresSpec{
			Image:       strings.TrimSpace(raw.Image),
			Database:    strings.TrimSpace(raw.Database),
			User:        strings.TrimSpace(raw.User),
			Password:    strings.TrimSpace(raw.Password),
			ServiceName: strings.TrimSpace(raw.ServiceName),
			StoragePath: strings.TrimSpace(raw.StoragePath),
		}
		if strings.TrimSpace(postgresSpec.Database) == "" {
			postgresSpec.Database = firstNonEmptyEnvValue(service.Environment, "POSTGRES_DB", "POSTGRES_DATABASE", "DB_NAME")
		}
		if strings.TrimSpace(postgresSpec.User) == "" {
			postgresSpec.User = firstNonEmptyEnvValue(service.Environment, "POSTGRES_USER", "DB_USER")
		}
		if strings.TrimSpace(postgresSpec.Password) == "" {
			postgresSpec.Password = firstNonEmptyEnvValue(service.Environment, "POSTGRES_PASSWORD", "DB_PASSWORD")
		}
		service.Postgres = &postgresSpec
		return service, nil
	case ComposeServiceKindApp:
		buildStrategy, sourceDir, dockerfilePath, buildContextDir, detectedPort, err := resolveFugueBuildInputs(repoDir, raw.Build)
		if err != nil {
			return ComposeService{}, fmt.Errorf("resolve build inputs for fugue service %q: %w", rawName, err)
		}
		if raw.Port > 0 {
			detectedPort = raw.Port
		}
		service.Kind = ComposeServiceKindApp
		service.BuildStrategy = buildStrategy
		service.SourceDir = sourceDir
		service.DockerfilePath = dockerfilePath
		service.BuildContextDir = buildContextDir
		service.InternalPort = detectedPort
		service.Published = raw.Public
		return service, nil
	default:
		return ComposeService{}, fmt.Errorf("unsupported fugue service type %q", kind)
	}
}

func resolveFugueManifestServiceKind(rawType, image string, hasBuild bool) (string, error) {
	switch strings.TrimSpace(strings.ToLower(rawType)) {
	case "":
		if !hasBuild && isComposePostgresService(image) {
			return ComposeServiceKindPostgres, nil
		}
		if !hasBuild {
			return "", fmt.Errorf("app services require build")
		}
		return ComposeServiceKindApp, nil
	case ComposeServiceKindApp:
		if !hasBuild {
			return "", fmt.Errorf("app services require build")
		}
		return ComposeServiceKindApp, nil
	case ComposeServiceKindPostgres:
		return ComposeServiceKindPostgres, nil
	default:
		return "", fmt.Errorf("unsupported service type %q", rawType)
	}
}

func resolveFugueBuildInputs(repoDir string, raw any) (string, string, string, string, int, error) {
	spec, err := parseFugueBuildSpec(raw)
	if err != nil {
		return "", "", "", "", 0, err
	}

	strategy := normalizeGitHubBuildStrategy(firstNonEmptyString(spec.Strategy, spec.BuildStrategy))
	if strategy == "" {
		strategy = model.AppBuildStrategyAuto
	}

	sourceDir := firstNonEmptyString(spec.SourceDir, spec.Context, spec.BuildContextDir)
	if sourceDir == "" {
		sourceDir = "."
	}
	buildContextDir := firstNonEmptyString(spec.BuildContextDir, spec.Context, spec.SourceDir)
	if buildContextDir == "" {
		buildContextDir = sourceDir
	}
	dockerfilePath := firstNonEmptyString(spec.DockerfilePath, spec.Dockerfile)
	dockerfilePath = normalizeBuildDockerfilePath(buildContextDir, dockerfilePath)

	switch strategy {
	case model.AppBuildStrategyAuto:
		buildStrategy, detectedSourceDir, detectedDockerfilePath, detectedBuildContextDir, err := detectAutoImportInputs(repoDir, sourceDir, dockerfilePath, buildContextDir)
		if err != nil {
			return "", "", "", "", 0, err
		}
		port, err := detectImportPortForStrategy(repoDir, buildStrategy, detectedSourceDir, detectedDockerfilePath)
		if err != nil {
			return "", "", "", "", 0, err
		}
		return buildStrategy, detectedSourceDir, detectedDockerfilePath, detectedBuildContextDir, port, nil
	case model.AppBuildStrategyDockerfile:
		detectedDockerfilePath, detectedBuildContextDir, err := detectDockerBuildInputs(repoDir, dockerfilePath, buildContextDir)
		if err != nil {
			return "", "", "", "", 0, err
		}
		port, err := detectDockerfilePort(repoDir, detectedDockerfilePath)
		if err != nil {
			return "", "", "", "", 0, err
		}
		return model.AppBuildStrategyDockerfile, "", detectedDockerfilePath, detectedBuildContextDir, port, nil
	case model.AppBuildStrategyStaticSite:
		staticDir, err := detectStaticSiteDir(repoDir, sourceDir)
		if err != nil {
			return "", "", "", "", 0, err
		}
		return model.AppBuildStrategyStaticSite, normalizeImportedSourceDirValue(relativeImportedSourceDir(repoDir, staticDir)), "", "", 80, nil
	case model.AppBuildStrategyBuildpacks:
		normalizedSourceDir, err := normalizeRepoSourceDir(repoDir, sourceDir)
		if err != nil {
			return "", "", "", "", 0, err
		}
		_, port := detectZeroConfigProviderAndPort(repoDir, normalizedSourceDir)
		return model.AppBuildStrategyBuildpacks, normalizeImportedSourceDirValue(normalizedSourceDir), "", "", port, nil
	case model.AppBuildStrategyNixpacks:
		normalizedSourceDir, err := normalizeRepoSourceDir(repoDir, sourceDir)
		if err != nil {
			return "", "", "", "", 0, err
		}
		_, port := detectZeroConfigProviderAndPort(repoDir, normalizedSourceDir)
		return model.AppBuildStrategyNixpacks, normalizeImportedSourceDirValue(normalizedSourceDir), "", "", port, nil
	default:
		return "", "", "", "", 0, fmt.Errorf("unsupported build strategy %q", strategy)
	}
}

func parseFugueBuildSpec(raw any) (fugueBuildSpec, error) {
	switch value := raw.(type) {
	case nil:
		return fugueBuildSpec{}, fmt.Errorf("build is required")
	case string:
		return fugueBuildSpec{Context: strings.TrimSpace(value)}, nil
	case map[string]any:
		spec := fugueBuildSpec{}
		if strategyRaw, ok := value["strategy"]; ok {
			spec.Strategy = stringifyComposeValue(strategyRaw)
		}
		if strategyRaw, ok := value["build_strategy"]; ok {
			spec.BuildStrategy = stringifyComposeValue(strategyRaw)
		}
		if contextRaw, ok := value["context"]; ok {
			spec.Context = stringifyComposeValue(contextRaw)
		}
		if sourceDirRaw, ok := value["source_dir"]; ok {
			spec.SourceDir = stringifyComposeValue(sourceDirRaw)
		}
		if dockerfileRaw, ok := value["dockerfile"]; ok {
			spec.Dockerfile = stringifyComposeValue(dockerfileRaw)
		}
		if dockerfilePathRaw, ok := value["dockerfile_path"]; ok {
			spec.DockerfilePath = stringifyComposeValue(dockerfilePathRaw)
		}
		if buildContextDirRaw, ok := value["build_context_dir"]; ok {
			spec.BuildContextDir = stringifyComposeValue(buildContextDirRaw)
		}
		return spec, nil
	default:
		return fugueBuildSpec{}, fmt.Errorf("unsupported build spec type %T", raw)
	}
}

func normalizeBuildDockerfilePath(contextDir, dockerfilePath string) string {
	contextDir = strings.TrimSpace(contextDir)
	dockerfilePath = strings.TrimSpace(dockerfilePath)
	if dockerfilePath == "" {
		return ""
	}
	if contextDir == "" || contextDir == "." {
		return filepath.ToSlash(dockerfilePath)
	}
	return filepath.ToSlash(filepath.Join(contextDir, dockerfilePath))
}

func detectImportPortForStrategy(repoDir, buildStrategy, sourceDir, dockerfilePath string) (int, error) {
	switch buildStrategy {
	case model.AppBuildStrategyDockerfile:
		return detectDockerfilePort(repoDir, dockerfilePath)
	case model.AppBuildStrategyStaticSite:
		return 80, nil
	case model.AppBuildStrategyBuildpacks, model.AppBuildStrategyNixpacks:
		_, port := detectZeroConfigProviderAndPort(repoDir, sourceDir)
		return port, nil
	default:
		return 80, nil
	}
}

func mergeFugueManifestEnvironment(primary, secondary any, vars map[string]string) map[string]string {
	first := parseComposeEnvironment(secondary, vars)
	second := parseComposeEnvironment(primary, vars)
	if len(first) == 0 && len(second) == 0 {
		return nil
	}
	out := make(map[string]string, len(first)+len(second))
	for key, value := range first {
		out[key] = value
	}
	for key, value := range second {
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func resolveFugueManifestPrimaryService(rawPrimary string, services []ComposeService) (string, error) {
	rawPrimary = strings.TrimSpace(rawPrimary)
	if rawPrimary != "" {
		primary := model.Slugify(rawPrimary)
		for _, service := range services {
			if service.Name != primary {
				continue
			}
			if service.Kind != ComposeServiceKindApp {
				return "", fmt.Errorf("primary_service %q must point to an app service", rawPrimary)
			}
			return primary, nil
		}
		return "", fmt.Errorf("primary_service %q does not exist", rawPrimary)
	}

	publicServices := make([]string, 0)
	for _, service := range services {
		if service.Kind == ComposeServiceKindApp && service.Published {
			publicServices = append(publicServices, service.Name)
		}
	}
	switch len(publicServices) {
	case 0:
		return "", nil
	case 1:
		return publicServices[0], nil
	default:
		sort.Strings(publicServices)
		return "", fmt.Errorf("multiple public services declared (%s); set primary_service explicitly", strings.Join(publicServices, ", "))
	}
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func firstNonEmptyEnvValue(env map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(env[key]); value != "" {
			return value
		}
	}
	return ""
}
