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
	RepoOwner         string
	RepoName          string
	Branch            string
	CommitSHA         string
	CommitCommittedAt string
	DefaultAppName    string
	ManifestPath      string
	PrimaryService    string
	Services          []ComposeService
	Template          *GitHubTemplateMetadata
	Warnings          []string
	InferenceReport   []TopologyInference
}

type fugueManifestFile struct {
	Version         any                             `yaml:"version"`
	PrimaryService  string                          `yaml:"primary_service"`
	Template        *fugueManifestTemplate          `yaml:"template"`
	Services        map[string]fugueManifestService `yaml:"services"`
	BackingServices map[string]fugueManifestService `yaml:"backing_services"`
}

type GitHubTemplateMetadata struct {
	DefaultRuntime string
	DemoURL        string
	Description    string
	DocsURL        string
	Name           string
	Slug           string
	SourceMode     string
	Variables      []GitHubTemplateVariable
}

type GitHubTemplateVariable struct {
	DefaultValue string
	Description  string
	Generate     string
	Key          string
	Label        string
	Required     bool
	Secret       bool
}

type GitHubTemplateInspection struct {
	Branch            string
	CommitCommittedAt string
	CommitSHA         string
	DefaultAppName    string
	Manifest          *GitHubFugueManifest
	RepoName          string
	RepoOwner         string
}

type fugueManifestTemplate struct {
	DefaultRuntime string                          `yaml:"default_runtime"`
	DemoURL        string                          `yaml:"demo_url"`
	Description    string                          `yaml:"description"`
	DocsURL        string                          `yaml:"docs_url"`
	Name           string                          `yaml:"name"`
	Slug           string                          `yaml:"slug"`
	SourceMode     string                          `yaml:"source_mode"`
	Variables      []fugueManifestTemplateVariable `yaml:"variables"`
}

type fugueManifestTemplateVariable struct {
	Default     any    `yaml:"default"`
	Description string `yaml:"description"`
	Generate    string `yaml:"generate"`
	Key         string `yaml:"key"`
	Label       string `yaml:"label"`
	Required    bool   `yaml:"required"`
	Secret      bool   `yaml:"secret"`
}

type fugueManifestService struct {
	Type              string                          `yaml:"type"`
	ServiceType       string                          `yaml:"service_type"`
	Public            bool                            `yaml:"public"`
	Image             string                          `yaml:"image"`
	Port              int                             `yaml:"port"`
	Build             any                             `yaml:"build"`
	Env               any                             `yaml:"env"`
	Environment       any                             `yaml:"environment"`
	EnvFile           any                             `yaml:"env_file"`
	PersistentStorage *fugueManifestPersistentStorage `yaml:"persistent_storage"`
	DependsOn         any                             `yaml:"depends_on"`
	Bindings          any                             `yaml:"bindings"`
	OwnerService      string                          `yaml:"owner_service"`
	Database          string                          `yaml:"database"`
	User              string                          `yaml:"user"`
	Password          string                          `yaml:"password"`
	ServiceName       string                          `yaml:"service_name"`
	Command           any                             `yaml:"command"`
	Entrypoint        any                             `yaml:"entrypoint"`
	Healthcheck       any                             `yaml:"healthcheck"`
	Profiles          any                             `yaml:"profiles"`
	Volumes           any                             `yaml:"volumes"`
	Secrets           any                             `yaml:"secrets"`
	Configs           any                             `yaml:"configs"`
	Networks          any                             `yaml:"networks"`
	Labels            any                             `yaml:"labels"`
	Deploy            any                             `yaml:"deploy"`
}

type fugueManifestPersistentStorage struct {
	StoragePath      string                                `yaml:"storage_path"`
	StorageSize      string                                `yaml:"storage_size"`
	StorageClassName string                                `yaml:"storage_class_name"`
	ResetToken       string                                `yaml:"reset_token"`
	Mounts           []fugueManifestPersistentStorageMount `yaml:"mounts"`
}

type fugueManifestPersistentStorageMount struct {
	Kind        string `yaml:"kind"`
	Path        string `yaml:"path"`
	SeedContent string `yaml:"seed_content"`
	Secret      bool   `yaml:"secret"`
	Mode        int32  `yaml:"mode"`
}

type fugueBuildSpec struct {
	Strategy        string `yaml:"strategy"`
	BuildStrategy   string `yaml:"build_strategy"`
	Context         string `yaml:"context"`
	SourceDir       string `yaml:"source_dir"`
	Dockerfile      string `yaml:"dockerfile"`
	DockerfilePath  string `yaml:"dockerfile_path"`
	BuildContextDir string `yaml:"build_context_dir"`
	Args            map[string]string
	Target          string `yaml:"target"`
}

func (i *Importer) InspectGitHubFugueManifest(ctx context.Context, req GitHubFugueManifestInspectRequest) (GitHubFugueManifest, error) {
	inspection, err := i.InspectGitHubTemplate(ctx, req)
	if err != nil {
		return GitHubFugueManifest{}, err
	}
	if inspection.Manifest == nil {
		return GitHubFugueManifest{}, ErrFugueManifestNotFound
	}
	return *inspection.Manifest, nil
}

func (i *Importer) InspectGitHubTemplate(ctx context.Context, req GitHubFugueManifestInspectRequest) (GitHubTemplateInspection, error) {
	repo, err := i.cloneGitHubRepo(ctx, req.RepoURL, req.RepoAuthToken, req.Branch, "github-fugue-manifest-inspect-*")
	if err != nil {
		return GitHubTemplateInspection{}, err
	}
	defer releaseClonedRepo(repo)

	inspection := GitHubTemplateInspection{
		Branch:            repo.Branch,
		CommitCommittedAt: repo.CommitCommittedAt,
		CommitSHA:         repo.CommitSHA,
		DefaultAppName:    repo.DefaultAppName,
		RepoName:          repo.RepoName,
		RepoOwner:         repo.RepoOwner,
	}

	manifest, err := inspectFugueManifestFromRepo(repo)
	switch {
	case err == nil:
		inspection.Manifest = &manifest
		return inspection, nil
	case errors.Is(err, ErrFugueManifestNotFound):
		return inspection, nil
	default:
		return GitHubTemplateInspection{}, err
	}
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
	if len(file.Services) == 0 && len(file.BackingServices) == 0 {
		return GitHubFugueManifest{}, fmt.Errorf("fugue manifest %q does not define services", manifestPath)
	}

	vars := readComposeEnvDefaults(repo.RepoDir)
	type manifestEntry struct {
		Raw             fugueManifestService
		DeclaredBacking bool
	}
	entries := make(map[string]manifestEntry, len(file.Services)+len(file.BackingServices))
	serviceNames := make([]string, 0, len(file.Services)+len(file.BackingServices))
	for name, raw := range file.Services {
		entries[name] = manifestEntry{Raw: raw}
		serviceNames = append(serviceNames, name)
	}
	for name, raw := range file.BackingServices {
		entries[name] = manifestEntry{Raw: raw, DeclaredBacking: true}
		serviceNames = append(serviceNames, name)
	}
	sort.Strings(serviceNames)

	manifest := GitHubFugueManifest{
		RepoOwner:         repo.RepoOwner,
		RepoName:          repo.RepoName,
		Branch:            repo.Branch,
		CommitSHA:         repo.CommitSHA,
		CommitCommittedAt: repo.CommitCommittedAt,
		DefaultAppName:    repo.DefaultAppName,
		ManifestPath:      manifestPath,
		Services:          make([]ComposeService, 0, len(serviceNames)),
	}

	seenNames := make(map[string]struct{}, len(serviceNames))
	for _, rawName := range serviceNames {
		entry := entries[rawName]
		service, err := resolveFugueManifestService(repo.RepoDir, rawName, entry.Raw, entry.DeclaredBacking, vars)
		if err != nil {
			return GitHubFugueManifest{}, err
		}
		if _, exists := seenNames[service.Name]; exists {
			return GitHubFugueManifest{}, fmt.Errorf("fugue manifest %q contains duplicate normalized service name %q", manifestPath, service.Name)
		}
		seenNames[service.Name] = struct{}{}
		if len(service.InferenceReport) > 0 {
			manifest.InferenceReport = append(manifest.InferenceReport, service.InferenceReport...)
		}
		manifest.Services = append(manifest.Services, service)
	}

	primaryService, err := resolveFugueManifestPrimaryService(file.PrimaryService, manifest.Services)
	if err != nil {
		return GitHubFugueManifest{}, fmt.Errorf("invalid fugue manifest %q: %w", manifestPath, err)
	}
	manifest.PrimaryService = primaryService
	template, err := resolveFugueManifestTemplate(repo, file.Template)
	if err != nil {
		return GitHubFugueManifest{}, fmt.Errorf("invalid fugue manifest %q: %w", manifestPath, err)
	}
	manifest.Template = template
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

func resolveFugueManifestService(repoDir, rawName string, raw fugueManifestService, declaredBacking bool, vars map[string]string) (ComposeService, error) {
	envFiles, fileEnv, missingEnvFiles, err := readComposeServiceEnvFiles(repoDir, raw.EnvFile, vars)
	if err != nil {
		return ComposeService{}, fmt.Errorf("load env_file for fugue service %q: %w", rawName, err)
	}
	service := ComposeService{
		Name:         slugifyOptional(rawName),
		Image:        strings.TrimSpace(raw.Image),
		Environment:  mergeComposeEnvironment(fileEnv, mergeFugueManifestEnvironment(raw.Env, raw.Environment, vars)),
		DependsOn:    parseComposeDependsOn(raw.DependsOn),
		Bindings:     parseServiceBindings(raw.Bindings),
		OwnerService: strings.TrimSpace(raw.OwnerService),
		EnvFiles:     envFiles,
		Command:      parseComposeStringList(raw.Command),
		Entrypoint:   parseComposeStringList(raw.Entrypoint),
		Healthcheck:  parseComposeHealthcheck(raw.Healthcheck),
		Profiles:     parseComposeStringList(raw.Profiles),
		Volumes:      parseComposeRefList(raw.Volumes),
		Secrets:      parseComposeRefList(raw.Secrets),
		Configs:      parseComposeRefList(raw.Configs),
		Networks:     parseComposeRefList(raw.Networks),
		Labels:       parseComposeStringMap(raw.Labels, vars),
		Deploy:       parseComposeLooseMap(raw.Deploy),
	}
	if service.Name == "" {
		return ComposeService{}, fmt.Errorf("fugue service name %q is invalid", rawName)
	}
	service.InferenceReport = appendMissingComposeEnvFileInference(service.InferenceReport, service.Name, missingEnvFiles)
	persistentStorage, persistentStorageSeedFiles, storageInferences, ignoredVolumeEntries, err := resolveComposePersistentStorage(repoDir, service.Name, raw.Volumes)
	if err != nil {
		return ComposeService{}, fmt.Errorf("resolve fugue service %q volumes: %w", rawName, err)
	}
	manifestStorage, manifestStorageSeedFiles, err := resolveFugueManifestPersistentStorage(raw.PersistentStorage)
	if err != nil {
		return ComposeService{}, fmt.Errorf("resolve fugue service %q persistent_storage: %w", rawName, err)
	}
	mergedPersistentStorage, err := mergePersistentStorageSpecs(persistentStorage, manifestStorage)
	if err != nil {
		return ComposeService{}, fmt.Errorf("merge fugue service %q persistent_storage: %w", rawName, err)
	}
	service.PersistentStorage = mergedPersistentStorage
	service.PersistentStorageSeedFiles = append(persistentStorageSeedFiles, manifestStorageSeedFiles...)
	service.InferenceReport = append(service.InferenceReport, storageInferences...)

	buildSpec, hasBuild, err := parseFugueBuildSpec(raw.Build)
	if err != nil && raw.Build != nil {
		return ComposeService{}, fmt.Errorf("resolve fugue service %q: %w", rawName, err)
	}
	service.BuildArgs = cloneStringMapLocal(buildSpec.Args)
	service.BuildTarget = strings.TrimSpace(buildSpec.Target)
	service.IgnoredFields = collectFugueIgnoredFields(raw, hasBuild, buildSpec)
	if !ignoredVolumeEntries {
		service.IgnoredFields = removeIgnoredField(service.IgnoredFields, "volumes")
	}
	if len(service.IgnoredFields) > 0 {
		service.InferenceReport = appendInference(
			service.InferenceReport,
			InferenceLevelInfo,
			"ignored_fields",
			service.Name,
			"preserved but not applied during import: %s",
			strings.Join(service.IgnoredFields, ", "),
		)
	}

	serviceType := firstNonEmptyServiceType(raw.ServiceType, raw.Type, detectServiceTypeFromImage(service.Image))
	if serviceType == "" && hasBuild {
		serviceType = ServiceTypeApp
	}
	if serviceType == "" && declaredBacking {
		serviceType = ServiceTypeCustom
	}
	if serviceType == "" && isComposePostgresService(service.Image) {
		serviceType = ServiceTypePostgres
	}
	service.ServiceType = firstNonEmptyServiceType(serviceType, ServiceTypeApp)
	service.BackingService = declaredBacking || (service.ServiceType != ServiceTypeApp && service.ServiceType != ServiceTypeCustom)

	switch service.ServiceType {
	case ServiceTypePostgres:
		if hasBuild {
			return ComposeService{}, fmt.Errorf("fugue postgres service %q must not define build", rawName)
		}
		if raw.Public {
			return ComposeService{}, fmt.Errorf("fugue postgres service %q cannot be public", rawName)
		}
		if raw.Port > 0 && raw.Port != 5432 {
			return ComposeService{}, fmt.Errorf("fugue postgres service %q only supports port 5432", rawName)
		}
		service.Kind = ComposeServiceKindPostgres
		service.BackingService = true
		service.InternalPort = 5432
		postgresSpec := model.AppPostgresSpec{
			Image:       strings.TrimSpace(raw.Image),
			Database:    strings.TrimSpace(raw.Database),
			User:        strings.TrimSpace(raw.User),
			Password:    strings.TrimSpace(raw.Password),
			ServiceName: strings.TrimSpace(raw.ServiceName),
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
		service.InferenceReport = appendInference(
			service.InferenceReport,
			InferenceLevelInfo,
			"classification",
			service.Name,
			"classified from fugue manifest as managed postgres backing service",
		)
		return service, nil
	default:
		if hasBuild {
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
		}
		if strings.TrimSpace(service.Image) == "" {
			return ComposeService{}, fmt.Errorf("fugue service %q requires build or image", rawName)
		}
		service.Kind = ComposeServiceKindApp
		service.InternalPort = raw.Port
		if service.InternalPort <= 0 {
			service.InternalPort = defaultPortForService(service)
		}
		service.Published = raw.Public
		if service.BackingService {
			service.InferenceReport = appendInference(
				service.InferenceReport,
				InferenceLevelInfo,
				"classification",
				service.Name,
				"classified from fugue manifest as service_type %q and imported as an image-backed mirrored workload",
				service.ServiceType,
			)
		}
		return service, nil
	}
}

func resolveFugueManifestPersistentStorage(raw *fugueManifestPersistentStorage) (*model.AppPersistentStorageSpec, []PersistentStorageSeedFile, error) {
	if raw == nil {
		return nil, nil, nil
	}

	storagePath, err := model.NormalizeAppPersistentStoragePath(raw.StoragePath)
	if err != nil {
		return nil, nil, err
	}

	spec := &model.AppPersistentStorageSpec{
		StoragePath:      storagePath,
		StorageSize:      strings.TrimSpace(raw.StorageSize),
		StorageClassName: strings.TrimSpace(raw.StorageClassName),
		ResetToken:       strings.TrimSpace(raw.ResetToken),
	}
	seedFiles := make([]PersistentStorageSeedFile, 0, len(raw.Mounts))
	for index, mount := range raw.Mounts {
		normalized, err := normalizeImportedPersistentStorageMount(model.AppPersistentStorageMount{
			Kind:        mount.Kind,
			Path:        mount.Path,
			SeedContent: mount.SeedContent,
			Secret:      mount.Secret,
			Mode:        mount.Mode,
		}, index)
		if err != nil {
			return nil, nil, err
		}
		for _, existing := range spec.Mounts {
			if model.AppPersistentStorageMountPathConflict(existing, normalized) {
				return nil, nil, fmt.Errorf("persistent_storage.mounts contains overlapping path %s", normalized.Path)
			}
		}
		spec.Mounts = append(spec.Mounts, normalized)
		if seedFile := persistentStorageSeedFileForMount(normalized); seedFile != nil {
			seedFiles = append(seedFiles, *seedFile)
		}
	}
	if len(spec.Mounts) == 0 && spec.StoragePath == "" && spec.StorageSize == "" && spec.StorageClassName == "" && spec.ResetToken == "" {
		return nil, nil, nil
	}
	return spec, seedFiles, nil
}

func normalizeImportedPersistentStorageMount(mount model.AppPersistentStorageMount, index int) (model.AppPersistentStorageMount, error) {
	kind, err := model.NormalizeAppPersistentStorageMountKind(mount.Kind)
	if err != nil {
		return model.AppPersistentStorageMount{}, fmt.Errorf("persistent_storage.mounts[%d].kind: %w", index, err)
	}
	pathValue, err := model.NormalizeAppPersistentStorageMountPath(kind, mount.Path)
	if err != nil {
		return model.AppPersistentStorageMount{}, fmt.Errorf("persistent_storage.mounts[%d].path: %w", index, err)
	}
	if mount.Mode < 0 || mount.Mode > 0o777 {
		return model.AppPersistentStorageMount{}, fmt.Errorf("persistent_storage.mounts[%d].mode must be between 0 and 0777", index)
	}
	normalized := mount
	normalized.Kind = kind
	normalized.Path = pathValue
	if normalized.Mode == 0 {
		normalized.Mode = defaultImportedPersistentStorageMountMode(normalized)
	}
	return normalized, nil
}

func defaultImportedPersistentStorageMountMode(mount model.AppPersistentStorageMount) int32 {
	switch mount.Kind {
	case model.AppPersistentStorageMountKindDirectory:
		return 0o755
	case model.AppPersistentStorageMountKindFile:
		if mount.Secret {
			return 0o600
		}
		return 0o644
	default:
		return 0
	}
}

func persistentStorageSeedFileForMount(mount model.AppPersistentStorageMount) *PersistentStorageSeedFile {
	if mount.Kind != model.AppPersistentStorageMountKindFile {
		return nil
	}
	return &PersistentStorageSeedFile{
		Path:        mount.Path,
		Mode:        mount.Mode,
		SeedContent: mount.SeedContent,
	}
}

func mergePersistentStorageSpecs(base, override *model.AppPersistentStorageSpec) (*model.AppPersistentStorageSpec, error) {
	switch {
	case base == nil && override == nil:
		return nil, nil
	case base == nil:
		cloned := clonePersistentStorageSpec(override)
		return &cloned, nil
	case override == nil:
		cloned := clonePersistentStorageSpec(base)
		return &cloned, nil
	}

	merged := clonePersistentStorageSpec(base)
	if strings.TrimSpace(override.StoragePath) != "" {
		merged.StoragePath = strings.TrimSpace(override.StoragePath)
	}
	if strings.TrimSpace(override.StorageSize) != "" {
		merged.StorageSize = strings.TrimSpace(override.StorageSize)
	}
	if strings.TrimSpace(override.StorageClassName) != "" {
		merged.StorageClassName = strings.TrimSpace(override.StorageClassName)
	}
	if strings.TrimSpace(override.ResetToken) != "" {
		merged.ResetToken = strings.TrimSpace(override.ResetToken)
	}
	for _, mount := range override.Mounts {
		for _, existing := range merged.Mounts {
			if model.AppPersistentStorageMountPathConflict(existing, mount) {
				return nil, fmt.Errorf("persistent_storage mounts overlap at %s", mount.Path)
			}
		}
		merged.Mounts = append(merged.Mounts, mount)
	}
	return &merged, nil
}

func clonePersistentStorageSpec(spec *model.AppPersistentStorageSpec) model.AppPersistentStorageSpec {
	if spec == nil {
		return model.AppPersistentStorageSpec{}
	}
	cloned := *spec
	cloned.Mounts = append([]model.AppPersistentStorageMount(nil), spec.Mounts...)
	return cloned
}

func resolveFugueBuildInputs(repoDir string, raw any) (string, string, string, string, int, error) {
	spec, hasBuild, err := parseFugueBuildSpec(raw)
	if err != nil {
		return "", "", "", "", 0, err
	}
	if !hasBuild {
		return "", "", "", "", 0, fmt.Errorf("build is required")
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

func parseFugueBuildSpec(raw any) (fugueBuildSpec, bool, error) {
	switch value := raw.(type) {
	case nil:
		return fugueBuildSpec{}, false, nil
	case string:
		return fugueBuildSpec{Context: strings.TrimSpace(value)}, true, nil
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
		spec.Target = stringifyComposeValue(value["target"])
		spec.Args = parseComposeStringMap(value["args"], nil)
		return spec, true, nil
	default:
		return fugueBuildSpec{}, false, fmt.Errorf("unsupported build spec type %T", raw)
	}
}

func collectFugueIgnoredFields(raw fugueManifestService, hasBuild bool, buildSpec fugueBuildSpec) []string {
	fields := make([]string, 0, 12)
	appendIfPresent := func(name string, raw any) {
		switch value := raw.(type) {
		case nil:
			return
		case string:
			if strings.TrimSpace(value) != "" {
				fields = append(fields, name)
			}
		case []any:
			if len(value) > 0 {
				fields = append(fields, name)
			}
		case map[string]any:
			if len(value) > 0 {
				fields = append(fields, name)
			}
		default:
			fields = append(fields, name)
		}
	}
	appendIfPresent("command", raw.Command)
	appendIfPresent("entrypoint", raw.Entrypoint)
	appendIfPresent("healthcheck", raw.Healthcheck)
	appendIfPresent("profiles", raw.Profiles)
	appendIfPresent("volumes", raw.Volumes)
	appendIfPresent("secrets", raw.Secrets)
	appendIfPresent("configs", raw.Configs)
	appendIfPresent("networks", raw.Networks)
	appendIfPresent("labels", raw.Labels)
	appendIfPresent("deploy", raw.Deploy)
	if hasBuild {
		if len(buildSpec.Args) > 0 {
			fields = append(fields, "build.args")
		}
		if strings.TrimSpace(buildSpec.Target) != "" {
			fields = append(fields, "build.target")
		}
	}
	if len(fields) == 0 {
		return nil
	}
	sort.Strings(fields)
	return fields
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
		primary := slugifyOptional(rawPrimary)
		for _, service := range services {
			if service.Name != primary {
				continue
			}
			if service.Kind != ComposeServiceKindApp || service.BackingService {
				return "", fmt.Errorf("primary_service %q must point to an app service", rawPrimary)
			}
			return primary, nil
		}
		return "", fmt.Errorf("primary_service %q does not exist", rawPrimary)
	}

	publicServices := make([]string, 0)
	for _, service := range services {
		if service.Kind == ComposeServiceKindApp && !service.BackingService && service.Published {
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

func resolveFugueManifestTemplate(repo clonedGitHubRepo, raw *fugueManifestTemplate) (*GitHubTemplateMetadata, error) {
	if raw == nil {
		return nil, nil
	}

	slug := model.Slugify(firstNonEmptyString(raw.Slug, raw.Name, repo.DefaultAppName))
	if slug == "" {
		return nil, fmt.Errorf("template.slug is required")
	}

	name := strings.TrimSpace(raw.Name)
	if name == "" {
		name = repo.RepoName
	}

	variables := make([]GitHubTemplateVariable, 0, len(raw.Variables))
	seenKeys := make(map[string]struct{}, len(raw.Variables))
	for _, variable := range raw.Variables {
		key := strings.TrimSpace(variable.Key)
		if key == "" {
			return nil, fmt.Errorf("template.variables[].key is required")
		}
		if _, exists := seenKeys[key]; exists {
			return nil, fmt.Errorf("template.variables %q is duplicated", key)
		}
		seenKeys[key] = struct{}{}

		variables = append(variables, GitHubTemplateVariable{
			DefaultValue: strings.TrimSpace(stringifyComposeValue(variable.Default)),
			Description:  strings.TrimSpace(variable.Description),
			Generate:     strings.TrimSpace(variable.Generate),
			Key:          key,
			Label:        strings.TrimSpace(variable.Label),
			Required:     variable.Required,
			Secret:       variable.Secret,
		})
	}

	return &GitHubTemplateMetadata{
		DefaultRuntime: strings.TrimSpace(raw.DefaultRuntime),
		DemoURL:        strings.TrimSpace(raw.DemoURL),
		Description:    strings.TrimSpace(raw.Description),
		DocsURL:        strings.TrimSpace(raw.DocsURL),
		Name:           name,
		Slug:           slug,
		SourceMode:     strings.TrimSpace(raw.SourceMode),
		Variables:      variables,
	}, nil
}
