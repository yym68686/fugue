package sourceimport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"fugue/internal/model"

	"gopkg.in/yaml.v3"
)

var ErrComposeNotFound = errors.New("docker compose file not found")

var composeFileCandidates = []string{
	"docker-compose.yml",
	"docker-compose.yaml",
	"compose.yml",
	"compose.yaml",
}

const (
	ComposeServiceKindApp      = "app"
	ComposeServiceKindPostgres = "postgres"
)

type GitHubComposeInspectRequest struct {
	RepoURL string
	Branch  string
}

type GitHubComposeStack struct {
	RepoOwner      string
	RepoName       string
	Branch         string
	CommitSHA      string
	DefaultAppName string
	ComposePath    string
	Services       []ComposeService
	Warnings       []string
}

type ComposeService struct {
	Name            string
	Kind            string
	Image           string
	BuildStrategy   string
	SourceDir       string
	DockerfilePath  string
	BuildContextDir string
	InternalPort    int
	Published       bool
	Environment     map[string]string
	DependsOn       []string
}

type composeFile struct {
	Services map[string]composeServiceRaw `yaml:"services"`
}

type composeServiceRaw struct {
	Image       string `yaml:"image"`
	Build       any    `yaml:"build"`
	Environment any    `yaml:"environment"`
	Ports       []any  `yaml:"ports"`
	DependsOn   any    `yaml:"depends_on"`
}

type composeBuildSpec struct {
	Context    string `yaml:"context"`
	Dockerfile string `yaml:"dockerfile"`
}

type composePortSpec struct {
	Target    int `yaml:"target"`
	Published int `yaml:"published"`
}

func (i *Importer) InspectPublicGitHubCompose(ctx context.Context, req GitHubComposeInspectRequest) (GitHubComposeStack, error) {
	repo, err := i.clonePublicGitHubRepo(ctx, req.RepoURL, req.Branch, "github-compose-inspect-*")
	if err != nil {
		return GitHubComposeStack{}, err
	}
	defer releaseClonedRepo(repo)

	return inspectComposeStackFromRepo(repo)
}

func inspectComposeStackFromRepo(repo clonedGitHubRepo) (GitHubComposeStack, error) {
	composePath, err := findComposeFile(repo.RepoDir)
	if err != nil {
		return GitHubComposeStack{}, err
	}

	data, err := os.ReadFile(filepath.Join(repo.RepoDir, filepath.FromSlash(composePath)))
	if err != nil {
		return GitHubComposeStack{}, fmt.Errorf("read compose file %q: %w", composePath, err)
	}

	var file composeFile
	if err := yaml.Unmarshal(data, &file); err != nil {
		return GitHubComposeStack{}, fmt.Errorf("parse compose file %q: %w", composePath, err)
	}
	if len(file.Services) == 0 {
		return GitHubComposeStack{}, fmt.Errorf("compose file %q does not define services", composePath)
	}

	vars := readComposeEnvDefaults(repo.RepoDir)
	serviceNames := make([]string, 0, len(file.Services))
	for name := range file.Services {
		serviceNames = append(serviceNames, name)
	}
	sort.Strings(serviceNames)

	stack := GitHubComposeStack{
		RepoOwner:      repo.RepoOwner,
		RepoName:       repo.RepoName,
		Branch:         repo.Branch,
		CommitSHA:      repo.CommitSHA,
		DefaultAppName: repo.DefaultAppName,
		ComposePath:    composePath,
		Services:       make([]ComposeService, 0, len(serviceNames)),
	}

	for _, name := range serviceNames {
		service, warning, err := resolveComposeService(repo.RepoDir, name, file.Services[name], vars)
		if err != nil {
			return GitHubComposeStack{}, err
		}
		if warning != "" {
			stack.Warnings = append(stack.Warnings, warning)
		}
		if service.Kind == "" {
			continue
		}
		stack.Services = append(stack.Services, service)
	}

	if len(stack.Services) == 0 {
		return GitHubComposeStack{}, fmt.Errorf("compose file %q does not contain importable services", composePath)
	}
	return stack, nil
}

func findComposeFile(repoDir string) (string, error) {
	for _, candidate := range composeFileCandidates {
		if _, err := os.Stat(filepath.Join(repoDir, candidate)); err == nil {
			return candidate, nil
		}
	}
	return "", ErrComposeNotFound
}

func resolveComposeService(repoDir, serviceName string, raw composeServiceRaw, vars map[string]string) (ComposeService, string, error) {
	service := ComposeService{
		Name:        model.Slugify(serviceName),
		Image:       strings.TrimSpace(raw.Image),
		Environment: parseComposeEnvironment(raw.Environment, vars),
		DependsOn:   parseComposeDependsOn(raw.DependsOn),
	}
	if service.Name == "" {
		return ComposeService{}, "", fmt.Errorf("compose service name %q is invalid", serviceName)
	}
	service.Published = composeServicePublishesPorts(raw.Ports)

	if isComposePostgresService(raw.Image) {
		service.Kind = ComposeServiceKindPostgres
		service.InternalPort = 5432
		return service, "", nil
	}

	buildSpec, hasBuild, err := parseComposeBuildSpec(raw.Build)
	if err != nil {
		return ComposeService{}, "", fmt.Errorf("parse build spec for compose service %q: %w", serviceName, err)
	}
	if !hasBuild {
		if strings.TrimSpace(raw.Image) != "" {
			return ComposeService{}, fmt.Sprintf("compose service %q uses image %q without build; Fugue currently skips non-build compose services except managed postgres", serviceName, strings.TrimSpace(raw.Image)), nil
		}
		return ComposeService{}, fmt.Sprintf("compose service %q is skipped because it has no build or supported managed backing service", serviceName), nil
	}

	buildStrategy, sourceDir, dockerfilePath, buildContextDir, detectedPort, err := resolveComposeBuildInputs(repoDir, buildSpec)
	if err != nil {
		return ComposeService{}, "", fmt.Errorf("resolve build inputs for compose service %q: %w", serviceName, err)
	}
	service.Kind = ComposeServiceKindApp
	service.BuildStrategy = buildStrategy
	service.SourceDir = sourceDir
	service.DockerfilePath = dockerfilePath
	service.BuildContextDir = buildContextDir
	service.InternalPort = detectedPort
	return service, "", nil
}

func resolveComposeBuildInputs(repoDir string, buildSpec composeBuildSpec) (string, string, string, string, int, error) {
	contextDir := strings.TrimSpace(buildSpec.Context)
	if contextDir == "" {
		contextDir = "."
	}
	contextDir, err := normalizeRepoSourceDir(repoDir, contextDir)
	if err != nil {
		return "", "", "", "", 0, err
	}

	if strings.TrimSpace(buildSpec.Dockerfile) != "" {
		dockerfilePath := filepath.ToSlash(filepath.Join(contextDir, buildSpec.Dockerfile))
		if contextDir == "." {
			dockerfilePath = filepath.ToSlash(strings.TrimSpace(buildSpec.Dockerfile))
		}
		dockerfilePath, contextDir, err = detectDockerBuildInputs(repoDir, dockerfilePath, contextDir)
		if err != nil {
			return "", "", "", "", 0, err
		}
		port, err := detectDockerfilePort(repoDir, dockerfilePath)
		if err != nil {
			return "", "", "", "", 0, err
		}
		return model.AppBuildStrategyDockerfile, "", dockerfilePath, contextDir, port, nil
	}

	buildStrategy, sourceDir, dockerfilePath, buildContextDir, err := detectAutoImportInputs(repoDir, contextDir, "", "")
	if err != nil {
		return "", "", "", "", 0, err
	}

	port := 80
	switch buildStrategy {
	case model.AppBuildStrategyDockerfile:
		port, err = detectDockerfilePort(repoDir, dockerfilePath)
		if err != nil {
			return "", "", "", "", 0, err
		}
	case model.AppBuildStrategyStaticSite:
		port = 80
	case model.AppBuildStrategyBuildpacks, model.AppBuildStrategyNixpacks:
		_, port = detectZeroConfigProviderAndPort(repoDir, sourceDir)
	default:
		port = 80
	}
	return buildStrategy, sourceDir, dockerfilePath, buildContextDir, port, nil
}

func parseComposeBuildSpec(raw any) (composeBuildSpec, bool, error) {
	switch value := raw.(type) {
	case nil:
		return composeBuildSpec{}, false, nil
	case string:
		return composeBuildSpec{Context: strings.TrimSpace(value)}, true, nil
	case map[string]any:
		spec := composeBuildSpec{}
		if contextRaw, ok := value["context"].(string); ok {
			spec.Context = contextRaw
		}
		if dockerfileRaw, ok := value["dockerfile"].(string); ok {
			spec.Dockerfile = dockerfileRaw
		}
		return spec, true, nil
	default:
		return composeBuildSpec{}, false, fmt.Errorf("unsupported build spec type %T", raw)
	}
}

func parseComposeEnvironment(raw any, vars map[string]string) map[string]string {
	switch value := raw.(type) {
	case nil:
		return nil
	case map[string]any:
		env := make(map[string]string, len(value))
		for key, rawValue := range value {
			env[strings.TrimSpace(key)] = resolveComposeInterpolation(stringifyComposeValue(rawValue), vars)
		}
		return dropEmptyComposeMap(env)
	case []any:
		env := make(map[string]string, len(value))
		for _, item := range value {
			entry := strings.TrimSpace(stringifyComposeValue(item))
			if entry == "" {
				continue
			}
			key, rawValue, hasValue := strings.Cut(entry, "=")
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			if hasValue {
				env[key] = resolveComposeInterpolation(rawValue, vars)
				continue
			}
			env[key] = resolveComposeInterpolation(vars[key], vars)
		}
		return dropEmptyComposeMap(env)
	default:
		return nil
	}
}

func parseComposeDependsOn(raw any) []string {
	switch value := raw.(type) {
	case nil:
		return nil
	case []any:
		deps := make([]string, 0, len(value))
		for _, item := range value {
			name := model.Slugify(strings.TrimSpace(stringifyComposeValue(item)))
			if name == "" {
				continue
			}
			deps = append(deps, name)
		}
		sort.Strings(deps)
		return deps
	case map[string]any:
		deps := make([]string, 0, len(value))
		for key := range value {
			name := model.Slugify(strings.TrimSpace(key))
			if name == "" {
				continue
			}
			deps = append(deps, name)
		}
		sort.Strings(deps)
		return deps
	default:
		return nil
	}
}

func composeServicePublishesPorts(rawPorts []any) bool {
	for _, raw := range rawPorts {
		if _, published, ok := parseComposePort(raw); ok && published {
			return true
		}
	}
	return false
}

func parseComposePort(raw any) (int, bool, bool) {
	switch value := raw.(type) {
	case int:
		if value > 0 {
			return value, true, true
		}
	case int64:
		if value > 0 {
			return int(value), true, true
		}
	case string:
		return parseComposePortString(value)
	case map[string]any:
		port := composePortSpec{}
		if targetRaw, ok := value["target"]; ok {
			port.Target, _ = atoiComposeValue(targetRaw)
		}
		if publishedRaw, ok := value["published"]; ok {
			port.Published, _ = atoiComposeValue(publishedRaw)
		}
		if port.Target > 0 {
			return port.Target, true, true
		}
	}
	return 0, false, false
}

func parseComposePortString(raw string) (int, bool, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false, false
	}
	if slash := strings.Index(raw, "/"); slash >= 0 {
		raw = raw[:slash]
	}
	parts := strings.Split(raw, ":")
	last := strings.TrimSpace(parts[len(parts)-1])
	if dash := strings.Index(last, "-"); dash >= 0 {
		last = last[:dash]
	}
	port, err := strconv.Atoi(last)
	if err != nil || port <= 0 {
		return 0, false, false
	}
	return port, true, true
}

func resolveComposeInterpolation(raw string, vars map[string]string) string {
	result := raw
	for {
		start := strings.Index(result, "${")
		if start < 0 {
			return result
		}
		end := strings.Index(result[start+2:], "}")
		if end < 0 {
			return result
		}
		end += start + 2

		expr := result[start+2 : end]
		replacement := resolveComposeExpression(expr, vars)
		result = result[:start] + replacement + result[end+1:]
	}
}

func resolveComposeExpression(expr string, vars map[string]string) string {
	switch {
	case strings.Contains(expr, ":-"):
		name, fallback, _ := strings.Cut(expr, ":-")
		name = strings.TrimSpace(name)
		if value, ok := vars[name]; ok && strings.TrimSpace(value) != "" {
			return value
		}
		return fallback
	case strings.Contains(expr, "-"):
		name, fallback, _ := strings.Cut(expr, "-")
		name = strings.TrimSpace(name)
		if value, ok := vars[name]; ok {
			return value
		}
		return fallback
	case strings.Contains(expr, ":?"):
		name, _, _ := strings.Cut(expr, ":?")
		return strings.TrimSpace(vars[strings.TrimSpace(name)])
	case strings.Contains(expr, "?"):
		name, _, _ := strings.Cut(expr, "?")
		return strings.TrimSpace(vars[strings.TrimSpace(name)])
	default:
		return strings.TrimSpace(vars[strings.TrimSpace(expr)])
	}
}

func readComposeEnvDefaults(repoDir string) map[string]string {
	defaults := map[string]string{}
	for _, candidate := range []string{".env"} {
		path := filepath.Join(repoDir, candidate)
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			key, value, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			defaults[key] = strings.TrimSpace(value)
		}
	}
	return defaults
}

func stringifyComposeValue(raw any) string {
	switch value := raw.(type) {
	case nil:
		return ""
	case string:
		return value
	case int:
		return strconv.Itoa(value)
	case int64:
		return strconv.FormatInt(value, 10)
	case float64:
		return strconv.FormatInt(int64(value), 10)
	case bool:
		if value {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprint(value)
	}
}

func atoiComposeValue(raw any) (int, error) {
	switch value := raw.(type) {
	case int:
		return value, nil
	case int64:
		return int(value), nil
	case float64:
		return int(value), nil
	case string:
		return strconv.Atoi(strings.TrimSpace(value))
	default:
		return 0, fmt.Errorf("unsupported numeric value %T", raw)
	}
}

func dropEmptyComposeMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = strings.TrimSpace(value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func isComposePostgresService(image string) bool {
	image = strings.TrimSpace(strings.ToLower(image))
	if image == "" {
		return false
	}
	image = strings.TrimPrefix(image, "docker.io/")
	image = strings.TrimPrefix(image, "index.docker.io/")
	return image == "postgres" ||
		strings.HasPrefix(image, "postgres:") ||
		strings.HasPrefix(image, "library/postgres:") ||
		strings.HasPrefix(image, "library/postgres@")
}
