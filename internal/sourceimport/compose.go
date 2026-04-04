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
	RepoURL       string
	RepoAuthToken string
	Branch        string
}

type GitHubComposeStack struct {
	RepoOwner       string
	RepoName        string
	Branch          string
	CommitSHA       string
	DefaultAppName  string
	ComposePath     string
	Services        []ComposeService
	Warnings        []string
	InferenceReport []TopologyInference
}

type ComposeService struct {
	Name            string
	Kind            string
	ServiceType     string
	BackingService  bool
	Image           string
	BuildStrategy   string
	SourceDir       string
	DockerfilePath  string
	BuildContextDir string
	BuildArgs       map[string]string
	BuildTarget     string
	InternalPort    int
	Published       bool
	Environment     map[string]string
	DependsOn       []string
	Bindings        []ServiceBinding
	OwnerService    string
	EnvFiles        []string
	Command         []string
	Entrypoint      []string
	Healthcheck     *ServiceHealthcheck
	Profiles        []string
	Volumes         []string
	Secrets         []string
	Configs         []string
	Networks        []string
	Labels          map[string]string
	Deploy          map[string]any
	IgnoredFields   []string
	InferenceReport []TopologyInference
	Postgres        *model.AppPostgresSpec
}

type composeFile struct {
	Services map[string]composeServiceRaw `yaml:"services"`
}

type composeServiceRaw struct {
	Image       string `yaml:"image"`
	Build       any    `yaml:"build"`
	Environment any    `yaml:"environment"`
	EnvFile     any    `yaml:"env_file"`
	Ports       []any  `yaml:"ports"`
	DependsOn   any    `yaml:"depends_on"`
	Command     any    `yaml:"command"`
	Entrypoint  any    `yaml:"entrypoint"`
	Healthcheck any    `yaml:"healthcheck"`
	Profiles    any    `yaml:"profiles"`
	Volumes     any    `yaml:"volumes"`
	Secrets     any    `yaml:"secrets"`
	Configs     any    `yaml:"configs"`
	Networks    any    `yaml:"networks"`
	Labels      any    `yaml:"labels"`
	Deploy      any    `yaml:"deploy"`
}

type composeBuildSpec struct {
	Context    string `yaml:"context"`
	Dockerfile string `yaml:"dockerfile"`
	Args       map[string]string
	Target     string `yaml:"target"`
}

type composePortSpec struct {
	Target    int `yaml:"target"`
	Published int `yaml:"published"`
}

func (i *Importer) InspectGitHubCompose(ctx context.Context, req GitHubComposeInspectRequest) (GitHubComposeStack, error) {
	repo, err := i.cloneGitHubRepo(ctx, req.RepoURL, req.RepoAuthToken, req.Branch, "github-compose-inspect-*")
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
		if len(service.InferenceReport) > 0 {
			stack.InferenceReport = append(stack.InferenceReport, service.InferenceReport...)
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
	envFiles, fileEnv, err := readComposeServiceEnvFiles(repoDir, raw.EnvFile, vars)
	if err != nil {
		return ComposeService{}, "", fmt.Errorf("load env_file for compose service %q: %w", serviceName, err)
	}

	service := ComposeService{
		Name:         slugifyOptional(serviceName),
		Image:        strings.TrimSpace(raw.Image),
		Environment:  mergeComposeEnvironment(fileEnv, parseComposeEnvironment(raw.Environment, vars)),
		DependsOn:    parseComposeDependsOn(raw.DependsOn),
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
		Bindings:     nil,
		OwnerService: "",
	}
	if service.Name == "" {
		return ComposeService{}, "", fmt.Errorf("compose service name %q is invalid", serviceName)
	}
	service.Published = composeServicePublishesPorts(raw.Ports)

	buildSpec, hasBuild, err := parseComposeBuildSpec(raw.Build)
	if err != nil {
		return ComposeService{}, "", fmt.Errorf("parse build spec for compose service %q: %w", serviceName, err)
	}
	service.BuildArgs = cloneStringMapLocal(buildSpec.Args)
	service.BuildTarget = strings.TrimSpace(buildSpec.Target)
	service.ServiceType = ServiceTypeApp
	detectedType := detectServiceTypeFromImage(raw.Image)
	ignoredFields := collectComposeIgnoredFields(raw, hasBuild, buildSpec)
	if len(ignoredFields) > 0 {
		service.IgnoredFields = append([]string(nil), ignoredFields...)
		service.InferenceReport = appendInference(
			service.InferenceReport,
			InferenceLevelInfo,
			"ignored_fields",
			service.Name,
			"preserved but not applied during import: %s",
			strings.Join(ignoredFields, ", "),
		)
	}

	if detectedType == ServiceTypePostgres && !hasBuild {
		service.Kind = ComposeServiceKindPostgres
		service.ServiceType = ServiceTypePostgres
		service.BackingService = true
		service.InternalPort = defaultPortForService(service)
		service.InferenceReport = appendInference(
			service.InferenceReport,
			InferenceLevelInfo,
			"classification",
			service.Name,
			"classified from image %q as managed postgres backing service",
			strings.TrimSpace(raw.Image),
		)
		return service, "", nil
	}
	if !hasBuild {
		if strings.TrimSpace(raw.Image) != "" {
			service.Kind = ComposeServiceKindApp
			service.ServiceType = firstNonEmptyServiceType(detectedType, ServiceTypeCustom)
			service.BackingService = detectedType != "" && detectedType != ServiceTypeApp && detectedType != ServiceTypeCustom
			service.InternalPort = detectComposeDeclaredPort(raw.Ports)
			if service.InternalPort == 0 {
				service.InternalPort = defaultPortForService(service)
			}
			if service.BackingService {
				service.InferenceReport = appendInference(
					service.InferenceReport,
					InferenceLevelInfo,
					"classification",
					service.Name,
					"classified from image %q as service_type %q and imported as a mirrored workload because Fugue has no managed adapter for it yet",
					strings.TrimSpace(raw.Image),
					service.ServiceType,
				)
			}
			return service, fmt.Sprintf("compose service %q uses image %q without build; Fugue will mirror the image directly and will not auto-sync repository commits for this service", serviceName, strings.TrimSpace(raw.Image)), nil
		}
		return ComposeService{}, fmt.Sprintf("compose service %q is skipped because it has no build or supported managed backing service", serviceName), nil
	}

	buildStrategy, sourceDir, dockerfilePath, buildContextDir, detectedPort, err := resolveComposeBuildInputs(repoDir, buildSpec)
	if err != nil {
		return ComposeService{}, "", fmt.Errorf("resolve build inputs for compose service %q: %w", serviceName, err)
	}
	service.Kind = ComposeServiceKindApp
	service.ServiceType = ServiceTypeApp
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
		spec.Context = stringifyComposeValue(value["context"])
		spec.Dockerfile = stringifyComposeValue(value["dockerfile"])
		spec.Target = stringifyComposeValue(value["target"])
		spec.Args = parseComposeStringMap(value["args"], nil)
		return spec, true, nil
	default:
		return composeBuildSpec{}, false, fmt.Errorf("unsupported build spec type %T", raw)
	}
}

func firstNonEmptyServiceType(values ...string) string {
	for _, value := range values {
		value = normalizeServiceType(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func mergeComposeEnvironment(base, override map[string]string) map[string]string {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(override))
	for key, value := range base {
		out[key] = value
	}
	for key, value := range override {
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func readComposeServiceEnvFiles(repoDir string, raw any, vars map[string]string) ([]string, map[string]string, error) {
	paths := parseComposeStringList(raw)
	if len(paths) == 0 {
		return nil, nil, nil
	}
	env := make(map[string]string)
	outPaths := make([]string, 0, len(paths))
	for _, path := range paths {
		path = resolveComposeInterpolation(path, vars)
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		fullPath, err := secureRepoJoin(repoDir, path)
		if err != nil {
			return nil, nil, err
		}
		data, err := os.ReadFile(fullPath)
		if err != nil {
			return nil, nil, err
		}
		outPaths = append(outPaths, filepath.ToSlash(path))
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
			env[key] = resolveComposeInterpolation(strings.TrimSpace(value), vars)
		}
	}
	return outPaths, dropEmptyComposeMap(env), nil
}

func parseComposeStringList(raw any) []string {
	switch value := raw.(type) {
	case nil:
		return nil
	case string:
		value = strings.TrimSpace(value)
		if value == "" {
			return nil
		}
		return []string{value}
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			entry := strings.TrimSpace(stringifyComposeValue(item))
			if entry == "" {
				continue
			}
			out = append(out, entry)
		}
		if len(out) == 0 {
			return nil
		}
		return out
	default:
		return nil
	}
}

func parseComposeRefList(raw any) []string {
	switch value := raw.(type) {
	case nil:
		return nil
	case string:
		value = strings.TrimSpace(value)
		if value == "" {
			return nil
		}
		return []string{value}
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			switch typed := item.(type) {
			case map[string]any:
				for key := range typed {
					key = strings.TrimSpace(key)
					if key != "" {
						out = append(out, key)
					}
					break
				}
			default:
				entry := strings.TrimSpace(stringifyComposeValue(item))
				if entry != "" {
					out = append(out, entry)
				}
			}
		}
		if len(out) == 0 {
			return nil
		}
		sort.Strings(out)
		return out
	case map[string]any:
		out := make([]string, 0, len(value))
		for key := range value {
			key = strings.TrimSpace(key)
			if key != "" {
				out = append(out, key)
			}
		}
		sort.Strings(out)
		if len(out) == 0 {
			return nil
		}
		return out
	default:
		return nil
	}
}

func parseServiceBindings(raw any) []ServiceBinding {
	switch value := raw.(type) {
	case nil:
		return nil
	case string:
		service := slugifyOptional(value)
		if service == "" {
			return nil
		}
		return []ServiceBinding{{Service: service, Source: BindingSourceExplicit}}
	case []any:
		bindings := make([]ServiceBinding, 0, len(value))
		for _, item := range value {
			switch typed := item.(type) {
			case map[string]any:
				service := slugifyOptional(stringifyComposeValue(typed["service"]))
				if service == "" {
					continue
				}
				bindings = append(bindings, ServiceBinding{Service: service, Source: BindingSourceExplicit})
			default:
				service := slugifyOptional(stringifyComposeValue(item))
				if service == "" {
					continue
				}
				bindings = append(bindings, ServiceBinding{Service: service, Source: BindingSourceExplicit})
			}
		}
		return uniqueBindings(bindings)
	default:
		return nil
	}
}

func parseComposeStringMap(raw any, vars map[string]string) map[string]string {
	switch value := raw.(type) {
	case nil:
		return nil
	case map[string]any:
		out := make(map[string]string, len(value))
		for key, rawValue := range value {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			resolved := stringifyComposeValue(rawValue)
			if vars != nil {
				resolved = resolveComposeInterpolation(resolved, vars)
			}
			out[key] = strings.TrimSpace(resolved)
		}
		return dropEmptyComposeMap(out)
	case []any:
		out := make(map[string]string, len(value))
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
			if !hasValue {
				out[key] = ""
				continue
			}
			if vars != nil {
				rawValue = resolveComposeInterpolation(rawValue, vars)
			}
			out[key] = strings.TrimSpace(rawValue)
		}
		return dropEmptyComposeMap(out)
	default:
		return nil
	}
}

func parseComposeLooseMap(raw any) map[string]any {
	value, ok := raw.(map[string]any)
	if !ok || len(value) == 0 {
		return nil
	}
	out := make(map[string]any, len(value))
	for key, item := range value {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = item
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseComposeHealthcheck(raw any) *ServiceHealthcheck {
	value, ok := raw.(map[string]any)
	if !ok || len(value) == 0 {
		return nil
	}
	healthcheck := &ServiceHealthcheck{
		Test:          parseComposeStringList(value["test"]),
		Interval:      strings.TrimSpace(stringifyComposeValue(value["interval"])),
		Timeout:       strings.TrimSpace(stringifyComposeValue(value["timeout"])),
		StartPeriod:   strings.TrimSpace(stringifyComposeValue(value["start_period"])),
		StartInterval: strings.TrimSpace(stringifyComposeValue(value["start_interval"])),
		Disable:       strings.EqualFold(strings.TrimSpace(stringifyComposeValue(value["disable"])), "true"),
	}
	if retries, err := atoiComposeValue(value["retries"]); err == nil && retries > 0 {
		healthcheck.Retries = retries
	}
	if len(healthcheck.Test) == 0 &&
		healthcheck.Interval == "" &&
		healthcheck.Timeout == "" &&
		healthcheck.StartPeriod == "" &&
		healthcheck.StartInterval == "" &&
		healthcheck.Retries == 0 &&
		!healthcheck.Disable {
		return nil
	}
	return healthcheck
}

func collectComposeIgnoredFields(raw composeServiceRaw, hasBuild bool, buildSpec composeBuildSpec) []string {
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
	if len(raw.Ports) > 1 {
		fields = append(fields, "ports")
	}
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
			name := slugifyOptional(stringifyComposeValue(item))
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
			name := slugifyOptional(key)
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

func detectComposeDeclaredPort(rawPorts []any) int {
	detected := 0
	for _, raw := range rawPorts {
		port, _, ok := parseComposePort(raw)
		if !ok || port <= 0 {
			continue
		}
		if detected == 0 || port < detected {
			detected = port
		}
	}
	return detected
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
