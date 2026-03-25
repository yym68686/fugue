package api

import (
	"strings"

	"fugue/internal/model"
)

func sanitizeAppForAPI(app model.App) model.App {
	out := cloneApp(app)
	out.Spec = redactSecretFilesInSpec(out.Spec)
	out.TechStack = buildAppTechStack(out)
	return out
}

func sanitizeAppsForAPI(apps []model.App) []model.App {
	if len(apps) == 0 {
		return []model.App{}
	}
	out := make([]model.App, 0, len(apps))
	for _, app := range apps {
		out = append(out, sanitizeAppForAPI(app))
	}
	return out
}

func sanitizeOperationForAPI(op model.Operation) model.Operation {
	out := op
	if op.DesiredSpec != nil {
		spec := cloneAppSpec(*op.DesiredSpec)
		spec = redactSecretFilesInSpec(spec)
		out.DesiredSpec = &spec
	}
	if op.DesiredSource != nil {
		source := *op.DesiredSource
		out.DesiredSource = &source
	}
	return out
}

func sanitizeOperationsForAPI(ops []model.Operation) []model.Operation {
	if len(ops) == 0 {
		return []model.Operation{}
	}
	out := make([]model.Operation, 0, len(ops))
	for _, op := range ops {
		out = append(out, sanitizeOperationForAPI(op))
	}
	return out
}

func redactSecretFilesInSpec(spec model.AppSpec) model.AppSpec {
	if len(spec.Files) == 0 {
		return spec
	}
	spec.Files = cloneAppFiles(spec.Files)
	for index := range spec.Files {
		if spec.Files[index].Secret {
			spec.Files[index].Content = ""
		}
	}
	return spec
}

func cloneApp(app model.App) model.App {
	out := app
	if app.Source != nil {
		source := *app.Source
		out.Source = &source
	}
	if app.Route != nil {
		route := *app.Route
		out.Route = &route
	}
	out.Spec = cloneAppSpec(app.Spec)
	out.Bindings = cloneServiceBindings(app.Bindings)
	out.BackingServices = cloneBackingServices(app.BackingServices)
	if len(app.TechStack) > 0 {
		out.TechStack = append([]model.AppTechnology(nil), app.TechStack...)
	}
	return out
}

func cloneAppSpec(spec model.AppSpec) model.AppSpec {
	out := spec
	if len(spec.Command) > 0 {
		out.Command = append([]string(nil), spec.Command...)
	}
	if len(spec.Args) > 0 {
		out.Args = append([]string(nil), spec.Args...)
	}
	if len(spec.Ports) > 0 {
		out.Ports = append([]int(nil), spec.Ports...)
	}
	out.Env = cloneStringMap(spec.Env)
	out.Files = cloneAppFiles(spec.Files)
	if spec.Postgres != nil {
		postgres := *spec.Postgres
		out.Postgres = &postgres
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneAppFiles(files []model.AppFile) []model.AppFile {
	if len(files) == 0 {
		return nil
	}
	out := make([]model.AppFile, len(files))
	copy(out, files)
	return out
}

func cloneBackingServices(services []model.BackingService) []model.BackingService {
	if len(services) == 0 {
		return nil
	}
	out := make([]model.BackingService, len(services))
	for index, service := range services {
		out[index] = cloneBackingService(service)
	}
	return out
}

func cloneBackingService(service model.BackingService) model.BackingService {
	out := service
	out.Spec = cloneBackingServiceSpec(service.Spec)
	return out
}

func cloneBackingServiceSpec(spec model.BackingServiceSpec) model.BackingServiceSpec {
	out := spec
	if spec.Postgres != nil {
		postgres := *spec.Postgres
		out.Postgres = &postgres
	}
	return out
}

func cloneServiceBindings(bindings []model.ServiceBinding) []model.ServiceBinding {
	if len(bindings) == 0 {
		return nil
	}
	out := make([]model.ServiceBinding, len(bindings))
	for index, binding := range bindings {
		out[index] = cloneServiceBinding(binding)
	}
	return out
}

func cloneServiceBinding(binding model.ServiceBinding) model.ServiceBinding {
	out := binding
	out.Env = cloneStringMap(binding.Env)
	return out
}

func buildAppTechStack(app model.App) []model.AppTechnology {
	stack := make([]model.AppTechnology, 0, 4)
	seen := make(map[string]struct{})
	add := func(kind, slug, name, source string) {
		kind = strings.TrimSpace(strings.ToLower(kind))
		slug = strings.TrimSpace(strings.ToLower(slug))
		name = strings.TrimSpace(name)
		source = strings.TrimSpace(strings.ToLower(source))
		if kind == "" || slug == "" || name == "" {
			return
		}
		key := kind + ":" + slug
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		stack = append(stack, model.AppTechnology{
			Kind:   kind,
			Slug:   slug,
			Name:   name,
			Source: source,
		})
	}

	if app.Source != nil {
		switch strings.TrimSpace(strings.ToLower(app.Source.Type)) {
		case model.AppSourceTypeGitHubPublic:
			add("source", "github", "GitHub", "declared")
		}

		switch strings.TrimSpace(strings.ToLower(app.Source.BuildStrategy)) {
		case model.AppBuildStrategyDockerfile:
			add("build", model.AppBuildStrategyDockerfile, "Dockerfile", "declared")
		case model.AppBuildStrategyBuildpacks:
			add("build", model.AppBuildStrategyBuildpacks, "Buildpacks", "declared")
		case model.AppBuildStrategyNixpacks:
			add("build", model.AppBuildStrategyNixpacks, "Nixpacks", "declared")
		case model.AppBuildStrategyStaticSite:
			add("build", model.AppBuildStrategyStaticSite, "Static Site", "declared")
		}

		if providerName, ok := appTechnologyName(strings.TrimSpace(app.Source.DetectedProvider)); ok {
			add("language", app.Source.DetectedProvider, providerName, "detected")
		}
	}

	for _, service := range app.BackingServices {
		switch strings.TrimSpace(strings.ToLower(service.Type)) {
		case model.BackingServiceTypePostgres:
			add("service", model.BackingServiceTypePostgres, "Postgres", "binding")
		}
	}

	if len(stack) == 0 {
		return nil
	}
	return stack
}

func appTechnologyName(slug string) (string, bool) {
	switch strings.TrimSpace(strings.ToLower(slug)) {
	case "node", "nodejs":
		return "Node.js", true
	case "python":
		return "Python", true
	case "go":
		return "Go", true
	case "java":
		return "Java", true
	case "ruby":
		return "Ruby", true
	case "php":
		return "PHP", true
	case "dotnet":
		return ".NET", true
	case "rust":
		return "Rust", true
	default:
		return "", false
	}
}
