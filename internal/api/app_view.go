package api

import (
	"strings"

	"fugue/internal/model"
)

func sanitizeAppForAPI(app model.App) model.App {
	out := cloneApp(app)
	out.Source = sanitizeAppSourceForAPI(out.Source)
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
		out.DesiredSource = sanitizeAppSourceForAPI(op.DesiredSource)
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
	out.CurrentResourceUsage = cloneResourceUsage(app.CurrentResourceUsage)
	out.Bindings = cloneServiceBindings(app.Bindings)
	out.BackingServices = cloneBackingServices(app.BackingServices)
	if len(app.TechStack) > 0 {
		out.TechStack = append([]model.AppTechnology(nil), app.TechStack...)
	}
	return out
}

func sanitizeAppSourceForAPI(source *model.AppSource) *model.AppSource {
	if source == nil {
		return nil
	}
	redacted := *source
	redacted.RepoAuthToken = ""
	return &redacted
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
	if spec.Workspace != nil {
		workspace := *spec.Workspace
		out.Workspace = &workspace
	}
	if spec.Resources != nil {
		resources := *spec.Resources
		out.Resources = &resources
	}
	if spec.Postgres != nil {
		postgres := *spec.Postgres
		if spec.Postgres.Resources != nil {
			resources := *spec.Postgres.Resources
			postgres.Resources = &resources
		}
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
	out.CurrentResourceUsage = cloneResourceUsage(service.CurrentResourceUsage)
	return out
}

func cloneBackingServiceSpec(spec model.BackingServiceSpec) model.BackingServiceSpec {
	out := spec
	if spec.Postgres != nil {
		postgres := *spec.Postgres
		if spec.Postgres.Resources != nil {
			resources := *spec.Postgres.Resources
			postgres.Resources = &resources
		}
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

func cloneResourceUsage(usage *model.ResourceUsage) *model.ResourceUsage {
	if usage == nil {
		return nil
	}
	copied := *usage
	return &copied
}

func buildAppTechStack(app model.App) []model.AppTechnology {
	slug, name, ok := appPrimaryTechStack(app.Source)
	if !ok {
		return nil
	}

	return []model.AppTechnology{{
		Kind:   "stack",
		Slug:   slug,
		Name:   name,
		Source: "detected",
	}}
}

func appPrimaryTechStack(source *model.AppSource) (string, string, bool) {
	if source == nil {
		return "", "", false
	}

	if slug, name, ok := normalizeAppTechStack(source.DetectedStack); ok {
		return slug, name, true
	}

	switch strings.TrimSpace(strings.ToLower(source.DetectedProvider)) {
	case "", "generic", model.AppBuildStrategyDockerfile:
		return "", "", false
	default:
		return normalizeAppTechStack(source.DetectedProvider)
	}
}

func normalizeAppTechStack(slug string) (string, string, bool) {
	switch strings.TrimSpace(strings.ToLower(slug)) {
	case "next", "nextjs":
		return "nextjs", "Next.js", true
	case "react":
		return "react", "React", true
	case "node", "nodejs":
		return "nodejs", "Node.js", true
	case "python":
		return "python", "Python", true
	case "go":
		return "go", "Go", true
	case "java":
		return "java", "Java", true
	case "ruby":
		return "ruby", "Ruby", true
	case "php":
		return "php", "PHP", true
	case "dotnet":
		return "dotnet", ".NET", true
	case "rust":
		return "rust", "Rust", true
	default:
		return "", "", false
	}
}
