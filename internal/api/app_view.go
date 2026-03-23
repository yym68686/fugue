package api

import "fugue/internal/model"

func sanitizeAppForAPI(app model.App) model.App {
	out := cloneApp(app)
	out.Spec = redactSecretFilesInSpec(out.Spec)
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
