package cli

import "fugue/internal/model"

const redactedSecretValue = "[redacted]"

func redactAppForOutput(app model.App) model.App {
	out := app
	out.Source = redactAppSourceForOutput(app.Source)
	out.OriginSource = redactAppSourceForOutput(app.OriginSource)
	out.BuildSource = redactAppSourceForOutput(app.BuildSource)
	model.NormalizeAppSourceState(&out)
	out.Spec = redactAppSpecForOutput(app.Spec)
	out.Bindings = cloneBindingsForOutput(app.Bindings, true)
	out.BackingServices = cloneBackingServicesForOutput(app.BackingServices, true)
	return out
}

func redactOperationForOutput(op model.Operation) model.Operation {
	out := op
	if op.DesiredSpec != nil {
		spec := redactAppSpecForOutput(*op.DesiredSpec)
		out.DesiredSpec = &spec
	}
	out.DesiredSource = redactAppSourceForOutput(op.DesiredSource)
	out.DesiredOriginSource = redactAppSourceForOutput(op.DesiredOriginSource)
	return out
}

func redactOverviewSnapshotForOutput(snapshot appOverviewSnapshot) appOverviewSnapshot {
	out := snapshot
	out.App = redactAppForOutput(snapshot.App)
	if len(snapshot.Bindings) > 0 {
		out.Bindings = cloneBindingsForOutput(snapshot.Bindings, true)
	}
	if len(snapshot.BackingServices) > 0 {
		out.BackingServices = cloneBackingServicesForOutput(snapshot.BackingServices, true)
	}
	if len(snapshot.Operations) > 0 {
		out.Operations = make([]model.Operation, 0, len(snapshot.Operations))
		for _, operation := range snapshot.Operations {
			out.Operations = append(out.Operations, redactOperationForOutput(operation))
		}
	}
	return out
}

func redactAppSourceForOutput(source *model.AppSource) *model.AppSource {
	if source == nil {
		return nil
	}
	cloned := *source
	if len(source.ComposeDependsOn) > 0 {
		cloned.ComposeDependsOn = append([]string(nil), source.ComposeDependsOn...)
	}
	if cloned.RepoAuthToken != "" {
		cloned.RepoAuthToken = redactedSecretValue
	}
	return &cloned
}

func redactAppSpecForOutput(spec model.AppSpec) model.AppSpec {
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
	out.Env = cloneStringMapForOutput(spec.Env, true)
	out.Files = cloneFilesForOutput(spec.Files, true)
	if spec.Workspace != nil {
		workspace := *spec.Workspace
		out.Workspace = &workspace
	}
	if spec.PersistentStorage != nil {
		storage := *spec.PersistentStorage
		storage.Mounts = cloneStorageMountsForOutput(spec.PersistentStorage.Mounts, true)
		out.PersistentStorage = &storage
	}
	if spec.Failover != nil {
		failover := *spec.Failover
		out.Failover = &failover
	}
	if spec.Resources != nil {
		resources := *spec.Resources
		out.Resources = &resources
	}
	if spec.Postgres != nil {
		postgres := *spec.Postgres
		if postgres.Password != "" {
			postgres.Password = redactedSecretValue
		}
		if spec.Postgres.Resources != nil {
			resources := *spec.Postgres.Resources
			postgres.Resources = &resources
		}
		out.Postgres = &postgres
	}
	if out.RestartToken != "" {
		out.RestartToken = redactedSecretValue
	}
	return out
}

func cloneBindingsForOutput(bindings []model.ServiceBinding, redactSecrets bool) []model.ServiceBinding {
	if len(bindings) == 0 {
		return nil
	}
	out := make([]model.ServiceBinding, len(bindings))
	for index, binding := range bindings {
		out[index] = binding
		out[index].Env = cloneStringMapForOutput(binding.Env, redactSecrets)
	}
	return out
}

func cloneBackingServicesForOutput(services []model.BackingService, redactSecrets bool) []model.BackingService {
	if len(services) == 0 {
		return nil
	}
	out := make([]model.BackingService, len(services))
	for index, service := range services {
		out[index] = service
		if service.Spec.Postgres != nil {
			postgres := *service.Spec.Postgres
			if redactSecrets && postgres.Password != "" {
				postgres.Password = redactedSecretValue
			}
			if service.Spec.Postgres.Resources != nil {
				resources := *service.Spec.Postgres.Resources
				postgres.Resources = &resources
			}
			out[index].Spec.Postgres = &postgres
		}
	}
	return out
}

func cloneStringMapForOutput(values map[string]string, redactSecrets bool) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		if redactSecrets && value != "" {
			out[key] = redactedSecretValue
			continue
		}
		out[key] = value
	}
	return out
}

func cloneFilesForOutput(files []model.AppFile, redactSecrets bool) []model.AppFile {
	if len(files) == 0 {
		return nil
	}
	out := make([]model.AppFile, len(files))
	copy(out, files)
	if redactSecrets {
		for index := range out {
			if out[index].Secret && out[index].Content != "" {
				out[index].Content = redactedSecretValue
			}
		}
	}
	return out
}

func cloneStorageMountsForOutput(mounts []model.AppPersistentStorageMount, redactSecrets bool) []model.AppPersistentStorageMount {
	if len(mounts) == 0 {
		return nil
	}
	out := make([]model.AppPersistentStorageMount, len(mounts))
	copy(out, mounts)
	if redactSecrets {
		for index := range out {
			if out[index].Secret && out[index].SeedContent != "" {
				out[index].SeedContent = redactedSecretValue
			}
		}
	}
	return out
}
