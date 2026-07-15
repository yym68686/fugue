package api

import (
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

const apiRedactedSecretValue = "[redacted]"

func sanitizeAppForAPI(app model.App) model.App {
	out := cloneApp(app)
	out.Source = sanitizeAppSourceForAPI(out.Source)
	out.OriginSource = sanitizeAppSourceForAPI(out.OriginSource)
	out.BuildSource = sanitizeAppSourceForAPI(out.BuildSource)
	model.NormalizeAppSourceState(&out)
	out.Spec = redactSecretFilesInSpec(out.Spec)
	out.Spec, _ = model.StripFugueInjectedAppEnvFromSpec(out.Spec)
	out.InternalService = buildAppInternalService(out)
	out.TechStack = buildAppTechStack(out)
	return out
}

func redactAppForDebugBundle(app model.App) model.App {
	out := cloneApp(app)
	out.Source = redactAppSourceForDebugBundle(out.Source)
	out.OriginSource = redactAppSourceForDebugBundle(out.OriginSource)
	out.BuildSource = redactAppSourceForDebugBundle(out.BuildSource)
	model.NormalizeAppSourceState(&out)
	out.Spec = redactAppSpecForDebugBundle(out.Spec)
	out.Bindings = redactServiceBindingsForDebugBundle(out.Bindings)
	out.BackingServices = redactBackingServicesForDebugBundle(out.BackingServices)
	out.InternalService = buildAppInternalService(out)
	out.TechStack = buildAppTechStack(out)
	return out
}

func redactOperationForDebugBundle(op model.Operation) model.Operation {
	out := op
	if op.DesiredSpec != nil {
		spec := redactAppSpecForDebugBundle(*op.DesiredSpec)
		out.DesiredSpec = &spec
	}
	out.DesiredSource = redactAppSourceForDebugBundle(op.DesiredSource)
	out.DesiredOriginSource = redactAppSourceForDebugBundle(op.DesiredOriginSource)
	out.ResultMessage = redactOperationDiagnosticString(op.ResultMessage)
	out.ErrorMessage = redactOperationDiagnosticString(op.ErrorMessage)
	return out
}

func redactAppSourceForDebugBundle(source *model.AppSource) *model.AppSource {
	if source == nil {
		return nil
	}
	out := sanitizeAppSourceForAPI(source)
	if out != nil && strings.TrimSpace(source.RepoAuthToken) != "" {
		out.RepoAuthToken = apiRedactedSecretValue
	}
	if out != nil {
		out.RepoURL = redactOperationDiagnosticString(out.RepoURL)
		out.ImageRef = redactOperationDiagnosticString(out.ImageRef)
		out.ResolvedImageRef = redactOperationDiagnosticString(out.ResolvedImageRef)
	}
	return out
}

func redactAppSpecForDebugBundle(spec model.AppSpec) model.AppSpec {
	out := cloneAppSpec(spec)
	out.Env = redactStringMapValues(out.Env)
	out.Files = redactAppFilesForDebugBundle(out.Files)
	if out.Workspace != nil && strings.TrimSpace(out.Workspace.ResetToken) != "" {
		workspace := *out.Workspace
		workspace.ResetToken = apiRedactedSecretValue
		out.Workspace = &workspace
	}
	if out.PersistentStorage != nil {
		storage := *out.PersistentStorage
		storage.Mounts = redactStorageMountsForDebugBundle(storage.Mounts)
		if strings.TrimSpace(storage.ResetToken) != "" {
			storage.ResetToken = apiRedactedSecretValue
		}
		out.PersistentStorage = &storage
	}
	if out.Postgres != nil && strings.TrimSpace(out.Postgres.Password) != "" {
		postgres := *out.Postgres
		postgres.Password = apiRedactedSecretValue
		out.Postgres = &postgres
	}
	if strings.TrimSpace(out.RestartToken) != "" {
		out.RestartToken = apiRedactedSecretValue
	}
	return out
}

func redactServiceBindingsForDebugBundle(bindings []model.ServiceBinding) []model.ServiceBinding {
	out := cloneServiceBindings(bindings)
	for index := range out {
		out[index].Env = redactStringMapValues(out[index].Env)
	}
	return out
}

func redactBackingServicesForDebugBundle(services []model.BackingService) []model.BackingService {
	out := cloneBackingServices(services)
	for index := range out {
		if out[index].Spec.Postgres != nil && strings.TrimSpace(out[index].Spec.Postgres.Password) != "" {
			postgres := *out[index].Spec.Postgres
			postgres.Password = apiRedactedSecretValue
			out[index].Spec.Postgres = &postgres
		}
	}
	return out
}

func redactStringMapValues(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		if strings.TrimSpace(value) == "" {
			out[key] = value
			continue
		}
		out[key] = apiRedactedSecretValue
	}
	return out
}

func redactAppFilesForDebugBundle(files []model.AppFile) []model.AppFile {
	out := cloneAppFiles(files)
	for index := range out {
		if out[index].Secret && strings.TrimSpace(out[index].Content) != "" {
			out[index].Content = apiRedactedSecretValue
		}
	}
	return out
}

func redactStorageMountsForDebugBundle(mounts []model.AppPersistentStorageMount) []model.AppPersistentStorageMount {
	out := cloneAppPersistentStorageMounts(mounts)
	for index := range out {
		if strings.TrimSpace(out[index].SeedContent) != "" {
			out[index].SeedContent = apiRedactedSecretValue
		}
	}
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
	return redactOperationForDebugBundle(op)
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
	if len(spec.Files) == 0 && len(spec.GeneratedEnv) == 0 && (spec.PersistentStorage == nil || len(spec.PersistentStorage.Mounts) == 0) {
		return spec
	}
	spec = cloneAppSpec(spec)
	for key := range spec.GeneratedEnv {
		if spec.Env != nil {
			spec.Env[key] = ""
		}
	}
	for index := range spec.Files {
		if spec.Files[index].Secret {
			spec.Files[index].Content = ""
		}
	}
	if spec.PersistentStorage != nil {
		for index := range spec.PersistentStorage.Mounts {
			if spec.PersistentStorage.Mounts[index].Secret {
				spec.PersistentStorage.Mounts[index].SeedContent = ""
			}
		}
	}
	return spec
}

func cloneApp(app model.App) model.App {
	out := app
	out.Source = cloneAppSource(app.Source)
	out.OriginSource = cloneAppSource(app.OriginSource)
	out.BuildSource = cloneAppSource(app.BuildSource)
	model.NormalizeAppSourceState(&out)
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
	redacted := cloneAppSource(source)
	if redacted == nil {
		return nil
	}
	redacted.RepoAuthToken = ""
	redacted.ComposeDependsOn = nil
	return redacted
}

func buildAppInternalService(app model.App) *model.AppInternalService {
	if !model.AppHasClusterService(app.Spec) {
		return nil
	}
	serviceName := strings.TrimSpace(runtime.RuntimeAppServiceName(app))
	namespace := strings.TrimSpace(runtime.NamespaceForTenant(app.TenantID))
	if serviceName == "" {
		return nil
	}
	host := serviceName
	if namespace != "" {
		host = serviceName + "." + namespace + ".svc.cluster.local"
	}
	return &model.AppInternalService{
		Name:      serviceName,
		Namespace: namespace,
		Host:      host,
		Port:      model.AppServicePort(app.Spec),
	}
}

func cloneAppSource(source *model.AppSource) *model.AppSource {
	if source == nil {
		return nil
	}
	cloned := *source
	if len(source.ComposeDependsOn) > 0 {
		cloned.ComposeDependsOn = append([]string(nil), source.ComposeDependsOn...)
	}
	return &cloned
}

func cloneAppSpec(spec model.AppSpec) model.AppSpec {
	spec, _ = model.StripFugueInjectedAppEnvFromSpec(spec)
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
	out.GeneratedEnv = cloneAppGeneratedEnv(spec.GeneratedEnv)
	out.Files = cloneAppFiles(spec.Files)
	if spec.Workspace != nil {
		workspace := *spec.Workspace
		out.Workspace = &workspace
	}
	if spec.NetworkPolicy != nil {
		out.NetworkPolicy = cloneAppNetworkPolicy(spec.NetworkPolicy)
	}
	if spec.PersistentStorage != nil {
		storage := *spec.PersistentStorage
		storage.Mounts = cloneAppPersistentStorageMounts(spec.PersistentStorage.Mounts)
		out.PersistentStorage = &storage
	}
	if spec.VolumeReplication != nil {
		replication := *spec.VolumeReplication
		out.VolumeReplication = &replication
	}
	if spec.Failover != nil {
		failover := *spec.Failover
		out.Failover = &failover
	}
	out.Continuity = model.CloneAppContinuityPolicy(spec.Continuity)
	if spec.Resources != nil {
		resources := *spec.Resources
		out.Resources = &resources
	}
	if spec.RightSizing != nil {
		rightSizing := *spec.RightSizing
		out.RightSizing = &rightSizing
	}
	if spec.Postgres != nil {
		postgres := *spec.Postgres
		if spec.Postgres.Resources != nil {
			resources := *spec.Postgres.Resources
			postgres.Resources = &resources
		}
		out.Postgres = &postgres
	}
	model.ApplyAppSpecDefaults(&out)
	return out
}

func cloneAppNetworkPolicy(in *model.AppNetworkPolicySpec) *model.AppNetworkPolicySpec {
	if in == nil {
		return nil
	}
	out := *in
	if in.Egress != nil {
		egress := *in.Egress
		egress.AllowApps = cloneAppNetworkPolicyPeers(in.Egress.AllowApps)
		out.Egress = &egress
	}
	if in.Ingress != nil {
		ingress := *in.Ingress
		ingress.AllowApps = cloneAppNetworkPolicyPeers(in.Ingress.AllowApps)
		out.Ingress = &ingress
	}
	return &out
}

func cloneAppNetworkPolicyPeers(in []model.AppNetworkPolicyAppPeer) []model.AppNetworkPolicyAppPeer {
	if len(in) == 0 {
		return nil
	}
	out := make([]model.AppNetworkPolicyAppPeer, len(in))
	for index, peer := range in {
		out[index] = peer
		if len(peer.Ports) > 0 {
			out[index].Ports = append([]int(nil), peer.Ports...)
		}
	}
	return out
}

func cloneAppGeneratedEnv(in map[string]model.AppGeneratedEnvSpec) map[string]model.AppGeneratedEnvSpec {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]model.AppGeneratedEnvSpec, len(in))
	for key, spec := range in {
		out[key] = spec
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

func cloneAppPersistentStorageMounts(mounts []model.AppPersistentStorageMount) []model.AppPersistentStorageMount {
	if len(mounts) == 0 {
		return nil
	}
	out := make([]model.AppPersistentStorageMount, len(mounts))
	copy(out, mounts)
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
	if service.RuntimeStatus != nil {
		status := *service.RuntimeStatus
		out.RuntimeStatus = &status
	}
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
