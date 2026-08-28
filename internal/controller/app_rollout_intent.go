package controller

import (
	"reflect"
	"strings"

	"fugue/internal/model"
)

func rolloutIntentForManagedOperation(op model.Operation, currentApp, desiredApp model.App) string {
	if !appSupportsOnlineRolloutIntent(desiredApp) {
		return ""
	}
	if managedDeployOperationIsRestartOnly(op, currentApp, desiredApp) {
		return model.AppRolloutIntentOnlineRestart
	}
	if managedDeployOperationIsConfigFileOnly(op, currentApp, desiredApp) {
		return model.AppRolloutIntentOnlineConfigUpdate
	}
	if managedDeployOperationIsImageOnly(op, currentApp, desiredApp) {
		return model.AppRolloutIntentOnlineImageUpdate
	}
	if managedDeployOperationIsEnvironmentOnly(op, currentApp, desiredApp) {
		return model.AppRolloutIntentOnlineEnvironmentUpdate
	}
	if managedDeployOperationIsResourceOnly(op, currentApp, desiredApp) {
		return model.AppRolloutIntentOnlineResourceUpdate
	}
	if managedDeployOperationIsLifecycleOnly(op, currentApp, desiredApp) {
		return model.AppRolloutIntentOnlineLifecycleUpdate
	}
	if managedDeployOperationIsZeroDowntimeRestart(op, currentApp, desiredApp) {
		return model.AppRolloutIntentOnlineRestart
	}
	return ""
}

func managedMigrateOperationIsStatelessRuntimeOnly(op model.Operation, currentApp, desiredApp model.App) bool {
	if op.Type != model.OperationTypeMigrate || op.DesiredSpec == nil {
		return false
	}
	if strings.TrimSpace(currentApp.Spec.RuntimeID) == "" ||
		strings.TrimSpace(desiredApp.Spec.RuntimeID) == "" ||
		strings.TrimSpace(currentApp.Spec.RuntimeID) == strings.TrimSpace(desiredApp.Spec.RuntimeID) {
		return false
	}
	if currentApp.Spec.Workspace != nil || desiredApp.Spec.Workspace != nil ||
		currentApp.Spec.PersistentStorage != nil || desiredApp.Spec.PersistentStorage != nil ||
		currentApp.Spec.VolumeReplication != nil || desiredApp.Spec.VolumeReplication != nil ||
		currentApp.Spec.Postgres != nil || desiredApp.Spec.Postgres != nil ||
		currentApp.Spec.Data != nil || desiredApp.Spec.Data != nil {
		return false
	}

	currentSpec, _ := model.StripFugueInjectedAppEnvFromSpec(currentApp.Spec)
	desiredSpec, _ := model.StripFugueInjectedAppEnvFromSpec(desiredApp.Spec)
	currentSpec.RuntimeID = ""
	desiredSpec.RuntimeID = ""
	currentSpec.RolloutIntent = ""
	desiredSpec.RolloutIntent = ""
	model.ApplyAppSpecDefaults(&currentSpec)
	model.ApplyAppSpecDefaults(&desiredSpec)
	if !reflect.DeepEqual(currentSpec, desiredSpec) {
		return false
	}
	if !reflect.DeepEqual(model.AppOriginSource(currentApp), model.AppOriginSource(desiredApp)) {
		return false
	}
	return reflect.DeepEqual(model.AppBuildSource(currentApp), model.AppBuildSource(desiredApp))
}

func rolloutIntentForManagedDesiredState(currentApp, desiredApp model.App) string {
	return rolloutIntentForManagedOperation(model.Operation{
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &desiredApp.Spec,
	}, currentApp, desiredApp)
}

func managedDeployOperationIsRestartOnly(op model.Operation, currentApp, desiredApp model.App) bool {
	if op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil {
		return false
	}
	currentToken := strings.TrimSpace(currentApp.Spec.RestartToken)
	desiredToken := strings.TrimSpace(desiredApp.Spec.RestartToken)
	if desiredToken == "" || desiredToken == currentToken {
		return false
	}

	currentSpec := comparableRestartSpec(currentApp.Spec)
	desiredSpec := comparableRestartSpec(desiredApp.Spec)
	if !reflect.DeepEqual(currentSpec, desiredSpec) {
		return false
	}
	if !reflect.DeepEqual(model.AppOriginSource(currentApp), model.AppOriginSource(desiredApp)) {
		return false
	}
	if !reflect.DeepEqual(model.AppBuildSource(currentApp), model.AppBuildSource(desiredApp)) {
		return false
	}
	return true
}

func managedDeployOperationIsConfigFileOnly(op model.Operation, currentApp, desiredApp model.App) bool {
	if op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil {
		return false
	}
	if !managedAppConfigOrRestartChanged(currentApp.Spec, desiredApp.Spec) {
		return false
	}
	currentSpec := comparableConfigFileUpdateSpec(currentApp.Spec)
	desiredSpec := comparableConfigFileUpdateSpec(desiredApp.Spec)
	if !reflect.DeepEqual(currentSpec, desiredSpec) {
		return false
	}
	if !reflect.DeepEqual(model.AppOriginSource(currentApp), model.AppOriginSource(desiredApp)) {
		return false
	}
	if !reflect.DeepEqual(model.AppBuildSource(currentApp), model.AppBuildSource(desiredApp)) {
		return false
	}
	return true
}

func managedAppConfigOrRestartChanged(currentSpec, desiredSpec model.AppSpec) bool {
	if strings.TrimSpace(currentSpec.RestartToken) != strings.TrimSpace(desiredSpec.RestartToken) {
		return true
	}
	if !reflect.DeepEqual(appFilesContentOnly(currentSpec.Files), appFilesContentOnly(desiredSpec.Files)) {
		return true
	}
	return !reflect.DeepEqual(persistentStorageSeedContentOnly(currentSpec.PersistentStorage), persistentStorageSeedContentOnly(desiredSpec.PersistentStorage))
}

func appFilesContentOnly(files []model.AppFile) []string {
	if len(files) == 0 {
		return nil
	}
	out := make([]string, len(files))
	for i, file := range files {
		out[i] = file.Content
	}
	return out
}

func persistentStorageSeedContentOnly(spec *model.AppPersistentStorageSpec) []string {
	if spec == nil || len(spec.Mounts) == 0 {
		return nil
	}
	out := make([]string, len(spec.Mounts))
	for i, mount := range spec.Mounts {
		out[i] = mount.SeedContent
	}
	return out
}

func comparableConfigFileUpdateSpec(spec model.AppSpec) model.AppSpec {
	normalized, _ := model.StripFugueInjectedAppEnvFromSpec(spec)
	normalized.RestartToken = ""
	normalized.RolloutIntent = ""
	for i := range normalized.Files {
		normalized.Files[i].Content = ""
	}
	if normalized.PersistentStorage != nil && len(normalized.PersistentStorage.Mounts) > 0 {
		persistent := *normalized.PersistentStorage
		persistent.Mounts = append([]model.AppPersistentStorageMount(nil), normalized.PersistentStorage.Mounts...)
		for i := range persistent.Mounts {
			persistent.Mounts[i].SeedContent = ""
		}
		normalized.PersistentStorage = &persistent
	}
	model.ApplyAppSpecDefaults(&normalized)
	return normalized
}

func comparableRestartSpec(spec model.AppSpec) model.AppSpec {
	normalized, _ := model.StripFugueInjectedAppEnvFromSpec(spec)
	normalized.RestartToken = ""
	normalized.RolloutIntent = ""
	model.ApplyAppSpecDefaults(&normalized)
	return normalized
}

func managedDeployOperationIsImageOnly(op model.Operation, currentApp, desiredApp model.App) bool {
	if op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil {
		return false
	}
	if strings.TrimSpace(desiredApp.Spec.Image) == "" {
		return false
	}
	if strings.TrimSpace(currentApp.Spec.Image) == strings.TrimSpace(desiredApp.Spec.Image) &&
		reflect.DeepEqual(model.AppOriginSource(currentApp), model.AppOriginSource(desiredApp)) &&
		reflect.DeepEqual(model.AppBuildSource(currentApp), model.AppBuildSource(desiredApp)) {
		return false
	}

	currentSpec := comparableImageOnlySpec(currentApp.Spec)
	desiredSpec := comparableImageOnlySpec(desiredApp.Spec)
	return reflect.DeepEqual(currentSpec, desiredSpec)
}

func comparableImageOnlySpec(spec model.AppSpec) model.AppSpec {
	normalized, _ := model.StripFugueInjectedAppEnvFromSpec(spec)
	normalized.Image = ""
	normalized.RestartToken = ""
	normalized.RolloutIntent = ""
	model.ApplyAppSpecDefaults(&normalized)
	return normalized
}

func managedDeployOperationIsEnvironmentOnly(op model.Operation, currentApp, desiredApp model.App) bool {
	if op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil {
		return false
	}
	currentSpec, _ := model.StripFugueInjectedAppEnvFromSpec(currentApp.Spec)
	desiredSpec, _ := model.StripFugueInjectedAppEnvFromSpec(desiredApp.Spec)
	if reflect.DeepEqual(currentSpec.Env, desiredSpec.Env) &&
		reflect.DeepEqual(currentSpec.GeneratedEnv, desiredSpec.GeneratedEnv) {
		return false
	}
	if !reflect.DeepEqual(comparableEnvironmentOnlySpec(currentApp.Spec), comparableEnvironmentOnlySpec(desiredApp.Spec)) {
		return false
	}
	if !reflect.DeepEqual(model.AppOriginSource(currentApp), model.AppOriginSource(desiredApp)) {
		return false
	}
	return reflect.DeepEqual(model.AppBuildSource(currentApp), model.AppBuildSource(desiredApp))
}

func comparableEnvironmentOnlySpec(spec model.AppSpec) model.AppSpec {
	normalized, _ := model.StripFugueInjectedAppEnvFromSpec(spec)
	normalized.Env = nil
	normalized.GeneratedEnv = nil
	normalized.RestartToken = ""
	normalized.RolloutIntent = ""
	model.ApplyAppSpecDefaults(&normalized)
	return normalized
}

func managedDeployOperationIsResourceOnly(op model.Operation, currentApp, desiredApp model.App) bool {
	if op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil {
		return false
	}
	if !managedDeployOperationResourcesDiffer(currentApp.Spec, desiredApp.Spec) {
		return false
	}

	currentSpec := comparableResourceOnlySpec(currentApp.Spec)
	desiredSpec := comparableResourceOnlySpec(desiredApp.Spec)
	if !reflect.DeepEqual(currentSpec, desiredSpec) {
		return false
	}
	if !reflect.DeepEqual(model.AppOriginSource(currentApp), model.AppOriginSource(desiredApp)) {
		return false
	}
	if !reflect.DeepEqual(model.AppBuildSource(currentApp), model.AppBuildSource(desiredApp)) {
		return false
	}
	return true
}

func managedDeployOperationResourcesDiffer(currentSpec, desiredSpec model.AppSpec) bool {
	return !reflect.DeepEqual(currentSpec.Resources, desiredSpec.Resources)
}

func comparableResourceOnlySpec(spec model.AppSpec) model.AppSpec {
	normalized, _ := model.StripFugueInjectedAppEnvFromSpec(spec)
	normalized.RolloutIntent = ""
	model.ApplyAppSpecDefaults(&normalized)
	normalized.Resources = nil
	// Right-sizing is control-plane policy, not part of the pod template. A
	// resource recommendation can be recorded in the same desired-spec
	// snapshot as a CPU/memory change; retaining it here would misclassify that
	// otherwise-online resource rollout as an unclassified restart and let the
	// durable-storage fallback choose Recreate.
	normalized.RightSizing = nil
	if normalized.Postgres != nil {
		postgres := *normalized.Postgres
		postgres.Resources = nil
		normalized.Postgres = &postgres
	}
	return normalized
}

func managedDeployOperationIsLifecycleOnly(op model.Operation, currentApp, desiredApp model.App) bool {
	if op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil {
		return false
	}
	if currentApp.Spec.TerminationGracePeriodSeconds == desiredApp.Spec.TerminationGracePeriodSeconds {
		return false
	}

	currentSpec := comparableLifecycleOnlySpec(currentApp.Spec)
	desiredSpec := comparableLifecycleOnlySpec(desiredApp.Spec)
	if !reflect.DeepEqual(currentSpec, desiredSpec) {
		return false
	}
	if !reflect.DeepEqual(model.AppOriginSource(currentApp), model.AppOriginSource(desiredApp)) {
		return false
	}
	if !reflect.DeepEqual(model.AppBuildSource(currentApp), model.AppBuildSource(desiredApp)) {
		return false
	}
	return true
}

func comparableLifecycleOnlySpec(spec model.AppSpec) model.AppSpec {
	normalized, _ := model.StripFugueInjectedAppEnvFromSpec(spec)
	normalized.RolloutIntent = ""
	normalized.TerminationGracePeriodSeconds = 0
	model.ApplyAppSpecDefaults(&normalized)
	return normalized
}

func managedDeployOperationIsZeroDowntimeRestart(op model.Operation, currentApp, desiredApp model.App) bool {
	if op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil {
		return false
	}
	if !managedAppRolloutRequiresZeroDowntime(currentApp, desiredApp) {
		return false
	}
	currentSpec, _ := model.StripFugueInjectedAppEnvFromSpec(currentApp.Spec)
	desiredSpec, _ := model.StripFugueInjectedAppEnvFromSpec(desiredApp.Spec)
	currentSpec.RolloutIntent = ""
	desiredSpec.RolloutIntent = ""
	model.ApplyAppSpecDefaults(&currentSpec)
	model.ApplyAppSpecDefaults(&desiredSpec)
	if !zeroDowntimeRestartInputsDiffer(currentSpec, desiredSpec) {
		return false
	}
	return reflect.DeepEqual(
		comparableZeroDowntimeRestartSpec(currentApp.Spec),
		comparableZeroDowntimeRestartSpec(desiredApp.Spec),
	)
}

func managedAppRolloutRequiresZeroDowntime(currentApp, desiredApp model.App) bool {
	if model.AppZeroDowntimeEnabled(currentApp.Spec) || model.AppZeroDowntimeEnabled(desiredApp.Spec) {
		return true
	}
	return managedAppHasLiveServiceToProtect(currentApp) || managedAppHasLiveServiceToProtect(desiredApp)
}

func zeroDowntimeRestartInputsDiffer(currentSpec, desiredSpec model.AppSpec) bool {
	if !reflect.DeepEqual(currentSpec.Image, desiredSpec.Image) ||
		!reflect.DeepEqual(currentSpec.Command, desiredSpec.Command) ||
		!reflect.DeepEqual(currentSpec.Args, desiredSpec.Args) ||
		!reflect.DeepEqual(currentSpec.Env, desiredSpec.Env) ||
		!reflect.DeepEqual(currentSpec.GeneratedEnv, desiredSpec.GeneratedEnv) ||
		!reflect.DeepEqual(currentSpec.SSH, desiredSpec.SSH) ||
		!reflect.DeepEqual(currentSpec.Resources, desiredSpec.Resources) ||
		strings.TrimSpace(currentSpec.WorkloadClass) != strings.TrimSpace(desiredSpec.WorkloadClass) ||
		currentSpec.TerminationGracePeriodSeconds != desiredSpec.TerminationGracePeriodSeconds ||
		strings.TrimSpace(currentSpec.RestartToken) != strings.TrimSpace(desiredSpec.RestartToken) ||
		model.AppZeroDowntimeEnabled(currentSpec) != model.AppZeroDowntimeEnabled(desiredSpec) {
		return true
	}
	if !reflect.DeepEqual(appFilesContentOnly(currentSpec.Files), appFilesContentOnly(desiredSpec.Files)) {
		return true
	}
	return !reflect.DeepEqual(
		persistentStorageSeedContentOnly(currentSpec.PersistentStorage),
		persistentStorageSeedContentOnly(desiredSpec.PersistentStorage),
	)
}

func comparableZeroDowntimeRestartSpec(spec model.AppSpec) model.AppSpec {
	normalized, _ := model.StripFugueInjectedAppEnvFromSpec(spec)
	normalized.Image = ""
	normalized.Command = nil
	normalized.Args = nil
	normalized.Env = nil
	normalized.GeneratedEnv = nil
	normalized.SSH = nil
	normalized.Resources = nil
	normalized.WorkloadClass = ""
	normalized.Continuity = nil
	normalized.TerminationGracePeriodSeconds = 0
	normalized.RestartToken = ""
	normalized.RolloutIntent = ""
	for i := range normalized.Files {
		normalized.Files[i].Content = ""
	}
	if normalized.PersistentStorage != nil && len(normalized.PersistentStorage.Mounts) > 0 {
		persistent := *normalized.PersistentStorage
		persistent.Mounts = append([]model.AppPersistentStorageMount(nil), normalized.PersistentStorage.Mounts...)
		for i := range persistent.Mounts {
			persistent.Mounts[i].SeedContent = ""
		}
		normalized.PersistentStorage = &persistent
	}
	model.ApplyAppSpecDefaults(&normalized)
	return normalized
}
