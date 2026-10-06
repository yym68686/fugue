package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"
)

// Offline recovery is opt-in. It runs every server-side preflight before the
// first operation and waits for each dependency before moving an app. A retry
// re-reads durable state and only resumes resources that still need moving.
func (c *CLI) moveProjectWithOfflineRecovery(client *Client, project model.Project, apps []model.App, services []model.BackingService, targetRuntimeID string, opts projectMoveCommandOptions) error {
	if opts.SkipBlocked || !opts.Wait {
		return fmt.Errorf("--recover-offline requires --wait and cannot be combined with --skip-blocked")
	}
	storageClass := strings.TrimSpace(opts.StorageClass)
	if storageClass == "" {
		return fmt.Errorf("--recover-offline requires an explicit --storage-class")
	}
	result := projectMoveResult{Project: project, TargetRuntimeID: targetRuntimeID, DryRun: opts.DryRun}
	for _, service := range services {
		if strings.TrimSpace(backingServiceRuntimeID(service)) == targetRuntimeID {
			result.SkippedServices = append(result.SkippedServices, projectMoveSkippedService{Service: service, Reason: "already on target runtime"})
			continue
		}
		if strings.TrimSpace(service.OwnerAppID) != "" || service.Spec.Postgres == nil {
			result.SkippedServices = append(result.SkippedServices, projectMoveSkippedService{Service: service, Reason: "blocked by unsupported offline recovery service; use the normal project move for other service types"})
			continue
		}
		if _, err := client.RecoverBackingService(service.ID, true, databaseLocalizeRequest{TargetRuntimeID: targetRuntimeID, StorageClassName: storageClass}); err != nil {
			result.SkippedServices = append(result.SkippedServices, projectMoveSkippedService{Service: service, Reason: "blocked by recovery preflight: " + err.Error()})
			continue
		}
		result.Services = append(result.Services, service)
	}
	for _, app := range apps {
		if strings.TrimSpace(appEffectiveRuntimeID(app)) == targetRuntimeID {
			result.Skipped = append(result.Skipped, projectMoveSkippedApp{App: app, Reason: "already on target runtime"})
			continue
		}
		offlineVolume, err := offlineProjectMoveVolume(app)
		if err != nil {
			result.Skipped = append(result.Skipped, projectMoveSkippedApp{App: app, Reason: "blocked by " + err.Error()})
			continue
		}
		var impact model.AppMoveDryRunResponse
		if offlineVolume {
			impact, err = client.MigrateAppWithOfflineStorageDryRun(app.ID, targetRuntimeID, storageClass)
		} else {
			impact, err = client.MigrateAppDryRun(app.ID, targetRuntimeID)
		}
		if err != nil {
			return fmt.Errorf("preflight app %s: %w", formatDisplayName(app.Name, app.ID, c.showIDs()), err)
		}
		result.AppImpacts = append(result.AppImpacts, impact.Impact)
		if !impact.Impact.Pass {
			item := projectMoveSkippedApp{App: app, Reason: "blocked by " + strings.Join(impact.Impact.Blockers, "; ")}
			if !appMoveSkippedForDatabaseDependency(item, result.AppImpacts) || !offlineProjectMoveHasServiceDependency(app, result.Services) {
				result.Skipped = append(result.Skipped, item)
				continue
			}
		}
		result.Apps = append(result.Apps, app)
	}
	result.Blocked = hasBlockedProjectMoveApps(result.Skipped) || hasBlockedProjectMoveServices(result.SkippedServices)
	if opts.DryRun {
		return c.renderProjectMoveResult(result)
	}
	if result.Blocked {
		return projectMoveBlockedError(project.Name, result.Skipped, result.SkippedServices)
	}

	for _, service := range result.Services {
		response, err := client.RecoverBackingService(service.ID, false, databaseLocalizeRequest{TargetRuntimeID: targetRuntimeID, StorageClassName: storageClass})
		if err != nil {
			return fmt.Errorf("recover service %s: %w", formatDisplayName(service.Name, service.ID, c.showIDs()), err)
		}
		if response.Operation == nil {
			return fmt.Errorf("recovery service %s returned no operation", service.ID)
		}
		final, err := c.waitForOperations(client, []model.Operation{*response.Operation})
		if err != nil {
			return err
		}
		result.Operations = append(result.Operations, final...)
		updated, err := client.GetBackingService(service.ID)
		if err != nil {
			return err
		}
		if strings.TrimSpace(backingServiceRuntimeID(updated)) != targetRuntimeID {
			return fmt.Errorf("service %s completed without target runtime convergence", service.ID)
		}
		result.UpdatedServices = append(result.UpdatedServices, updated)
	}

	// Database localization changes the app's server-side move evidence. Do not
	// queue a volume copy until the refreshed preflight accepts it.
	for _, planned := range result.Apps {
		app, err := client.GetApp(planned.ID)
		if err != nil {
			return err
		}
		offlineVolume, err := offlineProjectMoveVolume(app)
		if err != nil {
			return err
		}
		var impact model.AppMoveDryRunResponse
		if offlineVolume {
			impact, err = client.MigrateAppWithOfflineStorageDryRun(app.ID, targetRuntimeID, storageClass)
		} else {
			impact, err = client.MigrateAppDryRun(app.ID, targetRuntimeID)
		}
		if err != nil {
			return err
		}
		result.AppImpacts = append(result.AppImpacts, impact.Impact)
		if !impact.Impact.Pass {
			return fmt.Errorf("app %s remains blocked after database recovery: %s", app.ID, strings.Join(impact.Impact.Blockers, "; "))
		}
		var queued operationResponse
		if offlineVolume {
			queued, err = client.MigrateAppWithOfflineStorage(app.ID, targetRuntimeID, storageClass)
		} else {
			queued, err = client.MigrateApp(app.ID, targetRuntimeID)
		}
		if err != nil {
			return fmt.Errorf("move app %s: %w", app.ID, err)
		}
		final, err := c.waitForOperations(client, []model.Operation{queued.Operation})
		if err != nil {
			return err
		}
		result.Operations = append(result.Operations, final...)
		updated, err := client.GetApp(app.ID)
		if err != nil {
			return err
		}
		if strings.TrimSpace(appEffectiveRuntimeID(updated)) != targetRuntimeID || updated.Spec.Replicas != app.Spec.Replicas {
			return fmt.Errorf("app %s completed without target runtime or replica-state convergence", app.ID)
		}
	}
	return c.renderProjectMoveResult(result)
}

func offlineProjectMoveVolume(app model.App) (bool, error) {
	if app.Spec.Workspace != nil {
		return false, fmt.Errorf("legacy workspace is not supported by offline PVC migration")
	}
	if app.Spec.PersistentStorage == nil || model.AppPersistentStorageSpecIsMigratable(app.Spec.PersistentStorage) {
		return false, nil
	}
	mode, err := model.NormalizeAppPersistentStorageMode(app.Spec.PersistentStorage.Mode)
	if err != nil || mode != model.AppPersistentStorageModeDedicatedPVC {
		return false, fmt.Errorf("unsupported persistent storage mode")
	}
	if app.Spec.Replicas != 0 || app.Status.CurrentReplicas != 0 {
		return false, fmt.Errorf("dedicated PVC requires a stopped app with zero current replicas")
	}
	return true, nil
}

func offlineProjectMoveHasServiceDependency(app model.App, services []model.BackingService) bool {
	for _, service := range services {
		if strings.TrimSpace(service.OwnerAppID) == strings.TrimSpace(app.ID) {
			return true
		}
		for _, binding := range app.Bindings {
			if strings.TrimSpace(binding.ServiceID) == strings.TrimSpace(service.ID) && strings.TrimSpace(service.ID) != "" {
				return true
			}
		}
	}
	return false
}
