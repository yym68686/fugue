package api

import (
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) buildAppMoveImpact(app model.App, targetRuntimeID string) model.AppMoveImpact {
	targetRuntimeID = strings.TrimSpace(targetRuntimeID)
	impact := model.AppMoveImpact{
		AppID:           app.ID,
		TargetRuntimeID: targetRuntimeID,
		DryRun:          true,
		Pass:            true,
		RollbackRef:     "app-move://" + strings.TrimSpace(app.ID) + "/rollback/" + targetRuntimeID,
		OperationChain:  []string{"quiesce", "snapshot_or_dump", "target_pvc_create", "restore", "permission_verify", "switch", "cleanup"},
		GeneratedAt:     time.Now().UTC(),
	}
	if targetRuntimeID == "" {
		impact.Checks = append(impact.Checks, model.StoreInvariantCheck{Name: "target_runtime", Pass: false, Message: "target_runtime_id is required"})
		impact.Blockers = append(impact.Blockers, "target runtime is required")
		impact.Pass = false
		return impact
	}
	if _, err := s.store.GetRuntime(targetRuntimeID); err != nil {
		impact.Checks = append(impact.Checks, model.StoreInvariantCheck{Name: "target_runtime", Pass: false, Message: err.Error()})
		impact.Blockers = append(impact.Blockers, "target runtime is not available")
		impact.Pass = false
	} else {
		impact.Checks = append(impact.Checks, model.StoreInvariantCheck{Name: "target_runtime", Pass: true, Message: targetRuntimeID})
	}
	appendCheck := func(name string, pass bool, message string) {
		impact.Checks = append(impact.Checks, model.StoreInvariantCheck{Name: name, Pass: pass, Message: message})
		if !pass {
			impact.Blockers = append(impact.Blockers, message)
			impact.Pass = false
		}
	}
	if app.Spec.PersistentStorage != nil {
		storage := app.Spec.PersistentStorage
		mode, err := model.NormalizeAppPersistentStorageMode(storage.Mode)
		if err != nil {
			appendCheck("persistent_storage_mode", false, err.Error())
			mode = strings.TrimSpace(storage.Mode)
		}
		strategy := "shared_project_cache"
		switch mode {
		case model.AppPersistentStorageModeMovableRWO:
			strategy = "rwo_snapshot_restore"
			appendCheck("persistent_storage_class", strings.TrimSpace(storage.StorageClassName) != "", "movable RWO storage requires storage_class_name before move")
		case model.AppPersistentStorageModeSharedProjectRWX:
			strategy = "shared_rwx_no_copy"
			appendCheck("persistent_storage_shared", true, "shared project RWX can be remounted on target runtime")
		default:
			appendCheck("persistent_storage_migration", false, "persistent storage must be movable_rwo or shared_project_rwx before app move")
		}
		impact.Volumes = append(impact.Volumes, model.AppMoveVolumeImpact{
			Mode:             mode,
			StorageClassName: strings.TrimSpace(storage.StorageClassName),
			ClaimName:        strings.TrimSpace(storage.ClaimName),
			MountCount:       len(storage.Mounts),
			Strategy:         strategy,
		})
	}
	if app.Spec.Workspace != nil {
		appendCheck("workspace_migration", false, "legacy workspace storage must be moved to explicit HA storage or per-cell cache before app move")
	}
	database := store.OwnedManagedPostgresSpec(app)
	if database != nil {
		databaseRuntimeID := strings.TrimSpace(database.RuntimeID)
		if databaseRuntimeID == "" {
			databaseRuntimeID = strings.TrimSpace(app.Spec.RuntimeID)
		}
		requiresLocalization := databaseRuntimeID != "" && targetRuntimeID != "" && databaseRuntimeID != targetRuntimeID
		impact.Databases = append(impact.Databases, model.AppMoveDatabaseImpact{
			ServiceName:          strings.TrimSpace(database.ServiceName),
			CurrentRuntimeID:     databaseRuntimeID,
			TargetRuntimeID:      targetRuntimeID,
			BackupStatus:         "required",
			RestoreStatus:        "required",
			GrantVerification:    "required",
			RequiresLocalization: requiresLocalization,
		})
		appendCheck("managed_postgres_backup", true, "managed Postgres backup status must be green before switch")
		appendCheck("managed_postgres_restore", true, "managed Postgres restore status must be verified after restore")
		appendCheck("managed_postgres_grants", true, "owner/grant verification required after restore")
	}
	if app.Route != nil && strings.TrimSpace(app.Route.Hostname) != "" {
		hostname := normalizeExternalAppDomain(app.Route.Hostname)
		impact.Routes = append(impact.Routes, hostname)
		impact.DNS = append(impact.DNS, hostname)
	}
	if domains, err := s.store.ListVerifiedAppDomains(); err == nil {
		for _, domain := range domains {
			if strings.TrimSpace(domain.AppID) != strings.TrimSpace(app.ID) {
				continue
			}
			hostname := normalizeExternalAppDomain(domain.Hostname)
			if hostname == "" {
				continue
			}
			impact.Routes = append(impact.Routes, hostname)
			impact.DNS = append(impact.DNS, hostname)
		}
	}
	for _, binding := range app.Bindings {
		if strings.TrimSpace(binding.ServiceID) != "" {
			impact.Services = append(impact.Services, strings.TrimSpace(binding.ServiceID))
		}
	}
	if len(impact.Volumes) == 0 {
		appendCheck("volumes", true, "no persistent app volumes need migration")
	}
	if len(impact.Databases) == 0 {
		appendCheck("databases", true, "no app-owned managed Postgres database needs migration")
	}
	if len(impact.Routes) == 0 {
		appendCheck("routes", true, "no public routes need DNS/edge switch")
	}
	if !impact.Pass && len(impact.Blockers) == 0 {
		impact.Blockers = append(impact.Blockers, fmt.Sprintf("app %s cannot move to %s", app.ID, targetRuntimeID))
	}
	return impact
}
