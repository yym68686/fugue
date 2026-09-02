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
		GeneratedAt:     time.Now().UTC(),
	}
	if targetRuntimeID == "" {
		impact.Checks = append(impact.Checks, model.StoreInvariantCheck{Name: "target_runtime", Pass: false, Message: "target_runtime_id is required"})
		impact.Blockers = append(impact.Blockers, "target runtime is required")
		impact.Pass = false
		return impact
	}
	targetRuntime, runtimeErr := s.store.GetRuntime(targetRuntimeID)
	if runtimeErr != nil {
		impact.Checks = append(impact.Checks, model.StoreInvariantCheck{Name: "target_runtime", Pass: false, Message: runtimeErr.Error()})
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
	if targetRuntimeID != "" {
		visible, err := s.store.RuntimeVisibleToTenant(targetRuntimeID, app.TenantID, false)
		if err != nil {
			appendCheck("target_runtime_access", false, err.Error())
		} else if !visible {
			appendCheck("target_runtime_access", false, "target runtime is not visible to the app tenant")
		} else {
			appendCheck("target_runtime_access", true, "target runtime is visible to the app tenant")
		}
		desiredSpec := app.Spec
		prepareMigrateDesiredSpec(app, &desiredSpec, targetRuntimeID)
		if err := s.store.ValidateAppSpecRuntimeReservations(app.ProjectID, desiredSpec); err != nil {
			appendCheck("project_runtime_reservation", false, err.Error())
		} else {
			appendCheck("project_runtime_reservation", true, "target runtime is not reserved for another project")
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
			appendCheck("persistent_storage_shared", true, "legacy shared project RWX can be remounted on target runtime")
		default:
			appendCheck("persistent_storage_migration", false, "persistent storage must be movable_rwo before app move")
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
		if runtimeErr == nil && targetRuntime.Type != model.RuntimeTypeManagedOwned && targetRuntime.Type != model.RuntimeTypeManagedShared {
			appendCheck("managed_postgres_target_runtime", false, "invalid input: managed Postgres requires a managed target runtime")
		}
		databaseRuntimeID := strings.TrimSpace(database.RuntimeID)
		if databaseRuntimeID == "" {
			databaseRuntimeID = strings.TrimSpace(app.Spec.RuntimeID)
		}
		requiresLocalization := databaseRuntimeID != "" && targetRuntimeID != "" && databaseRuntimeID != targetRuntimeID
		databaseEvidence := s.managedPostgresMoveEvidence(app, requiresLocalization)
		impact.Databases = append(impact.Databases, model.AppMoveDatabaseImpact{
			ServiceName:          strings.TrimSpace(database.ServiceName),
			CurrentRuntimeID:     databaseRuntimeID,
			TargetRuntimeID:      targetRuntimeID,
			BackupStatus:         databaseEvidence.backupStatus,
			RestoreStatus:        databaseEvidence.restoreStatus,
			GrantVerification:    databaseEvidence.grantVerification,
			RequiresLocalization: requiresLocalization,
		})
		if requiresLocalization {
			// App migration does not move an app-owned database.  The database
			// localization operation is a real prerequisite and must converge
			// before the app operation is accepted; previously these checks were
			// hard-coded true and let the app race the database switchover.
			appendCheck("managed_postgres_localization", false, "managed Postgres must be localized to the target runtime before app move")
		} else {
			appendCheck("managed_postgres_localization", true, "managed Postgres is already on the target runtime")
		}
		appendCheck("managed_postgres_backup", managedPostgresEvidenceCheckPass(databaseEvidence.backupStatus, databaseEvidence.backupReady, requiresLocalization), fmt.Sprintf("managed Postgres backup evidence: %s", databaseEvidence.backupStatus))
		appendCheck("managed_postgres_restore", managedPostgresEvidenceCheckPass(databaseEvidence.restoreStatus, databaseEvidence.restoreReady, requiresLocalization), fmt.Sprintf("managed Postgres restore evidence: %s", databaseEvidence.restoreStatus))
		appendCheck("managed_postgres_grants", managedPostgresEvidenceCheckPass(databaseEvidence.grantVerification, databaseEvidence.grantsReady, requiresLocalization), fmt.Sprintf("managed Postgres grant evidence: %s", databaseEvidence.grantVerification))
	}
	imageReady, imageMessage, imageErr := s.appMoveImageBlobEvidence(app)
	if imageErr != nil {
		appendCheck("image_blob_integrity", false, imageErr.Error())
	} else {
		appendCheck("image_blob_integrity", imageReady, imageMessage)
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
	impact.OperationChain = appMoveOperationChain(impact)
	return impact
}

func appMoveOperationChain(impact model.AppMoveImpact) []string {
	chain := make([]string, 0, 8)
	for _, database := range impact.Databases {
		if database.RequiresLocalization {
			chain = append(chain, "database_localize")
			break
		}
	}
	for _, volume := range impact.Volumes {
		if volume.Strategy != "rwo_snapshot_restore" {
			continue
		}
		chain = append(chain, "quiesce", "snapshot_or_dump", "target_pvc_create", "restore", "permission_verify")
		break
	}
	return append(chain, "switch", "cleanup")
}

// The app-move API does not itself execute a backup/restore.  App-owned CNPG
// databases use the explicit localization operation instead.  We therefore
// only block on an *active* backup policy with missing evidence (or an
// unavailable ledger); a tenant with no backup policy is not accidentally
// blocked from the supported replication-based localization path.
func managedPostgresEvidenceCheckPass(status string, ready, requiresLocalization bool) bool {
	if !requiresLocalization || ready {
		return true
	}
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "", "not_required", "not_configured", "disabled", "pending":
		return true
	default:
		return false
	}
}
