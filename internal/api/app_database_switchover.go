package api

import (
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) handleGetAppDatabaseStatus(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.read") && !principal.HasScope("app.write") && !principal.HasScope("app.migrate") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.read, app.write, or app.migrate scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	status := s.managedPostgresStatus(app)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"status": status,
		"app":    sanitizeAppForAPI(app),
	})
}

func (s *Server) handleSwitchoverAppDatabase(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.migrate") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.migrate scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	if store.OwnedManagedPostgresSpec(app) == nil {
		httpx.WriteError(w, http.StatusBadRequest, "managed postgres is not configured for this app")
		return
	}

	var req struct {
		TargetRuntimeID string `json:"target_runtime_id"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	targetRuntimeID := strings.TrimSpace(req.TargetRuntimeID)
	if targetRuntimeID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "target_runtime_id is required")
		return
	}

	op, err := s.store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDatabaseSwitchover,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		AppID:           app.ID,
		TargetRuntimeID: targetRuntimeID,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	s.appendAudit(principal, "app.database_switchover", "operation", op.ID, app.TenantID, map[string]string{
		"app_id":            app.ID,
		"target_runtime_id": targetRuntimeID,
	})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"operation": sanitizeOperationForAPI(op)})
}

func (s *Server) managedPostgresStatus(app model.App) model.ManagedPostgresStatus {
	status := model.ManagedPostgresStatus{
		AppID:             app.ID,
		BackupStatus:      "not_configured",
		RestoreStatus:     "not_configured",
		GrantVerification: "not_configured",
		GeneratedAt:       time.Now().UTC(),
	}
	database := store.OwnedManagedPostgresSpec(app)
	if database == nil {
		return status
	}
	runtimeID := strings.TrimSpace(database.RuntimeID)
	if runtimeID == "" {
		runtimeID = strings.TrimSpace(app.Spec.RuntimeID)
	}
	status.Enabled = true
	status.ServiceName = strings.TrimSpace(database.ServiceName)
	status.Owner = strings.TrimSpace(database.User)
	status.RuntimeID = runtimeID
	status.FailoverRuntimeID = strings.TrimSpace(database.FailoverTargetRuntimeID)
	status.BackupStatus = "required"
	status.RestoreStatus = "required"
	status.GrantVerification = "required"
	if status.FailoverRuntimeID != "" && database.Instances > 1 {
		status.BackupStatus = "replicated"
		status.RestoreStatus = "standby_ready"
		status.GrantVerification = "pending_after_restore"
	}
	for _, service := range app.BackingServices {
		if strings.TrimSpace(service.OwnerAppID) != strings.TrimSpace(app.ID) {
			continue
		}
		if strings.TrimSpace(service.ID) == "" {
			continue
		}
		status.LastBackup = "managed-postgres://" + strings.TrimSpace(service.ID) + "/last-backup"
		status.LastRestore = "managed-postgres://" + strings.TrimSpace(service.ID) + "/last-restore"
		break
	}
	return status
}

func (s *Server) handleLocalizeAppDatabase(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.migrate") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.migrate scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	database := store.OwnedManagedPostgresSpec(app)
	if database == nil {
		httpx.WriteError(w, http.StatusBadRequest, "managed postgres is not configured for this app")
		return
	}

	var req struct {
		TargetNodeName  string `json:"target_node_name"`
		TargetRuntimeID string `json:"target_runtime_id"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	targetRuntimeID := strings.TrimSpace(app.Spec.RuntimeID)
	if requestedRuntimeID := strings.TrimSpace(req.TargetRuntimeID); requestedRuntimeID != "" {
		targetRuntimeID = requestedRuntimeID
	}
	if targetRuntimeID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "target runtime_id is required")
		return
	}

	desiredSpec := app.Spec
	databaseCopy := *database
	if database.Resources != nil {
		resources := *database.Resources
		databaseCopy.Resources = &resources
	}
	databaseCopy.RuntimeID = targetRuntimeID
	databaseCopy.FailoverTargetRuntimeID = ""
	databaseCopy.PrimaryNodeName = strings.TrimSpace(req.TargetNodeName)
	databaseCopy.PrimaryPlacementPendingRebalance = false
	databaseCopy.Instances = 1
	databaseCopy.SynchronousReplicas = 0
	desiredSpec.Postgres = &databaseCopy

	op, err := s.store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDatabaseLocalize,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		AppID:           app.ID,
		TargetRuntimeID: targetRuntimeID,
		DesiredSpec:     &desiredSpec,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	metadata := map[string]string{
		"app_id":            app.ID,
		"target_runtime_id": targetRuntimeID,
	}
	if nodeName := strings.TrimSpace(req.TargetNodeName); nodeName != "" {
		metadata["target_node_name"] = nodeName
	}
	s.appendAudit(principal, "app.database_localize", "operation", op.ID, app.TenantID, metadata)
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"operation": sanitizeOperationForAPI(op)})
}
