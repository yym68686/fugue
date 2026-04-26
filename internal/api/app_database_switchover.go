package api

import (
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

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
		TargetNodeName string `json:"target_node_name"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	targetRuntimeID := strings.TrimSpace(app.Spec.RuntimeID)
	if targetRuntimeID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "app runtime_id is required")
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
