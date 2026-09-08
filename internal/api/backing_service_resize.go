package api

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) handleResizeBackingService(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("project.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or project.write scope")
		return
	}

	service, allowed := s.loadAuthorizedBackingService(w, r, principal)
	if !allowed {
		return
	}
	metadata := map[string]string{
		"service_id": service.ID,
		"name":       service.Name,
		"project_id": service.ProjectID,
	}
	if !isMigratableBackingService(service) {
		metadata["result"] = "rejected_not_managed_postgres"
		s.appendAudit(principal, "backing_service.resize", "backing_service", service.ID, service.TenantID, metadata)
		httpx.WriteError(w, http.StatusBadRequest, "backing service is not managed PostgreSQL")
		return
	}

	var req struct {
		RuntimeResources model.ResourceSpec `json:"runtime_resources"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	addManagedPostgresResizeResourceAuditMetadata(metadata, req.RuntimeResources)
	if !validManagedPostgresResizeResourceEnvelope(req.RuntimeResources) {
		metadata["result"] = "invalid_resource_envelope"
		s.appendAudit(principal, "backing_service.resize", "backing_service", service.ID, service.TenantID, metadata)
		s.writeStoreError(w, store.ErrInvalidInput)
		return
	}

	app, err := s.backingServiceSwitchoverApp(service)
	if err != nil {
		metadata["result"] = managedPostgresResizeStoreErrorResult(err)
		s.appendAudit(principal, "backing_service.resize", "backing_service", service.ID, service.TenantID, metadata)
		s.writeBackingServiceResizeStoreError(w, err)
		return
	}
	metadata["app_id"] = app.ID
	op, createResult, err := s.createManagedPostgresResizeOperation(
		app,
		service,
		req.RuntimeResources,
		principal.ActorType,
		principal.ActorID,
	)
	if err != nil {
		metadata["result"] = managedPostgresResizeStoreErrorResult(err)
		s.appendAudit(principal, "backing_service.resize", "backing_service", service.ID, service.TenantID, metadata)
		s.writeBackingServiceResizeStoreError(w, err)
		return
	}

	metadata["operation_id"] = op.ID
	metadata["result"] = "reused_existing"
	if createResult.Created {
		metadata["result"] = "accepted"
	}
	s.appendAudit(principal, "backing_service.resize", "operation", op.ID, service.TenantID, metadata)
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"backing_service": redactLifecycleBackingService(service),
		"operation":       redactOperationForDebugBundle(op),
	})
}

func (s *Server) createManagedPostgresResizeOperation(
	app model.App,
	service model.BackingService,
	runtimeResources model.ResourceSpec,
	actorType, actorID string,
) (model.Operation, store.OperationCreateResult, error) {
	desiredSpec := &model.AppSpec{
		Postgres: &model.AppPostgresSpec{
			RuntimeResources: model.CloneResourceSpec(&runtimeResources),
		},
	}
	return s.store.CreateOperationWithResult(model.Operation{
		TenantID:        service.TenantID,
		Type:            model.OperationTypeDatabaseResize,
		RequestedByType: strings.TrimSpace(actorType),
		RequestedByID:   strings.TrimSpace(actorID),
		AppID:           app.ID,
		ServiceID:       service.ID,
		DesiredSpec:     desiredSpec,
	})
}

func validManagedPostgresResizeResourceEnvelope(resources model.ResourceSpec) bool {
	return resources.CPUMilliCores > 0 &&
		resources.MemoryMebibytes > 0 &&
		resources.CPULimitMilliCores >= resources.CPUMilliCores &&
		resources.MemoryLimitMebibytes >= resources.MemoryMebibytes
}

func addManagedPostgresResizeResourceAuditMetadata(metadata map[string]string, resources model.ResourceSpec) {
	metadata["cpu_millicores"] = strconv.FormatInt(resources.CPUMilliCores, 10)
	metadata["memory_mebibytes"] = strconv.FormatInt(resources.MemoryMebibytes, 10)
	metadata["cpu_limit_millicores"] = strconv.FormatInt(resources.CPULimitMilliCores, 10)
	metadata["memory_limit_mebibytes"] = strconv.FormatInt(resources.MemoryLimitMebibytes, 10)
}

func managedPostgresResizeStoreErrorResult(err error) string {
	switch {
	case errors.Is(err, store.ErrManagedPostgresBackupInProgressConflict):
		return "rejected_backup_in_progress"
	case errors.Is(err, store.ErrManagedPostgresImportInProgressConflict):
		return "rejected_import_in_progress"
	case errors.Is(err, store.ErrManagedPostgresRestoreInProgressConflict):
		return "rejected_restore_in_progress"
	case errors.Is(err, store.ErrBillingCapExceeded):
		return "rejected_billing_cap"
	case errors.Is(err, store.ErrConflict):
		return "conflict"
	case errors.Is(err, store.ErrNotFound):
		return "not_found"
	case errors.Is(err, store.ErrInvalidInput):
		return "invalid_input"
	default:
		return "error"
	}
}

func (s *Server) writeBackingServiceResizeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrManagedPostgresBackupInProgressConflict):
		httpx.WriteError(w, http.StatusConflict, store.ManagedPostgresBackupInProgressConflictMessage)
	case errors.Is(err, store.ErrManagedPostgresImportInProgressConflict):
		httpx.WriteError(w, http.StatusConflict, store.ManagedPostgresImportInProgressConflictMessage)
	case errors.Is(err, store.ErrManagedPostgresRestoreInProgressConflict):
		httpx.WriteError(w, http.StatusConflict, store.ManagedPostgresRestoreInProgressConflictMessage)
	default:
		s.writeStoreError(w, err)
	}
}
