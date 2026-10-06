package api

import (
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/storagerecovery"
	"k8s.io/apimachinery/pkg/api/resource"
)

func (s *Server) handleRecoverBackingService(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform.admin is required for host storage recovery")
		return
	}
	service, allowed := s.loadAuthorizedBackingService(w, r, principal)
	if !allowed {
		return
	}
	if !isMigratableBackingService(service) || strings.TrimSpace(service.OwnerAppID) != "" {
		httpx.WriteError(w, http.StatusBadRequest, "recovery requires an independent managed postgres backing service")
		return
	}
	var req struct {
		DryRun           bool   `json:"dry_run"`
		TargetRuntimeID  string `json:"target_runtime_id"`
		TargetNodeName   string `json:"target_node_name"`
		StorageSize      string `json:"storage_size"`
		StorageClassName string `json:"storage_class_name"`
	}
	req.DryRun = true
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	runtimeID, storageClass := strings.TrimSpace(req.TargetRuntimeID), strings.TrimSpace(req.StorageClassName)
	if runtimeID == "" || storageClass == "" {
		httpx.WriteError(w, http.StatusBadRequest, "target_runtime_id and storage_class_name are required")
		return
	}
	if size := strings.TrimSpace(req.StorageSize); size != "" {
		quantity, err := resource.ParseQuantity(size)
		if err != nil || quantity.Sign() <= 0 {
			httpx.WriteError(w, http.StatusBadRequest, "storage_size must be a positive quantity")
			return
		}
	}
	runtime, err := s.store.GetRuntime(runtimeID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	visible, err := s.store.RuntimeVisibleToTenant(runtimeID, service.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !visible {
		httpx.WriteError(w, http.StatusForbidden, "target runtime is not visible to this tenant")
		return
	}
	if runtime.Type != model.RuntimeTypeManagedOwned && runtime.Type != model.RuntimeTypeManagedShared {
		httpx.WriteError(w, http.StatusBadRequest, "recovery requires a managed target runtime")
		return
	}
	app, err := s.backingServiceSwitchoverApp(service)
	if err != nil {
		httpx.WriteError(w, http.StatusConflict, "recovery requires exactly one bound app to coordinate the database switchover")
		return
	}
	desired := app.Spec
	pg := *cloneAppPostgresSpec(service.Spec.Postgres)
	pg.RuntimeID = runtimeID
	pg.FailoverTargetRuntimeID = ""
	pg.PrimaryNodeName = strings.TrimSpace(req.TargetNodeName)
	pg.PrimaryPlacementPendingRebalance = false
	pg.Instances = 1
	pg.SynchronousReplicas = 0
	pg.StorageClassName = storageClass
	if size := strings.TrimSpace(req.StorageSize); size != "" {
		pg.StorageSize = size
	}
	desired.Postgres = &pg
	plan := map[string]any{
		"action": "database-storage-recovery", "service_id": service.ID,
		"target_runtime_id": runtimeID, "target_node_name": pg.PrimaryNodeName,
		"storage_class_name": storageClass, "storage_size": pg.StorageSize,
		"live_preflight_pending": true,
		"stages":                 []string{"verify source cluster and bound primary data claim", "preserve the largest observed storage size", "fence and verify stopped source without expanding it", "copy and verify physical files on target storage", "recover and verify matching system identity before stable endpoint cutover"},
	}
	ops, err := s.store.ListOperationsByApp(app.TenantID, true, app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	for _, existing := range ops {
		if existing.Status != model.OperationStatusPending && existing.Status != model.OperationStatusRunning {
			continue
		}
		if existing.Type != storagerecovery.OperationType || existing.ServiceID != service.ID || existing.TargetRuntimeID != runtimeID || existing.DesiredSpec == nil || existing.DesiredSpec.Postgres == nil || existing.DesiredSpec.Postgres.PrimaryNodeName != pg.PrimaryNodeName || existing.DesiredSpec.Postgres.StorageClassName != pg.StorageClassName || existing.DesiredSpec.Postgres.StorageSize != pg.StorageSize {
			httpx.WriteError(w, http.StatusConflict, "a conflicting app operation is active")
			return
		}
		httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"backing_service": cloneBackingService(service), "operation": sanitizeOperationForAPI(existing), "plan": plan, "resumed": true})
		return
	}
	if req.DryRun {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"backing_service": cloneBackingService(service), "plan": plan, "dry_run": true})
		return
	}
	op, err := s.store.CreateOperation(model.Operation{TenantID: service.TenantID, Type: storagerecovery.OperationType, RequestedByType: principal.ActorType, RequestedByID: principal.ActorID, AppID: app.ID, ServiceID: service.ID, TargetRuntimeID: runtimeID, DesiredSpec: &desired})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "backing_service.recover", "operation", op.ID, service.TenantID, map[string]string{"app_id": app.ID, "service_id": service.ID, "target_runtime_id": runtimeID, "storage_class_name": storageClass})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"backing_service": cloneBackingService(service), "operation": sanitizeOperationForAPI(op), "plan": plan})
}
