package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func (s *Server) handleSuspendBackingService(w http.ResponseWriter, r *http.Request) {
	s.handleBackingServiceLifecycle(w, r, true)
}

func (s *Server) handleResumeBackingService(w http.ResponseWriter, r *http.Request) {
	s.handleBackingServiceLifecycle(w, r, false)
}

func (s *Server) handleBackingServiceLifecycle(w http.ResponseWriter, r *http.Request, suspended bool) {
	principal := mustPrincipal(r)
	action := "backing_service.resume"
	operationType := model.OperationTypeDatabaseResume
	if suspended {
		action = "backing_service.suspend"
		operationType = model.OperationTypeDatabaseSuspend
	}
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("project.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or project.write scope")
		return
	}

	service, allowed := s.loadAuthorizedBackingService(w, r, principal)
	if !allowed {
		return
	}
	metadata := backingServiceLifecycleAuditMetadata(service, suspended)
	if !isMigratableBackingService(service) {
		metadata["result"] = "rejected_not_managed_postgres"
		s.appendAudit(principal, action, "backing_service", service.ID, service.TenantID, metadata)
		httpx.WriteError(w, http.StatusBadRequest, "backing service is not managed PostgreSQL")
		return
	}

	app, err := s.backingServiceSwitchoverApp(service)
	if err != nil {
		metadata["result"] = lifecycleStoreErrorResult(err)
		s.appendAudit(principal, action, "backing_service", service.ID, service.TenantID, metadata)
		s.writeStoreError(w, err)
		return
	}
	metadata["app_id"] = app.ID
	var consumerApps []model.App
	if suspended {
		consumerApps, err = s.backingServiceConsumerApps(service, app)
		if err != nil {
			metadata["result"] = lifecycleStoreErrorResult(err)
			s.appendAudit(principal, action, "backing_service", service.ID, service.TenantID, metadata)
			s.writeStoreError(w, err)
			return
		}
		if blockingApp := firstAppWithDesiredReplicas(consumerApps); blockingApp != nil {
			metadata["result"] = "rejected_bound_app_running"
			addBlockingAppAuditMetadata(metadata, *blockingApp)
			s.appendAudit(principal, action, "backing_service", service.ID, service.TenantID, metadata)
			httpx.WriteError(w, http.StatusConflict, "stop every app bound to this backing service before suspending it")
			return
		}
	}

	activeLifecycle, activeFound, err := s.store.GetActiveManagedPostgresLifecycleOperation(
		service.TenantID,
		app.ID,
		service.ID,
		operationType,
	)
	if err != nil {
		metadata["result"] = lifecycleStoreErrorResult(err)
		s.appendAudit(principal, action, "backing_service", service.ID, service.TenantID, metadata)
		s.writeStoreError(w, err)
		return
	}
	if activeFound {
		httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
			"backing_service": redactLifecycleBackingService(projectBackingServiceLifecycleState(service, suspended)),
			"operation":       redactOperationForDebugBundle(activeLifecycle),
			"already_current": false,
		})
		return
	}

	managedApps, err := s.managedAppInventory(r.Context(), true)
	if err != nil {
		metadata["result"] = "observation_unavailable"
		s.appendAudit(principal, action, "backing_service", service.ID, service.TenantID, metadata)
		httpx.WriteError(w, http.StatusServiceUnavailable, "fresh managed app runtime status is unavailable; lifecycle operation was not queued")
		return
	}
	if suspended {
		blockingApp, err := appWithoutFreshZeroReplicaProof(consumerApps, managedApps)
		if err != nil {
			metadata["result"] = "observation_unavailable"
			s.appendAudit(principal, action, "backing_service", service.ID, service.TenantID, metadata)
			httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		if blockingApp != nil {
			metadata["result"] = "rejected_bound_app_running"
			addBlockingAppAuditMetadata(metadata, *blockingApp)
			s.appendAudit(principal, action, "backing_service", service.ID, service.TenantID, metadata)
			httpx.WriteError(w, http.StatusConflict, "fresh runtime status does not confirm every bound app is scaled to zero")
			return
		}
	}
	service = firstBackingServiceOrDefault(overlayBackingServiceRuntimeStatusesWithInventory([]model.BackingService{service}, managedApps), service)
	if service.RuntimeStatus == nil || strings.EqualFold(strings.TrimSpace(service.RuntimeStatus.Phase), model.ManagedPostgresRuntimePhaseUnknown) {
		metadata["result"] = "observation_unavailable"
		s.appendAudit(principal, action, "backing_service", service.ID, service.TenantID, metadata)
		httpx.WriteError(w, http.StatusServiceUnavailable, "fresh managed PostgreSQL runtime status is unavailable; lifecycle operation was not queued")
		return
	}

	if backingServiceLifecycleConverged(service, suspended) {
		metadata["result"] = "already_current"
		if service.RuntimeStatus != nil {
			metadata["observed_phase"] = strings.TrimSpace(service.RuntimeStatus.Phase)
		}
		s.appendAudit(principal, action, "backing_service", service.ID, service.TenantID, metadata)
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"backing_service": redactLifecycleBackingService(service),
			"already_current": true,
		})
		return
	}

	desiredSpec := cloneAppSpec(app.Spec)
	postgres := cloneAppPostgresSpec(service.Spec.Postgres)
	postgres.Suspended = suspended
	desiredSpec.Postgres = postgres
	op, createResult, err := s.store.CreateOperationWithResult(model.Operation{
		TenantID:        service.TenantID,
		Type:            operationType,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		AppID:           app.ID,
		ServiceID:       service.ID,
		DesiredSpec:     &desiredSpec,
	})
	if err != nil {
		metadata["result"] = lifecycleStoreErrorResult(err)
		s.appendAudit(principal, action, "backing_service", service.ID, service.TenantID, metadata)
		s.writeBackingServiceLifecycleStoreError(w, err)
		return
	}

	if createResult.Created {
		metadata["result"] = "accepted"
		metadata["operation_id"] = op.ID
		s.appendAudit(principal, action, "operation", op.ID, service.TenantID, metadata)
	}
	projectedService := projectBackingServiceLifecycleState(service, suspended)
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"backing_service": redactLifecycleBackingService(projectedService),
		"operation":       redactOperationForDebugBundle(op),
		"already_current": false,
	})
}

func backingServiceLifecycleAuditMetadata(service model.BackingService, suspended bool) map[string]string {
	return map[string]string{
		"service_id":        service.ID,
		"name":              service.Name,
		"project_id":        service.ProjectID,
		"desired_suspended": strconv.FormatBool(suspended),
	}
}

func lifecycleStoreErrorResult(err error) string {
	switch {
	case errors.Is(err, store.ErrManagedPostgresBackupInProgressConflict):
		return "rejected_backup_in_progress"
	case errors.Is(err, store.ErrManagedPostgresImportInProgressConflict):
		return "rejected_import_in_progress"
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

func (s *Server) writeBackingServiceLifecycleStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrManagedPostgresBackupInProgressConflict):
		httpx.WriteError(w, http.StatusConflict, store.ManagedPostgresBackupInProgressConflictMessage)
	case errors.Is(err, store.ErrManagedPostgresImportInProgressConflict):
		httpx.WriteError(w, http.StatusConflict, store.ManagedPostgresImportInProgressConflictMessage)
	default:
		s.writeStoreError(w, err)
	}
}

func backingServiceLifecycleConverged(service model.BackingService, suspended bool) bool {
	if service.Spec.Postgres == nil || service.Spec.Postgres.Suspended != suspended || service.RuntimeStatus == nil {
		return false
	}
	status := service.RuntimeStatus
	if suspended {
		return strings.EqualFold(strings.TrimSpace(status.Phase), model.ManagedPostgresRuntimePhaseSuspended) && status.ReadyInstances == 0
	}
	return strings.EqualFold(strings.TrimSpace(status.Phase), model.ManagedPostgresRuntimePhaseActive) &&
		status.DesiredInstances > 0 &&
		status.ReadyInstances > 0
}

func (s *Server) backingServiceConsumerApps(service model.BackingService, owner model.App) ([]model.App, error) {
	appsUsingService := []model.App{owner}
	apps, err := s.store.ListApps(service.TenantID, false)
	if err != nil {
		return nil, err
	}
	for _, app := range apps {
		if strings.TrimSpace(app.ID) == strings.TrimSpace(owner.ID) {
			continue
		}
		if !appHasServiceBinding(app, service.ID) {
			continue
		}
		appsUsingService = append(appsUsingService, app)
	}
	return appsUsingService, nil
}

func firstAppWithDesiredReplicas(apps []model.App) *model.App {
	for _, app := range apps {
		if app.Spec.Replicas <= 0 {
			continue
		}
		appCopy := app
		return &appCopy
	}
	return nil
}

func appWithoutFreshZeroReplicaProof(apps []model.App, managedApps map[string]runtime.ManagedAppObject) (*model.App, error) {
	for _, app := range apps {
		managed, found := managedApps[strings.TrimSpace(app.ID)]
		if !found || strings.TrimSpace(managed.Spec.AppID) != strings.TrimSpace(app.ID) || strings.TrimSpace(managed.Status.Phase) == "" {
			return nil, errors.New("fresh managed app runtime status is missing; suspend was not queued")
		}
		if managed.Status.DesiredReplicas == 0 && managed.Status.ReadyReplicas == 0 {
			continue
		}
		appCopy := app
		appCopy.Spec.Replicas = managed.Status.DesiredReplicas
		appCopy.Status.CurrentReplicas = managed.Status.ReadyReplicas
		return &appCopy, nil
	}
	return nil, nil
}

func addBlockingAppAuditMetadata(metadata map[string]string, app model.App) {
	metadata["blocking_app_id"] = app.ID
	metadata["blocking_desired_replicas"] = strconv.Itoa(app.Spec.Replicas)
	metadata["blocking_ready_replicas"] = strconv.Itoa(app.Status.CurrentReplicas)
}

func projectBackingServiceLifecycleState(service model.BackingService, suspended bool) model.BackingService {
	out := cloneBackingService(service)
	if out.Spec.Postgres != nil {
		out.Spec.Postgres.Suspended = suspended
	}
	desiredInstances := 1
	if out.Spec.Postgres != nil && out.Spec.Postgres.Instances > 0 {
		desiredInstances = out.Spec.Postgres.Instances
	}
	readyInstances := 0
	message := "managed PostgreSQL resume accepted"
	phase := model.ManagedPostgresRuntimePhaseResuming
	if out.RuntimeStatus != nil {
		readyInstances = out.RuntimeStatus.ReadyInstances
		desiredInstances = out.RuntimeStatus.DesiredInstances
		if desiredInstances <= 0 {
			desiredInstances = 1
		}
	}
	if suspended {
		message = "managed PostgreSQL suspend accepted"
		phase = model.ManagedPostgresRuntimePhaseSuspending
	}
	out.RuntimeStatus = &model.BackingServiceRuntimeStatus{
		Phase:            phase,
		Message:          message,
		ReadyInstances:   readyInstances,
		DesiredInstances: desiredInstances,
	}
	return out
}

func redactLifecycleBackingService(service model.BackingService) model.BackingService {
	redacted := redactBackingServicesForDebugBundle([]model.BackingService{service})
	if len(redacted) == 0 {
		return model.BackingService{}
	}
	return redacted[0]
}

func (s *Server) overlayBackingServiceRuntimeStatuses(ctx context.Context, services []model.BackingService) []model.BackingService {
	if len(services) == 0 {
		return services
	}
	_, cacheOK, cacheExpired := s.managedAppStatusCache.getList()
	usedFreshCache := cacheOK && !cacheExpired
	managedApps, err := s.managedAppInventory(ctx, false)
	if err != nil {
		if s.shouldLogManagedAppStatusError(err) && s.log != nil {
			s.log.Printf("managed backing service status overlay error: %v", err)
		}
		return services
	}

	overlaid := overlayBackingServiceRuntimeStatusesWithInventory(services, managedApps)
	if !usedFreshCache || !backingServicesNeedFreshRuntimeObservation(overlaid) {
		return overlaid
	}
	fresh, err := s.managedAppInventory(ctx, true)
	if err != nil {
		if s.shouldLogManagedAppStatusError(err) && s.log != nil {
			s.log.Printf("managed backing service terminal status refresh error: %v", err)
		}
		return overlaid
	}
	return overlayBackingServiceRuntimeStatusesWithInventory(services, fresh)
}

func backingServicesNeedFreshRuntimeObservation(services []model.BackingService) bool {
	for _, service := range services {
		if !isMigratableBackingService(service) || service.Spec.Postgres == nil {
			continue
		}
		if service.RuntimeStatus == nil || !backingServiceLifecycleConverged(service, service.Spec.Postgres.Suspended) {
			return true
		}
	}
	return false
}

func overlayBackingServiceRuntimeStatusesWithInventory(services []model.BackingService, managedApps map[string]runtime.ManagedAppObject) []model.BackingService {
	statusByOwnerAndService := make(map[string]runtime.ManagedBackingServiceStatus)
	statusByService := make(map[string]runtime.ManagedBackingServiceStatus)
	for appID, managed := range managedApps {
		for _, status := range managed.Status.BackingServices {
			serviceID := strings.TrimSpace(status.ServiceID)
			if serviceID == "" {
				continue
			}
			statusByOwnerAndService[strings.TrimSpace(appID)+"\x00"+serviceID] = status
			statusByService[serviceID] = status
		}
	}

	out := cloneBackingServices(services)
	for index := range out {
		serviceID := strings.TrimSpace(out[index].ID)
		status, ok := statusByOwnerAndService[strings.TrimSpace(out[index].OwnerAppID)+"\x00"+serviceID]
		if !ok {
			status, ok = statusByService[serviceID]
		}
		if !ok {
			continue
		}
		phase := strings.TrimSpace(status.Phase)
		if phase == "" {
			phase = model.ManagedPostgresRuntimePhaseUnknown
		}
		out[index].RuntimeStatus = &model.BackingServiceRuntimeStatus{
			Phase:            phase,
			Message:          strings.TrimSpace(status.Message),
			ReadyInstances:   status.ReadyInstances,
			DesiredInstances: status.DesiredInstances,
		}
		if startedAt, ok := parseManagedBackingServiceTime(status.CurrentRuntimeStartedAt); ok {
			out[index].CurrentRuntimeStartedAt = &startedAt
		}
		if readyAt, ok := parseManagedBackingServiceTime(status.CurrentRuntimeReadyAt); ok {
			out[index].CurrentRuntimeReadyAt = &readyAt
		}
	}
	return out
}

func parseManagedBackingServiceTime(value string) (time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func (s *Server) managedAppInventory(ctx context.Context, requireFresh bool) (map[string]runtime.ManagedAppObject, error) {
	cached, ok, expired := s.managedAppStatusCache.getList()
	if !requireFresh && ok && !expired {
		return cached.items, nil
	}
	fresh, err := s.refreshManagedAppStatuses(ctx)
	if err != nil {
		if !requireFresh && ok {
			return cached.items, nil
		}
		return nil, err
	}
	return fresh.items, nil
}
