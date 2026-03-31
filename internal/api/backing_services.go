package api

import (
	"net/http"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) handleListBackingServices(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	services, err := s.store.ListBackingServices(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	services = s.overlayCurrentResourceUsageOnServices(r.Context(), services)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"backing_services": cloneBackingServices(services),
	})
}

func (s *Server) handleCreateBackingService(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("project.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or project.write scope")
		return
	}
	var req struct {
		TenantID    string                   `json:"tenant_id"`
		ProjectID   string                   `json:"project_id"`
		Name        string                   `json:"name"`
		Description string                   `json:"description"`
		Spec        model.BackingServiceSpec `json:"spec"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tenantID, ok := s.resolveTenantID(principal, req.TenantID)
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot create backing service for another tenant")
		return
	}
	service, err := s.store.CreateBackingService(tenantID, req.ProjectID, req.Name, req.Description, req.Spec)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "backing_service.create", "backing_service", service.ID, service.TenantID, map[string]string{"name": service.Name, "project_id": service.ProjectID})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{
		"backing_service": cloneBackingService(service),
	})
}

func (s *Server) handleGetBackingService(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	service, err := s.store.GetBackingService(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() && service.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "backing service is not visible to this tenant")
		return
	}
	service = firstBackingServiceOrDefault(s.overlayCurrentResourceUsageOnServices(r.Context(), []model.BackingService{service}), service)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"backing_service": cloneBackingService(service),
	})
}

func (s *Server) handleDeleteBackingService(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("project.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or project.write scope")
		return
	}
	service, allowed := s.loadAuthorizedBackingService(w, r, principal)
	if !allowed {
		return
	}
	service, err := s.store.DeleteBackingService(service.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "backing_service.delete", "backing_service", service.ID, service.TenantID, map[string]string{"name": service.Name, "project_id": service.ProjectID})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"backing_service": cloneBackingService(service),
	})
}

func (s *Server) handleListAppBindings(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	app = s.overlayCurrentResourceUsageOnApp(r.Context(), app)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"bindings":         cloneServiceBindings(app.Bindings),
		"backing_services": cloneBackingServices(app.BackingServices),
	})
}

func (s *Server) handleCreateAppBinding(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.deploy scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	var req struct {
		ServiceID string            `json:"service_id"`
		Alias     string            `json:"alias"`
		Env       map[string]string `json:"env"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	binding, err := s.store.BindBackingService(app.TenantID, app.ID, req.ServiceID, req.Alias, req.Env)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	op, err := s.queueAppDeployOperation(principal, app)
	if err != nil {
		if _, rollbackErr := s.store.UnbindBackingService(binding.ID); rollbackErr != nil {
			s.log.Printf("rollback binding create failed for app=%s binding=%s: %v", app.ID, binding.ID, rollbackErr)
		}
		s.writeStoreError(w, err)
		return
	}
	service, err := s.store.GetBackingService(binding.ServiceID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "service_binding.create", "service_binding", binding.ID, app.TenantID, map[string]string{"app_id": app.ID, "service_id": binding.ServiceID})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"binding":         cloneServiceBinding(binding),
		"backing_service": cloneBackingService(service),
		"operation":       sanitizeOperationForAPI(op),
	})
}

func (s *Server) handleDeleteAppBinding(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.deploy scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	binding, found := appBindingByID(app, r.PathValue("binding_id"))
	if !found {
		s.writeStoreError(w, store.ErrNotFound)
		return
	}
	binding, err := s.store.UnbindBackingService(binding.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	op, err := s.queueAppDeployOperation(principal, app)
	if err != nil {
		if _, rollbackErr := s.store.BindBackingService(binding.TenantID, binding.AppID, binding.ServiceID, binding.Alias, binding.Env); rollbackErr != nil {
			s.log.Printf("rollback binding delete failed for app=%s binding=%s: %v", app.ID, binding.ID, rollbackErr)
		}
		s.writeStoreError(w, err)
		return
	}
	service, err := s.store.GetBackingService(binding.ServiceID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "service_binding.delete", "service_binding", binding.ID, app.TenantID, map[string]string{"app_id": app.ID, "service_id": binding.ServiceID})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"binding":         cloneServiceBinding(binding),
		"backing_service": cloneBackingService(service),
		"operation":       sanitizeOperationForAPI(op),
	})
}

func (s *Server) loadAuthorizedBackingService(w http.ResponseWriter, r *http.Request, principal model.Principal) (model.BackingService, bool) {
	service, err := s.store.GetBackingService(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return model.BackingService{}, false
	}
	if !principal.IsPlatformAdmin() && service.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "backing service is not visible to this tenant")
		return model.BackingService{}, false
	}
	return service, true
}

func (s *Server) queueAppDeployOperation(principal model.Principal, app model.App) (model.Operation, error) {
	spec := cloneAppSpec(app.Spec)
	return s.store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		AppID:           app.ID,
		DesiredSpec:     &spec,
	})
}

func appBindingByID(app model.App, bindingID string) (model.ServiceBinding, bool) {
	for _, binding := range app.Bindings {
		if binding.ID == bindingID {
			return binding, true
		}
	}
	return model.ServiceBinding{}, false
}

func firstBackingServiceOrDefault(services []model.BackingService, fallback model.BackingService) model.BackingService {
	if len(services) == 0 {
		return fallback
	}
	return services[0]
}
