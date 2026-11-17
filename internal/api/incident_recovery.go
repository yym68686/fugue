package api

import (
	"net/http"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (s *Server) handleListAppRecoveries(w http.ResponseWriter, r *http.Request) {
	app, ok := s.loadAuthorizedAppMetadata(w, r, mustPrincipal(r))
	if !ok {
		return
	}
	records, err := s.store.ListAppRecoveries(app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	views := []model.AppRecovery{}
	for _, record := range records {
		views = append(views, record.AppRecovery)
	}
	httpx.WriteJSON(w, http.StatusOK, struct {
		AppID      string              `json:"app_id"`
		Recoveries []model.AppRecovery `json:"recoveries"`
	}{app.ID, views})
}

func (s *Server) handleGetAppRecovery(w http.ResponseWriter, r *http.Request) {
	app, ok := s.loadAuthorizedAppMetadata(w, r, mustPrincipal(r))
	if !ok {
		return
	}
	records, err := s.store.ListAppRecoveries(app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	for _, record := range records {
		if record.ID == r.PathValue("recovery_id") {
			httpx.WriteJSON(w, http.StatusOK, record.AppRecovery)
			return
		}
	}
	httpx.WriteError(w, http.StatusNotFound, "recovery not found")
}

func (s *Server) handleStartAppRecovery(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() && !p.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
		return
	}
	app, ok := s.loadAuthorizedAppMetadata(w, r, p)
	if !ok {
		return
	}
	// Hydrate the execution snapshot only after app authorization.
	var err error
	app, err = s.store.GetApp(app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	rt, err := s.store.GetRuntime(app.Spec.RuntimeID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !model.RuntimeIsInternal(rt) || rt.Type != model.RuntimeTypeManagedShared {
		httpx.WriteError(w, http.StatusBadRequest, "incident recovery requires a managed Kubernetes runtime")
		return
	}
	var req model.AppRecoveryStartRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.Validate(app.Spec); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	record, err := s.store.StartAppRecovery(app, req)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(p, "app.recovery.start", "app_recovery", record.ID, app.TenantID, map[string]string{"app_id": app.ID, "storage_strategy": "fresh"})
	httpx.WriteJSON(w, http.StatusAccepted, record.AppRecovery)
}

func (s *Server) handleReleaseAppRecovery(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() && !p.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
		return
	}
	app, ok := s.loadAuthorizedAppMetadata(w, r, p)
	if !ok {
		return
	}
	record, err := s.store.ReleaseAppRecovery(app.ID, r.PathValue("recovery_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(p, "app.recovery.release", "app_recovery", record.ID, app.TenantID, map[string]string{"app_id": app.ID})
	httpx.WriteJSON(w, http.StatusAccepted, record.AppRecovery)
}
