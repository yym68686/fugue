package api

import (
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (s *Server) handleGetRuntimeSharing(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	runtimeObj, ok := s.runtimeSharingOwner(w, principal, r.PathValue("id"))
	if !ok {
		return
	}

	grants, err := s.store.ListRuntimeAccessGrants(runtimeObj.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"runtime": runtimeObj,
		"grants":  grants,
	})
}

func (s *Server) handleGrantRuntimeAccess(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("runtime.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing runtime.write scope")
		return
	}
	runtimeObj, ok := s.runtimeSharingOwner(w, principal, r.PathValue("id"))
	if !ok {
		return
	}

	var req struct {
		TenantID string `json:"tenant_id"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	grant, err := s.store.GrantRuntimeAccess(runtimeObj.ID, runtimeObj.TenantID, req.TenantID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "runtime.share.grant", "runtime", runtimeObj.ID, runtimeObj.TenantID, map[string]string{
		"grantee_tenant_id": grant.TenantID,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"grant": grant})
}

func (s *Server) handleRevokeRuntimeAccess(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("runtime.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing runtime.write scope")
		return
	}
	runtimeObj, ok := s.runtimeSharingOwner(w, principal, r.PathValue("id"))
	if !ok {
		return
	}

	granteeTenantID := strings.TrimSpace(r.PathValue("tenant_id"))
	removed, err := s.store.RevokeRuntimeAccess(runtimeObj.ID, runtimeObj.TenantID, granteeTenantID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if removed {
		s.appendAudit(principal, "runtime.share.revoke", "runtime", runtimeObj.ID, runtimeObj.TenantID, map[string]string{
			"grantee_tenant_id": granteeTenantID,
		})
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"removed": removed})
}

func (s *Server) handleSetRuntimeAccessMode(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("runtime.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing runtime.write scope")
		return
	}
	runtimeObj, ok := s.runtimeSharingOwner(w, principal, r.PathValue("id"))
	if !ok {
		return
	}

	var req struct {
		AccessMode string `json:"access_mode"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.AccessMode = strings.TrimSpace(req.AccessMode)
	switch req.AccessMode {
	case model.RuntimeAccessModePrivate, model.RuntimeAccessModePublic, model.RuntimeAccessModePlatformShared:
	default:
		httpx.WriteError(w, http.StatusBadRequest, "invalid access_mode")
		return
	}
	if req.AccessMode == model.RuntimeAccessModePlatformShared && !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can set platform-shared mode")
		return
	}

	runtimeObj, err := s.store.SetRuntimeAccessMode(runtimeObj.ID, runtimeObj.TenantID, req.AccessMode)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "runtime.share.mode", "runtime", runtimeObj.ID, runtimeObj.TenantID, map[string]string{
		"access_mode": runtimeObj.AccessMode,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"runtime": runtimeObj})
}

func (s *Server) runtimeSharingOwner(w http.ResponseWriter, principal model.Principal, runtimeID string) (model.Runtime, bool) {
	return s.runtimeOwner(w, principal, runtimeID, "only runtime owner can manage sharing")
}

func (s *Server) runtimeOwner(
	w http.ResponseWriter,
	principal model.Principal,
	runtimeID string,
	forbiddenMessage string,
) (model.Runtime, bool) {
	runtimeObj, err := s.store.GetRuntime(runtimeID)
	if err != nil {
		s.writeStoreError(w, err)
		return model.Runtime{}, false
	}
	if runtimeObj.TenantID == "" || runtimeObj.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, forbiddenMessage)
		return model.Runtime{}, false
	}
	return runtimeObj, true
}
