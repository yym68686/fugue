package api

import (
	"errors"
	"net/http"

	"fugue/internal/httpx"
	"fugue/internal/sshidentity"
)

func (server *Server) handleGetAppSSHIdentity(writer http.ResponseWriter, request *http.Request) {
	principal := mustPrincipal(request)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.ssh.read") && !principal.HasScope("app.ssh.write") {
		httpx.WriteError(writer, http.StatusForbidden, "missing app.ssh.read scope")
		return
	}
	app, allowed := server.loadAuthorizedApp(writer, request, principal)
	if !allowed {
		return
	}
	identity, err := server.store.GetAppSSHIdentity(app.TenantID, app.ID)
	if err != nil {
		server.writeSSHIdentityError(writer, err)
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(writer, http.StatusOK, map[string]any{"identity": identity})
}

func (server *Server) handlePrepareAppSSHIdentity(writer http.ResponseWriter, request *http.Request) {
	principal := mustPrincipal(request)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.ssh.write") {
		httpx.WriteError(writer, http.StatusForbidden, "missing app.ssh.write scope")
		return
	}
	app, allowed := server.loadAuthorizedApp(writer, request, principal)
	if !allowed {
		return
	}
	identity, err := server.store.PrepareAppSSHIdentity(app.TenantID, app.ID)
	if err != nil {
		server.writeSSHIdentityError(writer, err)
		return
	}
	server.appendAudit(principal, "app.ssh.identity.prepare", "app", app.ID, app.TenantID, map[string]string{
		"identity_id": identity.ID,
		"fingerprint": identity.Fingerprint,
	})
	writer.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(writer, http.StatusOK, map[string]any{"identity": identity})
}

func (server *Server) writeSSHIdentityError(writer http.ResponseWriter, err error) {
	if errors.Is(err, sshidentity.ErrKeyUnavailable) {
		httpx.WriteError(writer, http.StatusServiceUnavailable, "SSH identity encryption key is unavailable; existing identity was not changed")
		return
	}
	if errors.Is(err, sshidentity.ErrIntegrity) || errors.Is(err, sshidentity.ErrInvalidIdentity) {
		httpx.WriteError(writer, http.StatusConflict, "SSH identity failed integrity validation; restore the existing identity before retrying")
		return
	}
	server.writeStoreError(writer, err)
}
