package api

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/managedssh"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (server *Server) authorizeManagedSSHApp(writer http.ResponseWriter, request *http.Request, write bool) (model.App, bool) {
	principal := mustPrincipal(request)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.ssh.write") && (write || !principal.HasScope("app.ssh.read")) {
		httpx.WriteError(writer, http.StatusForbidden, "missing app.ssh scope")
		return model.App{}, false
	}
	return server.loadAuthorizedApp(writer, request, principal)
}

func (server *Server) handleGetManagedSSHStatus(writer http.ResponseWriter, request *http.Request) {
	app, allowed := server.authorizeManagedSSHApp(writer, request, false)
	if !allowed {
		return
	}
	server.writeManagedSSHStatus(writer, app)
}

func (server *Server) writeManagedSSHStatus(writer http.ResponseWriter, app model.App) {
	intent, err := server.store.GetManagedSSHIntent(app.TenantID, app.ID)
	if err != nil {
		server.writeSSHIdentityError(writer, err)
		return
	}
	response := map[string]any{"intent": intent, "legacy_route_digest": ""}
	if endpoint, err := server.store.GetAppSSHEndpoint(app.ID); err == nil {
		response["legacy_route_digest"] = model.AppSSHRouteDigest(endpoint)
	} else if !errors.Is(err, store.ErrNotFound) {
		server.writeStoreError(writer, err)
		return
	}
	if binding, err := server.store.GetManagedSSHBinding(app.TenantID, app.ID); err == nil {
		response["binding"], response["legacy_route_digest"] = binding, binding.LegacyRouteDigest
	} else if !errors.Is(err, store.ErrNotFound) {
		server.writeStoreError(writer, err)
		return
	}
	if state, err := server.store.GetManagedSSHControl(app.TenantID, app.ID); err == nil {
		response["control"] = state.PublicStatus(time.Now().UTC())
	} else if !errors.Is(err, store.ErrNotFound) {
		server.writeStoreError(writer, err)
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(writer, http.StatusOK, response)
}

func (server *Server) handlePrepareManagedSSHBinding(writer http.ResponseWriter, request *http.Request) {
	app, allowed := server.authorizeManagedSSHApp(writer, request, true)
	if !allowed {
		return
	}
	var input struct {
		IdentityID        string  `json:"identity_id"`
		LegacyRouteDigest *string `json:"legacy_route_digest"`
	}
	request.Body = http.MaxBytesReader(writer, request.Body, 4096)
	if err := httpx.DecodeJSON(request, &input); err != nil || input.IdentityID == "" || input.LegacyRouteDigest == nil {
		httpx.WriteError(writer, http.StatusBadRequest, "identity_id and legacy_route_digest are required")
		return
	}
	start, end := server.sshPublicPortStart, server.sshPublicPortEnd
	if start == 0 {
		start = model.DefaultAppSSHPublicPortStart
	}
	if end == 0 {
		end = model.DefaultAppSSHPublicPortEnd
	}
	binding, err := server.store.PrepareManagedSSHBinding(app.TenantID, app.ID, input.IdentityID, *input.LegacyRouteDigest, server.sshPublicHost, start, end)
	if err != nil {
		server.writeSSHIdentityError(writer, err)
		return
	}
	server.appendAudit(mustPrincipal(request), "app.ssh.managed.binding.prepare", "app", app.ID, app.TenantID, map[string]string{"identity_id": binding.IdentityID, "binding_id": binding.ID, "public_port": strconv.Itoa(binding.PublicPort)})
	writer.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(writer, http.StatusOK, map[string]any{"binding": binding})
}

type managedSSHActivationRequest struct {
	ExpectedRevision         uint64 `json:"expected_revision"`
	ExpectedIntentGeneration uint64 `json:"expected_intent_generation"`
	ArtifactDigest           string `json:"artifact_digest"`
	Fingerprint              string `json:"fingerprint"`
	BindingID                string `json:"binding_id"`
	LegacyRouteDigest        string `json:"legacy_route_digest"`
}

func (server *Server) handleActivateManagedSSH(writer http.ResponseWriter, request *http.Request) {
	server.handlePublishManagedSSH(writer, request, false)
}

func (server *Server) handleRollbackManagedSSH(writer http.ResponseWriter, request *http.Request) {
	server.handlePublishManagedSSH(writer, request, true)
}

func (server *Server) handlePublishManagedSSH(writer http.ResponseWriter, request *http.Request, rollback bool) {
	app, allowed := server.authorizeManagedSSHApp(writer, request, true)
	if !allowed {
		return
	}
	var input struct {
		managedSSHActivationRequest
		SlotID string `json:"slot_id,omitempty"`
	}
	request.Body = http.MaxBytesReader(writer, request.Body, 8192)
	if err := httpx.DecodeJSON(request, &input); err != nil || input.ExpectedRevision == 0 || input.ExpectedIntentGeneration == 0 || input.ArtifactDigest == "" || input.Fingerprint == "" || input.BindingID == "" || input.LegacyRouteDigest == "" || (rollback != (input.SlotID != "")) {
		httpx.WriteError(writer, http.StatusBadRequest, "exact revision, intent generation, artifact, fingerprint, binding, and legacy route are required")
		return
	}
	principal := mustPrincipal(request)
	var state managedssh.ControlState
	var err error
	if rollback {
		state, err = server.store.RollbackManagedSSH(app.TenantID, app.ID, input.ExpectedRevision, input.ExpectedIntentGeneration, input.ArtifactDigest, input.Fingerprint, input.BindingID, input.LegacyRouteDigest, principal.ActorType+":"+principal.ActorID, input.SlotID)
	} else {
		state, err = server.store.ActivateManagedSSH(app.TenantID, app.ID, input.ExpectedRevision, input.ExpectedIntentGeneration, input.ArtifactDigest, input.Fingerprint, input.BindingID, input.LegacyRouteDigest, principal.ActorType+":"+principal.ActorID)
	}
	if err != nil {
		switch {
		case errors.Is(err, store.ErrManagedSSHPublicationUnsupported):
			httpx.WriteError(writer, http.StatusServiceUnavailable, err.Error())
		case errors.Is(err, managedssh.ErrConfiguration), errors.Is(err, managedssh.ErrCandidateUnverified):
			httpx.WriteError(writer, http.StatusConflict, err.Error())
		default:
			server.writeSSHIdentityError(writer, err)
		}
		return
	}
	action := "app.ssh.managed.activate"
	if rollback {
		action = "app.ssh.managed.rollback"
	}
	server.appendAudit(principal, action, "app", app.ID, app.TenantID, map[string]string{"identity_id": state.IdentityID, "artifact_digest": state.Active.Artifact.Digest, "route_generation": strconv.FormatUint(state.RouteGeneration, 10), "binding_id": input.BindingID})
	server.writeManagedSSHStatus(writer, app)
}

func (server *Server) allowNativeSSHMutation(writer http.ResponseWriter, app model.App) bool {
	_, err := server.store.GetManagedSSHBinding(app.TenantID, app.ID)
	if errors.Is(err, store.ErrNotFound) {
		return true
	}
	if err != nil {
		server.writeStoreError(writer, err)
		return false
	}
	httpx.WriteError(writer, http.StatusConflict, "public address belongs to platform-managed SSH; update managed SSH intent")
	return false
}
