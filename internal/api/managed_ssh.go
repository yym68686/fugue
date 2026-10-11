package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (server *Server) handleGetManagedSSHIntent(writer http.ResponseWriter, request *http.Request) {
	principal := mustPrincipal(request)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.ssh.read") && !principal.HasScope("app.ssh.write") {
		httpx.WriteError(writer, http.StatusForbidden, "missing app.ssh.read scope")
		return
	}
	app, allowed := server.loadAuthorizedApp(writer, request, principal)
	if !allowed {
		return
	}
	intent, err := server.store.GetManagedSSHIntent(app.TenantID, app.ID)
	if err != nil {
		server.writeSSHIdentityError(writer, err)
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(writer, http.StatusOK, map[string]any{"intent": intent})
}

func (server *Server) handlePutManagedSSHIntent(writer http.ResponseWriter, request *http.Request) {
	principal := mustPrincipal(request)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.ssh.write") {
		httpx.WriteError(writer, http.StatusForbidden, "missing app.ssh.write scope")
		return
	}
	app, allowed := server.loadAuthorizedApp(writer, request, principal)
	if !allowed {
		return
	}
	var input struct {
		ExpectedGeneration *uint64         `json:"expected_generation"`
		Spec               json.RawMessage `json:"spec"`
	}
	request.Body = http.MaxBytesReader(writer, request.Body, 64<<10)
	if err := httpx.DecodeJSON(request, &input); err != nil || input.ExpectedGeneration == nil || len(input.Spec) == 0 {
		httpx.WriteError(writer, http.StatusBadRequest, "expected_generation and valid spec are required")
		return
	}
	var spec model.ManagedSSHIntentSpec
	decoder := json.NewDecoder(bytes.NewReader(input.Spec))
	decoder.DisallowUnknownFields()
	var fields map[string]json.RawMessage
	if err := decoder.Decode(&spec); err != nil || json.Unmarshal(input.Spec, &fields) != nil {
		httpx.WriteError(writer, http.StatusBadRequest, "invalid managed SSH spec")
		return
	}
	for _, required := range []string{"enabled", "identity_id", "container", "user", "working_directory", "authorized_key_ids", "allow_pty", "allow_sftp", "max_connections", "max_connections_per_ip", "max_channels"} {
		if raw, exists := fields[required]; !exists || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			httpx.WriteError(writer, http.StatusBadRequest, "missing managed SSH spec field: "+required)
			return
		}
	}
	intent, err := server.store.PutManagedSSHIntent(app.TenantID, app.ID, *input.ExpectedGeneration, spec)
	if err != nil {
		server.writeSSHIdentityError(writer, err)
		return
	}
	server.appendAudit(principal, "app.ssh.managed.intent", "app", app.ID, app.TenantID, map[string]string{"identity_id": intent.Spec.IdentityID, "generation": strconv.FormatUint(intent.Generation, 10)})
	writer.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(writer, http.StatusOK, map[string]any{"intent": intent})
}
