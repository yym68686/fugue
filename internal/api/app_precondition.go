package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func appSpecPrecondition(w http.ResponseWriter, r *http.Request) (string, bool) {
	raw := r.Header.Get("If-Match")
	if raw == "" {
		return "", true
	}
	if len(raw) != 66 || raw[0] != '"' || raw[65] != '"' {
		httpx.WriteError(w, http.StatusBadRequest, "If-Match must be a quoted lowercase SHA-256")
		return "", false
	}
	hash := raw[1:65]
	decoded, err := hex.DecodeString(hash)
	if err != nil || len(decoded) != 32 || hash != strings.ToLower(hash) {
		httpx.WriteError(w, http.StatusBadRequest, "If-Match must be a quoted lowercase SHA-256")
		return "", false
	}
	return hash, true
}
func (s *Server) createAppOperationWithPrecondition(r *http.Request, op model.Operation, expected string, requestData any) (model.Operation, error) {
	key, err := resolveIdempotencyKey(r, "")
	if err != nil {
		return model.Operation{}, store.ErrInvalidInput
	}
	if key != "" {
		if expected == "" {
			return model.Operation{}, store.ErrInvalidInput
		}
		payload, _ := json.Marshal([]any{r.Method, r.URL.Path, expected, requestData})
		hash := sha256.Sum256(payload)
		principal := mustPrincipal(r)
		scope := "app-action:" + op.AppID + ":" + principal.ActorType + ":" + principal.ActorID
		return s.store.CreateAppActionOperation(op, expected, scope, key, hex.EncodeToString(hash[:]))
	}
	if expected == "" {
		return s.store.CreateOperation(op)
	}
	return s.store.CreateOperationForAppSpec(op, expected)
}
func (s *Server) writeAppPreconditionError(w http.ResponseWriter, err error, expected string) {
	if expected != "" && errors.Is(err, store.ErrConflict) {
		httpx.WriteError(w, http.StatusPreconditionFailed, "app intent changed or an operation is active; refresh the plan")
		return
	}
	s.writeStoreError(w, err)
}

func (s *Server) handleGetAppActionRequest(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.read") && !principal.HasScope("app.deploy") && !principal.HasScope("app.scale") {
		httpx.WriteError(w, http.StatusForbidden, "missing app read/deploy/scale scope")
		return
	}
	app, ok := s.loadAuthorizedAppMetadata(w, r, principal)
	if !ok {
		return
	}
	scope := "app-action:" + app.ID + ":" + principal.ActorType + ":" + principal.ActorID
	record, err := s.store.GetIdempotencyRecord(scope, app.TenantID, r.PathValue("request_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	op, err := s.store.GetOperation(record.OperationID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if op.AppID != app.ID {
		httpx.WriteError(w, http.StatusNotFound, fmt.Sprint("action receipt not found"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"operation": sanitizeOperationForAPI(op)})
}
