package api

import (
	"encoding/hex"
	"errors"
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
func (s *Server) createAppOperationWithPrecondition(op model.Operation, expected string) (model.Operation, error) {
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
