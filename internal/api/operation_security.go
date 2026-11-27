package api

import (
	"errors"
	"net/http"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/operationview"
)

var errOperationNotVisible = operationview.ErrNotVisible

func (s *Server) loadAuthorizedOperation(principal model.Principal, operationID string) (model.Operation, error) {
	return operationview.LoadAuthorized(s.store.OperationViews(), principal, operationID)
}

func (s *Server) writeOperationReadError(w http.ResponseWriter, err error) {
	if errors.Is(err, errOperationNotVisible) {
		httpx.WriteError(w, http.StatusForbidden, "operation is not visible to this tenant or project")
		return
	}
	s.writeStoreError(w, err)
}

func operationListProjectForPrincipal(principal model.Principal, requested string) (string, error) {
	return operationview.ScopeProject(principal, requested)
}
