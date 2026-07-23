package api

import (
	"bytes"
	"encoding/json"
	"net/http"

	"fugue/internal/httpx"
	"fugue/internal/releasecontract"
)

type prepareCompositeReleaseTransactionRequest struct {
	PlanDigest string          `json:"planDigest"`
	Plan       json.RawMessage `json:"plan"`
}

func (s *Server) handlePrepareCompositeReleaseTransaction(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	var request prepareCompositeReleaseTransactionRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid composite release transaction request")
		return
	}
	if request.PlanDigest == "" || len(request.Plan) == 0 {
		httpx.WriteError(w, http.StatusBadRequest, "planDigest and plan are required")
		return
	}
	plan, err := releasecontract.DecodeAndVerifyCompositeReleasePlan(bytes.NewReader(request.Plan), request.PlanDigest)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid composite release plan")
		return
	}
	record, err := s.store.CreateCompositeReleaseTransaction(plan)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "composite.transaction.prepare", "composite_transaction", record.ID, "", map[string]string{
		"plan_digest": record.Plan.Digest,
	})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"record": record})
}
