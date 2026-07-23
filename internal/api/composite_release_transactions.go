package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	"fugue/internal/compositecoordinator"
	"fugue/internal/httpx"
	"fugue/internal/releasecontract"
)

type prepareCompositeReleaseTransactionRequest struct {
	PlanDigest string          `json:"planDigest"`
	Plan       json.RawMessage `json:"plan"`
}

type runCompositeReleaseTransactionNoopRequest struct {
	Envelope json.RawMessage `json:"envelope"`
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

func (s *Server) handleRunCompositeReleaseTransactionNoop(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	recordID := strings.TrimSpace(r.PathValue("transaction_id"))
	var request runCompositeReleaseTransactionNoopRequest
	if err := httpx.DecodeJSON(r, &request); err != nil || recordID == "" || len(request.Envelope) == 0 {
		httpx.WriteError(w, http.StatusBadRequest, "invalid composite no-op execution request")
		return
	}
	record, err := s.store.GetCompositeReleaseTransaction(recordID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if record.State != compositecoordinator.StatePrepared {
		httpx.WriteError(w, http.StatusConflict, "composite release transaction is not prepared")
		return
	}
	if len(record.Plan.Steps) != 2 {
		httpx.WriteError(w, http.StatusBadRequest, "controlled composite no-op execution requires exactly two domains")
		return
	}
	authorization, err := compositecoordinator.DecodeAndAuthorizeNoop(record, bytes.NewReader(request.Envelope))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid composite no-op authorization")
		return
	}
	finalRecord, result, err := compositecoordinator.RunDurableNoop(s.store, record.ID, authorization, "")
	if err != nil {
		httpx.WriteError(w, http.StatusConflict, "composite no-op execution failed closed")
		return
	}
	if finalRecord.State != compositecoordinator.StateCommitted || result.ProductionWrite {
		httpx.WriteError(w, http.StatusConflict, "composite no-op execution did not commit safely")
		return
	}
	s.appendAudit(principal, "composite.transaction.noop.commit", "composite_transaction", finalRecord.ID, "", map[string]string{
		"plan_digest":          finalRecord.Plan.Digest,
		"authorization_digest": result.AuthorizationDigest,
		"result_digest":        result.Digest,
		"record_digest":        finalRecord.Digest,
		"final_revision":       result.FinalRevision,
		"production_write":     "false",
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"record": finalRecord, "result": result})
}
