package api

import (
	"net/http"

	"fugue/internal/httpx"
)

func (s *Server) handleGetPlatformArtifactCompilerInput(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() || !p.HasScope("artifact.read") {
		httpx.WriteError(w, http.StatusForbidden, "platform admin with artifact.read required")
		return
	}
	a, err := s.store.GetPlatformArtifact(r.PathValue("artifact_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if s.store.VerifyPlatformArtifactIntegrity(a) != nil {
		httpx.WriteError(w, http.StatusConflict, "compiler input artifact signature is invalid")
		return
	}
	digest := a.Metadata["input_snapshot_digest"]
	if digest == "" {
		httpx.WriteError(w, http.StatusNotFound, "artifact has no compiler runtime input")
		return
	}
	snapshot, err := s.store.GetPlatformCompilerInput(digest)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if snapshot.IntentGeneration != a.Metadata["intent_generation"] || snapshot.PolicyGeneration != a.Metadata["policy_generation"] {
		httpx.WriteError(w, http.StatusConflict, "compiler input integrity or lineage differs")
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"artifact_id": a.ID, "input_snapshot_digest": digest, "runtime_snapshot": snapshot})
}
