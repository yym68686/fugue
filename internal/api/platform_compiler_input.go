package api

import (
	"net/http"

	"fugue/internal/httpx"
	"fugue/internal/platformconfig"
)

func (s *Server) handleGetPlatformArtifactCompilerInput(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() || !p.HasScope("artifact.read") {
		httpx.WriteError(w, http.StatusForbidden, "platform admin with artifact.read required")
		return
	}
	views := r.URL.Query()["view"]
	view := r.URL.Query().Get("view")
	if len(views) > 1 || view != "" && view != "full" && view != "dns_configuration" {
		httpx.WriteError(w, http.StatusBadRequest, "unsupported compiler input view")
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
	response := map[string]any{"artifact_id": a.ID, "input_snapshot_digest": digest, "runtime_snapshot": snapshot}
	if view == "dns_configuration" {
		projected := dnsConfigurationInput(snapshot)
		projectedDigest, err := platformconfig.RuntimeSnapshotDigest(projected)
		if err != nil {
			httpx.WriteError(w, http.StatusConflict, "compiler DNS projection cannot be encoded")
			return
		}
		response["runtime_snapshot"], response["projection"], response["projection_digest"] = projected, view, projectedDigest
	}
	httpx.WriteJSON(w, http.StatusOK, response)
}

// Projection follows full stored-input verification. No observation time,
// selection, capability or readiness value is created or renewed here.
func dnsConfigurationInput(in platformconfig.RuntimeSnapshot) platformconfig.RuntimeSnapshot {
	out := platformconfig.RuntimeSnapshot{IntentGeneration: in.IntentGeneration, PolicyGeneration: in.PolicyGeneration, CapturedAt: in.CapturedAt,
		DNSEdgeEndpoints: in.DNSEdgeEndpoints, DNSConsumers: in.DNSConsumers, DNSPlacements: in.DNSPlacements, DNSFlatten: in.DNSFlatten,
		DNSSelections: append([]platformconfig.DNSSelectionObservation(nil), in.DNSSelections...)}
	for i := range out.DNSSelections {
		out.DNSSelections[i].PhysicalEvidence = nil
	}
	return out
}
