package api

import (
	"net/http"
	"time"

	"fugue/internal/auth"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func (s *Server) handleEdgeParticipationObservation(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		httpx.WriteError(w, http.StatusMethodNotAllowed, "only GET may read the participation observation")
		return
	}
	id := r.PathValue("edge_id")
	claims, ok := auth.PlatformComponentIdentityFromContext(r.Context())
	group, allowed := platformcontrol.ParticipationObserverGroup(claims, id)
	if !ok || !allowed {
		httpx.WriteError(w, http.StatusForbidden, "participation observation is not authorized for this node")
		return
	}
	node, _, err := s.store.GetEdgeNode(id)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if node.ID != id || node.EdgeGroupID != group {
		httpx.WriteError(w, http.StatusForbidden, "participation observation is not authorized for this node group")
		return
	}
	now := time.Now().UTC()
	o := observeEdgeNodeInventory(node, now)
	intent, holds := s.observeGlobalDNSAdmission(now)
	o.DNSAdmissionIntent = dnsAdmissionForNode(node, intent, holds)
	s.observeVerifiedDNSExclusion(node, o)
	dns := o.DNSAdmissionIntent
	httpx.WriteJSON(w, http.StatusOK, model.EdgeParticipationObservation{
		Node: node.ID, Group: node.EdgeGroupID, PublicIPv4: node.PublicIPv4, EvaluatedAt: o.EvaluatedAt,
		FreshUntil: o.FreshUntil, HeartbeatFresh: o.HeartbeatFresh, DNSState: dns.State, DNSReason: dns.Reason,
		DNSServingEvidence: dns.ServingEvidence,
	})
}
