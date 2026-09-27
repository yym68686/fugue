package dnsserver

import (
	"net/http"
	"time"

	"fugue/internal/httpx"
)

// Record proof loss is scoped to that record. It must not remove a working
// authoritative listener and make unrelated records unreachable. Full release
// health and handoff evidence continue to use their stricter readiness gates.
func (s *Service) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	listeners := s.udpListening.Load() && s.tcpListening.Load()
	artifactAuthority := s.platformServingRequired()
	reason := "ready"
	switch {
	case !listeners || s.listenerFailed.Load():
		reason = "listeners_unavailable"
	case artifactAuthority:
		st := s.platformServing.Load()
		now := time.Now().UTC()
		if st == nil || !st.record.Positive || st.record.AppliedAt.IsZero() || st.record.AppliedAt.After(now) || len(st.zones) == 0 {
			reason = "checkpoint_unavailable"
		} else if st.payload.Policy.MaxStaleSeconds <= 0 || !st.record.AppliedAt.Add(time.Duration(st.payload.Policy.MaxStaleSeconds)*time.Second).After(now) {
			reason = "checkpoint_expired"
		}
	default:
		if !s.Status().Healthy {
			reason = "legacy_unready"
		}
	}
	code := http.StatusOK
	if reason != "ready" {
		code = http.StatusServiceUnavailable
	}
	httpx.WriteJSON(w, code, struct {
		Ready  bool   `json:"ready"`
		Reason string `json:"reason"`
	}{code == http.StatusOK, reason})
}
