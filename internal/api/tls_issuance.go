package api

import (
	"net/http"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/tlsissuance"
)

func (s *Server) handleGetTLSIssuancePolicy(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	p, err := s.store.ReadTLSIssuancePolicy(r.Context())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if p.Generation == 0 {
		httpx.WriteError(w, http.StatusNotFound, "certificate issuance policy not configured")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, p)
}

func (s *Server) handlePutTLSIssuancePolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	var p tlsissuance.Policy
	if err := httpx.DecodeJSON(r, &p); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := p.Validate(); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	for _, d := range p.Domains {
		domain, err := s.store.GetAppDomain(d.Hostname)
		if err != nil || domain.AppID != d.AppID || domain.TenantID != d.TenantID || domain.Status != "verified" {
			httpx.WriteError(w, http.StatusConflict, "issuance domain ownership is not currently verified")
			return
		}
		if d.Mode == "dns-01" {
			zone, zoneErr := s.store.GetHostedZoneByName(d.DNSZone)
			if zoneErr != nil || zone.TenantID != d.TenantID || zone.Status != "active" || zone.DelegationStatus != "ready" {
				httpx.WriteError(w, http.StatusConflict, "DNS challenge zone is not delegated to this owner")
				return
			}
		}
	}
	if err := s.store.PutTLSIssuancePolicy(r.Context(), p); err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "tls_issuance.policy_updated", "tls_issuance_policy", p.IssuerID, "", nil)
	httpx.WriteJSON(w, http.StatusOK, p)
}

func (s *Server) handleHTTP01Challenge(w http.ResponseWriter, r *http.Request) {
	host, token := r.PathValue("hostname"), r.PathValue("token")
	if !tlsissuance.ValidHostname(host) || !tlsissuance.ValidToken(token) {
		http.NotFound(w, r)
		return
	}
	p, err := s.store.ReadTLSIssuancePolicy(r.Context())
	if err != nil {
		http.Error(w, "challenge temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	d, ok := p.Domain(host)
	if !p.Enabled || !ok || d.Mode != "http-01" {
		http.NotFound(w, r)
		return
	}
	state, err := s.store.ReadTLSIssuanceState(r.Context(), p.IssuerID, p.DirectoryURL, host)
	if err != nil {
		http.Error(w, "challenge temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	domain, err := s.store.GetAppDomain(host)
	if err != nil || domain.AppID != d.AppID || domain.TenantID != d.TenantID || domain.Status != "verified" {
		http.NotFound(w, r)
		return
	}
	if state.Token != token || state.KeyAuthorization == "" || !state.ChallengeExpires.After(time.Now()) {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(state.KeyAuthorization))
}
