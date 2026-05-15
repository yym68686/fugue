package api

import (
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

const defaultDNSACMEChallengeLifetime = time.Hour

type upsertDNSACMEChallengeRequest struct {
	Zone             string     `json:"zone,omitempty"`
	Name             string     `json:"name"`
	Value            string     `json:"value"`
	TTL              int        `json:"ttl,omitempty"`
	Owner            string     `json:"owner,omitempty"`
	ExpiresAt        *time.Time `json:"expires_at,omitempty"`
	ExpiresInSeconds int        `json:"expires_in_seconds,omitempty"`
}

func (s *Server) handleListDNSACMEChallenges(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("dns.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing dns.read scope")
		return
	}
	zone := strings.TrimSpace(r.URL.Query().Get("zone"))
	includeExpired := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("include_expired")), "true")
	challenges, err := s.store.ListDNSACMEChallenges(zone, includeExpired)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"challenges": challenges})
}

func (s *Server) handleUpsertDNSACMEChallenge(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("dns.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing dns.write scope")
		return
	}
	var req upsertDNSACMEChallengeRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	expiresAt := time.Now().UTC().Add(defaultDNSACMEChallengeLifetime)
	if req.ExpiresInSeconds > 0 {
		expiresAt = time.Now().UTC().Add(time.Duration(req.ExpiresInSeconds) * time.Second)
	}
	if req.ExpiresAt != nil {
		expiresAt = req.ExpiresAt.UTC()
	}
	challenge, err := s.store.UpsertDNSACMEChallenge(model.DNSACMEChallenge{
		Zone:      req.Zone,
		Name:      req.Name,
		Value:     req.Value,
		TTL:       req.TTL,
		Owner:     req.Owner,
		CreatedBy: principal.ActorType + ":" + principal.ActorID,
		ExpiresAt: expiresAt,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "dns_acme_challenge.upsert", "dns_acme_challenge", challenge.ID, "", map[string]string{
		"zone": challenge.Zone,
		"name": challenge.Name,
	})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"challenge": challenge})
}

func (s *Server) handleDeleteDNSACMEChallenge(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("dns.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing dns.write scope")
		return
	}
	challenge, err := s.store.DeleteDNSACMEChallenge(r.PathValue("challenge_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "dns_acme_challenge.delete", "dns_acme_challenge", challenge.ID, "", map[string]string{
		"zone": challenge.Zone,
		"name": challenge.Name,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"deleted": true, "challenge": challenge})
}
