package api

import (
	"net/http"
	"slices"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

const defaultDNSACMEChallengeLifetime = time.Hour

// Certificate automation is delegated for an exact certificate name, never a
// suffix or a tenant DNS management role.
func certificateDomains(p model.Principal) []string {
	var names []string
	for scope := range p.Scopes {
		name, ok := strings.CutPrefix(scope, "dns.acme:")
		if ok && name != "" && normalizeExternalAppDomain(name) == name && !strings.Contains(name, "*") {
			names = append(names, name)
		}
	}
	return names
}

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
	zone := normalizeExternalAppDomain(r.URL.Query().Get("zone"))
	if !principal.IsPlatformAdmin() && (zone == "" || !slices.Contains(certificateDomains(principal), zone)) {
		httpx.WriteError(w, http.StatusForbidden, "certificate domain grant required")
		return
	}
	includeExpired := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("include_expired")), "true")
	challenges, err := s.store.ListDNSACMEChallenges(zone, includeExpired)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() {
		challenges = slices.DeleteFunc(challenges, func(c model.DNSACMEChallenge) bool {
			return c.Name != "_acme-challenge."+zone || c.CreatedBy != principal.ActorType+":"+principal.ActorID
		})
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"challenges": challenges})
}

func (s *Server) handleUpsertDNSACMEChallenge(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && len(certificateDomains(principal)) == 0 {
		httpx.WriteError(w, http.StatusForbidden, "certificate domain grant required")
		return
	}
	var req upsertDNSACMEChallengeRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !principal.IsPlatformAdmin() {
		req.Zone = normalizeExternalAppDomain(req.Zone)
		if !slices.Contains(certificateDomains(principal), req.Zone) || strings.TrimSuffix(strings.ToLower(strings.TrimSpace(req.Name)), ".") != "_acme-challenge."+req.Zone {
			httpx.WriteError(w, http.StatusForbidden, "certificate domain grant required")
			return
		}
		req.Owner = principal.ActorType + ":" + principal.ActorID
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
	}, principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"challenge": challenge})
}

func (s *Server) handleDeleteDNSACMEChallenge(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	domains := certificateDomains(principal)
	if !principal.IsPlatformAdmin() && len(domains) == 0 {
		httpx.WriteError(w, http.StatusForbidden, "certificate domain grant required")
		return
	}
	challenge, err := s.store.DeleteAuthorizedDNSACMEChallenge(r.PathValue("challenge_id"), domains, principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"deleted": true, "challenge": challenge})
}
