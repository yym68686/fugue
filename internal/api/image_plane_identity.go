package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"
)

const (
	imageCachePlatformIdentityTTL        = 15 * time.Minute
	imageCachePlatformIdentityRenewAfter = 5 * time.Minute
)

// handleIssueNodeUpdaterImageCacheIdentity is deliberately a fixed-purpose
// issuer. The authenticated updater is the only source of node identity; no
// caller-supplied component, scope, node, or artifact capability is accepted.
// The endpoint is safe to expose before the image-cache client is deployed:
// it only mints a shadow-contract credential and does not publish or mutate
// image state.
func (s *Server) handleIssueNodeUpdaterImageCacheIdentity(w http.ResponseWriter, r *http.Request) {
	setPlatformCredentialNoStoreHeaders(w)
	principal := mustPrincipal(r)
	updater, err := s.nodeUpdaterByPrincipal(principal)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			httpx.WriteError(w, http.StatusForbidden, "node updater identity is not bound to a cluster node")
			return
		}
		s.writeStoreError(w, err)
		return
	}
	if !strings.EqualFold(strings.TrimSpace(updater.Status), model.NodeUpdaterStatusActive) {
		httpx.WriteError(w, http.StatusForbidden, "node updater is not active")
		return
	}
	if !nodeUpdaterHasCapability(updater, model.NodeUpdaterCapabilityImageCachePlatformIdentityV1) {
		httpx.WriteError(w, http.StatusForbidden, "node updater does not support image-cache platform identity v1")
		return
	}
	nodeID := strings.ToLower(strings.TrimSpace(updater.ClusterNodeName))
	if nodeID == "" {
		httpx.WriteError(w, http.StatusConflict, "node updater is not bound to a cluster node")
		return
	}

	now := time.Now().UTC()
	claims := platformcontrol.PlatformComponentIdentityClaims{
		CredentialID:  model.PlatformConsumerComponentImageCache + ":" + nodeID,
		Component:     model.PlatformConsumerComponentImageCache,
		NodeID:        nodeID,
		ScopeKey:      "node:" + nodeID,
		ArtifactKinds: []string{model.PlatformArtifactKindImageReplicationPlan},
	}
	keyring := s.auth.PlatformComponentIdentityKeyring
	token, err := platformcontrol.IssuePlatformComponentIdentity(
		keyring,
		claims,
		now,
		imageCachePlatformIdentityTTL,
	)
	if err != nil {
		s.appendAudit(principal, "image_cache.platform_identity.issue_failed", "node_updater", updater.ID, updater.TenantID, map[string]string{
			"component": model.PlatformConsumerComponentImageCache,
			"node_id":   nodeID,
			"reason":    "platform_identity_signer_unavailable",
		})
		if s.log != nil {
			s.log.Printf("image-cache platform identity unavailable for node updater %s: %v", updater.ID, err)
		}
		w.Header().Set("Retry-After", "30")
		httpx.WriteError(w, http.StatusServiceUnavailable, "platform component identity issuer is unavailable")
		return
	}
	issued, err := platformcontrol.ParsePlatformComponentIdentity(
		keyring,
		token,
		now,
	)
	if err != nil {
		// Issuance and local verification must agree before a secret crosses the
		// HTTP boundary. This is a fail-closed configuration or implementation
		// error, never a partially useful credential.
		if s.log != nil {
			s.log.Printf("image-cache platform identity self-check failed for node updater %s: %v", updater.ID, err)
		}
		s.appendAudit(principal, "image_cache.platform_identity.issue_failed", "node_updater", updater.ID, updater.TenantID, map[string]string{
			"component": model.PlatformConsumerComponentImageCache,
			"node_id":   nodeID,
			"reason":    "issued_token_self_check_failed",
		})
		w.Header().Set("Retry-After", "30")
		httpx.WriteError(w, http.StatusServiceUnavailable, "platform component identity issuer is unavailable")
		return
	}
	issuedAt := time.Unix(issued.IssuedAtUnix, 0).UTC()
	expiresAt := time.Unix(issued.ExpiresAtUnix, 0).UTC()
	credential := model.PlatformComponentCredential{
		APIVersion:    model.PlatformComponentCredentialAPIVersionV1,
		Kind:          model.PlatformComponentCredentialKind,
		CredentialID:  issued.CredentialID,
		Token:         token,
		TokenID:       issued.TokenID,
		Component:     issued.Component,
		NodeID:        issued.NodeID,
		ScopeKey:      issued.ScopeKey,
		ArtifactKinds: append([]string(nil), issued.ArtifactKinds...),
		IssuedAt:      issuedAt,
		ExpiresAt:     expiresAt,
		RenewAfter:    issuedAt.Add(imageCachePlatformIdentityRenewAfter),
	}
	s.appendAudit(principal, "image_cache.platform_identity.issued", "platform_component_credential", credential.CredentialID, updater.TenantID, map[string]string{
		"component":  credential.Component,
		"node_id":    credential.NodeID,
		"token_id":   credential.TokenID,
		"expires_at": credential.ExpiresAt.Format(time.RFC3339),
	})
	httpx.WriteJSON(w, http.StatusCreated, model.PlatformComponentCredentialResponse{Credential: credential})
}

func setPlatformCredentialNoStoreHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
}
