package api

import (
	"errors"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
	"fugue/internal/trafficoverride"
)

type putTrafficOverrideRequest struct {
	Answers            []string  `json:"answers"`
	RequiredHostRoutes []string  `json:"required_host_routes"`
	RouteGeneration    string    `json:"route_generation"`
	RouteDigest        string    `json:"route_digest"`
	ExpiresAt          time.Time `json:"expires_at"`
	Reason             string    `json:"reason"`
	ExpectedGeneration uint64    `json:"expected_generation"`
}

type revokeTrafficOverrideRequest struct {
	Reason             string `json:"reason"`
	ExpectedGeneration uint64 `json:"expected_generation"`
}

type rotateTrafficOverrideSigningKeyRequest struct {
	ExpectedGeneration uint64 `json:"expected_generation"`
}

const dnsTrafficOverrideFeedSchemaV1 = "traffic-override-feed.fugue.dev/v1"

type dnsTrafficOverrideFeed struct {
	Schema      string                                `json:"schema"`
	Generation  uint64                                `json:"generation"`
	GeneratedAt time.Time                             `json:"generated_at"`
	Overrides   []model.TrafficOverride               `json:"overrides"`
	SigningKey  model.TrafficOverrideSigningKeyStatus `json:"signing_key"`
}

func (s *Server) handleAdminListTrafficOverrides(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	overrides, err := s.store.ListTrafficOverrides()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"overrides": overrides})
}

// handleDNSTrafficOverrideFeed serves the independent, signed overlay feed.
// It is scoped by the existing edge token and is inert unless a DNS consumer
// explicitly opts into the overlay path.
func (s *Server) handleDNSTrafficOverrideFeed(w http.ResponseWriter, r *http.Request) {
	authContext, ok := s.authorizeEdgeRequest(w, r)
	if !ok {
		return
	}
	requestedGroup := strings.TrimSpace(r.URL.Query().Get("edge_group_id"))
	if authContext.Scoped {
		if requestedGroup == "" {
			requestedGroup = authContext.EdgeGroupID
		}
		if !strings.EqualFold(requestedGroup, authContext.EdgeGroupID) {
			httpx.WriteError(w, http.StatusForbidden, "edge token cannot access another edge_group_id")
			return
		}
	}
	overrides, err := s.store.ListTrafficOverrides()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	zone := normalizeExternalAppDomain(r.URL.Query().Get("zone"))
	nodeID := strings.TrimSpace(r.URL.Query().Get("dns_node_id"))
	if nodeID != "" {
		node, nodeErr := s.store.GetDNSNode(nodeID)
		if nodeErr != nil && !errors.Is(nodeErr, store.ErrNotFound) {
			s.writeStoreError(w, nodeErr)
			return
		}
		if nodeErr == nil {
			if requestedGroup != "" && !strings.EqualFold(strings.TrimSpace(node.EdgeGroupID), requestedGroup) {
				httpx.WriteError(w, http.StatusForbidden, "dns node is outside the requested edge group")
				return
			}
			if zone != "" && node.Zone != "" && !strings.EqualFold(normalizeExternalAppDomain(node.Zone), zone) {
				httpx.WriteError(w, http.StatusForbidden, "dns node is outside the requested zone")
				return
			}
		}
	}
	now := time.Now().UTC()
	active := make([]model.TrafficOverride, 0, len(overrides))
	for _, override := range overrides {
		if override.State != model.TrafficOverrideStateStaged || !override.ExpiresAt.After(now) {
			continue
		}
		if zone != "" && !nameWithinDNSZone(override.Hostname, zone) {
			continue
		}
		active = append(active, override)
	}
	keyring, err := s.store.GetTrafficOverrideSigningKeyring()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	feed := dnsTrafficOverrideFeed{
		Schema:      dnsTrafficOverrideFeedSchemaV1,
		Generation:  keyring.Generation,
		GeneratedAt: now,
		Overrides:   active,
		SigningKey:  keyring.Status(),
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"feed": feed})
}

func nameWithinDNSZone(name, zone string) bool {
	name = normalizeExternalAppDomain(name)
	zone = normalizeExternalAppDomain(zone)
	return name != "" && zone != "" && (name == zone || strings.HasSuffix(name, "."+zone))
}

func (s *Server) handleAdminGetTrafficOverride(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	override, err := s.store.GetTrafficOverride(r.PathValue("hostname"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"override": override})
}

func (s *Server) handleAdminPutTrafficOverride(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	var req putTrafficOverrideRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	hostname := normalizeExternalAppDomain(r.PathValue("hostname"))
	if hostname == "" {
		httpx.WriteError(w, http.StatusBadRequest, "invalid hostname")
		return
	}
	now := time.Now().UTC()
	createdAt := now
	if current, err := s.store.GetTrafficOverride(hostname); err == nil {
		createdAt = current.CreatedAt
	} else if !errors.Is(err, store.ErrNotFound) {
		s.writeStoreError(w, err)
		return
	}
	keyring, err := s.store.GetTrafficOverrideSigningKeyring()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	candidate := model.TrafficOverride{
		Hostname:           hostname,
		Generation:         req.ExpectedGeneration + 1,
		State:              model.TrafficOverrideStateStaged,
		Answers:            normalizeTrafficOverrideAnswers(req.Answers),
		RequiredHostRoutes: normalizeTrafficOverrideHostnames(req.RequiredHostRoutes),
		RouteGeneration:    strings.TrimSpace(req.RouteGeneration),
		RouteDigest:        strings.TrimSpace(strings.ToLower(req.RouteDigest)),
		ExpiresAt:          req.ExpiresAt.UTC(),
		Reason:             strings.TrimSpace(req.Reason),
		Operator:           strings.TrimSpace(principal.ActorType + "/" + principal.ActorID),
		SignedAt:           now,
		CreatedAt:          createdAt,
		UpdatedAt:          now,
	}
	candidate, err = trafficoverride.Sign(candidate, keyring.CurrentPrivateKey, keyring.CurrentKeyID)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "traffic override signer unavailable")
		return
	}
	stored, err := s.store.PutTrafficOverrideCAS(candidate, req.ExpectedGeneration)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "traffic_override.stage", "traffic_override", stored.Hostname, "", trafficOverrideAuditMetadata(stored))
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"override": stored})
}

func (s *Server) handleAdminRevokeTrafficOverride(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	var req revokeTrafficOverrideRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	current, err := s.store.GetTrafficOverride(r.PathValue("hostname"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if current.Generation != req.ExpectedGeneration {
		s.writeStoreError(w, store.ErrConflict)
		return
	}
	keyring, err := s.store.GetTrafficOverrideSigningKeyring()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	now := time.Now().UTC()
	current.Generation++
	current.State = model.TrafficOverrideStateRevoked
	current.Reason = strings.TrimSpace(req.Reason)
	current.Operator = strings.TrimSpace(principal.ActorType + "/" + principal.ActorID)
	current.SignedAt = now
	current.UpdatedAt = now
	if current.ExpiresAt.Before(now.Add(15 * time.Minute)) {
		current.ExpiresAt = now.Add(15 * time.Minute)
	}
	current, err = trafficoverride.Sign(current, keyring.CurrentPrivateKey, keyring.CurrentKeyID)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "traffic override signer unavailable")
		return
	}
	stored, err := s.store.PutTrafficOverrideCAS(current, req.ExpectedGeneration)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "traffic_override.revoke", "traffic_override", stored.Hostname, "", trafficOverrideAuditMetadata(stored))
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"override": stored})
}

func (s *Server) handleAdminGetTrafficOverrideSigningKey(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	keyring, err := s.store.GetTrafficOverrideSigningKeyring()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"signing_key": keyring.Status()})
}

func (s *Server) handleAdminRotateTrafficOverrideSigningKey(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	var req rotateTrafficOverrideSigningKeyRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	keyring, err := s.store.RotateTrafficOverrideSigningKeyring(req.ExpectedGeneration)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "traffic_override.signing_key.rotate", "traffic_override_signing_key", keyring.CurrentKeyID, "", map[string]string{
		"generation":      strconv.FormatUint(keyring.Generation, 10),
		"current_key_id":  keyring.CurrentKeyID,
		"previous_key_id": keyring.PreviousKeyID,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"signing_key": keyring.Status()})
}

func trafficOverrideAuditMetadata(override model.TrafficOverride) map[string]string {
	return map[string]string{
		"generation":       strconv.FormatUint(override.Generation, 10),
		"state":            override.State,
		"artifact_digest":  override.ArtifactDigest,
		"key_id":           override.KeyID,
		"route_generation": override.RouteGeneration,
		"route_digest":     override.RouteDigest,
		"expires_at":       override.ExpiresAt.UTC().Format(time.RFC3339Nano),
		"reason":           override.Reason,
	}
}

func normalizeTrafficOverrideAnswers(values []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, value := range values {
		if parsed := net.ParseIP(strings.TrimSpace(value)); parsed != nil {
			value = parsed.String()
		} else {
			value = strings.TrimSpace(value)
		}
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func normalizeTrafficOverrideHostnames(values []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, value := range values {
		value = normalizeExternalAppDomain(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
