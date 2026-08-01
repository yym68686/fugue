package api

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

type putEdgeRoutePolicyRequest struct {
	EdgeGroupID                 string     `json:"edge_group_id,omitempty"`
	ExcludedEdgeIDs             []string   `json:"excluded_edge_ids,omitempty"`
	ExcludedEdgeGroupIDs        []string   `json:"excluded_edge_group_ids,omitempty"`
	ExclusionReason             string     `json:"exclusion_reason,omitempty"`
	ExclusionExpiresAt          *time.Time `json:"exclusion_expires_at,omitempty"`
	MinHealthyEdgeNodes         int        `json:"min_healthy_edge_nodes,omitempty"`
	RoutePolicy                 string     `json:"route_policy"`
	Enabled                     *bool      `json:"enabled,omitempty"`
	ExpectedExclusionGeneration uint64     `json:"expected_exclusion_generation,omitempty"`
	ExpectedExclusionFence      string     `json:"expected_exclusion_fence,omitempty"`
}

func (s *Server) handleListEdgeRoutePolicies(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can manage edge route policies")
		return
	}
	policies, err := s.store.ListEdgeRoutePolicies()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	now, err := s.store.EdgeRoutePolicyTime()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	for index := range policies {
		policies[index] = s.edgeRoutePolicyReadView(policies[index], now)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"policies": policies})
}

func (s *Server) handleGetEdgeRoutePolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can manage edge route policies")
		return
	}
	policy, err := s.store.GetEdgeRoutePolicy(r.PathValue("hostname"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	now, err := s.store.EdgeRoutePolicyTime()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"policy": s.edgeRoutePolicyReadView(policy, now)})
}

func (s *Server) handlePutEdgeRoutePolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can manage edge route policies")
		return
	}

	hostname := normalizeExternalAppDomain(r.PathValue("hostname"))
	if hostname == "" {
		httpx.WriteError(w, http.StatusBadRequest, "hostname is required")
		return
	}
	var req putEdgeRoutePolicyRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	now, err := s.store.EdgeRoutePolicyTime()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	policyValue := model.NormalizeEdgeRoutePolicy(req.RoutePolicy)
	if policyValue == "" {
		httpx.WriteError(w, http.StatusBadRequest, "route_policy must be route_a_only, edge_canary, or edge_enabled")
		return
	}
	if req.Enabled != nil {
		switch {
		case *req.Enabled && policyValue == model.EdgeRoutePolicyRouteAOnly:
			policyValue = model.EdgeRoutePolicyCanary
		case !*req.Enabled:
			policyValue = model.EdgeRoutePolicyRouteAOnly
		}
	}
	edgeGroupID := strings.TrimSpace(strings.ToLower(req.EdgeGroupID))
	excludedEdgeIDs := normalizeEdgeRoutePolicyRequestIDs(req.ExcludedEdgeIDs)
	excludedEdgeGroupIDs := normalizeEdgeRoutePolicyRequestIDs(req.ExcludedEdgeGroupIDs)
	desiredHasExclusions := len(excludedEdgeIDs) > 0 || len(excludedEdgeGroupIDs) > 0
	if model.EdgeRoutePolicyAllowsTraffic(policyValue) && edgeGroupID == "" && len(excludedEdgeIDs) == 0 && len(excludedEdgeGroupIDs) == 0 {
		httpx.WriteError(w, http.StatusBadRequest, "edge_group_id or an exclusion list is required for edge_canary and edge_enabled policies")
		return
	}
	if desiredHasExclusions {
		if strings.TrimSpace(req.ExclusionReason) == "" {
			httpx.WriteError(w, http.StatusBadRequest, "exclusion_reason is required while exclusions are configured")
			return
		}
		if err := s.validateEdgeRoutePolicyExclusions(excludedEdgeIDs, excludedEdgeGroupIDs); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	app, err := s.store.GetAppByHostname(hostname)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			httpx.WriteError(w, http.StatusNotFound, "hostname does not resolve to a Fugue app route or verified custom domain")
			return
		}
		s.writeStoreError(w, err)
		return
	}
	minHealthyEdgeNodes := req.MinHealthyEdgeNodes
	if minHealthyEdgeNodes < 0 {
		httpx.WriteError(w, http.StatusBadRequest, "min_healthy_edge_nodes must be >= 0")
		return
	}
	if model.EdgeRoutePolicyAllowsTraffic(policyValue) && minHealthyEdgeNodes == 0 {
		minHealthyEdgeNodes = defaultMinHealthyEdgeNodesForPolicyHostname(s, hostname)
	}
	existing, existingErr := s.store.GetEdgeRoutePolicy(hostname)
	if existingErr != nil && !errors.Is(existingErr, store.ErrNotFound) {
		s.writeStoreError(w, existingErr)
		return
	}
	existingFound := existingErr == nil
	existingHasExclusions := existingFound && model.EdgeRoutePolicyHasExclusions(existing)
	removedEdgeIDs := removedEdgeRoutePolicyIDs(existing.ExcludedEdgeIDs, excludedEdgeIDs)
	removedEdgeGroupIDs := removedEdgeRoutePolicyIDs(existing.ExcludedEdgeGroupIDs, excludedEdgeGroupIDs)
	clearing := len(removedEdgeIDs) > 0 || len(removedEdgeGroupIDs) > 0
	ownerDigest := edgeExclusionPrincipalDigest(principal)
	if (desiredHasExclusions || existingHasExclusions) && ownerDigest == "" {
		httpx.WriteError(w, http.StatusForbidden, "authenticated exclusion owner identity is required")
		return
	}
	if existingHasExclusions {
		if req.ExpectedExclusionGeneration != existing.ExclusionGeneration || strings.TrimSpace(req.ExpectedExclusionFence) != strings.TrimSpace(existing.ExclusionFence) {
			httpx.WriteError(w, http.StatusConflict, "edge exclusion generation or fence is stale")
			return
		}
		if !edgeExclusionPrincipalCanMutate(principal, ownerDigest, existing.ExclusionOwnerDigest) {
			httpx.WriteError(w, http.StatusForbidden, "edge exclusion is owned by another principal")
			return
		}
	}
	if req.ExclusionExpiresAt != nil && !req.ExclusionExpiresAt.After(now) {
		preservingExpired := existingHasExclusions && existing.ExclusionExpiresAt != nil && existing.ExclusionExpiresAt.Equal(*req.ExclusionExpiresAt)
		if !preservingExpired {
			httpx.WriteError(w, http.StatusBadRequest, "exclusion_expires_at must be in the future")
			return
		}
	}
	if clearing {
		if !envBool("FUGUE_EDGE_EXCLUSION_CLEAR_ENABLED", false) {
			httpx.WriteError(w, http.StatusConflict, "edge exclusion clear is disabled")
			return
		}
		if err := s.validateEdgeExclusionClearEvidence(existing, removedEdgeIDs, removedEdgeGroupIDs, now); err != nil {
			httpx.WriteError(w, http.StatusConflict, err.Error())
			return
		}
	}
	candidate := model.EdgeRoutePolicy{
		Hostname:             hostname,
		AppID:                app.ID,
		TenantID:             app.TenantID,
		EdgeGroupID:          edgeGroupID,
		ExcludedEdgeIDs:      excludedEdgeIDs,
		ExcludedEdgeGroupIDs: excludedEdgeGroupIDs,
		ExclusionReason:      strings.TrimSpace(req.ExclusionReason),
		ExclusionExpiresAt:   req.ExclusionExpiresAt,
		MinHealthyEdgeNodes:  minHealthyEdgeNodes,
		RoutePolicy:          policyValue,
	}
	if desiredHasExclusions {
		candidate.ExclusionOwnerDigest = ownerDigest
		candidate.ExclusionCreatedAt = existing.ExclusionCreatedAt
		if existingHasExclusions && strings.TrimSpace(existing.ExclusionOwnerDigest) != "" {
			candidate.ExclusionOwnerDigest = existing.ExclusionOwnerDigest
		}
	}
	var policy model.EdgeRoutePolicy
	if desiredHasExclusions || existingHasExclusions {
		policy, err = s.store.PutEdgeRoutePolicyCAS(candidate, req.ExpectedExclusionGeneration, req.ExpectedExclusionFence)
	} else {
		policy, err = s.store.PutEdgeRoutePolicy(candidate)
	}
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "edge.route_policy.put", "edge_route_policy", hostname, app.TenantID, map[string]string{
		"hostname":               hostname,
		"app_id":                 app.ID,
		"edge_group_id":          policy.EdgeGroupID,
		"exclusion_ref_digest":   edgeExclusionRefDigest(policy),
		"excluded_edge_count":    fmt.Sprintf("%d", len(policy.ExcludedEdgeIDs)),
		"excluded_group_count":   fmt.Sprintf("%d", len(policy.ExcludedEdgeGroupIDs)),
		"min_healthy_edge_nodes": fmt.Sprintf("%d", policy.MinHealthyEdgeNodes),
		"route_policy":           policy.RoutePolicy,
		"exclusion_owner_digest": policy.ExclusionOwnerDigest,
		"exclusion_lifecycle":    model.EdgeRoutePolicyExclusionLifecycleAt(policy, now),
		"exclusion_generation":   fmt.Sprintf("%d", policy.ExclusionGeneration),
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"policy": policy})
}

func edgeExclusionRefDigest(policy model.EdgeRoutePolicy) string {
	if !model.EdgeRoutePolicyHasExclusions(policy) {
		return ""
	}
	digest := sha256.Sum256([]byte(strings.Join(policy.ExcludedEdgeIDs, ",") + "\x00" + strings.Join(policy.ExcludedEdgeGroupIDs, ",")))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func normalizeEdgeRoutePolicyRequestIDs(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(strings.ToLower(value))
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
	if len(out) == 0 {
		return nil
	}
	return out
}

func (s *Server) validateEdgeRoutePolicyExclusions(edgeIDs, edgeGroupIDs []string) error {
	for _, edgeID := range edgeIDs {
		if _, _, err := s.store.GetEdgeNode(edgeID); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return fmt.Errorf("excluded edge %q does not exist", edgeID)
			}
			return err
		}
	}
	for _, edgeGroupID := range edgeGroupIDs {
		_, groups, err := s.store.ListEdgeNodes(edgeGroupID)
		if err != nil {
			return err
		}
		found := false
		for _, group := range groups {
			if strings.EqualFold(strings.TrimSpace(group.ID), edgeGroupID) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("excluded edge group %q does not exist", edgeGroupID)
		}
	}
	return nil
}

func (s *Server) handleDeleteEdgeRoutePolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can manage edge route policies")
		return
	}
	hostname := normalizeExternalAppDomain(r.PathValue("hostname"))
	if hostname == "" {
		httpx.WriteError(w, http.StatusBadRequest, "hostname is required")
		return
	}
	current, err := s.store.GetEdgeRoutePolicy(hostname)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	now, err := s.store.EdgeRoutePolicyTime()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if model.EdgeRoutePolicyHasExclusions(current) {
		expectedGeneration, parseErr := strconv.ParseUint(strings.TrimSpace(r.URL.Query().Get("expected_exclusion_generation")), 10, 64)
		expectedFence := strings.TrimSpace(r.URL.Query().Get("expected_exclusion_fence"))
		ownerDigest := edgeExclusionPrincipalDigest(principal)
		if parseErr != nil || expectedGeneration != current.ExclusionGeneration || expectedFence != current.ExclusionFence {
			httpx.WriteError(w, http.StatusConflict, "edge exclusion generation or fence is stale")
			return
		}
		if !edgeExclusionPrincipalCanMutate(principal, ownerDigest, current.ExclusionOwnerDigest) {
			httpx.WriteError(w, http.StatusForbidden, "edge exclusion is owned by another principal")
			return
		}
		if !envBool("FUGUE_EDGE_EXCLUSION_CLEAR_ENABLED", false) {
			httpx.WriteError(w, http.StatusConflict, "edge exclusion clear is disabled")
			return
		}
		if err := s.validateEdgeExclusionClearEvidence(current, current.ExcludedEdgeIDs, current.ExcludedEdgeGroupIDs, now); err != nil {
			httpx.WriteError(w, http.StatusConflict, err.Error())
			return
		}
		policy, err := s.store.DeleteEdgeRoutePolicyCAS(hostname, expectedGeneration, expectedFence)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		s.appendAudit(principal, "edge.route_policy.delete", "edge_route_policy", hostname, policy.TenantID, map[string]string{
			"hostname": hostname, "app_id": policy.AppID, "edge_group_id": policy.EdgeGroupID,
			"route_policy": policy.RoutePolicy, "exclusion_owner_digest": policy.ExclusionOwnerDigest,
			"exclusion_generation": fmt.Sprintf("%d", policy.ExclusionGeneration),
		})
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"policy": policy, "deleted": true})
		return
	}
	policy, err := s.store.DeleteEdgeRoutePolicy(hostname)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "edge.route_policy.delete", "edge_route_policy", hostname, policy.TenantID, map[string]string{
		"hostname":      hostname,
		"app_id":        policy.AppID,
		"edge_group_id": policy.EdgeGroupID,
		"route_policy":  policy.RoutePolicy,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"policy":  policy,
		"deleted": true,
	})
}

func edgeExclusionPrincipalDigest(principal model.Principal) string {
	actorType := strings.TrimSpace(principal.ActorType)
	actorID := strings.TrimSpace(principal.ActorID)
	if actorType == "" || actorID == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(actorType + "\x00" + actorID))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func edgeExclusionPrincipalCanMutate(principal model.Principal, principalDigest, ownerDigest string) bool {
	ownerDigest = strings.TrimSpace(ownerDigest)
	return ownerDigest != "" && principalDigest == ownerDigest || principal.HasExplicitScope("edge.exclusion.override")
}

func removedEdgeRoutePolicyIDs(before, after []string) []string {
	remaining := make(map[string]struct{}, len(after))
	for _, value := range after {
		remaining[strings.TrimSpace(strings.ToLower(value))] = struct{}{}
	}
	removed := make([]string, 0)
	for _, value := range before {
		value = strings.TrimSpace(strings.ToLower(value))
		if _, ok := remaining[value]; !ok {
			removed = append(removed, value)
		}
	}
	sort.Strings(removed)
	return removed
}

func (s *Server) edgeRoutePolicyReadView(policy model.EdgeRoutePolicy, now time.Time) model.EdgeRoutePolicy {
	policy.ExclusionScope = model.EdgeRoutePolicyExclusionScope(policy)
	policy.ExclusionLifecycle = model.EdgeRoutePolicyExclusionLifecycleAt(policy, now)
	policy.ExclusionEvidenceFresh = false
	policy.ExclusionEvidenceReason = "clear evidence not evaluated"
	if !model.EdgeRoutePolicyHasExclusions(policy) {
		policy.ExclusionEvidenceReason = ""
		return policy
	}
	checked := now.UTC()
	policy.ExclusionEvidenceCheckedAt = &checked
	if err := s.validateEdgeExclusionClearEvidence(policy, policy.ExcludedEdgeIDs, policy.ExcludedEdgeGroupIDs, now); err != nil {
		policy.ExclusionEvidenceReason = err.Error()
		return policy
	}
	policy.ExclusionEvidenceFresh = true
	policy.ExclusionEvidenceReason = "fresh exact active-epoch TLS evidence"
	return policy
}

func (s *Server) validateEdgeExclusionClearEvidence(policy model.EdgeRoutePolicy, edgeIDs, edgeGroupIDs []string, _ time.Time) error {
	return s.store.CheckEdgeRoutePolicyClearEvidence(policy.Hostname, edgeIDs, edgeGroupIDs)
}
