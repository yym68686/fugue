package api

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

type putEdgeRoutePolicyRequest struct {
	EdgeGroupID          string     `json:"edge_group_id,omitempty"`
	ExcludedEdgeIDs      []string   `json:"excluded_edge_ids,omitempty"`
	ExcludedEdgeGroupIDs []string   `json:"excluded_edge_group_ids,omitempty"`
	ExclusionReason      string     `json:"exclusion_reason,omitempty"`
	ExclusionExpiresAt   *time.Time `json:"exclusion_expires_at,omitempty"`
	MinHealthyEdgeNodes  int        `json:"min_healthy_edge_nodes,omitempty"`
	RoutePolicy          string     `json:"route_policy"`
	Enabled              *bool      `json:"enabled,omitempty"`
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
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"policy": policy})
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
	if model.EdgeRoutePolicyAllowsTraffic(policyValue) && edgeGroupID == "" && len(excludedEdgeIDs) == 0 && len(excludedEdgeGroupIDs) == 0 {
		httpx.WriteError(w, http.StatusBadRequest, "edge_group_id or an exclusion list is required for edge_canary and edge_enabled policies")
		return
	}
	if len(excludedEdgeIDs) > 0 || len(excludedEdgeGroupIDs) > 0 {
		if req.ExclusionExpiresAt != nil && !req.ExclusionExpiresAt.After(time.Now().UTC()) {
			httpx.WriteError(w, http.StatusBadRequest, "exclusion_expires_at must be in the future")
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
	policy, err := s.store.PutEdgeRoutePolicy(model.EdgeRoutePolicy{
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
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "edge.route_policy.put", "edge_route_policy", hostname, app.TenantID, map[string]string{
		"hostname":                hostname,
		"app_id":                  app.ID,
		"edge_group_id":           policy.EdgeGroupID,
		"excluded_edge_ids":       strings.Join(policy.ExcludedEdgeIDs, ","),
		"excluded_edge_group_ids": strings.Join(policy.ExcludedEdgeGroupIDs, ","),
		"min_healthy_edge_nodes":  fmt.Sprintf("%d", policy.MinHealthyEdgeNodes),
		"route_policy":            policy.RoutePolicy,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"policy": policy})
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
