package api

import (
	"errors"
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

type putEdgeRoutePolicyRequest struct {
	EdgeGroupID string `json:"edge_group_id,omitempty"`
	RoutePolicy string `json:"route_policy"`
	Enabled     *bool  `json:"enabled,omitempty"`
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
	if model.EdgeRoutePolicyAllowsTraffic(policyValue) && edgeGroupID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "edge_group_id is required for edge_canary and edge_enabled policies")
		return
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
	policy, err := s.store.PutEdgeRoutePolicy(model.EdgeRoutePolicy{
		Hostname:    hostname,
		AppID:       app.ID,
		TenantID:    app.TenantID,
		EdgeGroupID: edgeGroupID,
		RoutePolicy: policyValue,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "edge.route_policy.put", "edge_route_policy", hostname, app.TenantID, map[string]string{
		"hostname":      hostname,
		"app_id":        app.ID,
		"edge_group_id": policy.EdgeGroupID,
		"route_policy":  policy.RoutePolicy,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"policy": policy})
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
