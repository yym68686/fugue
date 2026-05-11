package api

import (
	"errors"
	"net/http"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

type createEdgeNodeTokenRequest struct {
	EdgeGroupID    string `json:"edge_group_id"`
	Region         string `json:"region,omitempty"`
	Country        string `json:"country,omitempty"`
	PublicHostname string `json:"public_hostname,omitempty"`
	PublicIPv4     string `json:"public_ipv4,omitempty"`
	PublicIPv6     string `json:"public_ipv6,omitempty"`
	MeshIP         string `json:"mesh_ip,omitempty"`
	Draining       bool   `json:"draining,omitempty"`
}

type edgeHeartbeatRequest struct {
	EdgeID              string `json:"edge_id"`
	EdgeGroupID         string `json:"edge_group_id"`
	Region              string `json:"region,omitempty"`
	Country             string `json:"country,omitempty"`
	PublicHostname      string `json:"public_hostname,omitempty"`
	PublicIPv4          string `json:"public_ipv4,omitempty"`
	PublicIPv6          string `json:"public_ipv6,omitempty"`
	MeshIP              string `json:"mesh_ip,omitempty"`
	RouteBundleVersion  string `json:"route_bundle_version,omitempty"`
	DNSBundleVersion    string `json:"dns_bundle_version,omitempty"`
	CaddyRouteCount     int    `json:"caddy_route_count,omitempty"`
	CaddyAppliedVersion string `json:"caddy_applied_version,omitempty"`
	CaddyLastError      string `json:"caddy_last_error,omitempty"`
	CacheStatus         string `json:"cache_status,omitempty"`
	Status              string `json:"status"`
	Healthy             bool   `json:"healthy"`
	Draining            bool   `json:"draining"`
	LastError           string `json:"last_error,omitempty"`
}

func (s *Server) handleListEdgeNodes(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect edge nodes")
		return
	}
	nodes, groups, err := s.store.ListEdgeNodes(r.URL.Query().Get("edge_group_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"nodes":  nodes,
		"groups": groups,
	})
}

func (s *Server) handleGetEdgeNode(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect edge nodes")
		return
	}
	node, group, err := s.store.GetEdgeNode(r.PathValue("edge_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"node":  node,
		"group": group,
	})
}

func (s *Server) handleCreateEdgeNodeToken(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can create edge node tokens")
		return
	}
	edgeID := strings.TrimSpace(r.PathValue("edge_id"))
	if edgeID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "edge_id is required")
		return
	}
	var req createEdgeNodeTokenRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	node, token, err := s.store.CreateEdgeNodeToken(model.EdgeNode{
		ID:             edgeID,
		EdgeGroupID:    req.EdgeGroupID,
		Region:         req.Region,
		Country:        req.Country,
		PublicHostname: req.PublicHostname,
		PublicIPv4:     req.PublicIPv4,
		PublicIPv6:     req.PublicIPv6,
		MeshIP:         req.MeshIP,
		Status:         model.EdgeHealthUnknown,
		Draining:       req.Draining,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "edge.node.token.create", "edge_node", node.ID, "", map[string]string{
		"edge_id":       node.ID,
		"edge_group_id": node.EdgeGroupID,
	})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{
		"node":  node,
		"token": token,
	})
}

func (s *Server) handleEdgeHeartbeat(w http.ResponseWriter, r *http.Request) {
	authContext, ok := s.authorizeEdgeRequest(w, r)
	if !ok {
		return
	}
	var req edgeHeartbeatRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := authContext.constrain(&req.EdgeID, &req.EdgeGroupID); err != nil {
		httpx.WriteError(w, http.StatusForbidden, err.Error())
		return
	}
	status := model.NormalizeEdgeHealthStatus(req.Status)
	if status == "" {
		httpx.WriteError(w, http.StatusBadRequest, "status must be unknown, healthy, degraded, or unhealthy")
		return
	}
	node, _, err := s.store.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:                  req.EdgeID,
		EdgeGroupID:         req.EdgeGroupID,
		Region:              req.Region,
		Country:             req.Country,
		PublicHostname:      req.PublicHostname,
		PublicIPv4:          req.PublicIPv4,
		PublicIPv6:          req.PublicIPv6,
		MeshIP:              req.MeshIP,
		RouteBundleVersion:  req.RouteBundleVersion,
		DNSBundleVersion:    req.DNSBundleVersion,
		CaddyRouteCount:     req.CaddyRouteCount,
		CaddyAppliedVersion: req.CaddyAppliedVersion,
		CaddyLastError:      req.CaddyLastError,
		CacheStatus:         req.CacheStatus,
		Status:              status,
		Healthy:             req.Healthy,
		Draining:            req.Draining,
		LastError:           req.LastError,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"node":     node,
		"accepted": true,
	})
}

type edgeAuthContext struct {
	EdgeID      string
	EdgeGroupID string
	Scoped      bool
}

func (ctx edgeAuthContext) constrain(edgeID *string, edgeGroupID *string) error {
	if !ctx.Scoped {
		return nil
	}
	if strings.TrimSpace(*edgeID) == "" {
		*edgeID = ctx.EdgeID
	}
	if strings.TrimSpace(*edgeGroupID) == "" {
		*edgeGroupID = ctx.EdgeGroupID
	}
	if !strings.EqualFold(strings.TrimSpace(*edgeID), ctx.EdgeID) {
		return errors.New("edge token cannot access another edge_id")
	}
	if !strings.EqualFold(strings.TrimSpace(*edgeGroupID), ctx.EdgeGroupID) {
		return errors.New("edge token cannot access another edge_group_id")
	}
	return nil
}

func (s *Server) authorizeEdgeRequest(w http.ResponseWriter, r *http.Request) (edgeAuthContext, bool) {
	token := edgeTokenFromRequest(r)
	if token == "" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return edgeAuthContext{}, false
	}
	if s.store != nil {
		node, err := s.store.AuthenticateEdgeNode(token)
		if err == nil {
			return edgeAuthContext{
				EdgeID:      node.ID,
				EdgeGroupID: node.EdgeGroupID,
				Scoped:      true,
			}, true
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) && !errors.Is(err, store.ErrInvalidInput) {
			s.writeStoreError(w, err)
			return edgeAuthContext{}, false
		}
	}
	if strings.TrimSpace(s.edgeTLSAskToken) != "" && subtleConstantCompare(token, s.edgeTLSAskToken) {
		return edgeAuthContext{}, true
	}
	http.Error(w, "forbidden", http.StatusForbidden)
	return edgeAuthContext{}, false
}

func edgeTokenFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	if token := strings.TrimSpace(r.URL.Query().Get("token")); token != "" {
		return token
	}
	authz := strings.TrimSpace(r.Header.Get("Authorization"))
	parts := strings.SplitN(authz, " ", 2)
	if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
		return strings.TrimSpace(parts[1])
	}
	return ""
}
