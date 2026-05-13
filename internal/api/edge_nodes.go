package api

import (
	"context"
	"errors"
	"net"
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
	req = s.enrichEdgeHeartbeatFromClusterNode(r.Context(), req)
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

func (s *Server) enrichEdgeHeartbeatFromClusterNode(ctx context.Context, req edgeHeartbeatRequest) edgeHeartbeatRequest {
	endpoint := s.discoverClusterNodeEndpoint(ctx, req.EdgeID)
	if strings.TrimSpace(req.Region) == "" {
		req.Region = endpoint.Region
	}
	if strings.TrimSpace(req.Country) == "" {
		req.Country = endpoint.Country
	}
	if strings.TrimSpace(req.PublicIPv4) == "" {
		req.PublicIPv4 = endpoint.PublicIPv4
	}
	if strings.TrimSpace(req.PublicIPv6) == "" {
		req.PublicIPv6 = endpoint.PublicIPv6
	}
	if strings.TrimSpace(req.MeshIP) == "" {
		req.MeshIP = endpoint.MeshIP
	}
	return req
}

type discoveredClusterNodeEndpoint struct {
	Region     string
	Country    string
	PublicIPv4 string
	PublicIPv6 string
	MeshIP     string
}

func (s *Server) discoverClusterNodeEndpoint(ctx context.Context, nodeName string) discoveredClusterNodeEndpoint {
	nodeName = strings.TrimSpace(nodeName)
	if s == nil || nodeName == "" {
		return discoveredClusterNodeEndpoint{}
	}
	snapshots, err := s.loadClusterNodeInventory(ctx)
	if err != nil {
		if s.log != nil {
			s.log.Printf("edge node endpoint discovery skipped; node=%s error=%v", nodeName, err)
		}
		return discoveredClusterNodeEndpoint{}
	}
	for _, snapshot := range snapshots {
		if !strings.EqualFold(strings.TrimSpace(snapshot.node.Name), nodeName) {
			continue
		}
		var out discoveredClusterNodeEndpoint
		out.Region = strings.TrimSpace(snapshot.node.Region)
		out.Country = strings.ToLower(strings.TrimSpace(snapshot.countryCode))
		out.MeshIP = strings.TrimSpace(snapshot.node.InternalIP)
		for _, value := range []string{snapshot.node.PublicIP, snapshot.node.ExternalIP} {
			ipValue := publicIPLiteral(value)
			if ipValue == "" {
				continue
			}
			ip := net.ParseIP(ipValue)
			if ip == nil {
				continue
			}
			if ip.To4() != nil {
				if out.PublicIPv4 == "" {
					out.PublicIPv4 = ipValue
				}
			} else if out.PublicIPv6 == "" {
				out.PublicIPv6 = ipValue
			}
		}
		return out
	}
	return discoveredClusterNodeEndpoint{}
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
