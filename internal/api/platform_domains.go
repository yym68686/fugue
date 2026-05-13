package api

import (
	"errors"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

type putPlatformDomainBindingRequest struct {
	AppID       string `json:"app_id"`
	RoutePolicy string `json:"route_policy,omitempty"`
	EdgeGroupID string `json:"edge_group_id,omitempty"`
}

func (s *Server) handleListPlatformDomainBindings(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can manage platform domains")
		return
	}
	bindings, err := s.platformDomainBindings(r.URL.Query().Get("zone"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"bindings": bindings})
}

func (s *Server) handleGetPlatformDomainBinding(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can manage platform domains")
		return
	}
	hostname, reason := s.normalizeRequestedPlatformDomainBinding(r.PathValue("hostname"))
	if reason != "" {
		httpx.WriteError(w, http.StatusBadRequest, reason)
		return
	}
	binding, err := s.platformDomainBinding(hostname)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"binding": binding})
}

func (s *Server) handlePutPlatformDomainBinding(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can manage platform domains")
		return
	}
	hostname, reason := s.normalizeRequestedPlatformDomainBinding(r.PathValue("hostname"))
	if reason != "" {
		httpx.WriteError(w, http.StatusBadRequest, reason)
		return
	}
	var req putPlatformDomainBindingRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	appID := strings.TrimSpace(req.AppID)
	if appID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "app_id is required")
		return
	}
	app, err := s.store.GetApp(appID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	policyValue := model.NormalizeEdgeRoutePolicy(req.RoutePolicy)
	if strings.TrimSpace(req.RoutePolicy) == "" {
		policyValue = model.EdgeRoutePolicyEnabled
	}
	if policyValue == "" {
		httpx.WriteError(w, http.StatusBadRequest, "route_policy must be route_a_only, edge_canary, or edge_enabled")
		return
	}
	edgeGroupID := strings.TrimSpace(strings.ToLower(req.EdgeGroupID))
	if policyValue == model.EdgeRoutePolicyCanary && edgeGroupID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "edge_group_id is required for edge_canary")
		return
	}
	if policyValue == model.EdgeRoutePolicyEnabled && edgeGroupID != "" {
		httpx.WriteError(w, http.StatusBadRequest, "edge_group_id is only supported with edge_canary")
		return
	}

	if existing, err := s.store.GetAppDomain(hostname); err == nil && strings.TrimSpace(existing.AppID) != strings.TrimSpace(app.ID) {
		if _, deleteErr := s.store.DeleteAppDomain(existing.AppID, hostname); deleteErr != nil {
			s.writeStoreError(w, deleteErr)
			return
		}
	} else if err != nil && !errors.Is(err, store.ErrNotFound) {
		s.writeStoreError(w, err)
		return
	}

	now := time.Now().UTC()
	domain, err := s.store.PutAppDomain(model.AppDomain{
		Hostname:       hostname,
		AppID:          app.ID,
		TenantID:       app.TenantID,
		Status:         model.AppDomainStatusVerified,
		TLSStatus:      model.AppDomainTLSStatusReady,
		LastMessage:    "platform-owned domain binding",
		TLSLastMessage: "static platform TLS certificate",
		VerifiedAt:     &now,
		TLSReadyAt:     &now,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if err := s.putPlatformDomainRoutePolicy(domain, app, policyValue, edgeGroupID); err != nil {
		s.writeStoreError(w, err)
		return
	}
	binding, err := s.platformDomainBinding(hostname)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "platform.domain.put", "platform_domain", hostname, app.TenantID, map[string]string{
		"hostname":      hostname,
		"app_id":        app.ID,
		"route_policy":  binding.RoutePolicy,
		"edge_group_id": binding.EdgeGroupID,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"binding": binding})
}

func (s *Server) handleDeletePlatformDomainBinding(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can manage platform domains")
		return
	}
	hostname, reason := s.normalizeRequestedPlatformDomainBinding(r.PathValue("hostname"))
	if reason != "" {
		httpx.WriteError(w, http.StatusBadRequest, reason)
		return
	}
	binding, err := s.platformDomainBinding(hostname)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if _, err := s.store.DeleteAppDomain(binding.AppID, hostname); err != nil {
		s.writeStoreError(w, err)
		return
	}
	if _, err := s.store.DeleteEdgeRoutePolicy(hostname); err != nil && !errors.Is(err, store.ErrNotFound) {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "platform.domain.delete", "platform_domain", hostname, binding.TenantID, map[string]string{
		"hostname": hostname,
		"app_id":   binding.AppID,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"binding": binding,
		"deleted": true,
	})
}

func (s *Server) putPlatformDomainRoutePolicy(domain model.AppDomain, app model.App, routePolicy, edgeGroupID string) error {
	hostname := normalizeExternalAppDomain(domain.Hostname)
	switch routePolicy {
	case model.EdgeRoutePolicyEnabled:
		if _, err := s.store.DeleteEdgeRoutePolicy(hostname); err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
		return nil
	case model.EdgeRoutePolicyCanary, model.EdgeRoutePolicyRouteAOnly:
		_, err := s.store.PutEdgeRoutePolicy(model.EdgeRoutePolicy{
			Hostname:    hostname,
			AppID:       app.ID,
			TenantID:    app.TenantID,
			EdgeGroupID: edgeGroupID,
			RoutePolicy: routePolicy,
		})
		return err
	default:
		return store.ErrInvalidInput
	}
}

func (s *Server) platformDomainBindings(zone string) ([]model.PlatformDomainBinding, error) {
	domains, err := s.store.ListVerifiedAppDomains()
	if err != nil {
		return nil, err
	}
	apps, err := s.store.ListAppsMetadata("", true)
	if err != nil {
		return nil, err
	}
	policies, err := s.store.ListEdgeRoutePolicies()
	if err != nil {
		return nil, err
	}
	appByID := make(map[string]model.App, len(apps))
	for _, app := range apps {
		appByID[strings.TrimSpace(app.ID)] = app
	}
	policyByHostname := edgeRoutePolicyByHostname(policies)
	zone = normalizeExternalAppDomain(zone)
	out := make([]model.PlatformDomainBinding, 0)
	for _, domain := range domains {
		hostname := normalizeExternalAppDomain(domain.Hostname)
		if !s.isPlatformOwnedDomainBinding(hostname) {
			continue
		}
		if zone != "" && !edgeDNSTargetWithinZone(hostname, zone) {
			continue
		}
		app, ok := appByID[strings.TrimSpace(domain.AppID)]
		if !ok {
			continue
		}
		out = append(out, platformDomainBindingFromDomain(domain, app, policyByHostname[hostname], s.appBaseDomain))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Hostname < out[j].Hostname
	})
	return out, nil
}

func (s *Server) platformDomainBinding(hostname string) (model.PlatformDomainBinding, error) {
	domain, err := s.store.GetAppDomain(hostname)
	if err != nil {
		return model.PlatformDomainBinding{}, err
	}
	hostname = normalizeExternalAppDomain(domain.Hostname)
	if domain.Status != model.AppDomainStatusVerified || !s.isPlatformOwnedDomainBinding(hostname) {
		return model.PlatformDomainBinding{}, store.ErrNotFound
	}
	app, err := s.store.GetAppMetadata(domain.AppID)
	if err != nil {
		return model.PlatformDomainBinding{}, err
	}
	policy, err := s.store.GetEdgeRoutePolicy(hostname)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return model.PlatformDomainBinding{}, err
	}
	if errors.Is(err, store.ErrNotFound) {
		policy = model.EdgeRoutePolicy{}
	}
	return platformDomainBindingFromDomain(domain, app, policy, s.appBaseDomain), nil
}

func platformDomainBindingFromDomain(domain model.AppDomain, app model.App, policy model.EdgeRoutePolicy, appBaseDomain string) model.PlatformDomainBinding {
	edgeGroupID := strings.TrimSpace(policy.EdgeGroupID)
	routePolicy := ""
	if strings.TrimSpace(policy.Hostname) != "" {
		routePolicy = model.NormalizeEdgeRoutePolicy(policy.RoutePolicy)
	}
	if routePolicy == "" {
		routePolicy = model.EdgeRoutePolicyEnabled
		edgeGroupID = ""
	}
	return model.PlatformDomainBinding{
		Hostname:    normalizeExternalAppDomain(domain.Hostname),
		Zone:        normalizeExternalAppDomain(appBaseDomain),
		AppID:       domain.AppID,
		AppName:     app.Name,
		ProjectID:   app.ProjectID,
		TenantID:    domain.TenantID,
		Status:      domain.Status,
		TLSStatus:   domain.TLSStatus,
		RoutePolicy: routePolicy,
		EdgeGroupID: edgeGroupID,
		DNSKind:     model.EdgeDNSRecordKindPlatformDomain,
		CreatedAt:   domain.CreatedAt,
		UpdatedAt:   domain.UpdatedAt,
	}
}

func (s *Server) normalizeRequestedPlatformDomainBinding(raw string) (string, string) {
	hostname := normalizeExternalAppDomain(raw)
	if hostname == "" {
		return "", "hostname is required"
	}
	if net.ParseIP(hostname) != nil {
		return "", "hostname must be a domain name"
	}
	if strings.HasPrefix(hostname, "*.") {
		return "", "wildcard hostnames are not supported"
	}
	if !s.isPlatformOwnedDomainBinding(hostname) {
		return "", "hostname must be inside the platform app domain and outside reserved subzones"
	}
	labels := strings.Split(hostname, ".")
	if len(labels) < 2 {
		return "", "hostname must be a fully-qualified domain name"
	}
	for _, label := range labels {
		if !appRouteLabelPattern.MatchString(label) {
			return "", "hostname must use lowercase letters, numbers, or hyphens"
		}
	}
	return hostname, ""
}

func (s *Server) isPlatformOwnedDomainBinding(hostname string) bool {
	hostname = normalizeExternalAppDomain(hostname)
	if hostname == "" || s.isReservedAppHostname(hostname) {
		return false
	}
	appBase := normalizeExternalAppDomain(s.appBaseDomain)
	if appBase == "" || (hostname != appBase && !strings.HasSuffix(hostname, "."+appBase)) {
		return false
	}
	customBase := normalizeExternalAppDomain(s.customDomainBaseDomain)
	if customBase != "" && (hostname == customBase || strings.HasSuffix(hostname, "."+customBase)) {
		return false
	}
	return true
}
