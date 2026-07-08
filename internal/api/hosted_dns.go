package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

type createHostedDNSZoneRequest struct {
	ZoneName  string `json:"zone_name"`
	TenantID  string `json:"tenant_id,omitempty"`
	ProjectID string `json:"project_id,omitempty"`
}

type createHostedDNSRecordRequest struct {
	Name                  string   `json:"name"`
	Type                  string   `json:"type"`
	Values                []string `json:"values"`
	TTL                   int      `json:"ttl,omitempty"`
	Flatten               bool     `json:"flatten,omitempty"`
	FlattenMode           string   `json:"flatten_mode,omitempty"`
	FlattenTarget         string   `json:"flatten_target,omitempty"`
	FlattenIPv4Policy     string   `json:"flatten_ipv4_policy,omitempty"`
	FlattenIPv6Policy     string   `json:"flatten_ipv6_policy,omitempty"`
	FlattenTTLPolicy      string   `json:"flatten_ttl_policy,omitempty"`
	FlattenFallbackPolicy string   `json:"flatten_fallback_policy,omitempty"`
	Source                string   `json:"source,omitempty"`
	SourceRefType         string   `json:"source_ref_type,omitempty"`
	SourceRefID           string   `json:"source_ref_id,omitempty"`
	Overwrite             bool     `json:"overwrite,omitempty"`
}

type patchHostedDNSRecordRequest struct {
	Values                *[]string `json:"values,omitempty"`
	TTL                   *int      `json:"ttl,omitempty"`
	Flatten               *bool     `json:"flatten,omitempty"`
	FlattenMode           *string   `json:"flatten_mode,omitempty"`
	FlattenTarget         *string   `json:"flatten_target,omitempty"`
	FlattenIPv4Policy     *string   `json:"flatten_ipv4_policy,omitempty"`
	FlattenIPv6Policy     *string   `json:"flatten_ipv6_policy,omitempty"`
	FlattenTTLPolicy      *string   `json:"flatten_ttl_policy,omitempty"`
	FlattenFallbackPolicy *string   `json:"flatten_fallback_policy,omitempty"`
	Status                *string   `json:"status,omitempty"`
	Overwrite             bool      `json:"overwrite,omitempty"`
}

func (s *Server) handleListHostedDNSZones(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("dns.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing dns.read scope")
		return
	}
	zones, err := s.store.ListHostedZones(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "dns.zones.read", "dns_zone", "", principal.TenantID, nil)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"zones": zones})
}

func (s *Server) handleCreateHostedDNSZone(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("dns.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing dns.write scope")
		return
	}
	var req createHostedDNSZoneRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	zoneName := normalizeExternalAppDomain(req.ZoneName)
	if zoneName == "" {
		httpx.WriteError(w, http.StatusBadRequest, "zone_name is required")
		return
	}
	tenantID := strings.TrimSpace(principal.TenantID)
	if principal.IsPlatformAdmin() && strings.TrimSpace(req.TenantID) != "" {
		tenantID = strings.TrimSpace(req.TenantID)
	}
	if tenantID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "tenant_id is required when creating a hosted DNS zone as platform admin")
		return
	}
	projectID := strings.TrimSpace(req.ProjectID)
	if projectID != "" {
		project, err := s.store.GetProject(projectID)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		if strings.TrimSpace(project.TenantID) != tenantID {
			httpx.WriteError(w, http.StatusForbidden, "project does not belong to the hosted DNS tenant")
			return
		}
		if !principal.IsPlatformAdmin() && !principal.AllowsProject(projectID) {
			httpx.WriteError(w, http.StatusForbidden, "project is outside this credential scope")
			return
		}
	}

	now := time.Now().UTC()
	zone := model.HostedZone{
		TenantID:            tenantID,
		ProjectID:           projectID,
		ZoneName:            zoneName,
		Status:              model.HostedZoneStatusPendingDelegation,
		DelegationStatus:    model.HostedZoneDelegationStatusPending,
		ExpectedNameservers: append([]string(nil), s.dnsNameservers...),
		CreatedBy:           strings.TrimSpace(principal.ActorID),
		CreatedAt:           now,
		UpdatedAt:           now,
	}
	zone, err := s.store.PutHostedZone(zone)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	zone = s.refreshHostedDNSZonePreflight(r, principal, zone)
	s.appendAudit(principal, "dns.zone.create", "dns_zone", zone.ID, zone.TenantID, map[string]string{"zone": zone.ZoneName})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"zone": zone})
}

func (s *Server) handleGetHostedDNSZone(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("dns.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing dns.read scope")
		return
	}
	zone, ok := s.loadAuthorizedHostedDNSZone(w, r, principal)
	if !ok {
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"zone": zone})
}

func (s *Server) handleDeleteHostedDNSZone(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("dns.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing dns.write scope")
		return
	}
	zone, ok := s.loadAuthorizedHostedDNSZone(w, r, principal)
	if !ok {
		return
	}
	deleted, err := s.store.DeleteHostedZone(zone.ZoneName)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "dns.zone.delete", "dns_zone", deleted.ID, deleted.TenantID, map[string]string{"zone": deleted.ZoneName})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"deleted": true, "zone": deleted})
}

func (s *Server) handleHostedDNSZonePreflight(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("dns.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing dns.read scope")
		return
	}
	zone, ok := s.loadAuthorizedHostedDNSZone(w, r, principal)
	if !ok {
		return
	}
	response := s.buildDNSDelegationPreflight(r.Context(), principal, s.hostedDNSZonePreflightOptionsFromRequest(r, zone.ZoneName))
	zone = s.applyHostedDNSZonePreflight(zone, response)
	if stored, err := s.store.PutHostedZone(zone); err == nil {
		zone = stored
	} else if s.log != nil {
		s.log.Printf("hosted DNS preflight status update failed; zone=%s err=%v", zone.ZoneName, err)
	}
	s.appendAudit(principal, "dns.zone.preflight", "dns_zone", zone.ID, zone.TenantID, map[string]string{"zone": zone.ZoneName, "pass": strconvFormatBool(response.Pass)})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"zone": zone, "preflight": response})
}

func (s *Server) handleListHostedDNSRecords(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("dns.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing dns.read scope")
		return
	}
	zone, ok := s.loadAuthorizedHostedDNSZone(w, r, principal)
	if !ok {
		return
	}
	records, err := s.store.ListDNSRecords(zone.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"records": records})
}

func (s *Server) handleCreateHostedDNSRecord(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("dns.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing dns.write scope")
		return
	}
	zone, ok := s.loadAuthorizedHostedDNSZone(w, r, principal)
	if !ok {
		return
	}
	var req createHostedDNSRecordRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	source := model.NormalizeDNSRecordSource(req.Source)
	if source == "" {
		source = model.DNSRecordSourceUser
	}
	if source != model.DNSRecordSourceUser && !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can create non-user DNS record sources")
		return
	}
	record := model.DNSRecord{
		ZoneID:                zone.ID,
		TenantID:              zone.TenantID,
		Name:                  req.Name,
		Type:                  req.Type,
		Values:                append([]string(nil), req.Values...),
		TTL:                   req.TTL,
		FlattenMode:           req.FlattenMode,
		FlattenTarget:         req.FlattenTarget,
		FlattenIPv4Policy:     req.FlattenIPv4Policy,
		FlattenIPv6Policy:     req.FlattenIPv6Policy,
		FlattenTTLPolicy:      req.FlattenTTLPolicy,
		FlattenFallbackPolicy: req.FlattenFallbackPolicy,
		Source:                source,
		SourceRefType:         strings.TrimSpace(req.SourceRefType),
		SourceRefID:           strings.TrimSpace(req.SourceRefID),
		Status:                model.DNSRecordStatusActive,
		CreatedBy:             strings.TrimSpace(principal.ActorID),
	}
	if req.Flatten && strings.TrimSpace(record.FlattenMode) == "" {
		record.FlattenMode = model.DNSRecordFlattenModeAlways
	}
	record, err := s.store.PutDNSRecord(zone, record, req.Overwrite)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "dns.record.create", "dns_record", record.ID, zone.TenantID, map[string]string{"zone": zone.ZoneName, "name": record.Name, "type": record.Type})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"record": record})
}

func (s *Server) handlePatchHostedDNSRecord(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("dns.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing dns.write scope")
		return
	}
	zone, ok := s.loadAuthorizedHostedDNSZone(w, r, principal)
	if !ok {
		return
	}
	var req patchHostedDNSRecordRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	patch := model.DNSRecordPatch{
		Values:                req.Values,
		TTL:                   req.TTL,
		FlattenMode:           req.FlattenMode,
		FlattenTarget:         req.FlattenTarget,
		FlattenIPv4Policy:     req.FlattenIPv4Policy,
		FlattenIPv6Policy:     req.FlattenIPv6Policy,
		FlattenTTLPolicy:      req.FlattenTTLPolicy,
		FlattenFallbackPolicy: req.FlattenFallbackPolicy,
		Status:                req.Status,
	}
	if req.Flatten != nil {
		mode := model.DNSRecordFlattenModeNone
		if *req.Flatten {
			mode = model.DNSRecordFlattenModeAlways
		}
		if patch.FlattenMode == nil {
			patch.FlattenMode = &mode
		}
	}
	record, err := s.store.PatchDNSRecord(zone, strings.TrimSpace(r.PathValue("record_id")), patch, req.Overwrite)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "dns.record.patch", "dns_record", record.ID, zone.TenantID, map[string]string{"zone": zone.ZoneName, "name": record.Name, "type": record.Type})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"record": record})
}

func (s *Server) handleDeleteHostedDNSRecord(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("dns.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing dns.write scope")
		return
	}
	zone, ok := s.loadAuthorizedHostedDNSZone(w, r, principal)
	if !ok {
		return
	}
	record, err := s.store.GetDNSRecord(zone.ID, strings.TrimSpace(r.PathValue("record_id")))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if record.Source != model.DNSRecordSourceUser && !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can delete non-user DNS records")
		return
	}
	deleted, err := s.store.DeleteDNSRecord(zone.ID, record.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "dns.record.delete", "dns_record", deleted.ID, zone.TenantID, map[string]string{"zone": zone.ZoneName, "name": deleted.Name, "type": deleted.Type})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"deleted": true, "record": deleted})
}

func (s *Server) loadAuthorizedHostedDNSZone(w http.ResponseWriter, r *http.Request, principal model.Principal) (model.HostedZone, bool) {
	zoneName := normalizeExternalAppDomain(r.PathValue("zone"))
	if zoneName == "" {
		httpx.WriteError(w, http.StatusBadRequest, "zone is required")
		return model.HostedZone{}, false
	}
	zone, err := s.store.GetHostedZoneByName(zoneName)
	if err != nil {
		s.writeStoreError(w, err)
		return model.HostedZone{}, false
	}
	if !principal.IsPlatformAdmin() && strings.TrimSpace(zone.TenantID) != strings.TrimSpace(principal.TenantID) {
		s.writeStoreError(w, store.ErrNotFound)
		return model.HostedZone{}, false
	}
	return zone, true
}

func (s *Server) refreshHostedDNSZonePreflight(r *http.Request, principal model.Principal, zone model.HostedZone) model.HostedZone {
	response := s.buildDNSDelegationPreflight(r.Context(), principal, s.hostedDNSZonePreflightOptionsFromRequest(r, zone.ZoneName))
	zone = s.applyHostedDNSZonePreflight(zone, response)
	stored, err := s.store.PutHostedZone(zone)
	if err != nil {
		if s.log != nil {
			s.log.Printf("hosted DNS zone preflight refresh failed; zone=%s err=%v", zone.ZoneName, err)
		}
		return zone
	}
	return stored
}

func (s *Server) hostedDNSZonePreflightOptionsFromRequest(r *http.Request, zoneName string) dnsDelegationPreflightOptions {
	minHealthy := defaultDNSDelegationMinHealthyNodes
	if raw := strings.TrimSpace(r.URL.Query().Get("min_healthy_nodes")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			minHealthy = parsed
		}
	}
	return dnsDelegationPreflightOptions{
		Zone:            normalizeExternalAppDomain(zoneName),
		ProbeName:       normalizeExternalAppDomain(defaultEdgeDNSProbeLabel + "." + normalizeExternalAppDomain(zoneName)),
		MinHealthyNodes: minHealthy,
	}
}

func (s *Server) applyHostedDNSZonePreflight(zone model.HostedZone, response model.DNSDelegationPreflightResponse) model.HostedZone {
	now := time.Now().UTC()
	zone.ParentNameservers = append([]string(nil), response.DelegationPlan.CurrentParentNS...)
	if len(zone.ExpectedNameservers) == 0 {
		zone.ExpectedNameservers = append([]string(nil), s.dnsNameservers...)
	}
	zone.LastCheckedAt = &now
	zone.LastMessage = hostedDNSZonePreflightMessage(response)
	parentReady := hostedDNSNameserversCover(zone.ParentNameservers, zone.ExpectedNameservers)
	if response.Pass && parentReady {
		zone.Status = model.HostedZoneStatusActive
		zone.DelegationStatus = model.HostedZoneDelegationStatusReady
		return zone
	}
	zone.DelegationStatus = model.HostedZoneDelegationStatusPending
	zone.Status = model.HostedZoneStatusPendingDelegation
	if !parentReady {
		zone.LastMessage = "parent NS does not yet match Fugue nameservers"
		return zone
	}
	for _, check := range response.Checks {
		if strings.HasPrefix(check.Name, "dns_") && !check.Pass {
			zone.Status = model.HostedZoneStatusDegraded
			break
		}
	}
	return zone
}

func hostedDNSZonePreflightMessage(response model.DNSDelegationPreflightResponse) string {
	if response.Pass {
		return "delegation and Fugue DNS serving preflight passed"
	}
	for _, check := range response.Checks {
		if !check.Pass {
			return check.Message
		}
	}
	return "hosted DNS preflight did not pass"
}

func hostedDNSNameserversCover(parent, expected []string) bool {
	expectedSet := map[string]struct{}{}
	for _, value := range expected {
		value = normalizeExternalAppDomain(value)
		if value == "" {
			continue
		}
		expectedSet[value] = struct{}{}
	}
	if len(expectedSet) == 0 {
		return false
	}
	parentSet := map[string]struct{}{}
	for _, value := range parent {
		value = normalizeExternalAppDomain(value)
		if value == "" {
			continue
		}
		parentSet[value] = struct{}{}
	}
	for expectedNS := range expectedSet {
		if _, ok := parentSet[expectedNS]; !ok {
			return false
		}
	}
	return true
}
