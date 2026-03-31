package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

type appDomainDNSResolver interface {
	LookupCNAME(ctx context.Context, host string) (string, error)
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

type netAppDomainResolver struct{}

func (netAppDomainResolver) LookupCNAME(ctx context.Context, host string) (string, error) {
	return net.DefaultResolver.LookupCNAME(ctx, host)
}

func (netAppDomainResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return net.DefaultResolver.LookupIPAddr(ctx, host)
}

type appDomainAvailability struct {
	Input     string `json:"input,omitempty"`
	Hostname  string `json:"hostname,omitempty"`
	Valid     bool   `json:"valid"`
	Available bool   `json:"available"`
	Current   bool   `json:"current"`
	Reason    string `json:"reason,omitempty"`
}

type edgeDomainTLSReportRequest struct {
	Hostname       string `json:"hostname"`
	TLSStatus      string `json:"tls_status"`
	TLSLastMessage string `json:"tls_last_message,omitempty"`
}

func (s *Server) handleListAppDomains(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	domains, err := s.store.ListAppDomains(app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.domains.read", "app", app.ID, app.TenantID, nil)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"domains": domains})
}

func (s *Server) handleGetAppDomainAvailability(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	availability, _, err := s.inspectAppDomainAvailability(app, r.URL.Query().Get("hostname"), principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"availability": availability})
}

func (s *Server) handlePutAppDomain(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	var req struct {
		Hostname string `json:"hostname"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	availability, existing, err := s.inspectAppDomainAvailability(app, req.Hostname, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !availability.Valid {
		httpx.WriteError(w, http.StatusBadRequest, availability.Reason)
		return
	}
	if !availability.Available {
		httpx.WriteError(w, http.StatusConflict, availability.Reason)
		return
	}
	target := s.primaryCustomDomainTarget(app)
	if target == "" {
		httpx.WriteError(w, http.StatusBadRequest, "custom domain CNAME target is not configured")
		return
	}
	if existing != nil {
		if existing.Status == model.AppDomainStatusVerified {
			availability.Current = true
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"domain":          *existing,
				"availability":    availability,
				"already_current": true,
			})
			return
		}
		domain, verified, err := s.verifyAndPersistAppDomain(r.Context(), app, *existing)
		if err != nil {
			httpx.WriteError(w, http.StatusBadGateway, err.Error())
			return
		}
		availability.Current = verified
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"domain":          domain,
			"availability":    availability,
			"already_current": false,
		})
		return
	}

	now := time.Now().UTC()
	domain := model.AppDomain{
		Hostname:    availability.Hostname,
		AppID:       app.ID,
		TenantID:    app.TenantID,
		Status:      model.AppDomainStatusPending,
		RouteTarget: target,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	domain, verified, err := s.evaluateAppDomainVerification(r.Context(), app, domain)
	if err != nil {
		httpx.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	domain, err = s.store.PutAppDomain(domain)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.domain.put", "app", app.ID, app.TenantID, map[string]string{"hostname": domain.Hostname})
	availability.Current = verified
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"domain":          domain,
		"availability":    availability,
		"already_current": false,
	})
}

func (s *Server) handleVerifyAppDomain(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	var req struct {
		Hostname string `json:"hostname"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	host := normalizeExternalAppDomain(req.Hostname)
	if host == "" {
		httpx.WriteError(w, http.StatusBadRequest, "hostname is required")
		return
	}
	domain, err := s.store.GetAppDomain(host)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if domain.AppID != app.ID {
		s.writeStoreError(w, store.ErrNotFound)
		return
	}
	updated, verified, err := s.verifyAndPersistAppDomain(r.Context(), app, domain)
	if err != nil {
		httpx.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	s.appendAudit(principal, "app.domain.verify", "app", app.ID, app.TenantID, map[string]string{
		"hostname": domain.Hostname,
		"verified": strconvFormatBool(verified),
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"domain":   updated,
		"verified": verified,
	})
}

func (s *Server) handleDeleteAppDomain(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	hostname := normalizeExternalAppDomain(r.URL.Query().Get("hostname"))
	if hostname == "" {
		httpx.WriteError(w, http.StatusBadRequest, "hostname is required")
		return
	}
	domain, err := s.store.DeleteAppDomain(app.ID, hostname)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.domain.delete", "app", app.ID, app.TenantID, map[string]string{"hostname": domain.Hostname})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"domain": domain})
}

func (s *Server) handleEdgeTLSAsk(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeEdgeToken(w, r) {
		return
	}
	hostname := normalizeExternalAppDomain(r.URL.Query().Get("domain"))
	if hostname == "" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	domain, err := s.store.GetAppDomain(hostname)
	if err != nil {
		if err == store.ErrNotFound {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		http.Error(w, "domain lookup failed", http.StatusInternalServerError)
		return
	}
	if domain.Status == model.AppDomainStatusVerified {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	}
	app, err := s.store.GetApp(domain.AppID)
	if err != nil {
		if err == store.ErrNotFound {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		http.Error(w, "app lookup failed", http.StatusInternalServerError)
		return
	}
	_, verified, err := s.verifyAndPersistAppDomain(r.Context(), app, domain)
	if err != nil {
		http.Error(w, "verification failed", http.StatusBadGateway)
		return
	}
	if !verified {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleEdgeDomains(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeEdgeToken(w, r) {
		return
	}
	domains, err := s.store.ListVerifiedAppDomains()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	type edgeDomain struct {
		Hostname string `json:"hostname"`
	}
	filtered := make([]edgeDomain, 0, len(domains))
	for _, domain := range domains {
		if !s.managedEdgeCustomDomain(domain.Hostname) {
			continue
		}
		filtered = append(filtered, edgeDomain{Hostname: domain.Hostname})
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"domains": filtered})
}

func (s *Server) handleEdgeDomainTLSReport(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeEdgeToken(w, r) {
		return
	}

	var req edgeDomainTLSReportRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	hostname := normalizeExternalAppDomain(req.Hostname)
	if hostname == "" {
		httpx.WriteError(w, http.StatusBadRequest, "hostname is required")
		return
	}
	if !s.managedEdgeCustomDomain(hostname) {
		httpx.WriteError(w, http.StatusBadRequest, "hostname is not a managed edge custom domain")
		return
	}
	tlsStatus := model.NormalizeAppDomainTLSStatus(req.TLSStatus)
	if tlsStatus == "" {
		httpx.WriteError(w, http.StatusBadRequest, "tls_status must be pending, ready, or error")
		return
	}

	domain, err := s.store.GetAppDomain(hostname)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if domain.Status != model.AppDomainStatusVerified {
		httpx.WriteError(w, http.StatusConflict, "domain is not verified")
		return
	}

	now := time.Now().UTC()
	domain.TLSStatus = tlsStatus
	domain.TLSLastMessage = strings.TrimSpace(req.TLSLastMessage)
	domain.TLSLastCheckedAt = &now
	switch tlsStatus {
	case model.AppDomainTLSStatusReady:
		domain.TLSLastMessage = ""
		if domain.TLSReadyAt == nil {
			domain.TLSReadyAt = &now
		}
	default:
		domain.TLSReadyAt = nil
	}

	domain, err = s.store.PutAppDomain(domain)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"domain": domain})
}

func (s *Server) inspectAppDomainAvailability(app model.App, raw string, allowPlatformRoot bool) (appDomainAvailability, *model.AppDomain, error) {
	availability := appDomainAvailability{Input: strings.TrimSpace(raw)}
	hostname, reason := s.normalizeRequestedCustomDomain(raw, allowPlatformRoot)
	availability.Hostname = hostname
	if reason != "" {
		availability.Reason = reason
		return availability, nil, nil
	}
	availability.Valid = true

	existingDomain, err := s.store.GetAppDomain(hostname)
	switch {
	case err == nil:
		if existingDomain.AppID == app.ID {
			availability.Available = true
			availability.Current = existingDomain.Status == model.AppDomainStatusVerified
			return availability, &existingDomain, nil
		}
		availability.Reason = "hostname is already in use"
		return availability, nil, nil
	case err != nil && err != store.ErrNotFound:
		return availability, nil, err
	}

	owner, err := s.store.GetAppByHostname(hostname)
	switch {
	case err == nil:
		if owner.ID == app.ID {
			availability.Available = true
			availability.Current = true
			return availability, nil, nil
		}
		availability.Reason = "hostname is already in use"
		return availability, nil, nil
	case err != nil && err != store.ErrNotFound:
		return availability, nil, err
	}

	availability.Available = true
	return availability, nil, nil
}

func (s *Server) normalizeRequestedCustomDomain(raw string, allowPlatformRoot bool) (string, string) {
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
	if s.isReservedAppHostname(hostname) {
		return "", "hostname is reserved"
	}
	appBase := normalizeExternalAppDomain(s.appBaseDomain)
	customBase := normalizeExternalAppDomain(s.customDomainBaseDomain)
	allowExactPlatformRoot := allowPlatformRoot && appBase != "" && hostname == appBase
	if appBase != "" {
		if (hostname == appBase && !allowExactPlatformRoot) || strings.HasSuffix(hostname, "."+appBase) {
			return "", "platform-managed hostnames must be updated through the app route endpoint"
		}
	}
	if customBase != "" {
		exactCustomTargetRootAllowed := allowExactPlatformRoot && customBase == appBase && hostname == customBase
		if (hostname == customBase && !exactCustomTargetRootAllowed) || strings.HasSuffix(hostname, "."+customBase) {
			return "", "hostname is reserved for Fugue custom-domain targets"
		}
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

func (s *Server) evaluateAppDomainVerification(ctx context.Context, app model.App, domain model.AppDomain) (model.AppDomain, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	target := s.primaryCustomDomainTarget(app)
	if target == "" {
		return domain, false, store.ErrInvalidInput
	}

	now := time.Now().UTC()
	updated := domain
	wasVerified := updated.Status == model.AppDomainStatusVerified
	if domain.LastCheckedAt != nil {
		lastCheckedAt := *domain.LastCheckedAt
		updated.LastCheckedAt = &lastCheckedAt
	}
	if domain.VerifiedAt != nil {
		verifiedAt := *domain.VerifiedAt
		updated.VerifiedAt = &verifiedAt
	}
	legacyTarget := normalizeExternalAppDomain(updated.RouteTarget)
	updated.RouteTarget = target
	updated.VerificationTXTName = ""
	updated.VerificationTXTValue = ""
	updated.LastCheckedAt = &now

	verified, message, err := s.customDomainVerificationResult(ctx, app, updated, legacyTarget)
	if err != nil {
		return domain, false, err
	}
	if verified {
		updated.Status = model.AppDomainStatusVerified
		updated.LastMessage = ""
		if updated.VerifiedAt == nil {
			updated.VerifiedAt = &now
		}
		if !wasVerified {
			updated.TLSStatus = model.AppDomainTLSStatusPending
			updated.TLSLastMessage = ""
			updated.TLSLastCheckedAt = nil
			updated.TLSReadyAt = nil
		}
	} else {
		if updated.Status != model.AppDomainStatusVerified {
			updated.Status = model.AppDomainStatusPending
		}
		updated.LastMessage = message
	}
	return updated, verified, nil
}

func (s *Server) verifyAndPersistAppDomain(ctx context.Context, app model.App, domain model.AppDomain) (model.AppDomain, bool, error) {
	updated, verified, err := s.evaluateAppDomainVerification(ctx, app, domain)
	if err != nil {
		return domain, false, err
	}
	updated, err = s.store.PutAppDomain(updated)
	if err != nil {
		return domain, false, err
	}
	return updated, verified, nil
}

func (s *Server) customDomainVerificationResult(ctx context.Context, app model.App, domain model.AppDomain, legacyTarget string) (bool, string, error) {
	targets := s.customDomainTargets(app, legacyTarget)
	if len(targets) == 0 {
		return false, "custom domain CNAME target is not configured", nil
	}
	if !s.customDomainRoutesToAnyTarget(ctx, domain.Hostname, targets) {
		return false, "create a CNAME record for " + domain.Hostname + " pointing to " + targets[0], nil
	}
	return true, "", nil
}

func (s *Server) customDomainRoutesToAnyTarget(ctx context.Context, hostname string, targets []string) bool {
	cname, err := s.dnsResolver.LookupCNAME(ctx, hostname)
	if err == nil {
		cname = normalizeExternalAppDomain(cname)
		for _, target := range targets {
			if cname == normalizeExternalAppDomain(target) {
				return true
			}
		}
	}

	hostIPs, err := s.dnsResolver.LookupIPAddr(ctx, hostname)
	if err != nil || len(hostIPs) == 0 {
		return false
	}
	for _, target := range targets {
		targetIPs, targetErr := s.dnsResolver.LookupIPAddr(ctx, target)
		if targetErr != nil || len(targetIPs) == 0 {
			continue
		}
		if ipListsIntersect(hostIPs, targetIPs) {
			return true
		}
	}
	return false
}

func (s *Server) customDomainTargets(app model.App, legacyTargets ...string) []string {
	targets := make([]string, 0, 2+len(legacyTargets))
	if host := s.dedicatedCustomDomainTarget(app); host != "" {
		targets = append(targets, host)
	}
	if app.Route != nil {
		if host := normalizeExternalAppDomain(app.Route.Hostname); host != "" {
			targets = append(targets, host)
		}
	}
	targets = append(targets, legacyTargets...)
	return uniqueNormalizedAppDomainHosts(targets...)
}

func (s *Server) primaryCustomDomainTarget(app model.App) string {
	if host := s.dedicatedCustomDomainTarget(app); host != "" {
		return host
	}
	targets := s.customDomainTargets(app)
	if len(targets) == 0 {
		return ""
	}
	return targets[0]
}

func (s *Server) dedicatedCustomDomainTarget(app model.App) string {
	base := normalizeExternalAppDomain(s.customDomainBaseDomain)
	if base == "" {
		return ""
	}
	label := stableCustomDomainTargetLabel(app)
	if label == "" {
		return ""
	}
	return label + "." + base
}

func stableCustomDomainTargetLabel(app model.App) string {
	appID := strings.TrimSpace(strings.ToLower(app.ID))
	tenantID := strings.TrimSpace(strings.ToLower(app.TenantID))
	if appID == "" || tenantID == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(tenantID + ":" + appID))
	return "d-" + hex.EncodeToString(sum[:10])
}

func uniqueNormalizedAppDomainHosts(hosts ...string) []string {
	out := make([]string, 0, len(hosts))
	seen := make(map[string]struct{}, len(hosts))
	for _, host := range hosts {
		host = normalizeExternalAppDomain(host)
		if host == "" {
			continue
		}
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		out = append(out, host)
	}
	return out
}

func normalizeExternalAppDomain(raw string) string {
	return strings.Trim(strings.TrimSpace(strings.ToLower(raw)), ".")
}

func defaultCustomDomainBaseDomain(appBaseDomain string) string {
	base := normalizeExternalAppDomain(appBaseDomain)
	if base == "" {
		return ""
	}
	return "dns." + base
}

func (s *Server) managedEdgeCustomDomain(hostname string) bool {
	hostname = normalizeExternalAppDomain(hostname)
	if hostname == "" {
		return false
	}
	if s.isReservedAppHostname(hostname) {
		return false
	}
	appBase := normalizeExternalAppDomain(s.appBaseDomain)
	if appBase != "" && hostname != appBase && strings.HasSuffix(hostname, "."+appBase) {
		return false
	}
	customBase := normalizeExternalAppDomain(s.customDomainBaseDomain)
	if customBase != "" && (hostname == customBase || strings.HasSuffix(hostname, "."+customBase)) {
		return false
	}
	return true
}

func ipListsIntersect(left, right []net.IPAddr) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(left))
	for _, addr := range left {
		seen[addr.IP.String()] = struct{}{}
	}
	for _, addr := range right {
		if _, ok := seen[addr.IP.String()]; ok {
			return true
		}
	}
	return false
}

func (s *Server) authorizeEdgeToken(w http.ResponseWriter, r *http.Request) bool {
	if strings.TrimSpace(s.edgeTLSAskToken) == "" {
		http.NotFound(w, r)
		return false
	}
	if !subtleConstantCompare(r.URL.Query().Get("token"), s.edgeTLSAskToken) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return false
	}
	return true
}

func subtleConstantCompare(left, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if len(left) != len(right) {
		return false
	}
	var out byte
	for index := 0; index < len(left); index++ {
		out |= left[index] ^ right[index]
	}
	return out == 0
}

func strconvFormatBool(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
