package api

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
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

const appDomainReadVerifyMinInterval = 30 * time.Second

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
	CertificatePEM string `json:"certificate_pem,omitempty"`
	PrivateKeyPEM  string `json:"private_key_pem,omitempty"`
	MetadataJSON   string `json:"metadata_json,omitempty"`
	IssuerStorage  string `json:"issuer_storage,omitempty"`
}

type edgeTLSCertificateBundleRequest struct {
	CertificatePEM string `json:"certificate_pem"`
	PrivateKeyPEM  string `json:"private_key_pem"`
	MetadataJSON   string `json:"metadata_json,omitempty"`
	IssuerStorage  string `json:"issuer_storage,omitempty"`
}

type appDomainDNSObservation struct {
	Verified      bool     `json:"verified"`
	RecordKind    string   `json:"record_kind,omitempty"`
	CNAME         string   `json:"cname,omitempty"`
	MatchedTarget string   `json:"matched_target,omitempty"`
	HostIPs       []string `json:"host_ips,omitempty"`
	TargetIPs     []string `json:"target_ips,omitempty"`
	Message       string   `json:"message,omitempty"`
}

type appDomainDiagnosticCheck struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Message    string `json:"message,omitempty"`
	Repairable bool   `json:"repairable,omitempty"`
}

type appDomainTLSCertificateSummary struct {
	Present               bool       `json:"present"`
	CertificateSHA256     string     `json:"certificate_sha256,omitempty"`
	NotAfter              *time.Time `json:"not_after,omitempty"`
	IssuerStorage         string     `json:"issuer_storage,omitempty"`
	UploadedByEdgeID      string     `json:"uploaded_by_edge_id,omitempty"`
	UploadedByEdgeGroupID string     `json:"uploaded_by_edge_group_id,omitempty"`
	UpdatedAt             *time.Time `json:"updated_at,omitempty"`
}

type appDomainDiagnosis struct {
	Domain               model.AppDomain                `json:"domain"`
	DNSTargets           []string                       `json:"dns_targets"`
	DNSObservation       appDomainDNSObservation        `json:"dns_observation"`
	SharedTLSCertificate appDomainTLSCertificateSummary `json:"shared_tls_certificate"`
	Checks               []appDomainDiagnosticCheck     `json:"checks"`
	RecommendedActions   []string                       `json:"recommended_actions,omitempty"`
}

func (s *Server) handleListAppDomains(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	domains, err := s.store.ListAppDomains(app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	domains = s.refreshAppDomainsForRead(r.Context(), app, domains)
	s.appendAudit(principal, "app.domains.read", "app", app.ID, app.TenantID, nil)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"domains": domains})
}

func (s *Server) handleGetAppDomainAvailability(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
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
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
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
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
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

func (s *Server) handleGetAppDomainDiagnosis(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	hostname := normalizeExternalAppDomain(r.URL.Query().Get("hostname"))
	if hostname == "" {
		httpx.WriteError(w, http.StatusBadRequest, "hostname is required")
		return
	}
	domain, err := s.store.GetAppDomain(hostname)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if domain.AppID != app.ID {
		s.writeStoreError(w, store.ErrNotFound)
		return
	}
	domain = s.refreshAppDomainForRead(r.Context(), app, domain)
	diagnosis := s.buildAppDomainDiagnosis(r.Context(), app, domain)
	s.appendAudit(principal, "app.domain.diagnose", "app", app.ID, app.TenantID, map[string]string{"hostname": domain.Hostname})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"diagnosis": diagnosis})
}

func (s *Server) handleRepairAppDomain(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write scope")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
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
	hostname := normalizeExternalAppDomain(req.Hostname)
	if hostname == "" {
		httpx.WriteError(w, http.StatusBadRequest, "hostname is required")
		return
	}
	domain, err := s.store.GetAppDomain(hostname)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if domain.AppID != app.ID {
		s.writeStoreError(w, store.ErrNotFound)
		return
	}

	domain, verified, err := s.verifyAndPersistAppDomain(r.Context(), app, domain)
	if err != nil {
		httpx.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	if verified && domain.Status == model.AppDomainStatusVerified {
		now := time.Now().UTC()
		if _, certErr := s.store.GetEdgeTLSCertificate(domain.Hostname); certErr == nil {
			domain.TLSStatus = model.AppDomainTLSStatusReady
			domain.TLSLastMessage = ""
			domain.TLSLastCheckedAt = &now
			domain.TLSReadyAt = &now
			if stored, putErr := s.store.PutAppDomain(domain); putErr == nil {
				domain = stored
			} else {
				s.writeStoreError(w, putErr)
				return
			}
		} else if certErr == store.ErrNotFound {
			domain.TLSStatus = model.AppDomainTLSStatusPending
			domain.TLSLastMessage = "waiting for shared edge certificate bundle"
			domain.TLSLastCheckedAt = &now
			domain.TLSReadyAt = nil
			if stored, putErr := s.store.PutAppDomain(domain); putErr == nil {
				domain = stored
			} else {
				s.writeStoreError(w, putErr)
				return
			}
		} else {
			s.writeStoreError(w, certErr)
			return
		}
	}

	diagnosis := s.buildAppDomainDiagnosis(r.Context(), app, domain)
	s.appendAudit(principal, "app.domain.repair", "app", app.ID, app.TenantID, map[string]string{"hostname": domain.Hostname})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"domain":    domain,
		"diagnosis": diagnosis,
	})
}

func (s *Server) handleDeleteAppDomain(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write scope")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
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
	if domain.Status == model.AppDomainStatusVerified && domain.DNSStatus == model.AppDomainDNSStatusReady {
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

func (s *Server) handleGetEdgeTLSCertificateBundle(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeEdgeToken(w, r) {
		return
	}
	hostname := normalizeExternalAppDomain(r.PathValue("hostname"))
	if hostname == "" {
		httpx.WriteError(w, http.StatusBadRequest, "hostname is required")
		return
	}
	if !s.managedEdgeCustomDomain(hostname) {
		httpx.WriteError(w, http.StatusBadRequest, "hostname is not a managed edge custom domain")
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
	cert, err := s.store.GetEdgeTLSCertificate(hostname)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"certificate": cert})
}

func (s *Server) handlePutEdgeTLSCertificateBundle(w http.ResponseWriter, r *http.Request) {
	authContext, ok := s.authorizeEdgeRequest(w, r)
	if !ok {
		return
	}
	hostname := normalizeExternalAppDomain(r.PathValue("hostname"))
	if hostname == "" {
		httpx.WriteError(w, http.StatusBadRequest, "hostname is required")
		return
	}
	if !s.managedEdgeCustomDomain(hostname) {
		httpx.WriteError(w, http.StatusBadRequest, "hostname is not a managed edge custom domain")
		return
	}
	var req edgeTLSCertificateBundleRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
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
	cert, err := s.validateEdgeTLSCertificateBundle(hostname, domain, authContext, req)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	cert, err = s.store.PutEdgeTLSCertificate(cert)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	now := time.Now().UTC()
	domain.TLSStatus = model.AppDomainTLSStatusReady
	domain.TLSLastMessage = ""
	domain.TLSLastCheckedAt = &now
	domain.TLSReadyAt = &now
	domain, err = s.store.PutAppDomain(domain)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"certificate": cert,
		"domain":      domain,
	})
}

func (s *Server) handleEdgeDomainTLSReport(w http.ResponseWriter, r *http.Request) {
	authContext, ok := s.authorizeEdgeRequest(w, r)
	if !ok {
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
	if tlsStatus == model.AppDomainTLSStatusReady {
		if strings.TrimSpace(req.CertificatePEM) != "" || strings.TrimSpace(req.PrivateKeyPEM) != "" {
			cert, certErr := s.validateEdgeTLSCertificateBundle(hostname, domain, authContext, edgeTLSCertificateBundleRequest{
				CertificatePEM: req.CertificatePEM,
				PrivateKeyPEM:  req.PrivateKeyPEM,
				MetadataJSON:   req.MetadataJSON,
				IssuerStorage:  req.IssuerStorage,
			})
			if certErr != nil {
				httpx.WriteError(w, http.StatusBadRequest, certErr.Error())
				return
			}
			if _, certErr = s.store.PutEdgeTLSCertificate(cert); certErr != nil {
				s.writeStoreError(w, certErr)
				return
			}
		}
		if _, certErr := s.store.GetEdgeTLSCertificate(hostname); certErr != nil {
			if certErr != store.ErrNotFound {
				s.writeStoreError(w, certErr)
				return
			}
			tlsStatus = model.AppDomainTLSStatusPending
			if strings.TrimSpace(req.TLSLastMessage) == "" {
				req.TLSLastMessage = "waiting for shared edge certificate bundle"
			}
		}
	} else if _, certErr := s.store.GetEdgeTLSCertificate(hostname); certErr == nil {
		tlsStatus = model.AppDomainTLSStatusReady
		req.TLSLastMessage = ""
	} else if certErr != store.ErrNotFound {
		s.writeStoreError(w, certErr)
		return
	}
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
	updated.DNSLastCheckedAt = &now

	observation, err := s.inspectCustomDomainDNS(ctx, updated.Hostname, s.customDomainTargets(app, legacyTarget))
	if err != nil {
		return domain, false, err
	}
	verified := observation.Verified
	if verified {
		updated.Status = model.AppDomainStatusVerified
		updated.LastMessage = ""
		updated.DNSStatus = model.AppDomainDNSStatusReady
		updated.DNSRecordKind = observation.RecordKind
		updated.DNSLastMessage = ""
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
		updated.DNSStatus = model.AppDomainDNSStatusPending
		updated.DNSRecordKind = model.AppDomainDNSRecordKindNone
		updated.DNSLastMessage = observation.Message
		updated.LastMessage = observation.Message
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

func (s *Server) refreshAppDomainsForRead(ctx context.Context, app model.App, domains []model.AppDomain) []model.AppDomain {
	if len(domains) == 0 {
		return domains
	}
	now := time.Now().UTC()
	out := append([]model.AppDomain(nil), domains...)
	for index, domain := range out {
		out[index] = s.refreshAppDomainForReadAt(ctx, app, domain, now)
	}
	return out
}

func (s *Server) refreshAppDomainForRead(ctx context.Context, app model.App, domain model.AppDomain) model.AppDomain {
	return s.refreshAppDomainForReadAt(ctx, app, domain, time.Now().UTC())
}

func (s *Server) refreshAppDomainForReadAt(ctx context.Context, app model.App, domain model.AppDomain, now time.Time) model.AppDomain {
	if !shouldRefreshAppDomainOnRead(domain, now) {
		return domain
	}
	updated, _, err := s.verifyAndPersistAppDomain(ctx, app, domain)
	if err != nil {
		if s.log != nil {
			s.log.Printf("app domain read refresh failed; app_id=%s hostname=%s err=%v", app.ID, domain.Hostname, err)
		}
		return domain
	}
	return updated
}

func shouldRefreshAppDomainOnRead(domain model.AppDomain, now time.Time) bool {
	if domain.Status == model.AppDomainStatusVerified {
		return false
	}
	var lastChecked *time.Time
	if domain.DNSLastCheckedAt != nil {
		lastChecked = domain.DNSLastCheckedAt
	} else {
		lastChecked = domain.LastCheckedAt
	}
	if lastChecked == nil {
		return true
	}
	return now.Sub(lastChecked.UTC()) >= appDomainReadVerifyMinInterval
}

func (s *Server) customDomainVerificationResult(ctx context.Context, app model.App, domain model.AppDomain, legacyTarget string) (bool, string, error) {
	observation, err := s.inspectCustomDomainDNS(ctx, domain.Hostname, s.customDomainTargets(app, legacyTarget))
	if err != nil {
		return false, "", err
	}
	return observation.Verified, observation.Message, nil
}

func (s *Server) inspectCustomDomainDNS(ctx context.Context, hostname string, targets []string) (appDomainDNSObservation, error) {
	hostname = normalizeExternalAppDomain(hostname)
	targets = uniqueNormalizedAppDomainHosts(targets...)
	observation := appDomainDNSObservation{}
	if hostname == "" {
		observation.Message = "hostname is required"
		return observation, nil
	}
	if len(targets) == 0 {
		observation.Message = "custom domain CNAME target is not configured"
		return observation, nil
	}
	cname, cnameErr := s.dnsResolver.LookupCNAME(ctx, hostname)
	if cnameErr == nil {
		observation.CNAME = normalizeExternalAppDomain(cname)
		for _, target := range targets {
			if observation.CNAME == normalizeExternalAppDomain(target) {
				observation.Verified = true
				observation.RecordKind = model.AppDomainDNSRecordKindCNAME
				observation.MatchedTarget = target
				return observation, nil
			}
		}
	}

	hostIPs, hostErr := s.dnsResolver.LookupIPAddr(ctx, hostname)
	if hostErr == nil {
		observation.HostIPs = ipAddrStrings(hostIPs)
	}
	for _, target := range targets {
		targetIPs, targetErr := s.dnsResolver.LookupIPAddr(ctx, target)
		if targetErr != nil || len(targetIPs) == 0 {
			continue
		}
		if len(observation.TargetIPs) == 0 {
			observation.TargetIPs = ipAddrStrings(targetIPs)
		}
		if ipListsIntersect(hostIPs, targetIPs) {
			observation.Verified = true
			observation.RecordKind = model.AppDomainDNSRecordKindFlattened
			observation.MatchedTarget = target
			return observation, nil
		}
	}

	if observation.CNAME != "" {
		observation.Message = fmt.Sprintf("CNAME for %s points to %s; expected %s", hostname, observation.CNAME, targets[0])
		return observation, nil
	}
	if hostErr == nil && len(hostIPs) > 0 {
		observation.Message = fmt.Sprintf("DNS for %s resolves, but not to %s", hostname, targets[0])
		return observation, nil
	}
	observation.Message = "create a CNAME record for " + hostname + " pointing to " + targets[0]
	return observation, nil
}

func (s *Server) customDomainRoutesToAnyTarget(ctx context.Context, hostname string, targets []string) bool {
	observation, err := s.inspectCustomDomainDNS(ctx, hostname, targets)
	if err != nil {
		return false
	}
	return observation.Verified
}

func (s *Server) validateEdgeTLSCertificateBundle(hostname string, domain model.AppDomain, authContext edgeAuthContext, req edgeTLSCertificateBundleRequest) (model.EdgeTLSCertificate, error) {
	hostname = normalizeExternalAppDomain(hostname)
	certPEM := strings.TrimSpace(req.CertificatePEM)
	keyPEM := strings.TrimSpace(req.PrivateKeyPEM)
	if hostname == "" || certPEM == "" || keyPEM == "" {
		return model.EdgeTLSCertificate{}, fmt.Errorf("hostname, certificate_pem, and private_key_pem are required")
	}
	keyPair, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	if err != nil {
		return model.EdgeTLSCertificate{}, fmt.Errorf("invalid certificate/key pair: %w", err)
	}
	if keyPair.Leaf == nil {
		if len(keyPair.Certificate) == 0 {
			return model.EdgeTLSCertificate{}, fmt.Errorf("certificate chain is empty")
		}
		keyPair.Leaf, err = x509.ParseCertificate(keyPair.Certificate[0])
		if err != nil {
			return model.EdgeTLSCertificate{}, fmt.Errorf("parse leaf certificate: %w", err)
		}
	}
	if err := keyPair.Leaf.VerifyHostname(hostname); err != nil {
		return model.EdgeTLSCertificate{}, fmt.Errorf("certificate does not cover %s: %w", hostname, err)
	}
	notAfter := keyPair.Leaf.NotAfter.UTC()
	sum := sha256.Sum256(keyPair.Leaf.Raw)
	cert := model.EdgeTLSCertificate{
		Hostname:              hostname,
		TenantID:              domain.TenantID,
		AppID:                 domain.AppID,
		CertificatePEM:        certPEM,
		PrivateKeyPEM:         keyPEM,
		MetadataJSON:          strings.TrimSpace(req.MetadataJSON),
		IssuerStorage:         strings.Trim(strings.TrimSpace(req.IssuerStorage), "/"),
		CertificateSHA256:     hex.EncodeToString(sum[:]),
		NotAfter:              &notAfter,
		UploadedByEdgeID:      authContext.EdgeID,
		UploadedByEdgeGroupID: authContext.EdgeGroupID,
	}
	return cert, nil
}

func (s *Server) buildAppDomainDiagnosis(ctx context.Context, app model.App, domain model.AppDomain) appDomainDiagnosis {
	targets := s.customDomainTargets(app, domain.RouteTarget)
	observation, err := s.inspectCustomDomainDNS(ctx, domain.Hostname, targets)
	if err != nil {
		observation = appDomainDNSObservation{Message: err.Error()}
	}
	certSummary := appDomainTLSCertificateSummary{}
	if cert, certErr := s.store.GetEdgeTLSCertificate(domain.Hostname); certErr == nil {
		updatedAt := cert.UpdatedAt
		certSummary = appDomainTLSCertificateSummary{
			Present:               true,
			CertificateSHA256:     cert.CertificateSHA256,
			NotAfter:              cert.NotAfter,
			IssuerStorage:         cert.IssuerStorage,
			UploadedByEdgeID:      cert.UploadedByEdgeID,
			UploadedByEdgeGroupID: cert.UploadedByEdgeGroupID,
			UpdatedAt:             &updatedAt,
		}
	}

	checks := []appDomainDiagnosticCheck{
		{
			Name:    "dns_record",
			Status:  appDomainCheckStatus(observation.Verified),
			Message: observation.Message,
		},
		{
			Name:       "domain_verified",
			Status:     appDomainCheckStatus(domain.Status == model.AppDomainStatusVerified),
			Message:    domain.LastMessage,
			Repairable: domain.Status != model.AppDomainStatusVerified,
		},
		{
			Name:    "shared_tls_certificate",
			Status:  appDomainCheckStatus(certSummary.Present),
			Message: missingMessage(certSummary.Present, "waiting for an edge node to upload the shared certificate bundle"),
		},
		{
			Name:       "tls_ready",
			Status:     appDomainCheckStatus(domain.TLSStatus == model.AppDomainTLSStatusReady),
			Message:    domain.TLSLastMessage,
			Repairable: domain.Status == model.AppDomainStatusVerified && certSummary.Present && domain.TLSStatus != model.AppDomainTLSStatusReady,
		},
		{
			Name: "route_active",
			Status: appDomainCheckStatus(domain.Status == model.AppDomainStatusVerified &&
				domain.DNSStatus == model.AppDomainDNSStatusReady &&
				domain.TLSStatus == model.AppDomainTLSStatusReady),
		},
	}

	actions := make([]string, 0, 3)
	if !observation.Verified && observation.Message != "" {
		actions = append(actions, observation.Message)
	}
	if domain.Status == model.AppDomainStatusVerified && !certSummary.Present {
		actions = append(actions, "wait for an edge node to complete TLS issuance and upload the shared certificate bundle")
	}
	if domain.Status == model.AppDomainStatusVerified && certSummary.Present && domain.TLSStatus != model.AppDomainTLSStatusReady {
		actions = append(actions, "run domain repair to promote the verified shared certificate to ready")
	}
	return appDomainDiagnosis{
		Domain:               domain,
		DNSTargets:           targets,
		DNSObservation:       observation,
		SharedTLSCertificate: certSummary,
		Checks:               checks,
		RecommendedActions:   actions,
	}
}

func appDomainCheckStatus(ok bool) string {
	if ok {
		return "pass"
	}
	return "fail"
}

func missingMessage(present bool, message string) string {
	if present {
		return ""
	}
	return message
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

func ipAddrStrings(addrs []net.IPAddr) []string {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]string, 0, len(addrs))
	seen := make(map[string]struct{}, len(addrs))
	for _, addr := range addrs {
		value := addr.IP.String()
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (s *Server) authorizeEdgeToken(w http.ResponseWriter, r *http.Request) bool {
	if strings.TrimSpace(s.edgeTLSAskToken) == "" && s.store == nil {
		http.NotFound(w, r)
		return false
	}
	_, ok := s.authorizeEdgeRequest(w, r)
	return ok
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
