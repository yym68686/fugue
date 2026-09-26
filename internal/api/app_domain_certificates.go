package api

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
	"fugue/internal/tlscertificate"
)

var certificateFingerprint = regexp.MustCompile(`^([0-9a-f]{64})?$`)

type importAppDomainCertificateRequest struct {
	CertificatePEM            string  `json:"certificate_pem"`
	PrivateKeyPEM             string  `json:"private_key_pem"`
	ExpectedCertificateSHA256 *string `json:"expected_certificate_sha256"`
}

type appDomainCertificateMetadata struct {
	Hostname          string     `json:"hostname"`
	AppID             string     `json:"app_id"`
	Present           bool       `json:"present"`
	CertificateSHA256 string     `json:"certificate_sha256,omitempty"`
	NotAfter          *time.Time `json:"not_after,omitempty"`
	UpdatedAt         *time.Time `json:"updated_at,omitempty"`
}

func (s *Server) authorizeAppDomainCertificate(w http.ResponseWriter, r *http.Request, write bool) (model.App, model.AppDomain, bool) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() && !p.HasScope("app.tls.write") && (write || !p.HasScope("app.tls.read")) {
		httpx.WriteError(w, http.StatusForbidden, "missing dedicated app.tls scope")
		return model.App{}, model.AppDomain{}, false
	}
	app, ok := s.loadAuthorizedAppMetadata(w, r, p)
	if !ok {
		return model.App{}, model.AppDomain{}, false
	}
	host := normalizeExternalAppDomain(r.PathValue("hostname"))
	domain, err := s.store.GetAppDomain(host)
	if err != nil {
		s.writeStoreError(w, err)
		return app, domain, false
	}
	if domain.AppID != app.ID || domain.TenantID != app.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "domain is outside the app boundary")
		return app, domain, false
	}
	if domain.Status != model.AppDomainStatusVerified {
		httpx.WriteError(w, http.StatusConflict, "domain is not verified")
		return app, domain, false
	}
	return app, domain, true
}

func domainCertificateMetadata(host, appID string, c model.EdgeTLSCertificate) appDomainCertificateMetadata {
	m := appDomainCertificateMetadata{Hostname: host, AppID: appID, Present: c.Hostname != ""}
	if m.Present {
		m.CertificateSHA256, m.NotAfter, m.UpdatedAt = c.CertificateSHA256, c.NotAfter, &c.UpdatedAt
	}
	return m
}

func (s *Server) handleGetAppDomainCertificateMetadata(w http.ResponseWriter, r *http.Request) {
	app, domain, ok := s.authorizeAppDomainCertificate(w, r, false)
	if !ok {
		return
	}
	c, err := s.store.GetEdgeTLSCertificate(domain.Hostname)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		s.writeStoreError(w, err)
		return
	}
	if c.Hostname != "" && (c.AppID != app.ID || c.TenantID != app.TenantID) {
		httpx.WriteError(w, http.StatusConflict, "certificate ownership differs from verified domain")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, domainCertificateMetadata(domain.Hostname, app.ID, c))
}

func (s *Server) handleImportAppDomainCertificate(w http.ResponseWriter, r *http.Request) {
	app, domain, ok := s.authorizeAppDomainCertificate(w, r, true)
	if !ok {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 112<<10)
	var req importAppDomainCertificateRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid certificate import request")
		return
	}
	if req.ExpectedCertificateSHA256 == nil || !certificateFingerprint.MatchString(*req.ExpectedCertificateSHA256) ||
		len(req.CertificatePEM) > 65536 || len(req.PrivateKeyPEM) > 16384 {
		httpx.WriteError(w, http.StatusBadRequest, "invalid certificate import bounds or expected fingerprint")
		return
	}
	leaf, err := tlscertificate.ValidatePublic(domain.Hostname, req.CertificatePEM, req.PrivateKeyPEM, time.Now().UTC(), s.certificateImportRoots)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(leaf.DNSNames) == 0 || len(leaf.DNSNames) > 16 || len(leaf.IPAddresses) > 0 || !leaf.NotAfter.After(time.Now().Add(72*time.Hour)) {
		httpx.WriteError(w, http.StatusBadRequest, "certificate requires exact verified DNS names and at least 72 hours validity")
		return
	}
	for _, host := range leaf.DNSNames {
		if strings.Contains(host, "*") {
			httpx.WriteError(w, http.StatusBadRequest, "wildcard certificate names are not importable")
			return
		}
		d, err := s.store.GetAppDomain(host)
		if err != nil || d.Status != model.AppDomainStatusVerified || d.AppID != app.ID || d.TenantID != app.TenantID {
			httpx.WriteError(w, http.StatusForbidden, "certificate contains a DNS name outside the verified app boundary")
			return
		}
	}
	sum := sha256.Sum256(leaf.Raw)
	expires := leaf.NotAfter.UTC()
	c := model.EdgeTLSCertificate{Hostname: domain.Hostname, TenantID: app.TenantID, AppID: app.ID,
		CertificatePEM: strings.TrimSpace(req.CertificatePEM), PrivateKeyPEM: strings.TrimSpace(req.PrivateKeyPEM),
		MetadataJSON: "{}", IssuerStorage: tlscertificate.ImportedIssuerStorage, CertificateSHA256: hex.EncodeToString(sum[:]), NotAfter: &expires}
	c, err = s.store.ImportAppDomainCertificate(c, app.ProjectID, *req.ExpectedCertificateSHA256, leaf.DNSNames)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(mustPrincipal(r), "app.domain.certificate.import", "app", app.ID, app.TenantID, map[string]string{"hostname": domain.Hostname, "certificate_sha256": c.CertificateSHA256})
	httpx.WriteJSON(w, http.StatusOK, domainCertificateMetadata(domain.Hostname, app.ID, c))
}
