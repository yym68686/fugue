package store

import (
	"strings"

	"fugue/internal/model"
)

func (s *Store) GetEdgeTLSCertificate(hostname string) (model.EdgeTLSCertificate, error) {
	hostname = normalizeAppDomainHostname(hostname)
	if hostname == "" {
		return model.EdgeTLSCertificate{}, ErrInvalidInput
	}
	return s.pgGetEdgeTLSCertificate(hostname)
}

func (s *Store) PutEdgeTLSCertificate(cert model.EdgeTLSCertificate) (model.EdgeTLSCertificate, error) {
	cert, err := normalizeEdgeTLSCertificateForStore(cert)
	if err != nil {
		return model.EdgeTLSCertificate{}, err
	}
	return s.pgPutEdgeTLSCertificate(cert)
}

func (s *Store) DeleteEdgeTLSCertificate(hostname string) (model.EdgeTLSCertificate, error) {
	hostname = normalizeAppDomainHostname(hostname)
	if hostname == "" {
		return model.EdgeTLSCertificate{}, ErrInvalidInput
	}
	return s.pgDeleteEdgeTLSCertificate(hostname)
}

func normalizeEdgeTLSCertificateForStore(cert model.EdgeTLSCertificate) (model.EdgeTLSCertificate, error) {
	cert.Hostname = normalizeAppDomainHostname(cert.Hostname)
	cert.TenantID = strings.TrimSpace(cert.TenantID)
	cert.AppID = strings.TrimSpace(cert.AppID)
	cert.CertificatePEM = strings.TrimSpace(cert.CertificatePEM)
	cert.PrivateKeyPEM = strings.TrimSpace(cert.PrivateKeyPEM)
	cert.MetadataJSON = strings.TrimSpace(cert.MetadataJSON)
	cert.IssuerStorage = strings.Trim(strings.TrimSpace(cert.IssuerStorage), "/")
	cert.CertificateSHA256 = strings.TrimSpace(strings.ToLower(cert.CertificateSHA256))
	cert.UploadedByEdgeID = strings.TrimSpace(cert.UploadedByEdgeID)
	cert.UploadedByEdgeGroupID = normalizeEdgeGroupID(cert.UploadedByEdgeGroupID)
	if cert.Hostname == "" || cert.CertificatePEM == "" || cert.PrivateKeyPEM == "" {
		return model.EdgeTLSCertificate{}, ErrInvalidInput
	}
	if cert.NotAfter != nil {
		notAfter := cert.NotAfter.UTC()
		cert.NotAfter = &notAfter
	}
	return cert, nil
}

func cloneEdgeTLSCertificate(in model.EdgeTLSCertificate) model.EdgeTLSCertificate {
	out := in
	out.Hostname = normalizeAppDomainHostname(out.Hostname)
	out.TenantID = strings.TrimSpace(out.TenantID)
	out.AppID = strings.TrimSpace(out.AppID)
	out.IssuerStorage = strings.Trim(strings.TrimSpace(out.IssuerStorage), "/")
	out.CertificateSHA256 = strings.TrimSpace(strings.ToLower(out.CertificateSHA256))
	out.UploadedByEdgeGroupID = normalizeEdgeGroupID(out.UploadedByEdgeGroupID)
	if in.NotAfter != nil {
		notAfter := in.NotAfter.UTC()
		out.NotAfter = &notAfter
	}
	return out
}
