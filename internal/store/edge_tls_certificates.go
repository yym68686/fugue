package store

import (
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) GetEdgeTLSCertificate(hostname string) (model.EdgeTLSCertificate, error) {
	hostname = normalizeAppDomainHostname(hostname)
	if hostname == "" {
		return model.EdgeTLSCertificate{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetEdgeTLSCertificate(hostname)
	}

	var cert model.EdgeTLSCertificate
	err := s.withLockedState(false, func(state *model.State) error {
		index := findEdgeTLSCertificate(state, hostname)
		if index < 0 {
			return ErrNotFound
		}
		cert = cloneEdgeTLSCertificate(state.EdgeTLSCertificates[index])
		return nil
	})
	return cert, err
}

func (s *Store) PutEdgeTLSCertificate(cert model.EdgeTLSCertificate) (model.EdgeTLSCertificate, error) {
	cert, err := normalizeEdgeTLSCertificateForStore(cert)
	if err != nil {
		return model.EdgeTLSCertificate{}, err
	}
	if s.usingDatabase() {
		return s.pgPutEdgeTLSCertificate(cert)
	}

	var out model.EdgeTLSCertificate
	err = s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		index := findEdgeTLSCertificate(state, cert.Hostname)
		if index >= 0 {
			existing := state.EdgeTLSCertificates[index]
			if cert.CreatedAt.IsZero() {
				cert.CreatedAt = existing.CreatedAt
			}
		} else if cert.CreatedAt.IsZero() {
			cert.CreatedAt = now
		}
		cert.UpdatedAt = now
		if index >= 0 {
			state.EdgeTLSCertificates[index] = cloneEdgeTLSCertificate(cert)
		} else {
			state.EdgeTLSCertificates = append(state.EdgeTLSCertificates, cloneEdgeTLSCertificate(cert))
		}
		out = cloneEdgeTLSCertificate(cert)
		return nil
	})
	return out, err
}

func (s *Store) DeleteEdgeTLSCertificate(hostname string) (model.EdgeTLSCertificate, error) {
	hostname = normalizeAppDomainHostname(hostname)
	if hostname == "" {
		return model.EdgeTLSCertificate{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteEdgeTLSCertificate(hostname)
	}

	var removed model.EdgeTLSCertificate
	err := s.withLockedState(true, func(state *model.State) error {
		index := findEdgeTLSCertificate(state, hostname)
		if index < 0 {
			return ErrNotFound
		}
		removed = cloneEdgeTLSCertificate(state.EdgeTLSCertificates[index])
		state.EdgeTLSCertificates = append(state.EdgeTLSCertificates[:index], state.EdgeTLSCertificates[index+1:]...)
		return nil
	})
	return removed, err
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

func findEdgeTLSCertificate(state *model.State, hostname string) int {
	if state == nil {
		return -1
	}
	hostname = normalizeAppDomainHostname(hostname)
	for index, cert := range state.EdgeTLSCertificates {
		if strings.EqualFold(cert.Hostname, hostname) {
			return index
		}
	}
	return -1
}

func deleteEdgeTLSCertificateInState(state *model.State, hostname string) {
	index := findEdgeTLSCertificate(state, hostname)
	if index < 0 {
		return
	}
	state.EdgeTLSCertificates = append(state.EdgeTLSCertificates[:index], state.EdgeTLSCertificates[index+1:]...)
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
