// Package tlscertificate validates shared certificate material before it can
// replace local state or support a readiness observation.
package tlscertificate

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"time"
)

var ErrInvalid = errors.New("shared TLS certificate is unusable")

const ImportedIssuerStorage = "fugue-imported"

func Validate(hostname, certificatePEM, privateKeyPEM string, now time.Time) (*x509.Certificate, error) {
	pair, err := tls.X509KeyPair([]byte(certificatePEM), []byte(privateKeyPEM))
	if err != nil || len(pair.Certificate) == 0 {
		return nil, fmt.Errorf("%w: key pair is invalid", ErrInvalid)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("%w: leaf is invalid", ErrInvalid)
	}
	if leaf.VerifyHostname(hostname) != nil {
		return nil, fmt.Errorf("%w: hostname does not match", ErrInvalid)
	}
	if now.Before(leaf.NotBefore) {
		return nil, fmt.Errorf("%w: not yet valid", ErrInvalid)
	}
	if !now.Before(leaf.NotAfter) {
		return nil, fmt.Errorf("%w: expired", ErrInvalid)
	}
	return leaf, nil
}

// ValidatePublic requires a chain trusted by the consumer's roots in addition
// to the local material checks. A nil pool uses the system public roots.
func ValidatePublic(hostname, certificatePEM, privateKeyPEM string, now time.Time, roots *x509.CertPool) (*x509.Certificate, error) {
	leaf, err := Validate(hostname, certificatePEM, privateKeyPEM, now)
	if err != nil {
		return nil, err
	}
	pair, _ := tls.X509KeyPair([]byte(certificatePEM), []byte(privateKeyPEM))
	intermediates := x509.NewCertPool()
	for _, raw := range pair.Certificate[1:] {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid intermediate certificate", ErrInvalid)
		}
		intermediates.AddCert(cert)
	}
	if _, err = leaf.Verify(x509.VerifyOptions{DNSName: hostname, CurrentTime: now, Roots: roots, Intermediates: intermediates}); err != nil {
		return nil, fmt.Errorf("%w: public certificate chain is not trusted", ErrInvalid)
	}
	return leaf, nil
}
