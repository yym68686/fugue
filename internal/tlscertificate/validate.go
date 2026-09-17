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
