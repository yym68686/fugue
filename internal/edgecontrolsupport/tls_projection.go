package edgecontrolsupport

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	maxProjectedCertificateBytes = 1 << 20
	maxProjectedPrivateKeyBytes  = 64 << 10
)

// ProjectedServerCertificate consumes exactly one Kubernetes AtomicWriter
// generation. The explicit CA is used both to validate the server material
// before serving and by the Edge Control client as its only trust root.
type ProjectedServerCertificate struct {
	Directory       string
	CertificateFile string
	PrivateKeyFile  string
	CAFile          string
	ServerName      string
	Now             func() time.Time
}

func (source ProjectedServerCertificate) Load() (*tls.Certificate, error) {
	directory, names, serverName, now, err := source.normalized()
	if err != nil {
		return nil, err
	}
	generation, err := projectedGeneration(directory)
	if err != nil {
		return nil, err
	}
	generationDirectory := filepath.Join(directory, generation)
	certificatePEM, err := readProjectedGenerationFile(generationDirectory, names[0], maxProjectedCertificateBytes, false)
	if err != nil {
		return nil, err
	}
	privateKeyPEM, err := readProjectedGenerationFile(generationDirectory, names[1], maxProjectedPrivateKeyBytes, true)
	if err != nil {
		return nil, err
	}
	caPEM, err := readProjectedGenerationFile(generationDirectory, names[2], maxProjectedCertificateBytes, false)
	if err != nil {
		return nil, err
	}
	after, err := projectedGeneration(directory)
	if err != nil || after != generation {
		zeroBytes(privateKeyPEM)
		return nil, errors.New("fugue API TLS projection rotated during read")
	}

	certificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	zeroBytes(privateKeyPEM)
	if err != nil || len(certificate.Certificate) == 0 {
		return nil, errors.New("fugue API TLS certificate pair is invalid")
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return nil, errors.New("fugue API TLS leaf certificate is invalid")
	}
	roots, err := strictCertificatePool(caPEM)
	if err != nil {
		return nil, err
	}
	intermediates := x509.NewCertPool()
	for _, encoded := range certificate.Certificate[1:] {
		intermediate, err := x509.ParseCertificate(encoded)
		if err != nil {
			return nil, errors.New("fugue API TLS intermediate certificate is invalid")
		}
		intermediates.AddCert(intermediate)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		DNSName: serverName, Roots: roots, Intermediates: intermediates,
		CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		return nil, errors.New("fugue API TLS certificate trust or Service SAN is invalid")
	}
	certificate.Leaf = leaf
	return &certificate, nil
}

func (source ProjectedServerCertificate) TLSConfig() (*tls.Config, error) {
	_, _, serverName, _, err := source.normalized()
	if err != nil {
		return nil, err
	}
	if _, err := source.Load(); err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{"http/1.1"},
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			if hello == nil || strings.TrimSuffix(strings.ToLower(strings.TrimSpace(hello.ServerName)), ".") != serverName {
				return nil, errors.New("fugue API TLS SNI is invalid")
			}
			return source.Load()
		},
	}, nil
}

func (source ProjectedServerCertificate) normalized() (string, [3]string, string, time.Time, error) {
	directory := strings.TrimSpace(source.Directory)
	if directory == "" || !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return "", [3]string{}, "", time.Time{}, errors.New("fugue API TLS projection directory is invalid")
	}
	names := [3]string{strings.TrimSpace(source.CertificateFile), strings.TrimSpace(source.PrivateKeyFile), strings.TrimSpace(source.CAFile)}
	for _, name := range names {
		if name == "" || filepath.Base(name) != name || name == "." || name == ".." {
			return "", [3]string{}, "", time.Time{}, errors.New("fugue API TLS projection file name is invalid")
		}
	}
	if names[0] == names[1] || names[0] == names[2] || names[1] == names[2] {
		return "", [3]string{}, "", time.Time{}, errors.New("fugue API TLS projection file names are ambiguous")
	}
	serverName := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(source.ServerName)), ".")
	if serverName == "" || strings.ContainsAny(serverName, "/:@") || !strings.HasSuffix(serverName, ".svc") {
		return "", [3]string{}, "", time.Time{}, errors.New("fugue API TLS Service name is invalid")
	}
	now := time.Now().UTC()
	if source.Now != nil {
		now = source.Now().UTC()
	}
	if now.IsZero() {
		return "", [3]string{}, "", time.Time{}, errors.New("fugue API TLS validation time is invalid")
	}
	return directory, names, serverName, now, nil
}

func projectedGeneration(directory string) (string, error) {
	target, err := os.Readlink(filepath.Join(directory, "..data"))
	if err != nil || target == "" || filepath.IsAbs(target) || filepath.Base(target) != target || !strings.HasPrefix(target, "..") {
		return "", errors.New("fugue API TLS projection generation is unavailable")
	}
	info, err := os.Lstat(filepath.Join(directory, target))
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("fugue API TLS projection generation is invalid")
	}
	return target, nil
}

func readProjectedGenerationFile(directory, name string, limit int64, private bool) ([]byte, error) {
	path := filepath.Join(directory, name)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > limit {
		return nil, errors.New("fugue API TLS projection file is invalid")
	}
	if private && info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("fugue API TLS private key permissions are too broad")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("open fugue API TLS projection file")
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || opened.Size() != info.Size() {
		return nil, errors.New("fugue API TLS projection file changed during open")
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || len(data) == 0 || int64(len(data)) > limit {
		return nil, errors.New("read fugue API TLS projection file")
	}
	return data, nil
}

func strictCertificatePool(data []byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	remaining := data
	count := 0
	for len(bytes.TrimSpace(remaining)) > 0 {
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, errors.New("fugue API TLS CA bundle is invalid")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !certificate.IsCA {
			return nil, errors.New("fugue API TLS CA certificate is invalid")
		}
		pool.AddCert(certificate)
		count++
		remaining = rest
	}
	if count == 0 {
		return nil, fmt.Errorf("fugue API TLS CA bundle is empty")
	}
	return pool, nil
}

func zeroBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
