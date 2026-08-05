package edgecontrolsupport

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestProjectedServerCertificateLoadsOneAtomicGenerationAndRotates(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	now := time.Date(2026, 8, 4, 11, 0, 0, 0, time.UTC)
	writeTLSGeneration(t, directory, "..2026_08_04_11_00_00.1", "fugue-api-tls.fugue-system.svc", 11, now)
	if err := os.Symlink("..2026_08_04_11_00_00.1", filepath.Join(directory, "..data")); err != nil {
		t.Fatal(err)
	}
	source := ProjectedServerCertificate{
		Directory: directory, CertificateFile: "tls.crt", PrivateKeyFile: "tls.key", CAFile: "ca.crt",
		ServerName: "fugue-api-tls.fugue-system.svc", Now: func() time.Time { return now },
	}
	first, err := source.Load()
	if err != nil {
		t.Fatalf("load first generation: %v", err)
	}
	if first.Leaf == nil || first.Leaf.SerialNumber.Int64() != 11 {
		t.Fatalf("unexpected first certificate: %+v", first.Leaf)
	}

	writeTLSGeneration(t, directory, "..2026_08_04_11_05_00.2", "fugue-api-tls.fugue-system.svc", 12, now)
	if err := os.Symlink("..2026_08_04_11_05_00.2", filepath.Join(directory, "..data-next")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(directory, "..data-next"), filepath.Join(directory, "..data")); err != nil {
		t.Fatal(err)
	}
	second, err := source.Load()
	if err != nil {
		t.Fatalf("load rotated generation: %v", err)
	}
	if second.Leaf == nil || second.Leaf.SerialNumber.Int64() != 12 {
		t.Fatalf("rotation did not load the new exact generation: %+v", second.Leaf)
	}

	tlsConfig, err := source.TLSConfig()
	if err != nil {
		t.Fatalf("build TLS config: %v", err)
	}
	if tlsConfig.MinVersion != tls.VersionTLS13 || len(tlsConfig.NextProtos) != 1 || tlsConfig.NextProtos[0] != "http/1.1" {
		t.Fatalf("TLS policy is not TLS1.3/H1-only: %+v", tlsConfig)
	}
	if _, err := tlsConfig.GetCertificate(&tls.ClientHelloInfo{ServerName: "other.fugue-system.svc"}); err == nil {
		t.Fatal("wrong SNI must fail closed")
	}
}

func TestProjectedServerCertificateRejectsWrongSANAndEscapingGeneration(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 4, 11, 0, 0, 0, time.UTC)
	directory := t.TempDir()
	writeTLSGeneration(t, directory, "..2026_08_04_11_00_00.1", "wrong.fugue-system.svc", 21, now)
	if err := os.Symlink("..2026_08_04_11_00_00.1", filepath.Join(directory, "..data")); err != nil {
		t.Fatal(err)
	}
	source := ProjectedServerCertificate{Directory: directory, CertificateFile: "tls.crt", PrivateKeyFile: "tls.key", CAFile: "ca.crt", ServerName: "fugue-api-tls.fugue-system.svc", Now: func() time.Time { return now }}
	if _, err := source.Load(); err == nil {
		t.Fatal("certificate without the exact Service SAN must fail closed")
	}
	if err := os.Remove(filepath.Join(directory, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../outside", filepath.Join(directory, "..data")); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Load(); err == nil {
		t.Fatal("escaping Kubernetes projection generation must fail closed")
	}
}

func writeTLSGeneration(t *testing.T, directory, generation, serverName string, serial int64, now time.Time) {
	t.Helper()
	generationDirectory := filepath.Join(directory, generation)
	if err := os.Mkdir(generationDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	caPublic, caPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(serial * 100), Subject: pkix.Name{CommonName: "Fugue test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caPublic, caPrivate)
	if err != nil {
		t.Fatal(err)
	}
	serverPublic, serverPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: serverName}, DNSNames: []string{serverName},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(12 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, serverPublic, caPrivate)
	if err != nil {
		t.Fatal(err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(serverPrivate)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{
		"ca.crt":  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		"tls.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		"tls.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}),
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(generationDirectory, name), data, 0o400); err != nil {
			t.Fatal(err)
		}
	}
}
