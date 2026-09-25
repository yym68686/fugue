package staticedgemanager

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	c "fugue/internal/staticedgecontract"
)

type CredentialSlot struct {
	ServerCert string            `json:"server_cert"`
	ServerKey  string            `json:"server_key"`
	ClientCA   string            `json:"client_ca"`
	Grants     map[string]string `json:"grants"`
}
type loadedSlot struct {
	tls     *tls.Config
	roots   *x509.CertPool
	grants  map[string]string
	expires string
}
type TLSCredentials struct {
	mu      sync.RWMutex
	slots   map[string]CredentialSlot
	current *loadedSlot
	name    string
}

func NewCredentials(slots map[string]CredentialSlot, initial string) (*TLSCredentials, error) {
	p := &TLSCredentials{slots: slots}
	if e := p.Select(initial); e != nil {
		return nil, e
	}
	return p, nil
}
func (p *TLSCredentials) load(name string) (*loadedSlot, error) {
	cfg, ok := p.slots[name]
	if !ok || !c.ValidID(name) {
		return nil, errors.New("credential slot is not configured")
	}
	cert, e := tls.LoadX509KeyPair(cfg.ServerCert, cfg.ServerKey)
	if e != nil {
		return nil, e
	}
	leaf, e := x509.ParseCertificate(cert.Certificate[0])
	if e != nil {
		return nil, e
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return nil, errors.New("server certificate not currently valid")
	}
	raw, e := os.ReadFile(cfg.ClientCA)
	if e != nil {
		return nil, e
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(raw) {
		return nil, errors.New("invalid management client CA")
	}
	if len(cfg.Grants) == 0 {
		return nil, errors.New("explicit client fingerprint grants required")
	}
	for fp, g := range cfg.Grants {
		b, e := hex.DecodeString(fp)
		if e != nil || len(b) != 32 || (g != "read" && g != "operator" && g != "admin") {
			return nil, errors.New("invalid client fingerprint grant")
		}
	}
	return &loadedSlot{tls: &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool, SessionTicketsDisabled: true, NextProtos: []string{"http/1.1"}}, roots: pool, grants: cfg.Grants, expires: leaf.NotAfter.UTC().Format(time.RFC3339)}, nil
}
func (p *TLSCredentials) Validate(name string) error { _, e := p.load(name); return e }
func (p *TLSCredentials) Select(name string) error {
	s, e := p.load(name)
	if e != nil {
		return e
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current = s
	p.name = name
	return nil
}
func (p *TLSCredentials) Status() (string, string) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.name, p.current.expires
}
func (p *TLSCredentials) Config() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS13, GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
		p.mu.RLock()
		defer p.mu.RUnlock()
		return p.current.tls, nil
	}}
}
func (p *TLSCredentials) Authorize(r *http.Request) (string, string, error) {
	p.mu.RLock()
	s := p.current
	p.mu.RUnlock()
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return "", "", errors.New("mTLS client certificate required")
	}
	leaf := r.TLS.PeerCertificates[0]
	inter := x509.NewCertPool()
	for _, cert := range r.TLS.PeerCertificates[1:] {
		inter.AddCert(cert)
	}
	if _, e := leaf.Verify(x509.VerifyOptions{Roots: s.roots, Intermediates: inter, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); e != nil {
		return "", "", errors.New("client certificate rejected by current management trust")
	}
	sum := sha256.Sum256(leaf.Raw)
	fp := hex.EncodeToString(sum[:])
	grant, ok := s.grants[fp]
	if !ok {
		return "", "", errors.New("client identity not authorized")
	}
	return fp, grant, nil
}
func (m *Manager) Handler(auth func(*http.Request) (string, string, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost || r.URL.Path != "/v1/static-edge/manager" {
			http.Error(w, "not found", 404)
			return
		}
		actor, grant, e := auth(r)
		if e != nil {
			http.Error(w, "management identity rejected", 403)
			return
		}
		raw, e := io.ReadAll(http.MaxBytesReader(w, r.Body, c.MaxBytes))
		if e != nil {
			http.Error(w, "invalid request size", 400)
			return
		}
		var req c.Request
		if e = c.StrictJSON(raw, &req); e != nil {
			http.Error(w, "invalid management envelope", 400)
			return
		}
		out := m.Execute(req, actor, grant)
		w.WriteHeader(out.Status)
		_ = json.NewEncoder(w).Encode(out)
	})
}
func LocalAuth(grant string) func(*http.Request) (string, string, error) {
	return func(*http.Request) (string, string, error) { return "ssh-local", grant, nil }
}
func GrantValid(g string) bool { return g == "read" || g == "operator" || g == "admin" }
func ReadConfig(path string, value any) error {
	raw, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	if e = c.StrictJSON(raw, value); e != nil {
		return fmt.Errorf("invalid manager configuration: %w", e)
	}
	return nil
}
func RedactedError(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if strings.Contains(s, "PRIVATE KEY") {
		return "credential operation failed"
	}
	return s
}
