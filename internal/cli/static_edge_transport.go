package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	c "fugue/internal/staticedgecontract"
)

type staticEdgeContext struct {
	Name           string `json:"name"`
	EdgeID         string `json:"edge_id"`
	Transport      string `json:"transport"`
	ManagerURL     string `json:"manager_url,omitempty"`
	ServerName     string `json:"server_name,omitempty"`
	CAFile         string `json:"ca_file,omitempty"`
	ClientCert     string `json:"client_cert,omitempty"`
	ClientKey      string `json:"client_key,omitempty"`
	SSHHost        string `json:"ssh_host,omitempty"`
	ManagerCommand string `json:"manager_command,omitempty"`
}
type staticEdgeContextFile struct {
	SchemaVersion int                 `json:"schema_version"`
	Active        string              `json:"active,omitempty"`
	Contexts      []staticEdgeContext `json:"contexts"`
}

func staticEdgeContextPath() string {
	if p := os.Getenv("FUGUE_STATIC_EDGE_CONTEXT_FILE"); p != "" {
		return p
	}
	dir, e := os.UserConfigDir()
	if e != nil {
		dir = ".config"
	}
	return filepath.Join(dir, "fugue", "static-edge-contexts.json")
}
func validateStaticEdgeContext(x staticEdgeContext) error {
	if !c.ValidID(x.Name) || !c.ValidID(x.EdgeID) {
		return errors.New("valid context name and --edge-id required")
	}
	if x.Transport != "mtls" && x.Transport != "ssh" {
		return errors.New("transport must be mtls or ssh")
	}
	if x.ManagerURL != "" {
		u, e := url.Parse(x.ManagerURL)
		if e != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return errors.New("manager must be an HTTPS authority without credentials, path, query or fragment")
		}
		if x.CAFile == "" || x.ClientCert == "" || x.ClientKey == "" {
			return errors.New("mTLS requires --ca, --client-cert, --client-key")
		}
	}
	if x.Transport == "mtls" && x.ManagerURL == "" {
		return errors.New("mTLS manager URL required")
	}
	if x.SSHHost != "" && (!regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`).MatchString(x.SSHHost)) {
		return errors.New("ssh-host must be a local SSH alias, not options or shell input")
	}
	if x.ManagerCommand != "" && !regexp.MustCompile(`^/[a-zA-Z0-9_./-]+$`).MatchString(x.ManagerCommand) {
		return errors.New("manager-command must be one absolute executable path")
	}
	if x.Transport == "ssh" && (x.SSHHost == "" || x.ManagerCommand == "") {
		return errors.New("SSH alias and manager executable required")
	}
	return nil
}
func readStaticEdgeContexts() (staticEdgeContextFile, error) {
	cfg := staticEdgeContextFile{SchemaVersion: 1, Contexts: []staticEdgeContext{}}
	raw, e := os.ReadFile(staticEdgeContextPath())
	if os.IsNotExist(e) {
		return cfg, nil
	}
	if e != nil {
		return cfg, e
	}
	if e = c.StrictJSON(raw, &cfg); e != nil {
		return cfg, e
	}
	if cfg.SchemaVersion != 1 {
		return cfg, errors.New("unsupported static edge context schema")
	}
	seen := map[string]bool{}
	for _, x := range cfg.Contexts {
		if e = validateStaticEdgeContext(x); e != nil {
			return cfg, e
		}
		if seen[x.Name] {
			return cfg, errors.New("duplicate static edge context")
		}
		seen[x.Name] = true
	}
	if cfg.Active != "" && !seen[cfg.Active] {
		return cfg, errors.New("active static edge context missing")
	}
	return cfg, nil
}
func saveStaticEdgeContexts(cfg staticEdgeContextFile) error {
	raw, e := json.MarshalIndent(cfg, "", "  ")
	if e != nil {
		return e
	}
	return staticEdgeWriteFile(staticEdgeContextPath(), append(raw, '\n'))
}
func staticEdgeWriteFile(path string, raw []byte) error {
	dir := filepath.Dir(path)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(dir, ".static-edge-*")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	defer f.Close()
	if e = f.Chmod(0600); e != nil {
		return e
	}
	if _, e = f.Write(raw); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	return os.Rename(name, path)
}
func loadStaticEdgeContext(name string) (staticEdgeContext, error) {
	cfg, e := readStaticEdgeContexts()
	if e != nil {
		return staticEdgeContext{}, e
	}
	if name == "" {
		name = cfg.Active
	}
	for _, x := range cfg.Contexts {
		if x.Name == name {
			return x, nil
		}
	}
	return staticEdgeContext{}, fmt.Errorf("static edge context %q not found; use context add/use", name)
}
func staticEdgePath(p string) string {
	if strings.HasPrefix(p, "~/") {
		h, e := os.UserHomeDir()
		if e == nil {
			return filepath.Join(h, p[2:])
		}
	}
	return p
}
func newStaticRequestID() string {
	raw := make([]byte, 16)
	if _, e := rand.Read(raw); e != nil {
		panic(e)
	}
	return "fse_" + hex.EncodeToString(raw)
}
func staticEdgeCall(ctx context.Context, cfg staticEdgeContext, req c.Request) (c.Response, error) {
	if e := validateStaticEdgeContext(cfg); e != nil {
		return c.Response{}, e
	}
	raw, e := json.Marshal(req)
	if e != nil {
		return c.Response{}, e
	}
	if len(raw) > c.MaxBytes {
		return c.Response{}, errors.New("request too large")
	}
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	var body []byte
	switch cfg.Transport {
	case "mtls":
		body, e = staticEdgeCallMTLS(ctx, cfg, raw)
	case "ssh":
		body, e = staticEdgeCallSSH(ctx, cfg, raw)
	}
	if e != nil {
		return c.Response{}, fmt.Errorf("request_id=%s outcome unknown; query operation before retry: %w", req.RequestID, e)
	}
	var out c.Response
	if e = c.StrictJSON(body, &out); e != nil {
		return out, fmt.Errorf("invalid manager response for request_id=%s: %w", req.RequestID, e)
	}
	if out.Schema != c.RPCSchema || out.RequestID != req.RequestID || out.EdgeID != req.EdgeID {
		return out, errors.New("manager response identity mismatch")
	}
	if !out.OK {
		return out, fmt.Errorf("manager rejected request %s (HTTP %d): %s", req.RequestID, out.Status, out.Error)
	}
	return out, nil
}
func staticEdgeCallMTLS(ctx context.Context, cfg staticEdgeContext, raw []byte) ([]byte, error) {
	ca, e := os.ReadFile(staticEdgePath(cfg.CAFile))
	if e != nil {
		return nil, e
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid management CA")
	}
	cert, e := tls.LoadX509KeyPair(staticEdgePath(cfg.ClientCert), staticEdgePath(cfg.ClientKey))
	if e != nil {
		return nil, e
	}
	tr := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext, TLSHandshakeTimeout: 10 * time.Second, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{cert}, ServerName: cfg.ServerName}}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(cfg.ManagerURL, "/")+"/v1/static-edge/manager", bytes.NewReader(raw))
	if e != nil {
		return nil, e
	}
	req.Header.Set("Content-Type", "application/json")
	resp, e := client.Do(req)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return nil, errors.New("manager redirects are forbidden")
	}
	body, e := io.ReadAll(io.LimitReader(resp.Body, c.MaxBytes+1))
	if e != nil {
		return nil, e
	}
	if len(body) > c.MaxBytes {
		return nil, errors.New("manager response too large")
	}
	return body, nil
}

type staticEdgeBuffer struct {
	bytes.Buffer
	limit int
}

func (b *staticEdgeBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		return 0, errors.New("SSH response exceeds limit")
	}
	return b.Buffer.Write(p)
}
func staticEdgeCallSSH(ctx context.Context, cfg staticEdgeContext, raw []byte) ([]byte, error) {
	// Let OpenSSH resolve IdentityFile, User, Port, ProxyJump and known_hosts.
	// No shell-built command or secret appears in argv. Recovery is explicit.
	cmd := exec.CommandContext(ctx, "ssh", "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ForwardAgent=no", "-o", "ClearAllForwardings=yes", "-o", "ConnectTimeout=10", cfg.SSHHost, cfg.ManagerCommand, "ssh-rpc")
	cmd.Stdin = bytes.NewReader(raw)
	out := &staticEdgeBuffer{limit: c.MaxBytes}
	errout := &staticEdgeBuffer{limit: 8192}
	cmd.Stdout = out
	cmd.Stderr = errout
	cmd.WaitDelay = 3 * time.Second
	if e := cmd.Run(); e != nil {
		return nil, fmt.Errorf("SSH manager transport failed: %w", e)
	}
	return out.Bytes(), nil
}
