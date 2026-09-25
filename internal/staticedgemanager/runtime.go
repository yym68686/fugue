package staticedgemanager

import (
	"bytes"
	"context"
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
	"strings"
	"time"

	c "fugue/internal/staticedgecontract"
)

// Runtime is deliberately independent of systemd and the manager lifecycle.
// No method stops or restarts Caddy, terminates a connection or replays traffic.
type Runtime interface {
	Snapshot(context.Context) (json.RawMessage, error)
	Startup(context.Context) (json.RawMessage, error)
	Validate(context.Context, c.Bundle) error
	Apply(context.Context, json.RawMessage) error
	Persist(context.Context, json.RawMessage) error
	Probe(context.Context, []string) error
}
type Probe struct {
	URL    string `json:"url"`
	Host   string `json:"host,omitempty"`
	Status int    `json:"status"`
	Body   string `json:"body,omitempty"`
}
type CaddyConfig struct {
	Binary       string           `json:"binary"`
	BinarySHA256 string           `json:"binary_sha256"`
	AdminSocket  string           `json:"admin_socket"`
	ConfigFile   string           `json:"config_file"`
	Checks       map[string]Probe `json:"checks"`
}
type CaddyRuntime struct {
	cfg    CaddyConfig
	client *http.Client
}

func NewCaddyRuntime(cfg CaddyConfig) (*CaddyRuntime, error) {
	for _, p := range []string{cfg.Binary, cfg.AdminSocket, cfg.ConfigFile} {
		if !filepath.IsAbs(p) {
			return nil, errors.New("Caddy binary, admin_socket and config_file must be absolute")
		}
	}
	pin, pinErr := hex.DecodeString(strings.TrimPrefix(cfg.BinarySHA256, "sha256:"))
	if !strings.HasPrefix(cfg.BinarySHA256, "sha256:") || pinErr != nil || len(pin) != 32 {
		return nil, errors.New("Caddy binary SHA256 pin required")
	}
	// A permission-restricted Unix socket avoids granting every local app access
	// to the Caddy admin API. Both this socket and ConfigFile require one writer.
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", cfg.AdminSocket)
	}}
	return &CaddyRuntime{cfg: cfg, client: &http.Client{Transport: tr, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (r *CaddyRuntime) request(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	req, e := http.NewRequestWithContext(ctx, method, "http://localhost"+path, bytes.NewReader(body))
	if e != nil {
		return nil, e
	}
	req.Header.Set("Content-Type", "application/json")
	resp, e := r.client.Do(req)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(resp.Body, c.MaxBytes+1))
	if e != nil {
		return nil, e
	}
	if len(raw) > c.MaxBytes {
		return nil, errors.New("Caddy response exceeds limit")
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Caddy admin returned HTTP %d", resp.StatusCode)
	}
	return raw, nil
}
func (r *CaddyRuntime) Snapshot(ctx context.Context) (json.RawMessage, error) {
	return r.request(ctx, http.MethodGet, "/config/", nil)
}
func (r *CaddyRuntime) Startup(context.Context) (json.RawMessage, error) {
	return os.ReadFile(r.cfg.ConfigFile)
}
func (r *CaddyRuntime) Validate(ctx context.Context, b c.Bundle) error {
	raw, e := os.ReadFile(r.cfg.Binary)
	if e != nil {
		return e
	}
	if c.Hash(raw) != r.cfg.BinarySHA256 {
		return errors.New("Caddy binary digest changed")
	}
	var cfg struct {
		Admin struct {
			Listen   string `json:"listen"`
			Disabled bool   `json:"disabled"`
			Config   struct {
				Persist *bool `json:"persist"`
			} `json:"config"`
		} `json:"admin"`
	}
	if e = json.Unmarshal(b.CaddyConfig, &cfg); e != nil {
		return e
	}
	if cfg.Admin.Disabled || cfg.Admin.Listen != "unix/"+r.cfg.AdminSocket || cfg.Admin.Config.Persist == nil || *cfg.Admin.Config.Persist {
		return errors.New("bundle must preserve the dedicated Unix admin socket and explicitly disable Caddy autosave")
	}
	// Draining is an actual replacement routing policy, never just a label.
	// Allow a signed static-response-only HTTP app: new business requests must
	// receive 503 (a local health endpoint may return 200). No new upstream work.
	if b.Mode == "draining" {
		var full map[string]any
		if e = json.Unmarshal(b.CaddyConfig, &full); e != nil {
			return e
		}
		apps, ok := full["apps"].(map[string]any)
		if !ok || len(apps) != 1 || apps["http"] == nil {
			return errors.New("draining config must contain only the HTTP app")
		}
		responses := 0
		var check func(any) error
		check = func(value any) error {
			switch v := value.(type) {
			case map[string]any:
				if handler, exists := v["handler"]; exists {
					if handler != "static_response" && handler != "subroute" {
						return errors.New("draining config must not start proxy, file or dynamic requests")
					}
					if handler == "static_response" {
						responses++
					}
				}
				for _, child := range v {
					if e := check(child); e != nil {
						return e
					}
				}
			case []any:
				for _, child := range v {
					if e := check(child); e != nil {
						return e
					}
				}
			}
			return nil
		}
		if e = check(apps); e != nil {
			return e
		}
		if responses == 0 {
			return errors.New("draining config has no static response")
		}
	}
	for _, id := range b.HealthChecks {
		p, ok := r.cfg.Checks[id]
		if !ok {
			return fmt.Errorf("health check %q is not in local policy", id)
		}
		u, e := url.Parse(p.URL)
		if e != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") || p.Status < 100 || p.Status > 599 {
			return errors.New("invalid local health check policy")
		}
	}
	f, e := os.CreateTemp(filepath.Dir(r.cfg.ConfigFile), ".static-validate-*.json")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if _, e = f.Write(b.CaddyConfig); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	// Validation output can contain configuration secrets; retain only exit status.
	cmd := exec.CommandContext(ctx, r.cfg.Binary, "validate", "--config", name)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if e = cmd.Run(); e != nil {
		return fmt.Errorf("Caddy candidate validation failed: %w", e)
	}
	return nil
}
func (r *CaddyRuntime) Apply(ctx context.Context, raw json.RawMessage) error {
	_, e := r.request(ctx, http.MethodPost, "/load", raw)
	return e
}
func (r *CaddyRuntime) Persist(_ context.Context, raw json.RawMessage) error {
	return atomicBytes(r.cfg.ConfigFile, raw, 0640)
}

func (r *CaddyRuntime) Probe(ctx context.Context, ids []string) error {
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	for _, id := range ids {
		p, ok := r.cfg.Checks[id]
		if !ok {
			return fmt.Errorf("unknown local check %q", id)
		}
		req, e := http.NewRequestWithContext(ctx, http.MethodGet, p.URL, nil)
		if e != nil {
			return e
		}
		req.Host = p.Host
		resp, e := client.Do(req)
		if e != nil {
			return fmt.Errorf("health check %q failed: %w", id, e)
		}
		raw, re := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		if re != nil || resp.StatusCode != p.Status || (p.Body != "" && string(raw) != p.Body) {
			return fmt.Errorf("health check %q failed (HTTP %d)", id, resp.StatusCode)
		}
	}
	return nil
}
func atomicBytes(path string, raw []byte, mode os.FileMode) error {
	f, e := os.CreateTemp(filepath.Dir(path), ".static-state-*")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	defer f.Close()
	if e = f.Chmod(mode); e != nil {
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
	if e = os.Rename(name, path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
