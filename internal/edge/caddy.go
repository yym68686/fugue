package edge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	caddyConfigReapplyInterval = 5 * time.Minute
	defaultCaddyDataDir        = "/data/caddy"
	defaultCaddyIssuerStorage  = "acme-v02.api.letsencrypt.org-directory"

	caddyTLSModeOff            = "off"
	caddyTLSModeInternal       = "internal"
	caddyTLSModePublicOnDemand = "public-on-demand"
)

func (s *Service) applyCurrentCaddyConfig(ctx context.Context) error {
	if !s.Config.CaddyEnabled {
		return nil
	}
	bundle, ok := s.Bundle()
	if !ok {
		return nil
	}
	return s.applyCaddyConfig(ctx, bundle)
}

func (s *Service) applyCaddyConfig(ctx context.Context, bundle model.EdgeRouteBundle) error {
	if !s.Config.CaddyEnabled {
		return nil
	}
	version := strings.TrimSpace(bundle.Version)
	if version == "" {
		version = "unknown"
	}
	configSignature, err := s.caddyConfigSignature(bundle)
	if err != nil {
		s.recordCaddyApply(version, 0, "", err)
		return err
	}
	if s.needsCaddyApply(configSignature) {
		configBody, routeCount, err := s.buildCaddyConfig(bundle)
		if err != nil {
			s.recordCaddyApply(version, 0, configSignature, err)
			return err
		}
		endpoint, err := s.caddyAdminEndpoint("/load")
		if err != nil {
			s.recordCaddyApply(version, 0, configSignature, err)
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(configBody))
		if err != nil {
			err = fmt.Errorf("build caddy config request: %w", err)
			s.recordCaddyApply(version, 0, configSignature, err)
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := s.HTTPClient.Do(req)
		if err != nil {
			err = fmt.Errorf("apply caddy config: %w", err)
			s.recordCaddyApply(version, 0, configSignature, err)
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			err = fmt.Errorf("apply caddy config returned status %d", resp.StatusCode)
			s.recordCaddyApply(version, 0, configSignature, err)
			return err
		}
		s.recordCaddyApply(version, routeCount, configSignature, nil)
		if s.Logger != nil {
			s.Logger.Printf("edge caddy config applied; version=%s hosts=%d listen=%s tls_mode=%s proxy=%s", version, routeCount, strings.TrimSpace(s.Config.CaddyListenAddr), s.normalizedCaddyTLSMode(), caddyProxyDialAddress(s.Config.CaddyProxyListenAddr))
		}
	}
	if err := s.maybeWarmupCurrentCaddyTLS(ctx, bundle, configSignature); err != nil && s.Logger != nil {
		s.Logger.Printf("edge caddy TLS warmup failed; version=%s error=%s", version, s.redact(err.Error()))
	}
	if err := s.maybeWarmupCurrentEdgeCache(ctx, bundle, configSignature); err != nil && s.Logger != nil {
		s.Logger.Printf("edge cache warmup failed; version=%s error=%s", version, s.redact(err.Error()))
	}
	return nil
}

func (s *Service) needsCaddyApply(signature string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(s.metrics.CaddyAppliedSignature) != strings.TrimSpace(signature) {
		return true
	}
	if strings.TrimSpace(s.metrics.CaddyLastError) != "" {
		return true
	}
	if s.metrics.CaddyLastApplyAt == nil {
		return true
	}
	return time.Since(*s.metrics.CaddyLastApplyAt) >= caddyConfigReapplyInterval
}

func (s *Service) buildCaddyConfig(bundle model.EdgeRouteBundle) ([]byte, int, error) {
	adminURL, err := s.normalizedCaddyAdminURL()
	if err != nil {
		return nil, 0, err
	}
	listenAddr := strings.TrimSpace(s.Config.CaddyListenAddr)
	if listenAddr == "" {
		return nil, 0, fmt.Errorf("FUGUE_EDGE_CADDY_LISTEN_ADDR is required when caddy mode is enabled")
	}
	proxyDial := caddyProxyDialAddress(s.Config.CaddyProxyListenAddr)
	if proxyDial == "" {
		return nil, 0, fmt.Errorf("FUGUE_EDGE_PROXY_LISTEN_ADDR is required when caddy mode is enabled")
	}
	tlsMode := s.normalizedCaddyTLSMode()

	hosts := s.uniqueBundleHosts(bundle)
	routes := make([]any, 0, len(hosts))
	for _, host := range hosts {
		routes = append(routes, map[string]any{
			"match": []any{
				map[string]any{
					"host": []string{host},
				},
			},
			"handle": []any{
				map[string]any{
					"handler": "reverse_proxy",
					"upstreams": []any{
						map[string]string{"dial": proxyDial},
					},
					"headers": map[string]any{
						"request": map[string]any{
							"set": map[string][]string{
								"X-Fugue-Edge-Route-Host": []string{host},
								"X-Forwarded-Host":        []string{"{http.request.host}"},
							},
						},
					},
				},
			},
			"terminal": true,
		})
	}
	routes = append(routes, map[string]any{
		"handle": []any{
			map[string]any{
				"handler":     "static_response",
				"status_code": 404,
				"body":        "fugue route not found\n",
			},
		},
		"terminal": true,
	})

	server := map[string]any{
		"listen": []string{listenAddr},
		"routes": routes,
		"logs": map[string]any{
			"default_logger_name": "fugue_edge_access",
		},
	}
	apps := map[string]any{
		"http": map[string]any{
			"servers": map[string]any{
				"fugue_edge": server,
			},
		},
	}
	switch tlsMode {
	case caddyTLSModeOff:
		server["automatic_https"] = map[string]any{
			"disable": true,
		}
	case caddyTLSModeInternal:
		server["tls_connection_policies"] = []any{map[string]any{}}
		if len(hosts) > 0 {
			apps["tls"] = map[string]any{
				"automation": map[string]any{
					"policies": []any{
						map[string]any{
							"subjects": hosts,
							"issuers": []any{
								map[string]any{"module": "internal"},
							},
						},
					},
				},
			}
		}
	case caddyTLSModePublicOnDemand:
		askURL, err := s.normalizedCaddyTLSAskURL()
		if err != nil {
			return nil, 0, err
		}
		server["tls_connection_policies"] = []any{map[string]any{}}
		apps["tls"] = map[string]any{
			"automation": map[string]any{
				"policies": []any{
					map[string]any{"on_demand": true},
				},
				"on_demand": map[string]any{
					"permission": map[string]any{
						"module":   "http",
						"endpoint": askURL,
					},
				},
			},
		}
	default:
		return nil, 0, fmt.Errorf("FUGUE_EDGE_CADDY_TLS_MODE must be off, internal, or public-on-demand")
	}
	if certFile, keyFile := strings.TrimSpace(s.Config.CaddyStaticTLSCertFile), strings.TrimSpace(s.Config.CaddyStaticTLSKeyFile); certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return nil, 0, fmt.Errorf("FUGUE_EDGE_CADDY_STATIC_TLS_CERT_FILE and FUGUE_EDGE_CADDY_STATIC_TLS_KEY_FILE must be configured together")
		}
		if tlsMode == caddyTLSModeOff {
			return nil, 0, fmt.Errorf("static Caddy TLS files require TLS mode to be internal or public-on-demand")
		}
		tlsApp := ensureCaddyTLSApp(apps)
		tlsApp["certificates"] = map[string]any{
			"load_files": []any{
				map[string]any{
					"certificate": certFile,
					"key":         keyFile,
				},
			},
		}
	}

	config := map[string]any{
		"admin": map[string]any{
			"listen": adminURL.Host,
		},
		"logging": map[string]any{
			"logs": map[string]any{
				"fugue_edge_access": map[string]any{
					"writer": map[string]any{
						"output": "stdout",
					},
					"encoder": map[string]any{
						"format": "filter",
						"wrap": map[string]any{
							"format": "json",
						},
						"fields": map[string]any{
							"request>headers>Authorization":         map[string]string{"filter": "delete"},
							"request>headers>Cookie":                map[string]string{"filter": "delete"},
							"request>headers>Proxy-Authorization":   map[string]string{"filter": "delete"},
							"request>headers>X-Tailscale-Handshake": map[string]string{"filter": "delete"},
						},
					},
					"include": []string{"http.log.access.fugue_edge_access"},
				},
			},
		},
		"apps": apps,
	}
	data, err := json.Marshal(config)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal caddy config: %w", err)
	}
	return data, len(hosts), nil
}

func ensureCaddyTLSApp(apps map[string]any) map[string]any {
	if existing, ok := apps["tls"].(map[string]any); ok {
		return existing
	}
	tlsApp := map[string]any{}
	apps["tls"] = tlsApp
	return tlsApp
}

func (s *Service) caddyAdminEndpoint(path string) (string, error) {
	adminURL, err := s.normalizedCaddyAdminURL()
	if err != nil {
		return "", err
	}
	adminURL.Path = strings.TrimRight(adminURL.Path, "/") + "/" + strings.TrimLeft(path, "/")
	adminURL.RawQuery = ""
	adminURL.Fragment = ""
	return adminURL.String(), nil
}

func (s *Service) normalizedCaddyAdminURL() (*url.URL, error) {
	raw := strings.TrimSpace(s.Config.CaddyAdminURL)
	if raw == "" {
		raw = "http://127.0.0.1:2019"
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid FUGUE_EDGE_CADDY_ADMIN_URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("FUGUE_EDGE_CADDY_ADMIN_URL must use http or https")
	}
	host := parsed.Hostname()
	if !isLoopbackHost(host) {
		return nil, fmt.Errorf("FUGUE_EDGE_CADDY_ADMIN_URL must point to localhost")
	}
	parsed.User = nil
	return parsed, nil
}

func (s *Service) normalizedCaddyTLSMode() string {
	mode := strings.ToLower(strings.TrimSpace(s.Config.CaddyTLSMode))
	if mode == "" {
		return caddyTLSModeOff
	}
	return mode
}

func (s *Service) normalizedCaddyTLSAskURL() (string, error) {
	raw := strings.TrimSpace(s.Config.CaddyTLSAskURL)
	if raw == "" {
		raw = "http://" + caddyProxyDialAddress(s.Config.ListenAddr) + "/edge/tls/ask"
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("invalid FUGUE_EDGE_CADDY_TLS_ASK_URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("FUGUE_EDGE_CADDY_TLS_ASK_URL must use http or https")
	}
	if parsed.RawQuery != "" {
		return "", fmt.Errorf("FUGUE_EDGE_CADDY_TLS_ASK_URL must not include query parameters")
	}
	return parsed.String(), nil
}

func (s *Service) uniqueBundleHosts(bundle model.EdgeRouteBundle) []string {
	seen := map[string]struct{}{}
	for _, route := range bundle.Routes {
		if !s.routeAllowedForThisEdge(route) {
			continue
		}
		host := normalizeRouteHost(route.Hostname)
		if host == "" {
			continue
		}
		seen[host] = struct{}{}
	}
	hosts := make([]string, 0, len(seen))
	for host := range seen {
		hosts = append(hosts, host)
	}
	sort.Strings(hosts)
	return hosts
}

func (s *Service) caddyConfigSignature(bundle model.EdgeRouteBundle) (string, error) {
	version := strings.TrimSpace(bundle.Version)
	if version == "" {
		version = "unknown"
	}
	parts := []string{
		"version=" + version,
		"generation=" + strings.TrimSpace(bundle.Generation),
		"listen=" + strings.TrimSpace(s.Config.CaddyListenAddr),
		"proxy=" + caddyProxyDialAddress(s.Config.CaddyProxyListenAddr),
		"tls_mode=" + s.normalizedCaddyTLSMode(),
	}
	if s.normalizedCaddyTLSMode() == caddyTLSModePublicOnDemand {
		askURL, err := s.normalizedCaddyTLSAskURL()
		if err != nil {
			return "", err
		}
		parts = append(parts, "tls_ask="+askURL)
	}
	if certFile, keyFile := strings.TrimSpace(s.Config.CaddyStaticTLSCertFile), strings.TrimSpace(s.Config.CaddyStaticTLSKeyFile); certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return "", fmt.Errorf("FUGUE_EDGE_CADDY_STATIC_TLS_CERT_FILE and FUGUE_EDGE_CADDY_STATIC_TLS_KEY_FILE must be configured together")
		}
		fingerprint, err := staticTLSFileFingerprint(certFile, keyFile)
		if err != nil {
			return "", err
		}
		parts = append(parts, "static_tls="+fingerprint)
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(sum[:]), nil
}

func staticTLSFileFingerprint(certFile, keyFile string) (string, error) {
	cert, err := os.ReadFile(certFile)
	if err != nil {
		return "", fmt.Errorf("read Caddy static TLS certificate file: %w", err)
	}
	key, err := os.ReadFile(keyFile)
	if err != nil {
		return "", fmt.Errorf("read Caddy static TLS key file: %w", err)
	}
	h := sha256.New()
	_, _ = h.Write([]byte("cert\x00"))
	_, _ = h.Write(cert)
	_, _ = h.Write([]byte("\x00key\x00"))
	_, _ = h.Write(key)
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (s *Service) maybeWarmupCurrentCaddyTLS(ctx context.Context, bundle model.EdgeRouteBundle, configSignature string) error {
	if s.normalizedCaddyTLSMode() != caddyTLSModePublicOnDemand {
		return nil
	}
	hosts := s.caddyWarmupHosts(bundle)
	customDomainHosts := s.customDomainTLSHosts(bundle)
	if syncErr := s.syncSharedCaddyTLSCertificates(ctx, customDomainHosts); syncErr != nil && s.Logger != nil {
		s.Logger.Printf("edge caddy shared TLS sync failed; hosts=%d error=%s", len(customDomainHosts), s.redact(syncErr.Error()))
	}
	if len(hosts) == 0 {
		return nil
	}
	dialAddress := caddyProxyDialAddress(s.Config.CaddyListenAddr)
	if dialAddress == "" {
		return nil
	}
	warmupSignature := configSignature + ":" + strings.Join(hosts, ",")
	if !s.needsCaddyWarmup(warmupSignature) {
		return nil
	}
	warmup := s.caddyWarmup
	if warmup == nil {
		warmup = warmupCaddyTLS
	}
	reportHosts := s.customDomainTLSReportHosts(bundle)
	started := time.Now()
	var firstErr error
	for _, host := range hosts {
		err := warmup(ctx, dialAddress, host)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if currentTLSStatus, ok := reportHosts[host]; ok {
			currentReady := strings.EqualFold(strings.TrimSpace(currentTLSStatus), model.AppDomainTLSStatusReady)
			if err != nil && currentReady {
				continue
			}
			status := model.AppDomainTLSStatusReady
			message := ""
			if err != nil {
				status = model.AppDomainTLSStatusPending
				message = fmt.Sprintf("waiting for edge certificate issuance: %v", err)
			}
			var certBundle *caddyTLSCertificateBundle
			if err == nil && s.caddySharedTLSEnabled() {
				localBundle, certErr := s.readLocalCaddyTLSCertificate(host)
				if certErr != nil {
					if currentReady {
						continue
					}
					status = model.AppDomainTLSStatusPending
					message = fmt.Sprintf("certificate issued locally but shared bundle export failed: %v", certErr)
				} else {
					certBundle = &localBundle
				}
			}
			if reportErr := s.reportCustomDomainTLSStatus(ctx, host, status, message, certBundle); reportErr != nil && firstErr == nil {
				firstErr = reportErr
			}
		}
	}
	duration := time.Since(started)
	s.recordCaddyWarmup(warmupSignature, strings.Join(hosts, ","), duration, firstErr)
	if firstErr != nil {
		return firstErr
	}
	if s.Logger != nil {
		s.Logger.Printf("edge caddy TLS warmup complete; hosts=%d listen=%s duration=%s", len(hosts), dialAddress, duration)
	}
	return nil
}

type caddyTLSCertificateBundle struct {
	CertificatePEM string `json:"certificate_pem"`
	PrivateKeyPEM  string `json:"private_key_pem"`
	MetadataJSON   string `json:"metadata_json,omitempty"`
	IssuerStorage  string `json:"issuer_storage,omitempty"`
}

func (s *Service) caddyWarmupHosts(bundle model.EdgeRouteBundle) []string {
	seen := map[string]struct{}{}
	for _, route := range bundle.Routes {
		if !s.routeAllowedForThisEdge(route) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(route.Status), model.EdgeRouteStatusActive) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(route.TLSPolicy), model.EdgeRouteTLSPolicyPlatform) {
			continue
		}
		host := normalizeRouteHost(route.Hostname)
		if host == "" {
			continue
		}
		seen[host] = struct{}{}
	}
	for host := range s.customDomainTLSReportHosts(bundle) {
		seen[host] = struct{}{}
	}
	for _, host := range s.customDomainTLSHosts(bundle) {
		seen[host] = struct{}{}
	}
	if len(seen) == 0 {
		return nil
	}
	hosts := make([]string, 0, len(seen))
	for host := range seen {
		hosts = append(hosts, host)
	}
	sort.Strings(hosts)
	return hosts
}

func (s *Service) customDomainTLSReportHosts(bundle model.EdgeRouteBundle) map[string]string {
	allowlist := map[string]string{}
	for _, entry := range bundle.TLSAllowlist {
		host := normalizeRouteHost(entry.Hostname)
		if host == "" {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(entry.Status), model.AppDomainStatusVerified) {
			continue
		}
		allowlist[host] = model.NormalizeAppDomainTLSStatus(entry.TLSStatus)
	}
	if len(allowlist) == 0 {
		return nil
	}
	out := map[string]string{}
	for _, route := range bundle.Routes {
		if !s.routeAllowedForThisEdge(route) {
			continue
		}
		host := normalizeRouteHost(route.Hostname)
		if host == "" {
			continue
		}
		tlsStatus, ok := allowlist[host]
		if !ok {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(route.RouteKind), model.EdgeRouteKindCustomDomain) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(route.TLSPolicy), model.EdgeRouteTLSPolicyCustomDomain) {
			continue
		}
		out[host] = tlsStatus
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (s *Service) customDomainTLSHosts(bundle model.EdgeRouteBundle) []string {
	seen := map[string]struct{}{}
	for host := range s.customDomainTLSReportHosts(bundle) {
		seen[host] = struct{}{}
	}
	for _, route := range bundle.Routes {
		if !s.routeAllowedForThisEdge(route) {
			continue
		}
		host := normalizeRouteHost(route.Hostname)
		if host == "" {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(route.RouteKind), model.EdgeRouteKindCustomDomain) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(route.TLSPolicy), model.EdgeRouteTLSPolicyCustomDomain) {
			continue
		}
		seen[host] = struct{}{}
	}
	if len(seen) == 0 {
		return nil
	}
	hosts := make([]string, 0, len(seen))
	for host := range seen {
		hosts = append(hosts, host)
	}
	sort.Strings(hosts)
	return hosts
}

func (s *Service) syncSharedCaddyTLSCertificates(ctx context.Context, hosts []string) error {
	if !s.caddySharedTLSEnabled() || len(hosts) == 0 {
		return nil
	}
	var firstErr error
	for _, host := range hosts {
		bundle, err := s.fetchSharedCaddyTLSCertificate(ctx, host)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if bundle == nil {
			continue
		}
		if err := s.installSharedCaddyTLSCertificate(host, *bundle); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if s.Logger != nil {
			s.Logger.Printf("edge caddy shared TLS certificate installed; host=%s issuer_storage=%s", host, strings.TrimSpace(bundle.IssuerStorage))
		}
	}
	return firstErr
}

func (s *Service) fetchSharedCaddyTLSCertificate(ctx context.Context, hostname string) (*caddyTLSCertificateBundle, error) {
	hostname = normalizeRouteHost(hostname)
	if hostname == "" {
		return nil, nil
	}
	base, err := url.Parse(strings.TrimRight(strings.TrimSpace(s.Config.APIURL), "/"))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("invalid FUGUE_API_URL")
	}
	token := strings.TrimSpace(s.Config.EdgeToken)
	if token == "" {
		return nil, fmt.Errorf("FUGUE_EDGE_TOKEN is required to fetch shared TLS certificates")
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/v1/edge/domains/" + url.PathEscape(hostname) + "/tls-bundle"
	query := base.Query()
	query.Set("token", token)
	base.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build shared TLS certificate request: %w", err)
	}
	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch shared TLS certificate for %s: %w", hostname, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, statusError{
			StatusCode: resp.StatusCode,
			Body:       s.redact(strings.TrimSpace(string(body))),
		}
	}
	var out struct {
		Certificate caddyTLSCertificateBundle `json:"certificate"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode shared TLS certificate for %s: %w", hostname, err)
	}
	if strings.TrimSpace(out.Certificate.CertificatePEM) == "" || strings.TrimSpace(out.Certificate.PrivateKeyPEM) == "" {
		return nil, fmt.Errorf("shared TLS certificate for %s is incomplete", hostname)
	}
	return &out.Certificate, nil
}

func (s *Service) installSharedCaddyTLSCertificate(hostname string, bundle caddyTLSCertificateBundle) error {
	hostname = normalizeRouteHost(hostname)
	if hostname == "" {
		return nil
	}
	issuerStorage := strings.Trim(strings.TrimSpace(bundle.IssuerStorage), "/")
	if issuerStorage == "" {
		issuerStorage = defaultCaddyIssuerStorage
	}
	hostDir := filepath.Join(s.caddyDataDir(), "certificates", issuerStorage, hostname)
	if err := os.MkdirAll(hostDir, 0o700); err != nil {
		return fmt.Errorf("create Caddy certificate directory for %s: %w", hostname, err)
	}
	if err := os.WriteFile(filepath.Join(hostDir, hostname+".crt"), []byte(strings.TrimSpace(bundle.CertificatePEM)+"\n"), 0o644); err != nil {
		return fmt.Errorf("write Caddy certificate for %s: %w", hostname, err)
	}
	if err := os.WriteFile(filepath.Join(hostDir, hostname+".key"), []byte(strings.TrimSpace(bundle.PrivateKeyPEM)+"\n"), 0o600); err != nil {
		return fmt.Errorf("write Caddy private key for %s: %w", hostname, err)
	}
	metadata := strings.TrimSpace(bundle.MetadataJSON)
	if metadata == "" {
		metadata = "{}"
	}
	if err := os.WriteFile(filepath.Join(hostDir, hostname+".json"), []byte(metadata+"\n"), 0o644); err != nil {
		return fmt.Errorf("write Caddy certificate metadata for %s: %w", hostname, err)
	}
	return nil
}

func (s *Service) readLocalCaddyTLSCertificate(hostname string) (caddyTLSCertificateBundle, error) {
	hostname = normalizeRouteHost(hostname)
	if hostname == "" {
		return caddyTLSCertificateBundle{}, fmt.Errorf("hostname is required")
	}
	pattern := filepath.Join(s.caddyDataDir(), "certificates", "*", hostname, hostname+".crt")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return caddyTLSCertificateBundle{}, fmt.Errorf("search Caddy certificate for %s: %w", hostname, err)
	}
	sort.Strings(matches)
	for _, certPath := range matches {
		hostDir := filepath.Dir(certPath)
		keyPath := filepath.Join(hostDir, hostname+".key")
		certPEM, certErr := os.ReadFile(certPath)
		if certErr != nil {
			continue
		}
		keyPEM, keyErr := os.ReadFile(keyPath)
		if keyErr != nil {
			continue
		}
		metadataPath := filepath.Join(hostDir, hostname+".json")
		metadata, _ := os.ReadFile(metadataPath)
		issuerStorage := filepath.Base(filepath.Dir(hostDir))
		return caddyTLSCertificateBundle{
			CertificatePEM: strings.TrimSpace(string(certPEM)),
			PrivateKeyPEM:  strings.TrimSpace(string(keyPEM)),
			MetadataJSON:   strings.TrimSpace(string(metadata)),
			IssuerStorage:  strings.Trim(strings.TrimSpace(issuerStorage), "/"),
		}, nil
	}
	return caddyTLSCertificateBundle{}, fmt.Errorf("Caddy certificate files for %s were not found under %s", hostname, filepath.Join(s.caddyDataDir(), "certificates"))
}

func (s *Service) caddySharedTLSEnabled() bool {
	return s.Config.CaddySharedTLSEnabled && strings.TrimSpace(s.caddyDataDir()) != ""
}

func (s *Service) caddyDataDir() string {
	if value := strings.TrimSpace(s.Config.CaddyDataDir); value != "" {
		return value
	}
	return defaultCaddyDataDir
}

func (s *Service) reportCustomDomainTLSStatus(ctx context.Context, hostname, tlsStatus, tlsLastMessage string, certBundle *caddyTLSCertificateBundle) error {
	hostname = normalizeRouteHost(hostname)
	tlsStatus = model.NormalizeAppDomainTLSStatus(tlsStatus)
	if hostname == "" || tlsStatus == "" {
		return nil
	}
	base, err := url.Parse(strings.TrimRight(strings.TrimSpace(s.Config.APIURL), "/"))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return fmt.Errorf("invalid FUGUE_API_URL")
	}
	token := strings.TrimSpace(s.Config.EdgeToken)
	if token == "" {
		return fmt.Errorf("FUGUE_EDGE_TOKEN is required to report custom-domain TLS status")
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/v1/edge/domains/tls-report"
	query := base.Query()
	query.Set("token", token)
	base.RawQuery = query.Encode()

	payloadMap := map[string]string{
		"hostname":         hostname,
		"tls_status":       tlsStatus,
		"tls_last_message": strings.TrimSpace(tlsLastMessage),
	}
	if certBundle != nil {
		payloadMap["certificate_pem"] = strings.TrimSpace(certBundle.CertificatePEM)
		payloadMap["private_key_pem"] = strings.TrimSpace(certBundle.PrivateKeyPEM)
		payloadMap["metadata_json"] = strings.TrimSpace(certBundle.MetadataJSON)
		payloadMap["issuer_storage"] = strings.Trim(strings.TrimSpace(certBundle.IssuerStorage), "/")
	}
	payload, err := json.Marshal(payloadMap)
	if err != nil {
		return fmt.Errorf("marshal custom-domain TLS report: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base.String(), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build custom-domain TLS report request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("send custom-domain TLS report for %s: %w", hostname, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return statusError{
			StatusCode: resp.StatusCode,
			Body:       s.redact(strings.TrimSpace(string(body))),
		}
	}
	return nil
}

func warmupCaddyTLS(ctx context.Context, dialAddress, host string) error {
	dialAddress = strings.TrimSpace(dialAddress)
	host = normalizeRouteHost(host)
	if dialAddress == "" || host == "" {
		return nil
	}
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: 5 * time.Second},
		Config: &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: host,
		},
	}
	conn, err := dialer.DialContext(ctx, "tcp", dialAddress)
	if err != nil {
		return fmt.Errorf("warm SNI %s via %s: %w", host, dialAddress, err)
	}
	_ = conn.Close()
	return nil
}

func caddyProxyDialAddress(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	if strings.Contains(addr, "://") {
		if parsed, err := url.Parse(addr); err == nil && parsed.Host != "" {
			addr = parsed.Host
		}
	}
	if strings.HasPrefix(addr, ":") {
		return "127.0.0.1" + addr
	}
	return addr
}

func isLoopbackHost(host string) bool {
	host = strings.TrimSpace(strings.ToLower(host))
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
