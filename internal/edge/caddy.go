package edge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	caddyConfigReapplyInterval = 5 * time.Minute

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
	if !s.needsCaddyApply(version) {
		return nil
	}
	configBody, routeCount, err := s.buildCaddyConfig(bundle)
	if err != nil {
		s.recordCaddyApply(version, 0, err)
		return err
	}
	endpoint, err := s.caddyAdminEndpoint("/load")
	if err != nil {
		s.recordCaddyApply(version, 0, err)
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(configBody))
	if err != nil {
		err = fmt.Errorf("build caddy config request: %w", err)
		s.recordCaddyApply(version, 0, err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		err = fmt.Errorf("apply caddy config: %w", err)
		s.recordCaddyApply(version, 0, err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err = fmt.Errorf("apply caddy config returned status %d", resp.StatusCode)
		s.recordCaddyApply(version, 0, err)
		return err
	}
	s.recordCaddyApply(version, routeCount, nil)
	if s.Logger != nil {
		s.Logger.Printf("edge caddy config applied; version=%s hosts=%d listen=%s tls_mode=%s proxy=%s", version, routeCount, strings.TrimSpace(s.Config.CaddyListenAddr), s.normalizedCaddyTLSMode(), caddyProxyDialAddress(s.Config.CaddyProxyListenAddr))
	}
	return nil
}

func (s *Service) needsCaddyApply(version string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(s.metrics.CaddyAppliedVersion) != strings.TrimSpace(version) {
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

	hosts := uniqueBundleHosts(bundle)
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

	server := map[string]any{
		"listen": []string{listenAddr},
		"routes": routes,
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

	config := map[string]any{
		"admin": map[string]any{
			"listen": adminURL.Host,
		},
		"apps": apps,
	}
	data, err := json.Marshal(config)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal caddy config: %w", err)
	}
	return data, len(hosts), nil
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

func uniqueBundleHosts(bundle model.EdgeRouteBundle) []string {
	seen := map[string]struct{}{}
	for _, route := range bundle.Routes {
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
