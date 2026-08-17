package api

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

const trafficOverrideProbeTimeout = 5 * time.Second

type trafficOverrideRouteProbeFunc func(context.Context, string, string) error

// probeTrafficOverrideHostRoute verifies the candidate IP using the business
// hostname for both TLS SNI/certificate validation and the HTTP Host header.
// It deliberately does not consult TrafficEpoch or release state.
func probeTrafficOverrideHostRoute(ctx context.Context, hostname, answer string) error {
	hostname = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(hostname)), ".")
	ip := net.ParseIP(strings.TrimSpace(answer))
	if hostname == "" || ip == nil {
		return fmt.Errorf("invalid route probe target")
	}
	probeCtx, cancel := context.WithTimeout(ctx, trafficOverrideProbeTimeout)
	defer cancel()
	dialer := &net.Dialer{Timeout: trafficOverrideProbeTimeout}
	transport := &http.Transport{
		// The candidate IP is the object under test; an HTTP proxy would make
		// the probe validate the proxy instead of the edge endpoint.
		Proxy: nil,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			_, port, err := net.SplitHostPort(address)
			if err != nil {
				port = "443"
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		},
		TLSClientConfig:   &tls.Config{MinVersion: tls.VersionTLS12, ServerName: hostname},
		ForceAttemptHTTP2: true,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   trafficOverrideProbeTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	defer transport.CloseIdleConnections()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, "https://"+hostname+"/", nil)
	if err != nil {
		return fmt.Errorf("build route probe request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("TLS/Host probe failed: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return fmt.Errorf("read route probe response: %w", err)
	}
	if strings.Contains(strings.ToLower(string(body)), "edge route not found") {
		return fmt.Errorf("Host route is not loaded")
	}
	if resp.StatusCode >= http.StatusInternalServerError {
		return fmt.Errorf("route probe returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func (s *Server) validateTrafficOverrideRoutes(ctx context.Context, answers, routes []string) error {
	probe := s.trafficOverrideRouteProbe
	if probe == nil {
		probe = probeTrafficOverrideHostRoute
	}
	for _, answer := range answers {
		for _, route := range routes {
			if err := probe(ctx, route, answer); err != nil {
				return fmt.Errorf("candidate %s failed required Host route %s: %w", answer, route, err)
			}
		}
	}
	return nil
}
