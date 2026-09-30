package httpx

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// PublicRouteObservation is availability at one declared HTTP entry point.
// It is not a release identity proof or authority to change configuration.
type PublicRouteObservation struct {
	StatusCode    int
	Reachable     bool
	Reason        string
	RequestID     string
	BundleVersion string
}

// ObservePublicRoute sends no credentials and follows no redirects. HEAD avoids
// executing a GET handler and the bounded transport never reads a response body.
func ObservePublicRoute(ctx context.Context, rawURL string, client *http.Client) PublicRouteObservation {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return PublicRouteObservation{Reason: "invalid public route URL"}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if client == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = nil
		transport.DialContext = dialPublicRoute
		defer transport.CloseIdleConnections()
		client = &http.Client{Transport: transport}
	}
	bounded := *client
	bounded.Jar = nil
	bounded.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
	if err != nil {
		return PublicRouteObservation{Reason: "invalid public route request"}
	}
	req.Header.Set("User-Agent", "fugue-public-availability/1.0")
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := bounded.Do(req)
	if err != nil {
		return PublicRouteObservation{Reason: "public DNS, TLS or connection probe failed"}
	}
	defer resp.Body.Close()
	observation := PublicRouteObservation{StatusCode: resp.StatusCode, RequestID: boundedProbeHeader(resp.Header.Get("X-Fugue-Edge-Request-Id")), BundleVersion: boundedProbeHeader(resp.Header.Get("X-Fugue-Route-Bundle-Version"))}
	// An authenticated application can legitimately reject HEAD. A 404 cannot
	// distinguish a missing Edge route from an application-level missing path.
	observation.Reachable = resp.StatusCode >= 200 && resp.StatusCode < 500 && resp.StatusCode != http.StatusNotFound
	if !observation.Reachable {
		observation.Reason = fmt.Sprintf("public route returned HTTP %d", resp.StatusCode)
	}
	return observation
}

func boundedProbeHeader(value string) string {
	if len(value) > 160 || strings.ContainsAny(value, "\r\n") {
		return ""
	}
	return value
}

// Resolve once and dial the validated address to prevent DNS rebinding into
// an internal service from an application-controlled public hostname.
func dialPublicRoute(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("public hostname has no addresses")
	}
	for _, ip := range ips {
		if !ip.IP.IsGlobalUnicast() || ip.IP.IsPrivate() || ip.IP.IsLoopback() || ip.IP.IsLinkLocalUnicast() {
			return nil, fmt.Errorf("public route resolved to a non-public address")
		}
	}
	dialer := net.Dialer{Timeout: 5 * time.Second}
	for _, ip := range ips {
		conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
		if dialErr == nil {
			return conn, nil
		}
		err = dialErr
	}
	return nil, err
}
