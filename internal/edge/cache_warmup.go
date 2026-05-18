package edge

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	pathpkg "path"
	"regexp"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"

	"golang.org/x/net/html"
)

const (
	edgeWarmupAcceptEncoding = "br, gzip"
	edgeWarmupBodyLimit      = 1 << 20
)

var edgeWarmupCSSURLPattern = regexp.MustCompile(`url\(\s*(?:"([^"]+)"|'([^']+)'|([^)"']+))\s*\)`)
var edgeWarmupCSSImportPattern = regexp.MustCompile(`@import\s+(?:url\(\s*)?(?:"([^"]+)"|'([^']+)'|([^)"'\s;]+))`)

type edgeWarmupURL struct {
	URL   *url.URL
	Depth int
}

type edgeWarmupReport struct {
	Hosts             int
	DiscoveryTargets  int
	DiscoveryRequests int
	PrimeTargets      int
	PrimeRequests     int
	DiscoveredAssets  int
}

func (s *Service) maybeWarmupCurrentEdgeCache(ctx context.Context, bundle model.EdgeRouteBundle, configSignature string) error {
	if !s.edgeCacheWarmupEnabled() {
		return nil
	}
	if !s.Config.CaddyEnabled {
		return nil
	}
	hosts := s.edgeCacheWarmupHosts(bundle)
	if len(hosts) == 0 {
		return nil
	}
	scheme := s.edgeWarmupScheme()
	if scheme == "" {
		return nil
	}
	dialAddress := caddyProxyDialAddress(s.Config.CaddyListenAddr)
	if dialAddress == "" {
		return nil
	}
	warmupSignature := strings.Join([]string{
		strings.TrimSpace(configSignature),
		"cache",
		scheme,
		strings.Join(hosts, ","),
	}, ":")
	if !s.needsEdgeCacheWarmup(warmupSignature) {
		return nil
	}
	started := time.Now()
	report, err := s.runEdgeCacheWarmup(ctx, dialAddress, scheme, hosts)
	duration := time.Since(started)
	s.recordEdgeCacheWarmup(warmupSignature, strings.Join(hosts, ","), report, duration, err)
	if err != nil {
		return err
	}
	if s.Logger != nil {
		s.Logger.Printf("edge cache warmup complete; hosts=%d discovery_targets=%d prime_targets=%d duration=%s", report.Hosts, report.DiscoveryTargets, report.PrimeTargets, duration)
	}
	return nil
}

func (s *Service) edgeCacheWarmupEnabled() bool {
	return s != nil && s.Config.CacheWarmupEnabled
}

func (s *Service) edgeWarmupScheme() string {
	if s == nil || !s.Config.CaddyEnabled {
		return ""
	}
	if s.normalizedCaddyTLSMode() == caddyTLSModeOff {
		return "http"
	}
	return "https"
}

func (s *Service) edgeCacheWarmupHosts(bundle model.EdgeRouteBundle) []string {
	seen := map[string]struct{}{}
	for _, route := range bundle.Routes {
		if strings.TrimSpace(route.CachePolicyID) == "" {
			continue
		}
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

func (s *Service) edgeWarmupTimeout() time.Duration {
	if s == nil || s.Config.CacheWarmupTimeout <= 0 {
		return 15 * time.Second
	}
	return s.Config.CacheWarmupTimeout
}

func (s *Service) edgeWarmupMaxTargets() int {
	if s == nil || s.Config.CacheWarmupMaxTargets <= 0 {
		return 24
	}
	return s.Config.CacheWarmupMaxTargets
}

func (s *Service) edgeWarmupMaxDepth() int {
	if s == nil || s.Config.CacheWarmupMaxDepth <= 0 {
		return 2
	}
	return s.Config.CacheWarmupMaxDepth
}

func (s *Service) edgeWarmupPrimeReadLimit() int64 {
	maxBytes := int64(32 * 1024 * 1024)
	if s != nil && s.Config.AssetCacheMaxBytes > 0 {
		maxBytes = int64(s.Config.AssetCacheMaxBytes)
	}
	return maxBytes + 1
}

func (s *Service) runEdgeCacheWarmup(ctx context.Context, dialAddress, scheme string, hosts []string) (edgeWarmupReport, error) {
	report := edgeWarmupReport{Hosts: len(hosts)}
	var firstErr error
	for _, host := range hosts {
		hostReport, err := s.warmupEdgeCacheHost(ctx, dialAddress, scheme, host)
		report.DiscoveryTargets += hostReport.DiscoveryTargets
		report.DiscoveryRequests += hostReport.DiscoveryRequests
		report.PrimeTargets += hostReport.PrimeTargets
		report.PrimeRequests += hostReport.PrimeRequests
		report.DiscoveredAssets += hostReport.DiscoveredAssets
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return report, firstErr
}

func (s *Service) warmupEdgeCacheHost(ctx context.Context, dialAddress, scheme, host string) (edgeWarmupReport, error) {
	report := edgeWarmupReport{Hosts: 1}
	host = normalizeRouteHost(host)
	if host == "" {
		return report, nil
	}
	clientFactory := s.cacheWarmupClientFactory
	if clientFactory == nil {
		clientFactory = newEdgeCacheWarmupClient
	}
	client := clientFactory(dialAddress, scheme)
	if client == nil {
		return report, fmt.Errorf("edge cache warmup client is unavailable")
	}
	hostCtx, cancel := context.WithTimeout(ctx, s.edgeWarmupTimeout())
	defer cancel()

	baseURL := &url.URL{Scheme: scheme, Host: host}
	maxTargets := s.edgeWarmupMaxTargets()
	maxDepth := s.edgeWarmupMaxDepth()
	discoveredURLs := make(map[string]struct{})
	primeURLs := make(map[string]struct{})
	discoveryQueue := []edgeWarmupURL{{URL: mustResolveWarmupURL(baseURL, "/"), Depth: 0}}
	report.DiscoveryTargets = 1
	discoveryVisited := make(map[string]struct{})
	var firstErr error

	for len(discoveryQueue) > 0 {
		current := discoveryQueue[0]
		discoveryQueue = discoveryQueue[1:]
		if current.URL == nil {
			continue
		}
		currentKey := current.URL.String()
		if _, seen := discoveryVisited[currentKey]; seen {
			continue
		}
		discoveryVisited[currentKey] = struct{}{}
		if maxTargets > 0 && len(discoveredURLs) >= maxTargets {
			break
		}

		report.DiscoveryRequests++
		resp, body, err := s.edgeWarmupFetch(hostCtx, client, current.URL, true)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if resp == nil || resp.StatusCode != http.StatusOK {
			continue
		}

		if current.URL.Path == "/" {
			if _, seen := primeURLs[currentKey]; !seen {
				primeURLs[currentKey] = struct{}{}
				report.PrimeTargets++
			}
		}

		contentType := edgeWarmupContentType(resp)
		if !edgeWarmupResponseIsDiscoverable(contentType, current.URL.Path) {
			continue
		}

		children := edgeWarmupDiscoverURLs(baseURL, current.URL, body, contentType)
		for _, child := range children {
			if child.URL == nil {
				continue
			}
			childKey := child.URL.String()
			if _, seen := discoveredURLs[childKey]; !seen {
				discoveredURLs[childKey] = struct{}{}
				report.DiscoveredAssets++
			}
			if _, seen := primeURLs[childKey]; !seen {
				primeURLs[childKey] = struct{}{}
				report.PrimeTargets++
			}
			if child.Depth > 0 {
				nextDepth := current.Depth + child.Depth
				if nextDepth <= maxDepth {
					child.Depth = nextDepth
					discoveryQueue = append(discoveryQueue, child)
					report.DiscoveryTargets++
				}
			}
		}
	}

	primeList := make([]string, 0, len(primeURLs))
	for raw := range primeURLs {
		primeList = append(primeList, raw)
	}
	sort.Strings(primeList)

	for _, raw := range primeList {
		if maxTargets > 0 && report.PrimeRequests >= maxTargets {
			break
		}
		targetURL, err := url.Parse(raw)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("parse warmup url %q: %w", raw, err)
			}
			continue
		}
		report.PrimeRequests++
		if _, _, err := s.edgeWarmupFetch(hostCtx, client, targetURL, false); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return report, firstErr
}

func (s *Service) edgeWarmupFetch(ctx context.Context, client *http.Client, targetURL *url.URL, discovery bool) (*http.Response, []byte, error) {
	if client == nil || targetURL == nil {
		return nil, nil, fmt.Errorf("edge cache warmup client is unavailable")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	if discovery {
		req.Header.Set(edgeCacheWarmupDiscoveryHeader, "1")
		req.Header.Set("Accept-Encoding", "identity")
	} else {
		req.Header.Set("Accept-Encoding", edgeWarmupAcceptEncoding)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if !discovery {
		_, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, s.edgeWarmupPrimeReadLimit()))
		return resp, nil, readErr
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, edgeWarmupBodyLimit))
	return resp, body, readErr
}

func edgeWarmupContentType(resp *http.Response) string {
	if resp == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Type")))
}

func edgeWarmupResponseIsDiscoverable(contentType, path string) bool {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	path = edgeCacheNormalizePath(path)
	if strings.HasPrefix(contentType, "text/html") || strings.HasPrefix(contentType, "application/xhtml+xml") {
		return true
	}
	if strings.HasPrefix(contentType, "text/css") || strings.HasSuffix(strings.ToLower(path), ".css") {
		return true
	}
	return false
}

func edgeWarmupDiscoverURLs(baseURL, currentURL *url.URL, body []byte, contentType string) []edgeWarmupURL {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	switch {
	case strings.HasPrefix(contentType, "text/html"), strings.HasPrefix(contentType, "application/xhtml+xml"):
		return edgeWarmupDiscoverFromHTML(baseURL, body)
	case strings.HasPrefix(contentType, "text/css"), strings.HasSuffix(strings.ToLower(currentURL.Path), ".css"):
		return edgeWarmupDiscoverFromCSS(baseURL, currentURL, body)
	default:
		return nil
	}
}

func edgeWarmupDiscoverFromHTML(baseURL *url.URL, body []byte) []edgeWarmupURL {
	root, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return nil
	}
	seen := make(map[string]struct{})
	out := make([]edgeWarmupURL, 0, 16)
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n == nil {
			return
		}
		if n.Type == html.ElementNode {
			tag := strings.ToLower(n.Data)
			switch tag {
			case "script":
				if raw := edgeWarmupHTMLAttr(n, "src"); raw != "" {
					if target, ok := edgeWarmupResolveAssetURL(baseURL, raw); ok {
						edgeWarmupAppendURL(&out, seen, target, 0)
					}
				}
			case "link":
				href := edgeWarmupHTMLAttr(n, "href")
				if href == "" {
					break
				}
				rel := strings.Fields(strings.ToLower(edgeWarmupHTMLAttr(n, "rel")))
				asValue := strings.ToLower(edgeWarmupHTMLAttr(n, "as"))
				if edgeWarmupLinkIsDiscoverable(rel, asValue) {
					if target, ok := edgeWarmupResolveAssetURL(baseURL, href); ok {
						depth := 0
						if strings.HasSuffix(strings.ToLower(target.Path), ".css") || strings.Contains(strings.Join(rel, " "), "stylesheet") {
							depth = 1
						}
						edgeWarmupAppendURL(&out, seen, target, depth)
					}
				}
			case "img":
				if raw := edgeWarmupHTMLAttr(n, "src"); raw != "" {
					if target, ok := edgeWarmupResolveAssetURL(baseURL, raw); ok {
						edgeWarmupAppendURL(&out, seen, target, 0)
					}
				}
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return out
}

func edgeWarmupDiscoverFromCSS(baseURL, currentURL *url.URL, body []byte) []edgeWarmupURL {
	text := string(body)
	seen := make(map[string]struct{})
	out := make([]edgeWarmupURL, 0, 16)
	for _, match := range edgeWarmupCSSURLPattern.FindAllStringSubmatch(text, -1) {
		for _, raw := range match[1:] {
			if raw == "" {
				continue
			}
			if target, ok := edgeWarmupResolveAssetURL(currentURL, raw); ok {
				depth := 0
				if strings.HasSuffix(strings.ToLower(target.Path), ".css") {
					depth = 1
				}
				edgeWarmupAppendURL(&out, seen, target, depth)
			}
		}
	}
	for _, match := range edgeWarmupCSSImportPattern.FindAllStringSubmatch(text, -1) {
		for _, raw := range match[1:] {
			if raw == "" {
				continue
			}
			if target, ok := edgeWarmupResolveAssetURL(currentURL, raw); ok {
				depth := 1
				if strings.HasSuffix(strings.ToLower(target.Path), ".css") {
					depth = 1
				}
				edgeWarmupAppendURL(&out, seen, target, depth)
			}
		}
	}
	return out
}

func edgeWarmupLinkIsDiscoverable(rel []string, asValue string) bool {
	if len(rel) == 0 {
		return false
	}
	for _, item := range rel {
		switch strings.TrimSpace(strings.ToLower(item)) {
		case "stylesheet", "modulepreload", "preload", "icon", "shortcut", "apple-touch-icon":
			return true
		}
	}
	if asValue == "style" || asValue == "script" || asValue == "font" || asValue == "image" {
		return true
	}
	return false
}

func edgeWarmupHTMLAttr(n *html.Node, name string) string {
	for _, attr := range n.Attr {
		if strings.EqualFold(strings.TrimSpace(attr.Key), name) {
			return strings.TrimSpace(attr.Val)
		}
	}
	return ""
}

func edgeWarmupResolveAssetURL(baseURL *url.URL, raw string) (*url.URL, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	if strings.HasPrefix(strings.ToLower(raw), "data:") || strings.HasPrefix(strings.ToLower(raw), "javascript:") || strings.HasPrefix(strings.ToLower(raw), "mailto:") {
		return nil, false
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, false
	}
	resolved := baseURL.ResolveReference(parsed)
	if resolved == nil {
		return nil, false
	}
	if !strings.EqualFold(strings.TrimSpace(resolved.Host), strings.TrimSpace(baseURL.Host)) {
		return nil, false
	}
	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		return nil, false
	}
	if !edgeWarmupPathAllowed(resolved.Path) {
		return nil, false
	}
	resolved.Fragment = ""
	return resolved, true
}

func edgeWarmupPathAllowed(path string) bool {
	path = edgeCacheNormalizePath(path)
	lower := strings.ToLower(path)
	switch {
	case path == "/", lower == "/index.html", strings.HasSuffix(lower, ".html"):
		return true
	case strings.HasPrefix(lower, "/_next/static/"), strings.HasPrefix(lower, "/assets/"), strings.HasPrefix(lower, "/static/"):
		return true
	}
	switch ext := strings.ToLower(pathpkg.Ext(path)); ext {
	case ".js", ".css", ".woff", ".woff2", ".ttf", ".otf", ".png", ".jpg", ".jpeg", ".webp", ".svg", ".ico":
		return true
	}
	return false
}

func mustResolveWarmupURL(baseURL *url.URL, raw string) *url.URL {
	target, ok := edgeWarmupResolveAssetURL(baseURL, raw)
	if ok {
		return target
	}
	fallback := *baseURL
	fallback.Path = "/"
	fallback.RawQuery = ""
	fallback.Fragment = ""
	return &fallback
}

func edgeWarmupAppendURL(out *[]edgeWarmupURL, seen map[string]struct{}, target *url.URL, depth int) {
	if target == nil || out == nil {
		return
	}
	key := target.String()
	if _, ok := seen[key]; ok {
		return
	}
	seen[key] = struct{}{}
	*out = append(*out, edgeWarmupURL{URL: target, Depth: depth})
}

func (s *Service) needsEdgeCacheWarmup(signature string) bool {
	signature = strings.TrimSpace(signature)
	if signature == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(s.metrics.CacheWarmupSignature) != signature {
		return true
	}
	if strings.TrimSpace(s.metrics.CacheWarmupLastError) != "" {
		return true
	}
	return s.metrics.CacheWarmupAt == nil
}

func (s *Service) recordEdgeCacheWarmup(signature, targets string, report edgeWarmupReport, duration time.Duration, err error) {
	if duration < 0 {
		duration = 0
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics.CacheWarmupSignature = strings.TrimSpace(signature)
	s.metrics.CacheWarmupTargets = strings.TrimSpace(targets)
	s.metrics.CacheWarmupDuration = duration
	s.metrics.CacheWarmupAt = &now
	if err != nil {
		s.metrics.CacheWarmupError++
		s.metrics.CacheWarmupLastError = s.redact(err.Error())
		return
	}
	s.metrics.CacheWarmupSuccess++
	s.metrics.CacheWarmupLastError = ""
}

func newEdgeCacheWarmupClient(dialAddress, scheme string) *http.Client {
	dialAddress = strings.TrimSpace(dialAddress)
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	if dialAddress == "" || (scheme != "http" && scheme != "https") {
		return nil
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = false
	transport.ForceAttemptHTTP2 = scheme == "https"
	transport.ResponseHeaderTimeout = 5 * time.Second
	transport.TLSHandshakeTimeout = 5 * time.Second
	transport.IdleConnTimeout = 30 * time.Second
	transport.MaxIdleConns = 16
	transport.MaxIdleConnsPerHost = 16
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := &net.Dialer{Timeout: 5 * time.Second}
		return dialer.DialContext(ctx, network, dialAddress)
	}
	if scheme == "https" {
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
		}
	}
	return &http.Client{
		Transport: transport,
		Timeout:   0,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
