package edge

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	edgeCacheStatusHit         = "hit"
	edgeCacheStatusMiss        = "miss"
	edgeCacheStatusBypass      = "bypass"
	edgeCacheStatusStale       = "stale"
	edgeCacheStatusRevalidated = "revalidated"
	edgeCacheStatusError       = "error"
)

type edgeHTTPCacheDecision struct {
	Enabled    bool
	Policy     model.CachePolicy
	PolicyID   string
	Namespace  string
	Key        string
	KeyHash    string
	AssetClass string
	Status     string
	Reason     string
	TTL        time.Duration
	Cacheable  bool
}

type edgeHTTPCacheEntry struct {
	Version    int         `json:"version"`
	StoredAt   time.Time   `json:"stored_at"`
	ExpiresAt  time.Time   `json:"expires_at"`
	Namespace  string      `json:"namespace"`
	Key        string      `json:"key"`
	PolicyID   string      `json:"policy_id"`
	StatusCode int         `json:"status_code"`
	Header     http.Header `json:"header"`
	Body       []byte      `json:"body,omitempty"`
	BodySize   int64       `json:"body_size,omitempty"`
	AssetClass string      `json:"asset_class,omitempty"`
}

type edgeHTTPCacheCapture struct {
	http.ResponseWriter
	statusCode int
	header     http.Header
	body       []byte
	bodySize   int64
	maxBytes   int64
	overflow   bool
}

func newEdgeHTTPCacheCapture(w http.ResponseWriter, maxBytes int64) *edgeHTTPCacheCapture {
	return &edgeHTTPCacheCapture{ResponseWriter: w, maxBytes: maxBytes}
}

func (w *edgeHTTPCacheCapture) Header() http.Header {
	return w.ResponseWriter.Header()
}

func (w *edgeHTTPCacheCapture) WriteHeader(statusCode int) {
	if w.statusCode != 0 {
		return
	}
	w.statusCode = statusCode
	w.header = cloneHTTPHeader(w.ResponseWriter.Header())
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *edgeHTTPCacheCapture) Write(data []byte) (int, error) {
	if w.statusCode == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.maxBytes <= 0 || !w.overflow {
		remaining := w.maxBytes - int64(len(w.body))
		if w.maxBytes <= 0 || remaining > 0 {
			chunk := data
			if w.maxBytes > 0 && int64(len(chunk)) > remaining {
				chunk = chunk[:remaining]
				w.overflow = true
			}
			w.body = append(w.body, chunk...)
		} else {
			w.overflow = true
		}
	}
	w.bodySize += int64(len(data))
	return w.ResponseWriter.Write(data)
}

func (w *edgeHTTPCacheCapture) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *edgeHTTPCacheCapture) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response writer does not support hijacking")
	}
	return hijacker.Hijack()
}

func (s *Service) edgeCacheDecision(r *http.Request, route model.EdgeRouteBinding) edgeHTTPCacheDecision {
	decision := edgeHTTPCacheDecision{
		Status:     edgeCacheStatusBypass,
		Reason:     "no cache policy",
		AssetClass: edgeAssetClassForRequest(r),
	}
	if r == nil || !strings.EqualFold(r.Method, http.MethodGet) && !strings.EqualFold(r.Method, http.MethodHead) {
		decision.Reason = "method not cacheable"
		return decision
	}
	if edgeRequestIsWebSocket(r) || edgeRequestWantsSSE(r) || edgeRequestHasUpload(r) {
		decision.Reason = "streaming or upload request"
		return decision
	}
	if strings.TrimSpace(r.Header.Get("Authorization")) != "" {
		decision.Reason = "authorization present"
		return decision
	}
	policy, ok := s.edgeCachePolicyForRoute(route)
	if !ok {
		decision.Reason = "route has no cache policy"
		return decision
	}
	if strings.TrimSpace(s.Config.AssetCachePath) == "" {
		decision.Reason = "asset cache path not configured"
		return decision
	}
	if !edgeCachePathMatches(policy.PathPatterns, r.URL.Path) {
		decision.Reason = "path not covered by cache policy"
		return decision
	}
	if len(policy.MethodAllowlist) > 0 && !stringSliceContainsFold(policy.MethodAllowlist, r.Method) {
		decision.Reason = "method not allowed by cache policy"
		return decision
	}
	if strings.TrimSpace(r.Header.Get("Authorization")) != "" && policy.BypassOnAuthorization {
		decision.Reason = "authorization present"
		return decision
	}
	if policy.BypassOnCookie && strings.TrimSpace(r.Header.Get("Cookie")) != "" {
		decision.Reason = "cookie present"
		return decision
	}
	decision.Enabled = true
	decision.Policy = policy
	decision.PolicyID = strings.TrimSpace(policy.ID)
	decision.Namespace = edgeCacheNamespace(route)
	decision.Key = edgeCacheKey(r, decision.Namespace, decision.PolicyID)
	sum := sha256.Sum256([]byte(decision.Key))
	decision.KeyHash = hex.EncodeToString(sum[:])
	decision.Cacheable = true
	decision.Status = edgeCacheStatusMiss
	decision.Reason = "cacheable static asset"
	if ttl := time.Duration(policy.TTLSeconds) * time.Second; ttl > 0 {
		decision.TTL = ttl
	} else {
		decision.TTL = 24 * time.Hour
	}
	return decision
}

func (s *Service) edgeCachePolicyForRoute(route model.EdgeRouteBinding) (model.CachePolicy, bool) {
	policyID := strings.TrimSpace(route.CachePolicyID)
	if policyID == "" {
		return model.CachePolicy{}, false
	}
	bundle, ok := s.Bundle()
	if !ok {
		return model.CachePolicy{}, false
	}
	for _, policy := range bundle.CachePolicies {
		if strings.EqualFold(strings.TrimSpace(policy.ID), policyID) {
			return policy, true
		}
	}
	return model.CachePolicy{}, false
}

func edgeCacheNamespace(route model.EdgeRouteBinding) string {
	namespace := strings.TrimSpace(route.CacheNamespace)
	if namespace != "" {
		return namespace
	}
	namespace = strings.TrimSpace(route.AppID)
	if namespace == "" {
		return "global"
	}
	if gen := strings.TrimSpace(route.DeploymentGeneration); gen != "" {
		return namespace + "_" + gen
	}
	return namespace
}

func edgeCacheKey(r *http.Request, namespace, policyID string) string {
	parts := []string{
		strings.ToLower(strings.TrimSpace(namespace)),
		strings.ToLower(strings.TrimSpace(policyID)),
		strings.ToUpper(strings.TrimSpace(r.Method)),
		edgeCacheScheme(r),
		strings.ToLower(strings.TrimSpace(firstNonEmptyHeader(r, "X-Forwarded-Host", r.Host))),
		edgeCacheNormalizePath(r.URL.Path),
		edgeCacheNormalizeQuery(r.URL.Query()),
		edgeCacheEncodingBucket(strings.TrimSpace(r.Header.Get("Accept-Encoding"))),
	}
	return strings.Join(parts, "\x00")
}

func edgeCacheScheme(r *http.Request) string {
	if r == nil {
		return "http"
	}
	if scheme := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))); scheme != "" {
		return scheme
	}
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func edgeCacheNormalizePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

func edgeCacheNormalizeQuery(values url.Values) string {
	if len(values) == 0 {
		return ""
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		cloned := append([]string(nil), values[key]...)
		sort.Strings(cloned)
		for _, value := range cloned {
			parts = append(parts, url.QueryEscape(key)+"="+url.QueryEscape(value))
		}
	}
	return strings.Join(parts, "&")
}

func edgeCacheEncodingBucket(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	switch {
	case raw == "":
		return "identity"
	case strings.Contains(raw, "br"):
		return "br"
	case strings.Contains(raw, "gzip"):
		return "gzip"
	default:
		return "other"
	}
}

func edgeCachePathMatches(patterns []string, path string) bool {
	path = edgeCacheNormalizePath(path)
	if len(patterns) == 0 {
		return false
	}
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if strings.HasSuffix(pattern, "/*") {
			prefix := strings.TrimSuffix(pattern, "*")
			if strings.HasPrefix(path, prefix) {
				return true
			}
			continue
		}
		if strings.HasPrefix(pattern, "*.") {
			if strings.HasSuffix(strings.ToLower(path), strings.ToLower(strings.TrimPrefix(pattern, "*"))) {
				return true
			}
			continue
		}
		if strings.EqualFold(pattern, path) {
			return true
		}
		if ok, _ := pathMatch(pattern, path); ok {
			return true
		}
	}
	return false
}

func pathMatch(pattern, name string) (bool, error) {
	return filepath.Match(strings.TrimSpace(pattern), strings.TrimSpace(name))
}

func edgeAssetClassForRequest(r *http.Request) string {
	if r == nil || r.URL == nil {
		return "other"
	}
	path := edgeCacheNormalizePath(r.URL.Path)
	switch {
	case strings.HasPrefix(path, "/_next/static/"):
		return "next_static"
	case strings.HasPrefix(path, "/assets/"), strings.HasPrefix(path, "/static/"):
		return "static_asset"
	}
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".js", ".css", ".woff", ".woff2", ".ttf", ".otf", ".png", ".jpg", ".jpeg", ".webp", ".svg", ".ico":
		return "static_asset"
	default:
		return "other"
	}
}

func (s *Service) edgeCacheFilePath(decision edgeHTTPCacheDecision) string {
	if !decision.Enabled || strings.TrimSpace(s.Config.AssetCachePath) == "" || strings.TrimSpace(decision.KeyHash) == "" {
		return ""
	}
	return filepath.Join(strings.TrimSpace(s.Config.AssetCachePath), decision.Namespace, decision.KeyHash+".json")
}

func (s *Service) edgeCacheLoad(decision edgeHTTPCacheDecision) (edgeHTTPCacheEntry, bool) {
	path := s.edgeCacheFilePath(decision)
	if path == "" {
		return edgeHTTPCacheEntry{}, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return edgeHTTPCacheEntry{}, false
	}
	var entry edgeHTTPCacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return edgeHTTPCacheEntry{}, false
	}
	if entry.Version != 1 || entry.Key != decision.Key || strings.TrimSpace(entry.PolicyID) != strings.TrimSpace(decision.PolicyID) {
		return edgeHTTPCacheEntry{}, false
	}
	return entry, true
}

func (s *Service) edgeCacheStore(decision edgeHTTPCacheDecision, entry edgeHTTPCacheEntry) error {
	path := s.edgeCacheFilePath(decision)
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	entry.Version = 1
	entry.StoredAt = time.Now().UTC()
	if entry.ExpiresAt.IsZero() {
		entry.ExpiresAt = entry.StoredAt.Add(decision.TTL)
	}
	entry.Namespace = decision.Namespace
	entry.Key = decision.Key
	entry.PolicyID = decision.PolicyID
	entry.AssetClass = decision.AssetClass
	data, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s *Service) edgeCacheServeIfFresh(w http.ResponseWriter, decision edgeHTTPCacheDecision, entry edgeHTTPCacheEntry) (bool, int, string) {
	status := edgeCacheStatusHit
	now := time.Now().UTC()
	if !entry.ExpiresAt.IsZero() && now.After(entry.ExpiresAt) {
		staleWindow := time.Duration(decision.Policy.StaleWhileRevalidateSeconds) * time.Second
		if staleWindow <= 0 || now.After(entry.ExpiresAt.Add(staleWindow)) {
			return false, 0, ""
		}
		status = edgeCacheStatusStale
	}
	if s.edgeCacheServeWithStatus(w, decision, entry, status) {
		return true, entry.StatusCode, status
	}
	return false, 0, ""
}

func (s *Service) edgeCacheServeWithStatus(w http.ResponseWriter, decision edgeHTTPCacheDecision, entry edgeHTTPCacheEntry, cacheStatus string) bool {
	if entry.StatusCode == 0 {
		return false
	}
	headers := cloneHTTPHeader(entry.Header)
	if headers == nil {
		headers = make(http.Header)
	}
	headers.Set("X-Fugue-Cache", cacheStatus)
	if control := strings.TrimSpace(decision.Policy.EdgeCacheControl); control != "" {
		headers.Set("Cache-Control", control)
	}
	if decision.TTL > 0 {
		headers.Set("Age", fmt.Sprintf("%d", int(time.Since(entry.StoredAt).Seconds())))
	}
	copyHeader(w.Header(), headers)
	w.WriteHeader(entry.StatusCode)
	if len(entry.Body) > 0 {
		_, _ = w.Write(entry.Body)
	}
	return true
}

func (s *Service) edgeCacheShouldStore(decision edgeHTTPCacheDecision, capture *edgeProxyObservationResponseWriter) bool {
	if decision.PolicyID == "" || capture == nil || capture.overflow {
		return false
	}
	if capture.statusCode() != http.StatusOK {
		return false
	}
	if len(decision.Policy.StatusAllowlist) > 0 && !intSliceContains(decision.Policy.StatusAllowlist, capture.statusCode()) {
		return false
	}
	if capture.headerSnapshot != nil && len(capture.headerSnapshot.Values("Set-Cookie")) > 0 {
		return false
	}
	if capture.headerSnapshot != nil {
		cacheControl := strings.ToLower(strings.Join(capture.headerSnapshot.Values("Cache-Control"), ","))
		if strings.Contains(cacheControl, "no-store") || strings.Contains(cacheControl, "private") {
			return false
		}
	}
	return true
}

func copyHeader(dst, src http.Header) {
	for key := range dst {
		delete(dst, key)
	}
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func cloneHTTPHeader(in http.Header) http.Header {
	if len(in) == 0 {
		return make(http.Header)
	}
	out := make(http.Header, len(in))
	for key, values := range in {
		out[key] = append([]string(nil), values...)
	}
	return out
}

func intSliceContains(values []int, want int) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func stringSliceContainsFold(values []string, want string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(want)) {
			return true
		}
	}
	return false
}
