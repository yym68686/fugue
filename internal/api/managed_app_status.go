package api

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"golang.org/x/sync/singleflight"
)

const (
	defaultManagedAppStatusCacheTTL       = 15 * time.Second
	defaultManagedAppStatusRefreshTimeout = 5 * time.Second
	managedAppStatusListRefreshKey        = "list"
)

var errManagedAppStatusClientUnavailable = errors.New("managed app status client unavailable")

type managedAppStatusClient struct {
	client      *http.Client
	baseURL     string
	bearerToken string
}

type managedAppStatusCache struct {
	ttl            time.Duration
	refreshTimeout time.Duration
	mu             sync.RWMutex
	byApp          map[string]managedAppStatusCacheEntry
	list           managedAppStatusListCacheEntry
	group          singleflight.Group
}

type managedAppStatusCacheEntry struct {
	managed     runtime.ManagedAppObject
	found       bool
	ok          bool
	refreshedAt time.Time
	expiresAt   time.Time
}

type managedAppStatusListCacheEntry struct {
	items       map[string]runtime.ManagedAppObject
	ok          bool
	refreshedAt time.Time
	expiresAt   time.Time
}

type managedAppList struct {
	Items []map[string]any `json:"items"`
}

func newManagedAppStatusCache(ttl, refreshTimeout time.Duration) managedAppStatusCache {
	if ttl <= 0 {
		ttl = defaultManagedAppStatusCacheTTL
	}
	if refreshTimeout <= 0 {
		refreshTimeout = defaultManagedAppStatusRefreshTimeout
	}
	return managedAppStatusCache{
		ttl:            ttl,
		refreshTimeout: refreshTimeout,
		byApp:          make(map[string]managedAppStatusCacheEntry),
	}
}

func (c *managedAppStatusCache) cacheTTL() time.Duration {
	if c == nil || c.ttl <= 0 {
		return defaultManagedAppStatusCacheTTL
	}
	return c.ttl
}

func (c *managedAppStatusCache) refreshTimeoutDuration() time.Duration {
	if c == nil || c.refreshTimeout <= 0 {
		return defaultManagedAppStatusRefreshTimeout
	}
	return c.refreshTimeout
}

func (c *managedAppStatusCache) getApp(key string) (managedAppStatusCacheEntry, bool, bool) {
	if c == nil {
		return managedAppStatusCacheEntry{}, false, false
	}
	c.mu.RLock()
	entry, ok := c.byApp[key]
	c.mu.RUnlock()
	if !ok || !entry.ok {
		return managedAppStatusCacheEntry{}, false, false
	}
	return entry, true, time.Now().After(entry.expiresAt)
}

func (c *managedAppStatusCache) setApp(key string, entry managedAppStatusCacheEntry) {
	if c == nil || strings.TrimSpace(key) == "" {
		return
	}
	c.mu.Lock()
	if c.byApp == nil {
		c.byApp = make(map[string]managedAppStatusCacheEntry)
	}
	c.byApp[key] = entry
	c.mu.Unlock()
}

func (c *managedAppStatusCache) getList() (managedAppStatusListCacheEntry, bool, bool) {
	if c == nil {
		return managedAppStatusListCacheEntry{}, false, false
	}
	c.mu.RLock()
	entry := c.list
	c.mu.RUnlock()
	if !entry.ok {
		return managedAppStatusListCacheEntry{}, false, false
	}
	return entry, true, time.Now().After(entry.expiresAt)
}

func (c *managedAppStatusCache) setList(entry managedAppStatusListCacheEntry) {
	if c == nil {
		return
	}
	if entry.items == nil {
		entry.items = map[string]runtime.ManagedAppObject{}
	}

	c.mu.Lock()
	if c.byApp == nil {
		c.byApp = make(map[string]managedAppStatusCacheEntry)
	}

	missing := map[string]struct{}{}
	if c.list.ok {
		for appID := range c.list.items {
			missing[appID] = struct{}{}
		}
	}

	c.list = entry
	for appID, managed := range entry.items {
		c.byApp[appID] = managedAppStatusCacheEntry{
			managed:     managed,
			found:       true,
			ok:          true,
			refreshedAt: entry.refreshedAt,
			expiresAt:   entry.expiresAt,
		}
		delete(missing, appID)
	}
	for appID := range missing {
		c.byApp[appID] = managedAppStatusCacheEntry{
			found:       false,
			ok:          true,
			refreshedAt: entry.refreshedAt,
			expiresAt:   entry.expiresAt,
		}
	}
	c.mu.Unlock()
}

func newManagedAppStatusClient() (*managedAppStatusClient, error) {
	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT"))
	if host == "" || port == "" {
		return nil, fmt.Errorf("kubernetes service host/port is not available in the environment")
	}

	token, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	caData, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("read service account CA: %w", err)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caData) {
		return nil, fmt.Errorf("load service account CA")
	}

	return &managedAppStatusClient{
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: rootCAs},
			},
			Timeout: 10 * time.Second,
		},
		baseURL:     "https://" + host + ":" + port,
		bearerToken: strings.TrimSpace(string(token)),
	}, nil
}

func (c *managedAppStatusClient) getManagedApp(ctx context.Context, app model.App) (runtime.ManagedAppObject, bool, error) {
	var raw map[string]any
	namespace := runtime.NamespaceForTenant(app.TenantID)
	name := runtime.ManagedAppResourceName(app)
	if err := c.doJSON(ctx, "/apis/"+runtime.ManagedAppAPIGroup+"/v1alpha1/namespaces/"+url.PathEscape(namespace)+"/"+runtime.ManagedAppPlural+"/"+url.PathEscape(name), &raw); err != nil {
		if isKubeNotFound(err) {
			return runtime.ManagedAppObject{}, false, nil
		}
		return runtime.ManagedAppObject{}, false, err
	}

	managed, err := runtime.ManagedAppObjectFromMap(raw)
	if err != nil {
		return runtime.ManagedAppObject{}, false, err
	}
	return managed, true, nil
}

func (c *managedAppStatusClient) listManagedAppsByAppID(ctx context.Context) (map[string]runtime.ManagedAppObject, error) {
	var list managedAppList
	if err := c.doJSON(ctx, "/apis/"+runtime.ManagedAppAPIGroup+"/v1alpha1/"+runtime.ManagedAppPlural, &list); err != nil {
		return nil, err
	}

	items := make(map[string]runtime.ManagedAppObject, len(list.Items))
	for _, raw := range list.Items {
		managed, err := runtime.ManagedAppObjectFromMap(raw)
		if err != nil {
			return nil, err
		}
		appID := strings.TrimSpace(managed.Spec.AppID)
		if appID == "" {
			continue
		}
		items[appID] = managed
	}
	return items, nil
}

func (c *managedAppStatusClient) doJSON(ctx context.Context, apiPath string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+apiPath, nil)
	if err != nil {
		return fmt.Errorf("create kubernetes request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("kubernetes request GET %s: %w", apiPath, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return &kubeStatusError{
			StatusCode: resp.StatusCode,
			Message:    fmt.Sprintf("kubernetes request GET %s failed: status=%d body=%s", apiPath, resp.StatusCode, strings.TrimSpace(string(body))),
		}
	}
	if out != nil && len(body) > 0 {
		if err := json.Unmarshal(body, out); err != nil {
			return fmt.Errorf("decode kubernetes response: %w", err)
		}
	}
	return nil
}

func (s *Server) overlayManagedAppStatuses(ctx context.Context, apps []model.App) []model.App {
	if len(apps) == 0 {
		return apps
	}

	cached, ok, expired := s.managedAppStatusCache.getList()
	if ok && !expired {
		return overlayAppsWithManagedStatuses(apps, cached.items)
	}

	fresh, err := s.refreshManagedAppStatuses(ctx)
	if err != nil {
		if s.shouldLogManagedAppStatusError(err) && s.log != nil {
			s.log.Printf("managed app status overlay list error: %v", err)
		}
		if ok {
			return overlayAppsWithManagedStatuses(apps, cached.items)
		}
		return apps
	}
	return overlayAppsWithManagedStatuses(apps, fresh.items)
}

func (s *Server) overlayManagedAppStatus(ctx context.Context, app model.App) model.App {
	cached, ok, expired := s.managedAppStatusCache.getApp(managedAppStatusCacheKey(app))
	if ok && !expired {
		return applyManagedAppStatusCacheEntry(app, cached)
	}

	fresh, err := s.refreshManagedAppStatus(ctx, app)
	if err != nil {
		if s.shouldLogManagedAppStatusError(err) && s.log != nil {
			s.log.Printf("managed app status overlay get error for app %s: %v", app.ID, err)
		}
		if ok {
			return applyManagedAppStatusCacheEntry(app, cached)
		}
		return app
	}
	return applyManagedAppStatusCacheEntry(app, fresh)
}

func (s *Server) overlayManagedAppStatusCached(app model.App) model.App {
	cached, ok, expired := s.managedAppStatusCache.getApp(managedAppStatusCacheKey(app))
	if ok {
		if expired {
			s.refreshManagedAppStatusAsync(app)
		}
		return applyManagedAppStatusCacheEntry(app, cached)
	}
	s.refreshManagedAppStatusAsync(app)
	return app
}

func overlayAppsWithManagedStatuses(apps []model.App, managedByAppID map[string]runtime.ManagedAppObject) []model.App {
	out := make([]model.App, 0, len(apps))
	for _, app := range apps {
		managed, found := managedByAppID[strings.TrimSpace(app.ID)]
		if found {
			app = runtime.OverlayAppStatusFromManagedApp(app, managed)
		}
		out = append(out, app)
	}
	return out
}

func applyManagedAppStatusCacheEntry(app model.App, entry managedAppStatusCacheEntry) model.App {
	if !entry.ok || !entry.found {
		return app
	}
	return runtime.OverlayAppStatusFromManagedApp(app, entry.managed)
}

func managedAppStatusCacheKey(app model.App) string {
	if id := strings.TrimSpace(app.ID); id != "" {
		return id
	}
	tenantID := strings.TrimSpace(app.TenantID)
	name := strings.TrimSpace(app.Name)
	if tenantID == "" && name == "" {
		return ""
	}
	return tenantID + "/" + name
}

func (s *Server) managedAppStatusClient() (*managedAppStatusClient, error) {
	clientFactory := s.newManagedAppStatusClient
	if clientFactory == nil {
		clientFactory = newManagedAppStatusClient
	}
	client, err := clientFactory()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errManagedAppStatusClientUnavailable, err)
	}
	return client, nil
}

func (s *Server) managedAppStatusRefreshContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := s.managedAppStatusCache.refreshTimeoutDuration()
	if parent == nil {
		return context.WithTimeout(context.Background(), timeout)
	}
	return context.WithTimeout(parent, timeout)
}

func (s *Server) shouldLogManagedAppStatusError(err error) bool {
	return err != nil && !errors.Is(err, errManagedAppStatusClientUnavailable)
}

func (s *Server) fetchManagedAppStatus(ctx context.Context, app model.App) (managedAppStatusCacheEntry, error) {
	client, err := s.managedAppStatusClient()
	if err != nil {
		return managedAppStatusCacheEntry{}, err
	}

	refreshCtx, cancel := s.managedAppStatusRefreshContext(ctx)
	defer cancel()

	managed, found, err := client.getManagedApp(refreshCtx, app)
	if err != nil {
		return managedAppStatusCacheEntry{}, err
	}

	now := time.Now()
	entry := managedAppStatusCacheEntry{
		managed:     managed,
		found:       found,
		ok:          true,
		refreshedAt: now,
		expiresAt:   now.Add(s.managedAppStatusCache.cacheTTL()),
	}
	s.managedAppStatusCache.setApp(managedAppStatusCacheKey(app), entry)
	return entry, nil
}

func (s *Server) refreshManagedAppStatus(ctx context.Context, app model.App) (managedAppStatusCacheEntry, error) {
	key := managedAppStatusCacheKey(app)
	if key == "" {
		return managedAppStatusCacheEntry{}, fmt.Errorf("managed app cache key is empty")
	}

	value, err, _ := s.managedAppStatusCache.group.Do("app:"+key, func() (any, error) {
		return s.fetchManagedAppStatus(ctx, app)
	})
	if err != nil {
		return managedAppStatusCacheEntry{}, err
	}

	entry, _ := value.(managedAppStatusCacheEntry)
	return entry, nil
}

func (s *Server) refreshManagedAppStatusAsync(app model.App) {
	key := managedAppStatusCacheKey(app)
	if key == "" {
		return
	}
	s.managedAppStatusCache.group.DoChan("app:"+key, func() (any, error) {
		entry, err := s.fetchManagedAppStatus(context.Background(), app)
		if err != nil && s.shouldLogManagedAppStatusError(err) && s.log != nil {
			s.log.Printf("managed app status background refresh error for app %s: %v", app.ID, err)
		}
		return entry, err
	})
}

func (s *Server) fetchManagedAppStatuses(ctx context.Context) (managedAppStatusListCacheEntry, error) {
	client, err := s.managedAppStatusClient()
	if err != nil {
		return managedAppStatusListCacheEntry{}, err
	}

	refreshCtx, cancel := s.managedAppStatusRefreshContext(ctx)
	defer cancel()

	items, err := client.listManagedAppsByAppID(refreshCtx)
	if err != nil {
		return managedAppStatusListCacheEntry{}, err
	}

	now := time.Now()
	entry := managedAppStatusListCacheEntry{
		items:       items,
		ok:          true,
		refreshedAt: now,
		expiresAt:   now.Add(s.managedAppStatusCache.cacheTTL()),
	}
	s.managedAppStatusCache.setList(entry)
	return entry, nil
}

func (s *Server) refreshManagedAppStatuses(ctx context.Context) (managedAppStatusListCacheEntry, error) {
	value, err, _ := s.managedAppStatusCache.group.Do(managedAppStatusListRefreshKey, func() (any, error) {
		return s.fetchManagedAppStatuses(ctx)
	})
	if err != nil {
		return managedAppStatusListCacheEntry{}, err
	}

	entry, _ := value.(managedAppStatusListCacheEntry)
	return entry, nil
}
