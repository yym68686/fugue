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
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

const (
	defaultManagedAppStatusCacheTTL       = 15 * time.Second
	defaultManagedAppStatusRefreshTimeout = 5 * time.Second
	defaultManagedAppStatusRefreshBackoff = 15 * time.Second
	managedAppStatusListRefreshKey        = "list"
	managedAppInventoryRefreshKey         = "inventory"
)

var (
	errManagedAppStatusClientUnavailable = errors.New("managed app status client unavailable")
	errManagedAppStatusRefreshBackoff    = errors.New("managed app status refresh backoff active")
)

type managedAppStatusClient struct {
	client      *http.Client
	baseURL     string
	bearerToken string
}

type managedAppStatusCache struct {
	ttl                  time.Duration
	refreshTimeout       time.Duration
	refreshBackoff       time.Duration
	mu                   sync.RWMutex
	byApp                map[string]managedAppStatusCacheEntry
	list                 managedAppStatusListCacheEntry
	listRefreshNotBefore time.Time
	group                singleflight.Group
}

type managedAppStatusCacheEntry struct {
	managed     runtime.ManagedAppObject
	found       bool
	ok          bool
	clusterID   string
	evidence    managedAppRuntimeEvidence
	refreshedAt time.Time
	expiresAt   time.Time
}

type managedAppStatusListCacheEntry struct {
	items       map[string]runtime.ManagedAppObject
	evidence    map[string]managedAppRuntimeEvidence
	ok          bool
	clusterID   string
	refreshedAt time.Time
	expiresAt   time.Time
}

// managedAppRuntimeEvidence contains only authoritative results from the same
// Kubernetes snapshot as the ManagedApp observation. Nil fields mean that a
// resource is not applicable (for example a background app has no Service),
// while a non-nil false is an authoritative absence.
type managedAppRuntimeEvidence struct {
	namespacePresent        *bool
	servicePresent          *bool
	endpointPresent         *bool
	endpointReady           *bool
	physicalReplicas        *int
	physicalDesiredReplicas *int
	imagePresent            *bool
	imageRef                string
	invariantViolations     []string
	evidenceSources         []string
}

type managedAppKubeSnapshot struct {
	namespaces  map[string]struct{}
	deployments map[string]kubeDeploymentRuntimeEvidence
	services    map[string]struct{}
	endpoints   map[string]kubeEndpointRuntimeEvidence
}

type kubeResourceList struct {
	Items    []map[string]any `json:"items"`
	Metadata struct {
		Continue string `json:"continue"`
	} `json:"metadata"`
}

type kubeDeploymentRuntimeEvidence struct {
	Metadata struct {
		Name       string `json:"name"`
		Namespace  string `json:"namespace"`
		Generation int64  `json:"generation"`
	} `json:"metadata"`
	Spec struct {
		Replicas *int `json:"replicas"`
		Template struct {
			Spec struct {
				Containers []struct {
					Name  string `json:"name"`
					Image string `json:"image"`
				} `json:"containers"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
	Status struct {
		Replicas           int   `json:"replicas"`
		ReadyReplicas      int   `json:"readyReplicas"`
		AvailableReplicas  int   `json:"availableReplicas"`
		ObservedGeneration int64 `json:"observedGeneration"`
	} `json:"status"`
}

type kubeEndpointRuntimeEvidence struct {
	Present           bool
	ReadyAddresses    int
	NotReadyAddresses int
}

type managedAppList struct {
	Items    []map[string]any `json:"items"`
	Metadata struct {
		Continue string `json:"continue"`
	} `json:"metadata"`
}

type kubeNamespaceIdentity struct {
	Metadata struct {
		UID string `json:"uid"`
	} `json:"metadata"`
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
		refreshBackoff: defaultManagedAppStatusRefreshBackoff,
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

func (c *managedAppStatusCache) refreshBackoffDuration() time.Duration {
	if c == nil || c.refreshBackoff <= 0 {
		return defaultManagedAppStatusRefreshBackoff
	}
	return c.refreshBackoff
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

func (c *managedAppStatusCache) getObservedApp(key string) (managedAppStatusCacheEntry, bool, bool) {
	entry, ok, expired := c.getApp(key)
	if ok && strings.TrimSpace(entry.clusterID) == "" {
		expired = true
	}
	return entry, ok, expired
}

func (c *managedAppStatusCache) listRefreshAllowed(now time.Time) bool {
	if c == nil {
		return true
	}
	if now.IsZero() {
		now = time.Now()
	}
	c.mu.RLock()
	notBefore := c.listRefreshNotBefore
	c.mu.RUnlock()
	return notBefore.IsZero() || !now.Before(notBefore)
}

func (c *managedAppStatusCache) recordListRefreshResult(err error) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if err != nil {
		c.listRefreshNotBefore = time.Now().Add(c.refreshBackoffDuration())
	} else {
		c.listRefreshNotBefore = time.Time{}
	}
	c.mu.Unlock()
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

func (c *managedAppStatusCache) getObservedList() (managedAppStatusListCacheEntry, bool, bool) {
	entry, ok, expired := c.getList()
	if ok && strings.TrimSpace(entry.clusterID) == "" {
		expired = true
	}
	return entry, ok, expired
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
	for appID := range c.byApp {
		missing[appID] = struct{}{}
	}

	c.list = entry
	for appID, managed := range entry.items {
		c.byApp[appID] = managedAppStatusCacheEntry{
			managed:     managed,
			found:       true,
			ok:          true,
			clusterID:   entry.clusterID,
			evidence:    entry.evidence[appID],
			refreshedAt: entry.refreshedAt,
			expiresAt:   entry.expiresAt,
		}
		delete(missing, appID)
	}
	for appID := range missing {
		c.byApp[appID] = managedAppStatusCacheEntry{
			found:       false,
			ok:          true,
			clusterID:   entry.clusterID,
			evidence:    entry.evidence[appID],
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
				TLSClientConfig:     &tls.Config{RootCAs: rootCAs},
				MaxIdleConns:        clusterNodeHTTPMaxIdleConns,
				MaxIdleConnsPerHost: clusterNodeHTTPMaxIdleConnsPerHost,
				IdleConnTimeout:     clusterNodeHTTPIdleConnTimeout,
				TLSHandshakeTimeout: clusterNodeHTTPTLSHandshakeTimeout,
			},
			Timeout: 10 * time.Second,
		},
		baseURL:     "https://" + host + ":" + port,
		bearerToken: strings.TrimSpace(string(token)),
	}, nil
}

func (c *managedAppStatusClient) closeIdleConnections() {
	if c == nil || c.client == nil {
		return
	}
	c.client.CloseIdleConnections()
}

func (c *managedAppStatusClient) getClusterID(ctx context.Context) (string, error) {
	var namespace kubeNamespaceIdentity
	if err := c.doJSON(ctx, "/api/v1/namespaces/kube-system", &namespace); err != nil {
		return "", err
	}
	clusterID := strings.TrimSpace(namespace.Metadata.UID)
	if clusterID == "" {
		return "", fmt.Errorf("kubernetes kube-system namespace has no uid")
	}
	return clusterID, nil
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
	basePath := "/apis/" + runtime.ManagedAppAPIGroup + "/v1alpha1/" + runtime.ManagedAppPlural
	items := make(map[string]runtime.ManagedAppObject)
	seenContinue := make(map[string]struct{})
	path := basePath
	for {
		var list managedAppList
		if err := c.doJSON(ctx, path, &list); err != nil {
			return nil, err
		}
		for _, raw := range list.Items {
			managed, err := runtime.ManagedAppObjectFromMap(raw)
			if err != nil {
				return nil, err
			}
			appID := strings.TrimSpace(managed.Spec.AppID)
			if appID == "" {
				continue
			}
			if _, exists := items[appID]; exists {
				return nil, fmt.Errorf("managed app inventory contains duplicate appID %q", appID)
			}
			items[appID] = managed
		}

		continuation := strings.TrimSpace(list.Metadata.Continue)
		if continuation == "" {
			return items, nil
		}
		if _, exists := seenContinue[continuation]; exists {
			return nil, fmt.Errorf("managed app inventory pagination repeated continue token")
		}
		seenContinue[continuation] = struct{}{}
		values := url.Values{}
		values.Set("continue", continuation)
		path = basePath + "?" + values.Encode()
	}
}

func (c *managedAppStatusClient) listKubeResources(ctx context.Context, basePath string) ([]map[string]any, error) {
	basePath = strings.TrimSpace(basePath)
	if basePath == "" {
		return nil, fmt.Errorf("kubernetes resource list path is empty")
	}
	items := make([]map[string]any, 0)
	seenContinue := make(map[string]struct{})
	path := basePath
	for {
		var list kubeResourceList
		if err := c.doJSON(ctx, path, &list); err != nil {
			return nil, err
		}
		items = append(items, list.Items...)
		continuation := strings.TrimSpace(list.Metadata.Continue)
		if continuation == "" {
			return items, nil
		}
		if _, exists := seenContinue[continuation]; exists {
			return nil, fmt.Errorf("kubernetes resource inventory pagination repeated continue token for %s", basePath)
		}
		seenContinue[continuation] = struct{}{}
		values := url.Values{}
		values.Set("continue", continuation)
		path = basePath + "?" + values.Encode()
	}
}

func (c *managedAppStatusClient) readRuntimeSnapshot(ctx context.Context) (managedAppKubeSnapshot, error) {
	var namespaceItems, deploymentItems, serviceItems, endpointItems []map[string]any
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		var err error
		namespaceItems, err = c.listKubeResources(groupCtx, "/api/v1/namespaces")
		if err != nil {
			return fmt.Errorf("list kubernetes namespaces: %w", err)
		}
		return nil
	})
	group.Go(func() error {
		var err error
		deploymentItems, err = c.listKubeResources(groupCtx, "/apis/apps/v1/deployments")
		if err != nil {
			return fmt.Errorf("list kubernetes deployments: %w", err)
		}
		return nil
	})
	group.Go(func() error {
		var err error
		serviceItems, err = c.listKubeResources(groupCtx, "/api/v1/services")
		if err != nil {
			return fmt.Errorf("list kubernetes services: %w", err)
		}
		return nil
	})
	group.Go(func() error {
		var err error
		endpointItems, err = c.listKubeResources(groupCtx, "/api/v1/endpoints")
		if err != nil {
			return fmt.Errorf("list kubernetes endpoints: %w", err)
		}
		return nil
	})
	if err := group.Wait(); err != nil {
		return managedAppKubeSnapshot{}, err
	}

	snapshot := managedAppKubeSnapshot{
		namespaces:  make(map[string]struct{}, len(namespaceItems)),
		deployments: make(map[string]kubeDeploymentRuntimeEvidence, len(deploymentItems)),
		services:    make(map[string]struct{}, len(serviceItems)),
		endpoints:   make(map[string]kubeEndpointRuntimeEvidence, len(endpointItems)),
	}
	for _, raw := range namespaceItems {
		if name := kubeObjectNamespaceOrName(raw, ""); name != "" {
			snapshot.namespaces[name] = struct{}{}
		}
	}
	for _, raw := range deploymentItems {
		var deployment kubeDeploymentRuntimeEvidence
		if err := decodeKubeObject(raw, &deployment); err != nil {
			return managedAppKubeSnapshot{}, fmt.Errorf("decode kubernetes deployment: %w", err)
		}
		key := kubeNamespacedKey(deployment.Metadata.Namespace, deployment.Metadata.Name)
		if key == "/" {
			continue
		}
		snapshot.deployments[key] = deployment
	}
	for _, raw := range serviceItems {
		key := kubeObjectNamespacedKey(raw)
		if key != "/" {
			snapshot.services[key] = struct{}{}
		}
	}
	for _, raw := range endpointItems {
		metadata := kubeObjectMetadata(raw)
		key := kubeNamespacedKey(metadata.namespace, metadata.name)
		if key == "/" {
			continue
		}
		evidence := kubeEndpointRuntimeEvidence{Present: true}
		if subsets, ok := raw["subsets"].([]any); ok {
			for _, rawSubset := range subsets {
				subset, ok := rawSubset.(map[string]any)
				if !ok {
					continue
				}
				evidence.ReadyAddresses += kubeAddressCount(subset["addresses"])
				evidence.NotReadyAddresses += kubeAddressCount(subset["notReadyAddresses"])
			}
		}
		snapshot.endpoints[key] = evidence
	}
	return snapshot, nil
}

func decodeKubeObject(raw map[string]any, out any) error {
	data, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

type kubeObjectMetadataView struct {
	name      string
	namespace string
}

func kubeObjectMetadata(raw map[string]any) kubeObjectMetadataView {
	metadata, _ := raw["metadata"].(map[string]any)
	name, _ := metadata["name"].(string)
	namespace, _ := metadata["namespace"].(string)
	return kubeObjectMetadataView{name: strings.TrimSpace(name), namespace: strings.TrimSpace(namespace)}
}

func kubeObjectNamespaceOrName(raw map[string]any, fallback string) string {
	metadata := kubeObjectMetadata(raw)
	if metadata.name != "" {
		return metadata.name
	}
	return strings.TrimSpace(fallback)
}

func kubeObjectNamespacedKey(raw map[string]any) string {
	metadata := kubeObjectMetadata(raw)
	return kubeNamespacedKey(metadata.namespace, metadata.name)
}

func kubeNamespacedKey(namespace, name string) string {
	return strings.TrimSpace(namespace) + "/" + strings.TrimSpace(name)
}

func kubeAddressCount(value any) int {
	items, ok := value.([]any)
	if !ok {
		return 0
	}
	return len(items)
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
	runtimeByID := s.observationRuntimeByID(apps)

	cached, ok, expired := s.managedAppStatusCache.getObservedList()
	if ok && !expired {
		return s.applyManagedAppListObservation(apps, cached, runtimeByID, true, "")
	}

	fresh, err := s.refreshManagedAppStatuses(ctx)
	if err != nil {
		if s.shouldLogManagedAppStatusError(err) && s.log != nil {
			s.log.Printf("managed app status overlay list error: %v", err)
		}
		if ok {
			return s.applyManagedAppListObservation(apps, cached, runtimeByID, false, err.Error())
		}
		return s.applyUnknownManagedAppObservation(apps, runtimeByID, err.Error())
	}
	return s.applyManagedAppListObservation(apps, fresh, runtimeByID, true, "")
}

func (s *Server) overlayManagedAppStatus(ctx context.Context, app model.App) model.App {
	runtimeByID := s.observationRuntimeByID([]model.App{app})
	cached, ok, expired := s.managedAppStatusCache.getObservedApp(managedAppStatusCacheKey(app))
	if ok && !expired {
		return s.applyManagedAppObservation(app, cached, runtimeByID, true, "")
	}

	fresh, err := s.refreshManagedAppStatus(ctx, app)
	if err != nil {
		if s.shouldLogManagedAppStatusError(err) && s.log != nil {
			s.log.Printf("managed app status overlay get error for app %s: %v", app.ID, err)
		}
		if ok {
			return s.applyManagedAppObservation(app, cached, runtimeByID, false, err.Error())
		}
		return s.applyUnknownManagedAppObservation([]model.App{app}, runtimeByID, err.Error())[0]
	}
	return s.applyManagedAppObservation(app, fresh, runtimeByID, true, "")
}

func (s *Server) overlayManagedAppStatusCached(app model.App) model.App {
	cached, ok, expired := s.managedAppStatusCache.getObservedApp(managedAppStatusCacheKey(app))
	if ok {
		if expired {
			s.refreshManagedAppStatusesAsync()
		}
		return s.applyManagedAppObservation(app, cached, s.observationRuntimeByID([]model.App{app}), !expired, "")
	}

	list, listOK, listExpired := s.managedAppStatusCache.getObservedList()
	if listOK {
		if listExpired {
			s.refreshManagedAppStatusesAsync()
		}
		entry := managedAppStatusCacheEntry{
			managed:     list.items[strings.TrimSpace(app.ID)],
			found:       false,
			ok:          true,
			clusterID:   list.clusterID,
			evidence:    list.evidence[strings.TrimSpace(app.ID)],
			refreshedAt: list.refreshedAt,
			expiresAt:   list.expiresAt,
		}
		if managed, found := list.items[strings.TrimSpace(app.ID)]; found {
			entry.managed = managed
			entry.found = true
			entry.evidence = list.evidence[strings.TrimSpace(app.ID)]
		}
		return s.applyManagedAppObservation(app, entry, s.observationRuntimeByID([]model.App{app}), !listExpired, "")
	}

	s.refreshManagedAppStatusesAsync()
	return s.applyUnknownManagedAppObservation([]model.App{app}, s.observationRuntimeByID([]model.App{app}), "live runtime observation is pending")[0]
}

func (s *Server) overlayManagedAppStatusesCached(apps []model.App) []model.App {
	if len(apps) == 0 {
		return apps
	}
	cached, ok, expired := s.managedAppStatusCache.getObservedList()
	if ok {
		if expired {
			s.refreshManagedAppStatusesAsync()
		}
		return s.applyManagedAppListObservation(apps, cached, s.observationRuntimeByID(apps), !expired, "")
	}
	s.refreshManagedAppStatusesAsync()
	return s.applyUnknownManagedAppObservation(apps, s.observationRuntimeByID(apps), "live runtime observation is pending")
}

func (s *Server) overlayManagedAppStatusesForEdgeRoutesCached(apps []model.App, runtimeByID map[string]model.Runtime) []model.App {
	if len(apps) == 0 {
		return apps
	}
	cached, ok, expired := s.managedAppStatusCache.getObservedList()
	if ok {
		if expired {
			s.refreshManagedAppStatusesAsync()
		}
		return s.applyManagedAppListObservation(apps, cached, runtimeByID, !expired, "")
	}
	s.refreshManagedAppStatusesAsync()
	return s.applyUnknownManagedAppObservation(apps, runtimeByID, "live runtime observation is pending")
}

func (s *Server) applyManagedAppObservation(app model.App, entry managedAppStatusCacheEntry, runtimeByID map[string]model.Runtime, fresh bool, errorMessage string) model.App {
	if !entry.found && !appMayUseManagedRuntime(app, runtimeByID) {
		return app
	}
	complete := entry.ok
	if !complete && strings.TrimSpace(errorMessage) == "" {
		errorMessage = "live runtime observation is unavailable"
	}
	observed := runtime.CalculateAppObservedStatus(app, runtime.AppRuntimeObservation{
		ManagedApp:              entry.managed,
		Found:                   entry.found,
		Complete:                complete,
		Fresh:                   fresh && complete,
		ObservedAt:              entry.refreshedAt,
		ClusterID:               entry.clusterID,
		EvidenceSource:          runtime.AppObservationSourceKubernetesAPI,
		EvidenceSources:         entry.evidence.evidenceSources,
		NamespacePresent:        entry.evidence.namespacePresent,
		ServicePresent:          entry.evidence.servicePresent,
		EndpointPresent:         entry.evidence.endpointPresent,
		EndpointReady:           entry.evidence.endpointReady,
		PhysicalReplicas:        entry.evidence.physicalReplicas,
		PhysicalDesiredReplicas: entry.evidence.physicalDesiredReplicas,
		ImagePresent:            entry.evidence.imagePresent,
		ImageRef:                entry.evidence.imageRef,
		InvariantViolations:     entry.evidence.invariantViolations,
		ErrorMessage:            errorMessage,
	})
	return runtime.ApplyAppObservedStatus(app, observed)
}

func (s *Server) applyManagedAppListObservation(apps []model.App, entry managedAppStatusListCacheEntry, runtimeByID map[string]model.Runtime, fresh bool, errorMessage string) []model.App {
	out := make([]model.App, 0, len(apps))
	for _, app := range apps {
		managed, found := entry.items[strings.TrimSpace(app.ID)]
		evidence := entry.evidence[strings.TrimSpace(app.ID)]
		out = append(out, s.applyManagedAppObservation(app, managedAppStatusCacheEntry{
			managed:     managed,
			found:       found,
			ok:          entry.ok,
			clusterID:   entry.clusterID,
			evidence:    evidence,
			refreshedAt: entry.refreshedAt,
			expiresAt:   entry.expiresAt,
		}, runtimeByID, fresh, errorMessage))
	}
	return out
}

func (s *Server) applyUnknownManagedAppObservation(apps []model.App, runtimeByID map[string]model.Runtime, errorMessage string) []model.App {
	entry := managedAppStatusCacheEntry{ok: false, refreshedAt: time.Now().UTC()}
	out := make([]model.App, 0, len(apps))
	for _, app := range apps {
		out = append(out, s.applyManagedAppObservation(app, entry, runtimeByID, false, errorMessage))
	}
	return out
}

func (s *Server) observationRuntimeByID(apps []model.App) map[string]model.Runtime {
	runtimes := make(map[string]model.Runtime)
	if s != nil && s.store != nil {
		if values, err := s.store.ListRuntimes("", true); err == nil {
			for _, value := range values {
				runtimes[strings.TrimSpace(value.ID)] = value
			}
		}
	}
	for _, app := range apps {
		if strings.TrimSpace(app.Spec.RuntimeID) == model.DefaultManagedRuntimeID {
			runtimes[model.DefaultManagedRuntimeID] = model.Runtime{ID: model.DefaultManagedRuntimeID, Type: model.RuntimeTypeManagedShared}
		}
	}
	return runtimes
}

func appUsesManagedRuntime(app model.App, runtimeByID map[string]model.Runtime) bool {
	runtimeObj, ok := runtimeByID[strings.TrimSpace(appProxyRuntimeID(app))]
	if !ok {
		return false
	}
	switch strings.TrimSpace(runtimeObj.Type) {
	case "", model.RuntimeTypeManagedShared, model.RuntimeTypeManagedOwned:
		return true
	default:
		return false
	}
}

func appMayUseManagedRuntime(app model.App, runtimeByID map[string]model.Runtime) bool {
	if strings.TrimSpace(app.Spec.RuntimeID) == model.DefaultManagedRuntimeID {
		return true
	}
	if appUsesManagedRuntime(app, runtimeByID) {
		return true
	}
	// A nil/empty runtime inventory is an observation failure, not evidence
	// that an app uses an external runtime. Treat a non-empty runtime id as
	// eligible so the caller reports unknown instead of reusing stale state.
	return len(runtimeByID) == 0 && strings.TrimSpace(appProxyRuntimeID(app)) != ""
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
	return err != nil && !errors.Is(err, errManagedAppStatusClientUnavailable) && !errors.Is(err, errManagedAppStatusRefreshBackoff)
}

func (s *Server) buildManagedAppRuntimeEvidence(
	app model.App,
	managed runtime.ManagedAppObject,
	found bool,
	snapshot managedAppKubeSnapshot,
) (managedAppRuntimeEvidence, error) {
	evidence := managedAppRuntimeEvidence{
		evidenceSources: []string{runtime.AppObservationSourceKubernetesAPI},
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	_, namespaceExists := snapshot.namespaces[strings.TrimSpace(namespace)]
	namespacePresent := namespaceExists
	evidence.namespacePresent = &namespacePresent

	// Use the durable spec as the identity source. A found ManagedApp can
	// carry a stale/foreign spec during a migration; its metadata identity is
	// still authoritative for generation, while child names must remain tied
	// to the app being observed.
	serviceRequired := model.AppHasClusterService(app.Spec) || model.AppSSHEnabled(app.Spec)
	deploymentRequired := app.Spec.Replicas > 0 || strings.TrimSpace(app.Spec.Image) != ""
	serviceName := runtime.RuntimeAppServiceName(app)
	deploymentName := runtime.RuntimeAppResourceName(app)
	serviceKey := kubeNamespacedKey(namespace, serviceName)
	deploymentKey := kubeNamespacedKey(namespace, deploymentName)

	if serviceRequired {
		_, serviceExists := snapshot.services[serviceKey]
		servicePresent := serviceExists
		evidence.servicePresent = &servicePresent
		endpoint, endpointExists := snapshot.endpoints[serviceKey]
		endpointPresent := endpointExists && endpoint.Present
		endpointReady := endpointPresent && endpoint.ReadyAddresses > 0
		evidence.endpointPresent = &endpointPresent
		evidence.endpointReady = &endpointReady
	}
	if deploymentRequired {
		deployment, deploymentExists := snapshot.deployments[deploymentKey]
		physicalDesired := 0
		physicalReady := 0
		if deploymentExists {
			if deployment.Spec.Replicas != nil {
				physicalDesired = *deployment.Spec.Replicas
			}
			physicalReady = maxInt(deployment.Status.ReadyReplicas, deployment.Status.AvailableReplicas)
		}
		evidence.physicalDesiredReplicas = &physicalDesired
		evidence.physicalReplicas = &physicalReady
		if deploymentExists && (deployment.Metadata.Generation <= 0 || deployment.Status.ObservedGeneration < deployment.Metadata.Generation) {
			evidence.invariantViolations = append(evidence.invariantViolations, "deployment_generation_unobserved")
		}
	}

	imageRef := strings.TrimSpace(app.Spec.Image)
	if imageRef == "" && found {
		imageRef = strings.TrimSpace(managed.Spec.AppSpec.Image)
	}
	if imageRef != "" {
		evidence.imageRef = imageRef
		managedImage := strings.Contains(imageRef, "/fugue-apps/")
		if managedImage && s != nil && s.store != nil {
			present, err := s.currentManagedImagePresence(app, imageRef)
			if err != nil {
				return managedAppRuntimeEvidence{}, err
			}
			evidence.imagePresent = present
			evidence.evidenceSources = append(evidence.evidenceSources, "image_location_store")
		}
		if deployment, deploymentExists := snapshot.deployments[deploymentKey]; deploymentExists &&
			deployment.Metadata.Generation > 0 &&
			deployment.Status.ObservedGeneration >= deployment.Metadata.Generation &&
			maxInt(deployment.Status.ReadyReplicas, deployment.Status.AvailableReplicas) > 0 {
			deployedImage := firstDeploymentContainerImage(deployment)
			if deployedImage != "" {
				matches := deployedImage == imageRef
				evidence.imagePresent = &matches
				if !matches {
					evidence.invariantViolations = append(evidence.invariantViolations, "current_image_mismatch")
				}
			}
		}
	}

	// Record contradictions as explicit invariant evidence. The calculator
	// decides the observed phase; this list is never inferred from stored
	// replicas alone.
	if found && app.Spec.Replicas > 0 {
		if strings.EqualFold(strings.TrimSpace(app.Status.Phase), "deployed") && managed.Status.ReadyReplicas == 0 {
			evidence.invariantViolations = append(evidence.invariantViolations, "stored_deployed_but_observed_ready_zero")
		}
	}
	return evidence, nil
}

func firstDeploymentContainerImage(deployment kubeDeploymentRuntimeEvidence) string {
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if image := strings.TrimSpace(container.Image); image != "" {
			return image
		}
	}
	return ""
}

func (s *Server) currentManagedImagePresence(app model.App, imageRef string) (*bool, error) {
	refs := []string{strings.TrimSpace(imageRef)}
	if app.Source != nil {
		refs = append(refs, strings.TrimSpace(app.Source.ResolvedImageRef), strings.TrimSpace(app.Source.ImageRef))
	}
	seenRefs := make(map[string]struct{}, len(refs))
	cutoff := time.Now().UTC().Add(-defaultImageCacheInventoryTTL)
	hasFreshNegative := false
	hasFreshPending := false
	for _, ref := range refs {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		if _, exists := seenRefs[ref]; exists {
			continue
		}
		seenRefs[ref] = struct{}{}
		for _, status := range []string{
			model.ImageLocationStatusPresent,
			model.ImageLocationStatusPulling,
			model.ImageLocationStatusMissing,
			model.ImageLocationStatusFailed,
		} {
			locations, err := s.store.ListImageLocations(model.ImageLocationFilter{
				TenantID: strings.TrimSpace(app.TenantID),
				AppID:    strings.TrimSpace(app.ID),
				ImageRef: ref,
				Status:   status,
			})
			if err != nil {
				return nil, fmt.Errorf("list current image locations: %w", err)
			}
			for _, location := range locations {
				observedAt := location.UpdatedAt.UTC()
				if location.LastSeenAt != nil && location.LastSeenAt.After(observedAt) {
					observedAt = location.LastSeenAt.UTC()
				}
				if observedAt.IsZero() || observedAt.Before(cutoff) {
					continue
				}
				switch status {
				case model.ImageLocationStatusPresent:
					present := true
					return &present, nil
				case model.ImageLocationStatusPulling:
					hasFreshPending = true
				case model.ImageLocationStatusMissing, model.ImageLocationStatusFailed:
					hasFreshNegative = true
				}
			}
		}
	}
	if hasFreshPending {
		return nil, nil
	}
	if hasFreshNegative {
		present := false
		return &present, nil
	}
	// No current physical report is unknown evidence, not proof that an image
	// is missing.  This distinction keeps historical inventory gaps from
	// turning healthy apps red while still failing closed on a fresh explicit
	// missing/failed report.
	return nil, nil
}

func (s *Server) fetchManagedAppStatus(ctx context.Context, app model.App) (managedAppStatusCacheEntry, error) {
	client, err := s.managedAppStatusClient()
	if err != nil {
		return managedAppStatusCacheEntry{}, err
	}
	defer client.closeIdleConnections()

	refreshCtx, cancel := s.managedAppStatusRefreshContext(ctx)
	defer cancel()

	clusterID, err := client.getClusterID(refreshCtx)
	if err != nil {
		return managedAppStatusCacheEntry{}, err
	}
	managed, found, err := client.getManagedApp(refreshCtx, app)
	if err != nil {
		return managedAppStatusCacheEntry{}, err
	}
	confirmedClusterID, err := client.getClusterID(refreshCtx)
	if err != nil {
		return managedAppStatusCacheEntry{}, err
	}
	if confirmedClusterID != clusterID {
		return managedAppStatusCacheEntry{}, fmt.Errorf("kubernetes cluster identity changed during managed app observation")
	}
	var evidence managedAppRuntimeEvidence
	if s != nil && s.store != nil {
		snapshot, snapshotErr := client.readRuntimeSnapshot(refreshCtx)
		if snapshotErr != nil {
			return managedAppStatusCacheEntry{}, snapshotErr
		}
		finalClusterID, finalErr := client.getClusterID(refreshCtx)
		if finalErr != nil {
			return managedAppStatusCacheEntry{}, finalErr
		}
		if finalClusterID != clusterID {
			return managedAppStatusCacheEntry{}, fmt.Errorf("kubernetes cluster identity changed during runtime evidence observation")
		}
		evidence, err = s.buildManagedAppRuntimeEvidence(app, managed, found, snapshot)
		if err != nil {
			return managedAppStatusCacheEntry{}, err
		}
	}

	now := time.Now().UTC()
	entry := managedAppStatusCacheEntry{
		managed:     managed,
		found:       found,
		ok:          true,
		clusterID:   clusterID,
		evidence:    evidence,
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

func (s *Server) fetchManagedAppStatuses(ctx context.Context) (managedAppStatusListCacheEntry, error) {
	return s.fetchManagedAppInventoryWithClusterIdentity(ctx, true)
}

func (s *Server) fetchManagedAppInventory(ctx context.Context) (managedAppStatusListCacheEntry, error) {
	return s.fetchManagedAppInventoryWithClusterIdentity(ctx, false)
}

func (s *Server) fetchManagedAppInventoryWithClusterIdentity(ctx context.Context, requireClusterIdentity bool) (managedAppStatusListCacheEntry, error) {
	client, err := s.managedAppStatusClient()
	if err != nil {
		return managedAppStatusListCacheEntry{}, err
	}
	defer client.closeIdleConnections()

	refreshCtx, cancel := s.managedAppStatusRefreshContext(ctx)
	defer cancel()

	clusterID := ""
	if requireClusterIdentity {
		clusterID, err = client.getClusterID(refreshCtx)
		if err != nil {
			return managedAppStatusListCacheEntry{}, err
		}
	}
	items, err := client.listManagedAppsByAppID(refreshCtx)
	if err != nil {
		return managedAppStatusListCacheEntry{}, err
	}
	if requireClusterIdentity {
		confirmedClusterID, err := client.getClusterID(refreshCtx)
		if err != nil {
			return managedAppStatusListCacheEntry{}, err
		}
		if confirmedClusterID != clusterID {
			return managedAppStatusListCacheEntry{}, fmt.Errorf("kubernetes cluster identity changed during managed app inventory observation")
		}
	}
	evidenceByAppID := make(map[string]managedAppRuntimeEvidence)
	if requireClusterIdentity && s != nil && s.store != nil {
		snapshot, snapshotErr := client.readRuntimeSnapshot(refreshCtx)
		if snapshotErr != nil {
			return managedAppStatusListCacheEntry{}, snapshotErr
		}
		finalClusterID, finalErr := client.getClusterID(refreshCtx)
		if finalErr != nil {
			return managedAppStatusListCacheEntry{}, finalErr
		}
		if finalClusterID != clusterID {
			return managedAppStatusListCacheEntry{}, fmt.Errorf("kubernetes cluster identity changed during runtime evidence observation")
		}
		var apps []model.App
		if s != nil && s.store != nil {
			apps, err = s.store.ListApps("", true)
			if err != nil {
				return managedAppStatusListCacheEntry{}, fmt.Errorf("list apps for runtime evidence: %w", err)
			}
		}
		appsByID := make(map[string]model.App, len(apps))
		for _, app := range apps {
			appsByID[strings.TrimSpace(app.ID)] = app
		}
		for appID, managed := range items {
			app, ok := appsByID[strings.TrimSpace(appID)]
			if !ok {
				continue
			}
			evidence, evidenceErr := s.buildManagedAppRuntimeEvidence(app, managed, true, snapshot)
			if evidenceErr != nil {
				return managedAppStatusListCacheEntry{}, evidenceErr
			}
			evidenceByAppID[appID] = evidence
		}
		// Complete absence is meaningful only for apps represented in the
		// durable store. Their namespace/service/image evidence is still
		// collected from the same successful snapshot.
		for _, app := range apps {
			appID := strings.TrimSpace(app.ID)
			if appID == "" {
				continue
			}
			if _, exists := evidenceByAppID[appID]; exists {
				continue
			}
			evidence, evidenceErr := s.buildManagedAppRuntimeEvidence(app, runtime.ManagedAppObject{}, false, snapshot)
			if evidenceErr != nil {
				return managedAppStatusListCacheEntry{}, evidenceErr
			}
			evidenceByAppID[appID] = evidence
		}
	}

	now := time.Now().UTC()
	entry := managedAppStatusListCacheEntry{
		items:       items,
		evidence:    evidenceByAppID,
		ok:          true,
		clusterID:   clusterID,
		refreshedAt: now,
		expiresAt:   now.Add(s.managedAppStatusCache.cacheTTL()),
	}
	// Only a cluster-identified inventory may populate the observed-status
	// cache.  The backing-service inventory path intentionally predates the
	// observed contract and may be used with a lightweight list-only client;
	// allowing it to overwrite this cache would erase cluster identity and
	// fresh runtime evidence, making Console/Edge report unknown or (worse)
	// reuse an unrelated inventory as observed state.
	if requireClusterIdentity {
		s.managedAppStatusCache.setList(entry)
	}
	return entry, nil
}

func (s *Server) refreshManagedAppInventory(ctx context.Context) (managedAppStatusListCacheEntry, error) {
	value, err, _ := s.managedAppStatusCache.group.Do(managedAppInventoryRefreshKey, func() (any, error) {
		return s.fetchManagedAppInventory(ctx)
	})
	if err != nil {
		return managedAppStatusListCacheEntry{}, err
	}
	entry, _ := value.(managedAppStatusListCacheEntry)
	return entry, nil
}

func (s *Server) refreshManagedAppStatuses(ctx context.Context) (managedAppStatusListCacheEntry, error) {
	if !s.managedAppStatusCache.listRefreshAllowed(time.Now()) {
		return managedAppStatusListCacheEntry{}, errManagedAppStatusRefreshBackoff
	}
	value, err, _ := s.managedAppStatusCache.group.Do(managedAppStatusListRefreshKey, func() (any, error) {
		if !s.managedAppStatusCache.listRefreshAllowed(time.Now()) {
			return managedAppStatusListCacheEntry{}, errManagedAppStatusRefreshBackoff
		}
		entry, err := s.fetchManagedAppStatuses(ctx)
		s.managedAppStatusCache.recordListRefreshResult(err)
		return entry, err
	})
	if err != nil {
		return managedAppStatusListCacheEntry{}, err
	}

	entry, _ := value.(managedAppStatusListCacheEntry)
	return entry, nil
}

func (s *Server) refreshManagedAppStatusesAsync() {
	if !s.managedAppStatusCache.listRefreshAllowed(time.Now()) {
		return
	}
	s.managedAppStatusCache.group.DoChan(managedAppStatusListRefreshKey, func() (any, error) {
		if !s.managedAppStatusCache.listRefreshAllowed(time.Now()) {
			return managedAppStatusListCacheEntry{}, errManagedAppStatusRefreshBackoff
		}
		entry, err := s.fetchManagedAppStatuses(context.Background())
		s.managedAppStatusCache.recordListRefreshResult(err)
		if err != nil && s.shouldLogManagedAppStatusError(err) && s.log != nil {
			s.log.Printf("managed app status background list refresh error: %v", err)
		}
		return entry, err
	})
}
