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

	"fugue/internal/appimages"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

const (
	defaultManagedAppStatusCacheTTL       = 15 * time.Second
	defaultManagedAppStatusRefreshTimeout = 5 * time.Second
	defaultManagedAppStatusRefreshBackoff = 15 * time.Second
	defaultAppObservedStatusMaxAge        = time.Minute
	managedAppStatusListRefreshKey        = "list"
	managedAppInventoryRefreshKey         = "inventory"
)

var (
	errManagedAppStatusClientUnavailable = errors.New("managed app status client unavailable")
	errManagedAppStatusRefreshBackoff    = errors.New("managed app status refresh backoff active")
)

func appObservedStatusFresh(observed *model.AppObservedStatus, now time.Time) bool {
	if observed == nil || !observed.Fresh {
		return false
	}
	return appObservationTimestampFresh(observed.ObservedAt, now)
}

func appObservationTimestampFresh(observedAt, now time.Time) bool {
	if observedAt.IsZero() {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	observedAt = observedAt.UTC()
	return !observedAt.After(now.Add(30*time.Second)) && now.Sub(observedAt) <= defaultAppObservedStatusMaxAge
}

// appObservedReadyForServing is the shared publication gate for API surfaces
// that may render or advertise a green/active application. The observed phase
// is necessary but never sufficient: every affirmative field must belong to a
// fresh, cluster-identified, current-generation observation.
func appObservedReadyForServing(app model.App, now time.Time) bool {
	observed := app.ObservedStatus
	if observed == nil || !strings.EqualFold(strings.TrimSpace(observed.Phase), "deployed") ||
		app.Spec.Replicas <= 0 || observed.DesiredReplicas != app.Spec.Replicas ||
		!appObservedStatusFresh(observed, now) || strings.TrimSpace(observed.ClusterID) == "" ||
		strings.TrimSpace(observed.EvidenceSource) == "" || observed.Generation <= 0 ||
		observed.ObservedGeneration < observed.Generation || len(observed.InvariantViolations) > 0 {
		return false
	}
	if observed.RuntimeObjectPresent == nil || !*observed.RuntimeObjectPresent ||
		observed.NamespacePresent == nil || !*observed.NamespacePresent ||
		observed.EndpointPresent == nil || !*observed.EndpointPresent ||
		observed.EndpointReady == nil || !*observed.EndpointReady ||
		observed.ImagePresent == nil || !*observed.ImagePresent {
		return false
	}
	if observed.ReadyReplicas == nil || *observed.ReadyReplicas < app.Spec.Replicas ||
		observed.PhysicalReplicas == nil || *observed.PhysicalReplicas < app.Spec.Replicas ||
		(app.Spec.Replicas > 0 && *observed.PhysicalReplicas <= 0) {
		return false
	}
	if observed.PhysicalDesired != nil && *observed.PhysicalDesired < app.Spec.Replicas {
		return false
	}
	if model.AppHasClusterService(app.Spec) || model.AppSSHEnabled(app.Spec) {
		if observed.ServicePresent == nil || !*observed.ServicePresent {
			return false
		}
	}
	return true
}

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
	observationSequence  uint64
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
	sequence    managedAppObservationSequence
}

type managedAppStatusListCacheEntry struct {
	items       map[string]runtime.ManagedAppObject
	evidence    map[string]managedAppRuntimeEvidence
	ok          bool
	clusterID   string
	refreshedAt time.Time
	expiresAt   time.Time
	sequence    managedAppObservationSequence
}

type managedAppObservationSequence struct {
	refreshStarted   uint64
	managedAppsRead  uint64
	kubeSnapshotRead uint64
	durableAppsRead  uint64
	refreshCompleted uint64
}

// managedAppRuntimeEvidence contains only authoritative results from the same
// Kubernetes snapshot as the ManagedApp observation. Nil fields mean that a
// resource is not applicable (for example a background app has no Service),
// while a non-nil false is an authoritative absence.
type managedAppRuntimeEvidence struct {
	appObservationKey            string
	namespacePresent             *bool
	servicePresent               *bool
	endpointPresent              *bool
	endpointReady                *bool
	physicalReplicas             *int
	physicalDesiredReplicas      *int
	imagePresent                 *bool
	imageRef                     string
	invariantViolations          []string
	evidenceSources              []string
	managedGeneration            int64
	managedObservedGeneration    int64
	managedImageDigest           string
	managedDesiredReplicas       int
	managedReadyReplicas         int
	deploymentGeneration         int64
	deploymentObservedGeneration int64
	deploymentImageDigest        string
	deploymentReplicas           int
	deploymentUpdatedReplicas    int
	deploymentReadyReplicas      int
	deploymentAvailableReplicas  int
	imageLocationStatus          string
	imageLocationSource          string
	imageLocationObservedAt      time.Time
	servingReleaseID             string
	servingReleaseReady          bool
}

type managedAppKubeSnapshot struct {
	namespaces              map[string]struct{}
	deployments             map[string]kubeDeploymentRuntimeEvidence
	services                map[string]struct{}
	endpoints               map[string]kubeEndpointRuntimeEvidence
	endpointSlices          map[string]kubeEndpointRuntimeEvidence
	endpointsAvailable      bool
	endpointSlicesAvailable bool
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
		UpdatedReplicas    int   `json:"updatedReplicas"`
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

func (c *managedAppStatusCache) nextObservationSequence() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	c.observationSequence++
	sequence := c.observationSequence
	c.mu.Unlock()
	return sequence
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
			sequence:    entry.sequence,
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
			sequence:    entry.sequence,
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
	if err := validateObservedManagedAppObject(managed); err != nil {
		return runtime.ManagedAppObject{}, false, err
	}
	return managed, true, nil
}

func validateObservedManagedAppObject(managed runtime.ManagedAppObject) error {
	if strings.TrimSpace(managed.Metadata.Name) == "" || strings.TrimSpace(managed.Metadata.Namespace) == "" {
		return fmt.Errorf("managed app object identity is incomplete")
	}
	if strings.TrimSpace(managed.Spec.AppID) == "" || strings.TrimSpace(managed.Spec.TenantID) == "" {
		return fmt.Errorf("managed app spec identity is incomplete")
	}
	return nil
}

// confirmManagedAppLookupAfterNotFound distinguishes a missing namespaced
// object from a missing/disabled ManagedApp CRD. Kubernetes uses 404 for both
// cases, so a point GET alone is not sufficient evidence for an unavailable
// runtime. A successful, fully paginated cluster inventory is the authoritative
// absence check; any failure must remain unknown.
func (c *managedAppStatusClient) confirmManagedAppLookupAfterNotFound(ctx context.Context, app model.App) (runtime.ManagedAppObject, bool, error) {
	if strings.TrimSpace(app.ID) == "" {
		return runtime.ManagedAppObject{}, false, fmt.Errorf("cannot confirm managed app absence without app id")
	}
	items, err := c.listObservedManagedAppsByAppID(ctx)
	if err != nil {
		return runtime.ManagedAppObject{}, false, fmt.Errorf("confirm managed app absence from inventory: %w", err)
	}
	managed, found := items[strings.TrimSpace(app.ID)]
	return managed, found, nil
}

func (c *managedAppStatusClient) listManagedAppsByAppID(ctx context.Context) (map[string]runtime.ManagedAppObject, error) {
	return c.listManagedAppsByAppIDWithValidation(ctx, false)
}

// listObservedManagedAppsByAppID is used only for the cluster-identified
// observed-status path. Unlike the compatibility inventory used by backing
// service lifecycle reads, it rejects malformed objects rather than skipping
// them; otherwise a successful list could falsely authorize absence.
func (c *managedAppStatusClient) listObservedManagedAppsByAppID(ctx context.Context) (map[string]runtime.ManagedAppObject, error) {
	return c.listManagedAppsByAppIDWithValidation(ctx, true)
}

func (c *managedAppStatusClient) listManagedAppsByAppIDWithValidation(ctx context.Context, requireIdentity bool) (map[string]runtime.ManagedAppObject, error) {
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
			if requireIdentity {
				if err := validateObservedManagedAppObject(managed); err != nil {
					return nil, err
				}
			}
			appID := strings.TrimSpace(managed.Spec.AppID)
			if appID == "" {
				// Compatibility callers historically use this inventory for
				// backing-service status and may encounter retained objects that
				// predate app identity fields. They cannot participate in an
				// observed-status absence decision, so simply omit them here.
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

// listKubeResourcesOptional is used for EndpointSlice, which is not present
// on a few older/locked-down Kubernetes APIs. A 404/405 means the API group is
// unavailable and the legacy Endpoints snapshot remains the authoritative
// source; all other failures are incomplete evidence and must fail closed.
func (c *managedAppStatusClient) listKubeResourcesOptional(ctx context.Context, basePath string) ([]map[string]any, bool, error) {
	items, err := c.listKubeResources(ctx, basePath)
	if err == nil {
		return items, true, nil
	}
	var statusErr *kubeStatusError
	if errors.As(err, &statusErr) && (statusErr.StatusCode == http.StatusNotFound || statusErr.StatusCode == http.StatusMethodNotAllowed) {
		return nil, false, nil
	}
	return nil, false, err
}

func (c *managedAppStatusClient) readRuntimeSnapshot(ctx context.Context) (managedAppKubeSnapshot, error) {
	var namespaceItems, deploymentItems, serviceItems, endpointItems, endpointSliceItems []map[string]any
	endpointsAvailable := false
	endpointSlicesAvailable := false
	var endpointsErr, endpointSlicesErr error
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
		endpointItems, endpointsAvailable, endpointsErr = c.listKubeResourcesOptional(groupCtx, "/api/v1/endpoints")
		return nil
	})
	group.Go(func() error {
		endpointSliceItems, endpointSlicesAvailable, endpointSlicesErr = c.listKubeResourcesOptional(groupCtx, "/apis/discovery.k8s.io/v1/endpointslices")
		return nil
	})
	if err := group.Wait(); err != nil {
		return managedAppKubeSnapshot{}, err
	}
	// An alternate endpoint API may be used only when the other API is
	// genuinely unsupported (404/405, which listKubeResourcesOptional maps to
	// a nil error). Any transport, auth, decode, or server failure means the
	// snapshot is incomplete and the shared calculator must publish unknown;
	// a successful alternate API cannot prove that the failed query was safe.
	if endpointsErr != nil {
		return managedAppKubeSnapshot{}, fmt.Errorf("list kubernetes endpoints: %w", endpointsErr)
	}
	if endpointSlicesErr != nil {
		return managedAppKubeSnapshot{}, fmt.Errorf("list kubernetes endpoint slices: %w", endpointSlicesErr)
	}
	if !endpointsAvailable && !endpointSlicesAvailable {
		return managedAppKubeSnapshot{}, fmt.Errorf("no kubernetes endpoint API is available")
	}

	snapshot := managedAppKubeSnapshot{
		namespaces:              make(map[string]struct{}, len(namespaceItems)),
		deployments:             make(map[string]kubeDeploymentRuntimeEvidence, len(deploymentItems)),
		services:                make(map[string]struct{}, len(serviceItems)),
		endpoints:               make(map[string]kubeEndpointRuntimeEvidence, len(endpointItems)),
		endpointSlices:          make(map[string]kubeEndpointRuntimeEvidence, len(endpointSliceItems)),
		endpointsAvailable:      endpointsAvailable,
		endpointSlicesAvailable: endpointSlicesAvailable,
	}
	for _, raw := range namespaceItems {
		name := kubeObjectNamespaceOrName(raw, "")
		if name == "" {
			return managedAppKubeSnapshot{}, fmt.Errorf("decode kubernetes namespace: metadata.name is missing")
		}
		snapshot.namespaces[name] = struct{}{}
	}
	for _, raw := range deploymentItems {
		var deployment kubeDeploymentRuntimeEvidence
		if err := decodeKubeObject(raw, &deployment); err != nil {
			return managedAppKubeSnapshot{}, fmt.Errorf("decode kubernetes deployment: %w", err)
		}
		key := kubeNamespacedKey(deployment.Metadata.Namespace, deployment.Metadata.Name)
		if key == "/" {
			return managedAppKubeSnapshot{}, fmt.Errorf("decode kubernetes deployment: metadata name/namespace is missing")
		}
		snapshot.deployments[key] = deployment
	}
	for _, raw := range serviceItems {
		key := kubeObjectNamespacedKey(raw)
		if key != "/" {
			snapshot.services[key] = struct{}{}
		} else {
			return managedAppKubeSnapshot{}, fmt.Errorf("decode kubernetes service: metadata name/namespace is missing")
		}
	}
	for _, raw := range endpointItems {
		metadata := kubeObjectMetadata(raw)
		key := kubeNamespacedKey(metadata.namespace, metadata.name)
		if key == "/" {
			return managedAppKubeSnapshot{}, fmt.Errorf("decode kubernetes endpoint: metadata name/namespace is missing")
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
	if endpointSlicesAvailable {
		for _, raw := range endpointSliceItems {
			metadata := kubeObjectMetadata(raw)
			if metadata.namespace == "" || metadata.name == "" {
				return managedAppKubeSnapshot{}, fmt.Errorf("decode kubernetes endpoint slice: metadata name/namespace is missing")
			}
			serviceName := ""
			if metadataRaw, ok := raw["metadata"].(map[string]any); ok {
				if labels, ok := metadataRaw["labels"].(map[string]any); ok {
					serviceName, _ = labels["kubernetes.io/service-name"].(string)
				}
			}
			key := kubeNamespacedKey(metadata.namespace, serviceName)
			if key == "/" || strings.TrimSpace(serviceName) == "" {
				continue
			}
			evidence := snapshot.endpointSlices[key]
			evidence.Present = true
			if endpoints, ok := raw["endpoints"].([]any); ok {
				for _, rawEndpoint := range endpoints {
					endpoint, ok := rawEndpoint.(map[string]any)
					if !ok {
						continue
					}
					addresses := kubeAddressCount(endpoint["addresses"])
					conditions, _ := endpoint["conditions"].(map[string]any)
					ready, hasReady := conditions["ready"].(bool)
					if hasReady && ready {
						evidence.ReadyAddresses += addresses
					} else {
						evidence.NotReadyAddresses += addresses
					}
				}
			}
			snapshot.endpointSlices[key] = evidence
		}
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
			sequence:    list.sequence,
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
	overlaid, _ := s.overlayManagedAppStatusesForEdgeRoutesCachedWithProvenance(apps, runtimeByID)
	return overlaid
}

func (s *Server) overlayManagedAppStatusesForEdgeRoutesCachedWithProvenance(apps []model.App, runtimeByID map[string]model.Runtime) ([]model.App, map[string]managedAppObservationProvenance) {
	if len(apps) == 0 {
		return apps, nil
	}
	cached, ok, expired := s.managedAppStatusCache.getObservedList()
	if ok {
		if expired {
			s.refreshManagedAppStatusesAsync()
		}
		// Cache expiry schedules a refresh; it does not invalidate a successful
		// runtime observation that is still inside the publication freshness SLA.
		// Marking that short refresh window stale makes edge routes flap on every
		// cache cycle even though the underlying evidence has not aged out.
		fresh := appObservationTimestampFresh(cached.refreshedAt, time.Now().UTC())
		return s.applyManagedAppListObservation(apps, cached, runtimeByID, fresh, ""), managedAppListObservationProvenance(apps, cached, expired, fresh)
	}
	s.refreshManagedAppStatusesAsync()
	return s.applyUnknownManagedAppObservation(apps, runtimeByID, "live runtime observation is pending"), managedAppUnknownObservationProvenance(apps)
}

func (s *Server) applyManagedAppObservation(app model.App, entry managedAppStatusCacheEntry, runtimeByID map[string]model.Runtime, fresh bool, errorMessage string) model.App {
	if !entry.found && !appMayUseManagedRuntime(app, runtimeByID) {
		return app
	}
	complete := entry.ok
	if complete && entry.evidence.appObservationKey != "" && entry.evidence.appObservationKey != managedAppRuntimeEvidenceObservationKey(app) {
		complete = false
		fresh = false
		errorMessage = "cached runtime observation belongs to a different app revision"
		s.refreshManagedAppStatusesAsync()
	}
	if !complete && strings.TrimSpace(errorMessage) == "" {
		errorMessage = "live runtime observation is unavailable"
	}
	managed := entry.managed
	if entry.evidence.servingReleaseReady {
		if managed.Metadata.Generation <= 0 {
			managed.Metadata.Generation = 1
		}
		managed.Status.ObservedGeneration = managed.Metadata.Generation
		managed.Status.Phase = runtime.ManagedAppPhaseReady
		managed.Status.DesiredReplicas = app.Spec.Replicas
		managed.Status.ReadyReplicas = app.Spec.Replicas
		managed.Status.Message = "stable release " + entry.evidence.servingReleaseID + " is serving the desired image"
	}
	observed := runtime.CalculateAppObservedStatus(app, runtime.AppRuntimeObservation{
		ManagedApp:              managed,
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
			sequence:    entry.sequence,
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
	// During a migration the durable desired runtime can already be managed
	// while Status.CurrentRuntimeID still names the source runtime (or vice
	// versa). Inspect both identities; choosing only the current one would
	// skip the live observer and leave a stale deployed projection in place.
	for _, runtimeID := range []string{app.Status.CurrentRuntimeID, app.Spec.RuntimeID} {
		runtimeID = strings.TrimSpace(runtimeID)
		if runtimeID == "" || runtimeID == appProxyRuntimeID(app) {
			continue
		}
		runtimeObj, ok := runtimeByID[runtimeID]
		if !ok {
			continue
		}
		switch strings.TrimSpace(runtimeObj.Type) {
		case "", model.RuntimeTypeManagedShared, model.RuntimeTypeManagedOwned:
			return true
		}
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

func managedAppRuntimeEvidenceObservationKey(app model.App) string {
	type imageSource struct {
		Type             string `json:"type,omitempty"`
		ImageRef         string `json:"image_ref,omitempty"`
		ResolvedImageRef string `json:"resolved_image_ref,omitempty"`
	}
	type observationInput struct {
		ID               string      `json:"id"`
		TenantID         string      `json:"tenant_id"`
		Name             string      `json:"name"`
		Image            string      `json:"image"`
		Ports            []int       `json:"ports,omitempty"`
		Replicas         int         `json:"replicas"`
		RuntimeID        string      `json:"runtime_id"`
		CurrentRuntimeID string      `json:"current_runtime_id"`
		NetworkMode      string      `json:"network_mode,omitempty"`
		SSHEnabled       bool        `json:"ssh_enabled"`
		StoredPhase      string      `json:"stored_phase,omitempty"`
		Source           imageSource `json:"source"`
		BuildSource      imageSource `json:"build_source"`
	}

	storedPhase := app.Status.Phase
	if app.StoredStatus != nil {
		storedPhase = app.StoredStatus.Phase
	}
	source := imageSource{}
	if app.Source != nil {
		source = imageSource{
			Type:             strings.TrimSpace(app.Source.Type),
			ImageRef:         strings.TrimSpace(app.Source.ImageRef),
			ResolvedImageRef: strings.TrimSpace(app.Source.ResolvedImageRef),
		}
	}
	buildSource := imageSource{}
	if build := model.AppBuildSource(app); build != nil {
		buildSource = imageSource{
			Type:             strings.TrimSpace(build.Type),
			ImageRef:         strings.TrimSpace(build.ImageRef),
			ResolvedImageRef: strings.TrimSpace(build.ResolvedImageRef),
		}
	}
	payload, _ := json.Marshal(observationInput{
		ID:               strings.TrimSpace(app.ID),
		TenantID:         strings.TrimSpace(app.TenantID),
		Name:             strings.TrimSpace(app.Name),
		Image:            strings.TrimSpace(app.Spec.Image),
		Ports:            append([]int(nil), app.Spec.Ports...),
		Replicas:         app.Spec.Replicas,
		RuntimeID:        strings.TrimSpace(app.Spec.RuntimeID),
		CurrentRuntimeID: strings.TrimSpace(app.Status.CurrentRuntimeID),
		NetworkMode:      model.NormalizeAppNetworkMode(app.Spec.NetworkMode),
		SSHEnabled:       model.AppSSHEnabled(app.Spec),
		StoredPhase:      strings.TrimSpace(storedPhase),
		Source:           source,
		BuildSource:      buildSource,
	})
	return string(payload)
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
		appObservationKey: managedAppRuntimeEvidenceObservationKey(app),
		evidenceSources:   []string{runtime.AppObservationSourceKubernetesAPI},
	}
	if found {
		evidence.managedGeneration = managed.Metadata.Generation
		evidence.managedObservedGeneration = managed.Status.ObservedGeneration
		evidence.managedImageDigest = edgeObservationDigest(managed.Spec.AppSpec.Image)
		evidence.managedDesiredReplicas = managed.Status.DesiredReplicas
		evidence.managedReadyReplicas = managed.Status.ReadyReplicas
		expectedName := strings.TrimSpace(runtime.ManagedAppResourceName(app))
		expectedNamespace := strings.TrimSpace(runtime.NamespaceForTenant(app.TenantID))
		if observedName := strings.TrimSpace(managed.Metadata.Name); observedName != expectedName {
			evidence.invariantViolations = append(evidence.invariantViolations, "managed_app_name_mismatch")
		}
		if observedNamespace := strings.TrimSpace(managed.Metadata.Namespace); observedNamespace != expectedNamespace {
			evidence.invariantViolations = append(evidence.invariantViolations, "managed_app_namespace_mismatch")
		}
		if observedAppID := strings.TrimSpace(managed.Spec.AppID); observedAppID != "" && observedAppID != strings.TrimSpace(app.ID) {
			evidence.invariantViolations = append(evidence.invariantViolations, "managed_app_identity_mismatch")
		}
		if observedTenantID := strings.TrimSpace(managed.Spec.TenantID); observedTenantID != "" && observedTenantID != strings.TrimSpace(app.TenantID) {
			evidence.invariantViolations = append(evidence.invariantViolations, "managed_app_tenant_mismatch")
		}
		observedRuntimeID := strings.TrimSpace(managed.Spec.AppSpec.RuntimeID)
		if observedRuntimeID != "" {
			expectedRuntimeIDs := map[string]struct{}{}
			for _, runtimeID := range []string{app.Spec.RuntimeID, app.Status.CurrentRuntimeID} {
				if runtimeID = strings.TrimSpace(runtimeID); runtimeID != "" {
					expectedRuntimeIDs[runtimeID] = struct{}{}
				}
			}
			if len(expectedRuntimeIDs) > 0 {
				if _, matches := expectedRuntimeIDs[observedRuntimeID]; !matches {
					evidence.invariantViolations = append(evidence.invariantViolations, "managed_app_runtime_mismatch")
				}
			}
		}
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
	canonicalServiceName := runtime.RuntimeAppServiceName(app)
	canonicalDeploymentName := runtime.RuntimeAppResourceName(app)
	serviceName := canonicalServiceName
	deploymentName := canonicalDeploymentName
	servingRelease, servingReleaseFound := s.servingReleaseTrafficTarget(app)
	if servingReleaseFound {
		if name := strings.TrimSpace(servingRelease.ServiceName); name != "" {
			serviceName = name
		}
		if name := strings.TrimSpace(servingRelease.DeploymentName); name != "" {
			deploymentName = name
		}
		releaseDeployment, releaseDeploymentFound := snapshot.deployments[kubeNamespacedKey(namespace, deploymentName)]
		releaseDeploymentReady := releaseDeploymentFound && deploymentCurrentCohortComplete(releaseDeployment) &&
			s.observedRuntimeImageRefsEquivalent(app, servingRelease.ResolvedImageRef, firstDeploymentContainerImage(releaseDeployment))
		if !releaseDeploymentReady {
			canonicalDeployment, canonicalDeploymentFound := snapshot.deployments[kubeNamespacedKey(namespace, canonicalDeploymentName)]
			canonicalDeploymentReady := canonicalDeploymentFound && deploymentCurrentCohortComplete(canonicalDeployment) &&
				s.observedRuntimeImageRefsEquivalent(app, servingRelease.ResolvedImageRef, firstDeploymentContainerImage(canonicalDeployment))
			if canonicalDeploymentReady {
				deploymentName = canonicalDeploymentName
				serviceName = canonicalServiceName
			}
		}
		evidence.servingReleaseID = strings.TrimSpace(servingRelease.ID)
		evidence.evidenceSources = append(evidence.evidenceSources, "app_release_traffic_policy")
	}
	serviceKey := kubeNamespacedKey(namespace, serviceName)
	deploymentKey := kubeNamespacedKey(namespace, deploymentName)

	if serviceRequired {
		_, serviceExists := snapshot.services[serviceKey]
		servicePresent := serviceExists
		evidence.servicePresent = &servicePresent
		endpoint, endpointExists := snapshot.endpoints[serviceKey]
		slice := snapshot.endpointSlices[serviceKey]
		// Prefer EndpointSlice whenever that API is available, even when this
		// service has no slice. Falling back to legacy Endpoints merely because
		// the slice map lacks a key could advertise a stale endpoint from the
		// older API after the current slice inventory has gone empty.
		// Combining the two sources with OR could advertise a ready route from a
		// stale legacy Endpoints object while the current slices report zero
		// ready addresses. The fallback is used only when the EndpointSlice API
		// itself is unavailable.
		endpointPresent := endpointExists && endpoint.Present
		endpointReady := endpointPresent && endpoint.ReadyAddresses > 0
		if snapshot.endpointSlicesAvailable {
			endpointPresent = slice.Present
			endpointReady = slice.Present && slice.ReadyAddresses > 0
		}
		evidence.endpointPresent = &endpointPresent
		evidence.endpointReady = &endpointReady
	}
	deploymentImageMatches := false
	if deploymentRequired {
		deployment, deploymentExists := snapshot.deployments[deploymentKey]
		physicalDesired := 0
		physicalReady := 0
		if deploymentExists {
			evidence.deploymentGeneration = deployment.Metadata.Generation
			evidence.deploymentObservedGeneration = deployment.Status.ObservedGeneration
			evidence.deploymentImageDigest = edgeObservationDigest(firstDeploymentContainerImage(deployment))
			evidence.deploymentReplicas = deployment.Status.Replicas
			evidence.deploymentUpdatedReplicas = deployment.Status.UpdatedReplicas
			evidence.deploymentReadyReplicas = deployment.Status.ReadyReplicas
			evidence.deploymentAvailableReplicas = deployment.Status.AvailableReplicas
			if deployment.Spec.Replicas != nil {
				physicalDesired = *deployment.Spec.Replicas
			}
			// Deployment readyReplicas can still belong to the previous
			// ReplicaSet after the template/generation changed. Only replicas
			// that are simultaneously updated, ready, and available are current
			// physical proof for this generation.
			physicalReady = minObservedReplicaCount(
				deployment.Status.UpdatedReplicas,
				deployment.Status.ReadyReplicas,
				deployment.Status.AvailableReplicas,
			)
			if servingReleaseFound {
				deploymentImageMatches = s.observedRuntimeImageRefsEquivalent(app, servingRelease.ResolvedImageRef, firstDeploymentContainerImage(deployment))
			}
		}
		evidence.physicalDesiredReplicas = &physicalDesired
		evidence.physicalReplicas = &physicalReady
		if deploymentExists && (deployment.Metadata.Generation <= 0 || deployment.Status.ObservedGeneration < deployment.Metadata.Generation) {
			evidence.invariantViolations = append(evidence.invariantViolations, "deployment_generation_unobserved")
		}
	}

	imageRef := strings.TrimSpace(app.Spec.Image)
	if found && !servingReleaseFound {
		imageRef = strings.TrimSpace(managed.Spec.AppSpec.Image)
	}
	if servingReleaseFound {
		imageRef = strings.TrimSpace(servingRelease.ResolvedImageRef)
	}
	if imageRef != "" {
		evidence.imageRef = imageRef
		managedImage := strings.Contains(imageRef, "/fugue-apps/")
		if managedImage && s != nil && s.store != nil {
			observedRuntimeID := strings.TrimSpace(app.Spec.RuntimeID)
			if found {
				// Runtime identity from the object in this Kubernetes snapshot is
				// the only image-location scope that can be associated with the
				// current target during a migration. Do not use the durable source
				// runtime when the target object has already been created.
				if runtimeID := strings.TrimSpace(managed.Spec.AppSpec.RuntimeID); runtimeID != "" {
					observedRuntimeID = runtimeID
				}
			}
			present, locationObservation, err := s.currentManagedImagePresenceWithObservation(app, imageRef, observedRuntimeID)
			if err != nil {
				return managedAppRuntimeEvidence{}, err
			}
			evidence.imagePresent = present
			evidence.imageLocationStatus = locationObservation.status
			evidence.imageLocationSource = locationObservation.source
			evidence.imageLocationObservedAt = locationObservation.observedAt
			evidence.evidenceSources = append(evidence.evidenceSources, "image_location_store")
		}
		if deployment, deploymentExists := snapshot.deployments[deploymentKey]; deploymentExists &&
			deploymentCurrentCohortComplete(deployment) {
			deployedImage := firstDeploymentContainerImage(deployment)
			if deployedImage != "" {
				matches := s.observedRuntimeImageRefsEquivalent(app, imageRef, deployedImage)
				if matches {
					if evidence.imagePresent == nil {
						evidence.imagePresent = &matches
					}
				} else {
					evidence.invariantViolations = append(evidence.invariantViolations, "current_image_mismatch")
				}
			}
		}
	}
	if servingReleaseFound && deploymentImageMatches &&
		evidence.physicalDesiredReplicas != nil && *evidence.physicalDesiredReplicas >= app.Spec.Replicas &&
		evidence.physicalReplicas != nil && *evidence.physicalReplicas >= app.Spec.Replicas &&
		(!serviceRequired || (boolPointerTrue(evidence.servicePresent) && boolPointerTrue(evidence.endpointReady))) {
		evidence.servingReleaseReady = true
	}

	// Record contradictions as explicit invariant evidence. The calculator
	// decides the observed phase; this list is never inferred from stored
	// replicas alone.
	if found && app.Spec.Replicas > 0 {
		storedPhase := app.Status.Phase
		if app.StoredStatus != nil {
			storedPhase = app.StoredStatus.Phase
		}
		if strings.EqualFold(strings.TrimSpace(storedPhase), "deployed") && managed.Status.ReadyReplicas == 0 {
			evidence.invariantViolations = append(evidence.invariantViolations, "stored_deployed_but_observed_ready_zero")
		}
	}
	return evidence, nil
}

func (s *Server) servingReleaseTrafficTarget(app model.App) (model.AppRelease, bool) {
	if s == nil || s.store == nil || strings.TrimSpace(app.ID) == "" {
		return model.AppRelease{}, false
	}
	policy, err := s.store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err != nil || !strings.EqualFold(strings.TrimSpace(policy.Mode), model.AppTrafficModeSingle) ||
		policy.StableWeight != 100 || policy.CandidateWeight != 0 || strings.TrimSpace(policy.StableReleaseID) == "" {
		return model.AppRelease{}, false
	}
	release, err := s.store.GetAppRelease(app.TenantID, true, policy.StableReleaseID)
	if err != nil || strings.TrimSpace(release.AppID) != strings.TrimSpace(app.ID) ||
		!strings.EqualFold(strings.TrimSpace(release.Role), model.AppReleaseRoleStable) ||
		!strings.EqualFold(strings.TrimSpace(release.Status), model.AppReleaseStatusServing) ||
		strings.TrimSpace(release.DeploymentName) == "" || strings.TrimSpace(release.ResolvedImageRef) == "" ||
		!s.observedRuntimeImageRefsEquivalent(app, app.Spec.Image, release.ResolvedImageRef) {
		return model.AppRelease{}, false
	}
	return release, true
}

func boolPointerTrue(value *bool) bool {
	return value != nil && *value
}

func deploymentCurrentCohortComplete(deployment kubeDeploymentRuntimeEvidence) bool {
	if deployment.Metadata.Generation <= 0 ||
		deployment.Status.ObservedGeneration < deployment.Metadata.Generation ||
		deployment.Spec.Replicas == nil {
		return false
	}
	desired := *deployment.Spec.Replicas
	return desired > 0 &&
		deployment.Status.UpdatedReplicas == desired &&
		deployment.Status.ReadyReplicas >= desired &&
		deployment.Status.AvailableReplicas >= desired
}

func minObservedReplicaCount(values ...int) int {
	if len(values) == 0 {
		return 0
	}
	minimum := values[0]
	for _, value := range values[1:] {
		if value < minimum {
			minimum = value
		}
	}
	if minimum < 0 {
		return 0
	}
	return minimum
}

func (s *Server) observedRuntimeImageRefsEquivalent(app model.App, expected, actual string) bool {
	expected = strings.TrimSpace(expected)
	actual = strings.TrimSpace(actual)
	if expected == "" || actual == "" {
		return false
	}
	if expected == actual {
		return true
	}
	// A managed image has a registry push address and a runtime pull address.
	// They identify the same artifact when both normalize through the server's
	// configured mapping; raw string comparison would create a false mismatch.
	expected = appimages.NormalizeRuntimeImageRefForSource(
		expected,
		model.AppBuildSource(app),
		s.registryPushBase,
		s.registryPullBase,
	)
	actual = appimages.NormalizeRuntimeImageRefForSource(
		actual,
		model.AppBuildSource(app),
		s.registryPushBase,
		s.registryPullBase,
	)
	return expected != "" && expected == actual
}

func firstDeploymentContainerImage(deployment kubeDeploymentRuntimeEvidence) string {
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if image := strings.TrimSpace(container.Image); image != "" {
			return image
		}
	}
	return ""
}

type managedImageLocationObservation struct {
	status     string
	source     string
	observedAt time.Time
}

func (s *Server) currentManagedImagePresence(app model.App, imageRef, runtimeID string) (*bool, error) {
	present, _, err := s.currentManagedImagePresenceWithObservation(app, imageRef, runtimeID)
	return present, err
}

func (s *Server) currentManagedImagePresenceWithObservation(app model.App, imageRef, runtimeID string) (*bool, managedImageLocationObservation, error) {
	// Image-location rows without a runtime identity cannot be tied to the
	// current migration target (and may have been written by the source
	// cluster). They are diagnostic history, not observed runtime evidence.
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return nil, managedImageLocationObservation{}, nil
	}
	refs := []string{strings.TrimSpace(imageRef)}
	if app.Source != nil {
		refs = append(refs, strings.TrimSpace(app.Source.ResolvedImageRef), strings.TrimSpace(app.Source.ImageRef))
	}
	seenRefs := make(map[string]struct{}, len(refs))
	cutoff := time.Now().UTC().Add(-defaultImageCacheInventoryTTL)
	hasFreshNegative := false
	hasFreshPending := false
	pendingObservation := managedImageLocationObservation{}
	negativeObservation := managedImageLocationObservation{}
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
				TenantID:  strings.TrimSpace(app.TenantID),
				AppID:     strings.TrimSpace(app.ID),
				ImageRef:  ref,
				Status:    status,
				RuntimeID: runtimeID,
			})
			if err != nil {
				return nil, managedImageLocationObservation{}, fmt.Errorf("list current image locations: %w", err)
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
					return &present, managedImageLocationObservation{status: status, source: "image_location_store", observedAt: observedAt}, nil
				case model.ImageLocationStatusPulling:
					hasFreshPending = true
					if pendingObservation.observedAt.IsZero() || observedAt.After(pendingObservation.observedAt) {
						pendingObservation = managedImageLocationObservation{status: status, source: "image_location_store", observedAt: observedAt}
					}
				case model.ImageLocationStatusMissing, model.ImageLocationStatusFailed:
					hasFreshNegative = true
					if negativeObservation.observedAt.IsZero() || observedAt.After(negativeObservation.observedAt) {
						negativeObservation = managedImageLocationObservation{status: status, source: "image_location_store", observedAt: observedAt}
					}
				}
			}
		}
	}
	if hasFreshPending {
		return nil, pendingObservation, nil
	}
	if hasFreshNegative {
		present := false
		return &present, negativeObservation, nil
	}
	// No current physical report is unknown evidence, not proof that an image
	// is missing.  This distinction keeps historical inventory gaps from
	// turning healthy apps red while still failing closed on a fresh explicit
	// missing/failed report.
	return nil, managedImageLocationObservation{source: "image_location_store"}, nil
}

func (s *Server) fetchManagedAppStatus(ctx context.Context, app model.App) (entry managedAppStatusCacheEntry, err error) {
	sequence := managedAppObservationSequence{refreshStarted: s.managedAppStatusCache.nextObservationSequence()}
	s.logManagedAppRefreshEvent("by_app", "start", "started", sequence.refreshStarted, nil)
	defer func() {
		if sequence.refreshCompleted == 0 {
			failedSequence := s.managedAppStatusCache.nextObservationSequence()
			s.logManagedAppRefreshEvent("by_app", "end", "error", failedSequence, err)
		}
	}()
	sequence.durableAppsRead = s.managedAppStatusCache.nextObservationSequence()
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
	if !found {
		// A namespaced GET 404 is ambiguous (object missing vs. CRD/API group
		// unavailable). Only a successful complete inventory can authorize the
		// unavailable/ready=0 result.
		managed, found, err = client.confirmManagedAppLookupAfterNotFound(refreshCtx, app)
		if err != nil {
			return managedAppStatusCacheEntry{}, err
		}
	}
	sequence.managedAppsRead = s.managedAppStatusCache.nextObservationSequence()
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
		sequence.kubeSnapshotRead = s.managedAppStatusCache.nextObservationSequence()
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
	sequence.refreshCompleted = s.managedAppStatusCache.nextObservationSequence()
	entry = managedAppStatusCacheEntry{
		managed:     managed,
		found:       found,
		ok:          true,
		clusterID:   clusterID,
		evidence:    evidence,
		refreshedAt: now,
		expiresAt:   now.Add(s.managedAppStatusCache.cacheTTL()),
		sequence:    sequence,
	}
	s.managedAppStatusCache.setApp(managedAppStatusCacheKey(app), entry)
	s.logManagedAppRefreshEvent("by_app", "end", "success", sequence.refreshCompleted, nil)
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

func (s *Server) fetchManagedAppInventoryWithClusterIdentity(ctx context.Context, requireClusterIdentity bool) (entry managedAppStatusListCacheEntry, err error) {
	cacheLayer := "inventory"
	if requireClusterIdentity {
		cacheLayer = "list"
	}
	sequence := managedAppObservationSequence{refreshStarted: s.managedAppStatusCache.nextObservationSequence()}
	s.logManagedAppRefreshEvent(cacheLayer, "start", "started", sequence.refreshStarted, nil)
	defer func() {
		if sequence.refreshCompleted == 0 {
			failedSequence := s.managedAppStatusCache.nextObservationSequence()
			s.logManagedAppRefreshEvent(cacheLayer, "end", "error", failedSequence, err)
		}
	}()
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
	var items map[string]runtime.ManagedAppObject
	if requireClusterIdentity {
		items, err = client.listObservedManagedAppsByAppID(refreshCtx)
	} else {
		items, err = client.listManagedAppsByAppID(refreshCtx)
	}
	if err != nil {
		return managedAppStatusListCacheEntry{}, err
	}
	sequence.managedAppsRead = s.managedAppStatusCache.nextObservationSequence()
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
		sequence.kubeSnapshotRead = s.managedAppStatusCache.nextObservationSequence()
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
			sequence.durableAppsRead = s.managedAppStatusCache.nextObservationSequence()
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
	sequence.refreshCompleted = s.managedAppStatusCache.nextObservationSequence()
	entry = managedAppStatusListCacheEntry{
		items:       items,
		evidence:    evidenceByAppID,
		ok:          true,
		clusterID:   clusterID,
		refreshedAt: now,
		expiresAt:   now.Add(s.managedAppStatusCache.cacheTTL()),
		sequence:    sequence,
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
	s.logManagedAppRefreshEvent(cacheLayer, "end", "success", sequence.refreshCompleted, nil)
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
