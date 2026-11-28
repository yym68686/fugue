package api

import (
	"bytes"
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
	"time"

	"fugue/internal/appimages"
	"fugue/internal/managedappstatus"
	"fugue/internal/model"
	"fugue/internal/releaseflow"
	"fugue/internal/releaseproof"
	"fugue/internal/runtime"

	"golang.org/x/sync/errgroup"
)

var (
	errManagedAppStatusClientUnavailable = errors.New("managed app status client unavailable")
)

type managedAppStatusClient struct {
	client      *http.Client
	baseURL     string
	bearerToken string
}

type managedAppStatusCache = managedappstatus.Cache
type managedAppStatusCacheEntry = managedappstatus.AppSnapshot
type managedAppStatusListCacheEntry = managedappstatus.InventorySnapshot
type managedAppStatusListInput = managedappstatus.InventoryInput
type managedAppObservationSequence = managedappstatus.Sequence
type managedAppRuntimeEvidence = managedappstatus.Evidence

type managedAppKubeSnapshot struct {
	namespaces              map[string]struct{}
	deployments             map[string]kubeDeploymentRuntimeEvidence
	services                map[string]struct{}
	serviceDetails          map[string]kubeServiceRuntimeEvidence
	releaseEndpointSlices   map[string][]kubeEndpointSliceEvidence
	releasePods             map[string]kubeReleasePodEvidence
	releaseReplicaSets      map[string]kubeReleaseReplicaSetEvidence
	endpoints               map[string]kubeEndpointRuntimeEvidence
	endpointSlices          map[string]kubeEndpointRuntimeEvidence
	endpointsAvailable      bool
	endpointSlicesAvailable bool
}

type kubeTypedResourceList[T any] struct {
	Items    []T `json:"items"`
	Metadata struct {
		Continue string `json:"continue"`
	} `json:"metadata"`
}
type kubeMetadataEvidence = releaseproof.MetadataEvidence
type kubeServiceRuntimeEvidence = releaseproof.Service
type kubeEndpointsEvidence struct {
	kubeMetadataEvidence
	Subsets []struct {
		Addresses         []json.RawMessage `json:"addresses"`
		NotReadyAddresses []json.RawMessage `json:"notReadyAddresses"`
	} `json:"subsets"`
}
type kubeEndpointSliceEvidence = releaseproof.EndpointSlice
type kubeDeploymentRuntimeEvidence = releaseproof.Deployment
type kubeEndpointRuntimeEvidence = managedappstatus.Endpoint

type kubeNamespaceIdentity struct {
	Metadata struct {
		UID string `json:"uid"`
	} `json:"metadata"`
}

func newManagedAppStatusCache(ttl, refreshTimeout time.Duration) managedAppStatusCache {
	return managedappstatus.NewCache(ttl, refreshTimeout)
}

func newManagedAppInventory(input managedAppStatusListInput) managedAppStatusListCacheEntry {
	return managedappstatus.NewInventory(input)
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

func managedAppAPIPath(namespace, name string) string {
	return "/apis/" + runtime.ManagedAppAPIGroup + "/v1alpha1/namespaces/" + url.PathEscape(strings.TrimSpace(namespace)) + "/" + runtime.ManagedAppPlural + "/" + url.PathEscape(strings.TrimSpace(name))
}

func (c *managedAppStatusClient) patchManagedAppBackingServicesSuspended(ctx context.Context, managed runtime.ManagedAppObject, suspended bool) error {
	if strings.TrimSpace(managed.Metadata.UID) == "" || strings.TrimSpace(managed.Metadata.ResourceVersion) == "" {
		return errors.New("managed PostgreSQL orphan lifecycle update requires exact UID and resource version")
	}
	ops := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": strings.TrimSpace(managed.Metadata.UID)},
		{"op": "test", "path": "/metadata/resourceVersion", "value": strings.TrimSpace(managed.Metadata.ResourceVersion)},
	}
	for index := range managed.Spec.BackingServices {
		postgres := managed.Spec.BackingServices[index].Spec.Postgres
		if !strings.EqualFold(strings.TrimSpace(managed.Spec.BackingServices[index].Type), model.BackingServiceTypePostgres) ||
			!strings.EqualFold(strings.TrimSpace(managed.Spec.BackingServices[index].Provisioner), model.BackingServiceProvisionerManaged) || postgres == nil {
			continue
		}
		if postgres.Suspended == suspended {
			continue
		}
		ops = append(ops, map[string]any{
			"op":    "add",
			"path":  fmt.Sprintf("/spec/backingServices/%d/spec/postgres/suspended", index),
			"value": suspended,
		})
	}
	if managed.Metadata.Annotations[runtime.FugueAnnotationOrphanPostgresLifecycle] != runtime.OrphanPostgresLifecycleExplicit {
		annotations := make(map[string]string, len(managed.Metadata.Annotations)+1)
		for key, value := range managed.Metadata.Annotations {
			annotations[key] = value
		}
		annotations[runtime.FugueAnnotationOrphanPostgresLifecycle] = runtime.OrphanPostgresLifecycleExplicit
		ops = append(ops, map[string]any{"op": "add", "path": "/metadata/annotations", "value": annotations})
	}
	if len(ops) == 2 {
		return nil
	}
	return c.doRequest(ctx, http.MethodPatch, managedAppAPIPath(managed.Metadata.Namespace, managed.Metadata.Name), "application/json-patch+json", ops, nil)
}

func (c *managedAppStatusClient) deleteManagedApp(ctx context.Context, managed runtime.ManagedAppObject) error {
	request := map[string]any{
		"apiVersion":        "v1",
		"kind":              "DeleteOptions",
		"propagationPolicy": "Foreground",
		"preconditions": map[string]string{
			"uid":             strings.TrimSpace(managed.Metadata.UID),
			"resourceVersion": strings.TrimSpace(managed.Metadata.ResourceVersion),
		},
	}
	return c.doRequest(ctx, http.MethodDelete, managedAppAPIPath(managed.Metadata.Namespace, managed.Metadata.Name), "application/json", request, nil)
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
		var list kubeTypedResourceList[runtime.ManagedAppObject]
		if err := c.doJSON(ctx, path, &list); err != nil {
			return nil, err
		}
		for _, managed := range list.Items {
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

func listTypedKubeResources[T any](ctx context.Context, c *managedAppStatusClient, basePath string) ([]T, error) {
	basePath = strings.TrimSpace(basePath)
	if basePath == "" {
		return nil, fmt.Errorf("kubernetes resource list path is empty")
	}
	items := make([]T, 0)
	seenContinue := make(map[string]struct{})
	path := basePath
	for {
		var list kubeTypedResourceList[T]
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
func listTypedKubeResourcesOptional[T any](ctx context.Context, c *managedAppStatusClient, basePath string) ([]T, bool, error) {
	items, err := listTypedKubeResources[T](ctx, c, basePath)
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
	var namespaceItems []kubeMetadataEvidence
	var serviceItems []kubeServiceRuntimeEvidence
	var deploymentItems []kubeDeploymentRuntimeEvidence
	var endpointItems []kubeEndpointsEvidence
	var endpointSliceItems []kubeEndpointSliceEvidence
	endpointsAvailable := false
	endpointSlicesAvailable := false
	var endpointsErr, endpointSlicesErr error
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		var err error
		namespaceItems, err = listTypedKubeResources[kubeMetadataEvidence](groupCtx, c, "/api/v1/namespaces")
		if err != nil {
			return fmt.Errorf("list kubernetes namespaces: %w", err)
		}
		return nil
	})
	group.Go(func() error {
		var err error
		deploymentItems, err = listTypedKubeResources[kubeDeploymentRuntimeEvidence](groupCtx, c, "/apis/apps/v1/deployments")
		if err != nil {
			return fmt.Errorf("list kubernetes deployments: %w", err)
		}
		return nil
	})
	group.Go(func() error {
		var err error
		serviceItems, err = listTypedKubeResources[kubeServiceRuntimeEvidence](groupCtx, c, "/api/v1/services")
		if err != nil {
			return fmt.Errorf("list kubernetes services: %w", err)
		}
		return nil
	})
	group.Go(func() error {
		endpointItems, endpointsAvailable, endpointsErr = listTypedKubeResourcesOptional[kubeEndpointsEvidence](groupCtx, c, "/api/v1/endpoints")
		return nil
	})
	group.Go(func() error {
		endpointSliceItems, endpointSlicesAvailable, endpointSlicesErr = listTypedKubeResourcesOptional[kubeEndpointSliceEvidence](groupCtx, c, "/apis/discovery.k8s.io/v1/endpointslices")
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
		serviceDetails:          make(map[string]kubeServiceRuntimeEvidence, len(serviceItems)),
		releaseEndpointSlices:   make(map[string][]kubeEndpointSliceEvidence),
		endpoints:               make(map[string]kubeEndpointRuntimeEvidence, len(endpointItems)),
		endpointSlices:          make(map[string]kubeEndpointRuntimeEvidence, len(endpointSliceItems)),
		endpointsAvailable:      endpointsAvailable,
		endpointSlicesAvailable: endpointSlicesAvailable,
	}

	for _, item := range namespaceItems {
		name := strings.TrimSpace(item.Metadata.Name)
		if name == "" {
			return managedAppKubeSnapshot{}, fmt.Errorf("decode kubernetes namespace: metadata.name is missing")
		}
		snapshot.namespaces[name] = struct{}{}
	}
	for _, deployment := range deploymentItems {
		if strings.TrimSpace(deployment.Metadata.Name) == "" || strings.TrimSpace(deployment.Metadata.Namespace) == "" {
			return managedAppKubeSnapshot{}, fmt.Errorf("decode kubernetes deployment: metadata name/namespace is missing")
		}
		snapshot.deployments[kubeNamespacedKey(deployment.Metadata.Namespace, deployment.Metadata.Name)] = deployment
	}
	for _, item := range serviceItems {
		if strings.TrimSpace(item.Metadata.Name) == "" || strings.TrimSpace(item.Metadata.Namespace) == "" {
			return managedAppKubeSnapshot{}, fmt.Errorf("decode kubernetes service: metadata name/namespace is missing")
		}
		snapshot.services[kubeNamespacedKey(item.Metadata.Namespace, item.Metadata.Name)] = struct{}{}
		snapshot.serviceDetails[kubeNamespacedKey(item.Metadata.Namespace, item.Metadata.Name)] = item
	}
	for _, item := range endpointItems {
		if strings.TrimSpace(item.Metadata.Name) == "" || strings.TrimSpace(item.Metadata.Namespace) == "" {
			return managedAppKubeSnapshot{}, fmt.Errorf("decode kubernetes endpoint: metadata name/namespace is missing")
		}
		evidence := kubeEndpointRuntimeEvidence{Present: true}
		for _, subset := range item.Subsets {
			evidence.ReadyAddresses += len(subset.Addresses)
			evidence.NotReadyAddresses += len(subset.NotReadyAddresses)
		}
		snapshot.endpoints[kubeNamespacedKey(item.Metadata.Namespace, item.Metadata.Name)] = evidence
	}
	for _, item := range endpointSliceItems {
		if strings.TrimSpace(item.Metadata.Name) == "" || strings.TrimSpace(item.Metadata.Namespace) == "" {
			return managedAppKubeSnapshot{}, fmt.Errorf("decode kubernetes endpoint slice: metadata name/namespace is missing")
		}
		serviceName := strings.TrimSpace(item.Metadata.Labels["kubernetes.io/service-name"])
		if serviceName == "" {
			continue
		}
		key := kubeNamespacedKey(item.Metadata.Namespace, serviceName)
		snapshot.releaseEndpointSlices[key] = append(snapshot.releaseEndpointSlices[key], item)
		evidence := snapshot.endpointSlices[key]
		evidence.Present = true
		for _, endpoint := range item.Endpoints {
			if endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready {
				evidence.ReadyAddresses += len(endpoint.Addresses)
			} else {
				evidence.NotReadyAddresses += len(endpoint.Addresses)
			}
		}
		snapshot.endpointSlices[key] = evidence
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

func kubeNamespacedKey(namespace, name string) string {
	return strings.TrimSpace(namespace) + "/" + strings.TrimSpace(name)
}

func (c *managedAppStatusClient) doJSON(ctx context.Context, apiPath string, out any) error {
	return c.doRequest(ctx, http.MethodGet, apiPath, "", nil, out)
}

func (c *managedAppStatusClient) doRequest(ctx context.Context, method, apiPath, contentType string, body any, out any) error {
	var payload io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal kubernetes request: %w", err)
		}
		payload = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+apiPath, payload)
	if err != nil {
		return fmt.Errorf("create kubernetes request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	req.Header.Set("Accept", "application/json")
	if strings.TrimSpace(contentType) != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("kubernetes request %s %s: %w", method, apiPath, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if readErr != nil {
			return fmt.Errorf("read kubernetes error response: %w", readErr)
		}
		return &kubeStatusError{StatusCode: resp.StatusCode, Message: fmt.Sprintf("kubernetes request %s %s failed: status=%d body=%s", method, apiPath, resp.StatusCode, strings.TrimSpace(string(responseBody)))}
	}
	if out != nil {
		limited := &io.LimitedReader{R: resp.Body, N: (32 << 20) + 1}
		decoder := json.NewDecoder(limited)
		if err := decoder.Decode(out); err != nil {
			return fmt.Errorf("decode kubernetes response: %w", err)
		}
		var trailing json.RawMessage
		if err := decoder.Decode(&trailing); err != io.EOF {
			return fmt.Errorf("kubernetes response contains trailing data or read error: %v", err)
		}
		if limited.N <= 0 {
			return fmt.Errorf("kubernetes response exceeds 32 MiB")
		}
	} else {
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			return fmt.Errorf("read kubernetes response: %w", err)
		}
	}

	return nil
}

func (s *Server) overlayManagedAppStatuses(ctx context.Context, apps []model.App) []model.App {
	if len(apps) == 0 {
		return apps
	}
	started := time.Now()
	runtimeByID := s.observationRuntimeByID(apps)
	serverTimingFromContext(ctx).Add("observation_runtimes", time.Since(started))

	cached, ok, expired := s.managedAppStatusCache.Inventory()
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
	cached, ok, expired := s.managedAppStatusCache.App(managedAppStatusCacheKey(app))
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
	cached, ok, expired := s.managedAppStatusCache.App(managedAppStatusCacheKey(app))
	if ok {
		if expired {
			s.refreshManagedAppStatusesAsync()
		}
		return s.applyManagedAppObservation(app, cached, s.observationRuntimeByID([]model.App{app}), !expired, "")
	}

	list, listOK, listExpired := s.managedAppStatusCache.Inventory()
	if listOK {
		if listExpired {
			s.refreshManagedAppStatusesAsync()
		}
		entry := list.App(strings.TrimSpace(app.ID))
		return s.applyManagedAppObservation(app, entry, s.observationRuntimeByID([]model.App{app}), !listExpired, "")
	}

	s.refreshManagedAppStatusesAsync()
	return s.applyUnknownManagedAppObservation([]model.App{app}, s.observationRuntimeByID([]model.App{app}), "live runtime observation is pending")[0]
}

func (s *Server) overlayManagedAppStatusesCached(apps []model.App) []model.App {
	if len(apps) == 0 {
		return apps
	}
	cached, ok, expired := s.managedAppStatusCache.Inventory()
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
	cached, ok, expired := s.managedAppStatusCache.Inventory()
	if ok {
		if expired {
			s.refreshManagedAppStatusesAsync()
		}
		// Cache expiry schedules a refresh; it does not invalidate a successful
		// runtime observation that is still inside the publication freshness SLA.
		// Marking that short refresh window stale makes edge routes flap on every
		// cache cycle even though the underlying evidence has not aged out.
		fresh := managedappstatus.TimestampFresh(cached.RefreshedAt, time.Now().UTC())
		return s.applyManagedAppListObservation(apps, cached, runtimeByID, fresh, ""), managedAppListObservationProvenance(apps, cached, expired, fresh)
	}
	s.refreshManagedAppStatusesAsync()
	return s.applyUnknownManagedAppObservation(apps, runtimeByID, "live runtime observation is pending"), managedAppUnknownObservationProvenance(apps)
}

func (s *Server) applyManagedAppObservation(app model.App, entry managedAppStatusCacheEntry, runtimeByID map[string]model.Runtime, fresh bool, errorMessage string) model.App {
	observed, refresh := managedappstatus.Project(app, entry, runtimeByID, fresh, errorMessage)
	if refresh {
		s.refreshManagedAppStatusesAsync()
	}
	return observed
}

func (s *Server) applyManagedAppListObservation(apps []model.App, entry managedAppStatusListCacheEntry, runtimeByID map[string]model.Runtime, fresh bool, errorMessage string) []model.App {
	out := make([]model.App, 0, len(apps))
	for _, app := range apps {
		out = append(out, s.applyManagedAppObservation(app, entry.App(strings.TrimSpace(app.ID)), runtimeByID, fresh, errorMessage))
	}
	return out
}

func (s *Server) applyUnknownManagedAppObservation(apps []model.App, runtimeByID map[string]model.Runtime, errorMessage string) []model.App {
	entry := managedAppStatusCacheEntry{Ok: false, RefreshedAt: time.Now().UTC()}
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
	return managedappstatus.ObservationKey(app)
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
	timeout := s.managedAppStatusCache.RefreshTimeout()
	if parent == nil {
		return context.WithTimeout(context.Background(), timeout)
	}
	return context.WithTimeout(parent, timeout)
}

func (s *Server) shouldLogManagedAppStatusError(err error) bool {
	return err != nil && !errors.Is(err, errManagedAppStatusClientUnavailable) && !errors.Is(err, managedappstatus.ErrRefreshBackoff)
}

func (s *Server) buildManagedAppRuntimeEvidence(
	app model.App,
	managed runtime.ManagedAppObject,
	found bool,
	snapshot managedAppKubeSnapshot,
) (managedAppRuntimeEvidence, error) {
	return s.buildManagedAppRuntimeEvidenceWithStoreSnapshot(app, managed, found, snapshot, nil)
}

func (s *Server) buildManagedAppRuntimeEvidenceWithStoreSnapshot(
	app model.App,
	managed runtime.ManagedAppObject,
	found bool,
	snapshot managedAppKubeSnapshot,
	storeSnapshot *managedAppStoreSnapshot,
) (managedAppRuntimeEvidence, error) {
	serving, foundServing := s.servingReleaseTrafficTargetWithSnapshot(app, storeSnapshot)
	inputs := managedappstatus.EvidenceInputs{
		ServingRelease: serving, HasServingRelease: foundServing,
		ImagesEquivalent: func(expected, actual string) bool {
			return s.observedRuntimeImageRefsEquivalent(app, expected, actual)
		},
	}
	if s != nil && s.store != nil {
		inputs.ReadImagePresence = func(ref, runtimeID string) (*bool, managedappstatus.ImageLocationObservation, error) {
			value, obs, err := s.currentManagedImagePresenceWithStoreSnapshot(app, ref, runtimeID, storeSnapshot)
			return value, managedappstatus.ImageLocationObservation{Status: obs.status, Source: obs.source, ObservedAt: obs.observedAt}, err
		}
	}
	return managedappstatus.EvaluateEvidence(app, managed, found, managedappstatus.RuntimeSnapshot{
		Namespaces: snapshot.namespaces, Deployments: snapshot.deployments, Services: snapshot.services,
		Endpoints: snapshot.endpoints, EndpointSlices: snapshot.endpointSlices, EndpointSlicesAvailable: snapshot.endpointSlicesAvailable,
	}, inputs)
}

func (s *Server) servingReleaseTrafficTarget(app model.App) (model.AppRelease, bool) {
	return s.servingReleaseTrafficTargetWithSnapshot(app, nil)
}

func (s *Server) servingReleaseTrafficTargetWithSnapshot(app model.App, snapshot *managedAppStoreSnapshot) (model.AppRelease, bool) {
	if s == nil || s.store == nil || strings.TrimSpace(app.ID) == "" {
		return model.AppRelease{}, false
	}
	var policy model.AppTrafficPolicy
	var err error
	if snapshot == nil {
		policy, err = s.store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	} else {
		policy = snapshot.policies[app.ID]
	}
	if err != nil || !strings.EqualFold(strings.TrimSpace(policy.Mode), model.AppTrafficModeSingle) ||
		policy.StableWeight != 100 || policy.CandidateWeight != 0 || strings.TrimSpace(policy.StableReleaseID) == "" {
		return model.AppRelease{}, false
	}
	var release model.AppRelease
	if snapshot == nil {
		release, err = s.store.GetAppRelease(app.TenantID, true, policy.StableReleaseID)
	} else {
		release = snapshot.releases[policy.StableReleaseID]
	}
	release = releaseflow.CanonicalStableTarget(app, release)
	if err != nil || strings.TrimSpace(release.AppID) != strings.TrimSpace(app.ID) ||
		!strings.EqualFold(strings.TrimSpace(release.Role), model.AppReleaseRoleStable) ||
		!(strings.EqualFold(strings.TrimSpace(release.Status), model.AppReleaseStatusServing) || strings.EqualFold(strings.TrimSpace(release.Status), model.AppReleaseStatusReady)) ||
		strings.TrimSpace(release.DeploymentName) == "" || strings.TrimSpace(release.ResolvedImageRef) == "" ||
		!s.observedRuntimeImageRefsEquivalent(app, app.Spec.Image, release.ResolvedImageRef) {
		return model.AppRelease{}, false
	}
	return release, true
}

func deploymentCurrentCohortComplete(deployment kubeDeploymentRuntimeEvidence) bool {
	return releaseproof.DeploymentCurrentCohortComplete(deployment)
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
	return s.currentManagedImagePresenceWithStoreSnapshot(app, imageRef, runtimeID, nil)
}

func (s *Server) currentManagedImagePresenceWithStoreSnapshot(app model.App, imageRef, runtimeID string, snapshot *managedAppStoreSnapshot) (*bool, managedImageLocationObservation, error) {
	// Image-location rows without a runtime identity cannot be tied to the
	// current migration target (and may have been written by the source
	// cluster). They are diagnostic history, not observed runtime evidence.
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return nil, managedImageLocationObservation{}, nil
	}
	listLocations := s.store.ListImageLocations
	if snapshot != nil {
		listLocations = snapshot.imageLocations
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
			locations, err := listLocations(model.ImageLocationFilter{
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
	sequence := managedAppObservationSequence{RefreshStarted: s.managedAppStatusCache.NextSequence()}
	s.logManagedAppRefreshEvent("by_app", "start", "started", sequence.RefreshStarted, nil)
	defer func() {
		if sequence.RefreshCompleted == 0 {
			failedSequence := s.managedAppStatusCache.NextSequence()
			s.logManagedAppRefreshEvent("by_app", "end", "error", failedSequence, err)
		}
	}()
	sequence.DurableAppsRead = s.managedAppStatusCache.NextSequence()
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
	sequence.ManagedAppsRead = s.managedAppStatusCache.NextSequence()
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
		sequence.KubeSnapshotRead = s.managedAppStatusCache.NextSequence()
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
	sequence.RefreshCompleted = s.managedAppStatusCache.NextSequence()
	entry = managedAppStatusCacheEntry{
		Managed:     managed,
		Found:       found,
		Ok:          true,
		ClusterID:   clusterID,
		Evidence:    evidence,
		RefreshedAt: now,
		ExpiresAt:   now.Add(s.managedAppStatusCache.TTL()),
		Sequence:    sequence,
	}
	s.managedAppStatusCache.PublishApp(managedAppStatusCacheKey(app), entry)
	s.logManagedAppRefreshEvent("by_app", "end", "success", sequence.RefreshCompleted, nil)
	return entry, nil
}

func (s *Server) refreshManagedAppStatus(ctx context.Context, app model.App) (managedAppStatusCacheEntry, error) {
	return s.managedAppStatusCache.RefreshApp(managedAppStatusCacheKey(app), func() (managedAppStatusCacheEntry, error) {
		return s.fetchManagedAppStatus(ctx, app)
	})
}

func (s *Server) fetchManagedAppStatuses(ctx context.Context) (managedAppStatusListCacheEntry, error) {
	return s.fetchManagedAppInventoryWithClusterIdentity(ctx, true)
}

func (s *Server) fetchManagedAppInventory(ctx context.Context) (managedAppStatusListCacheEntry, error) {
	return s.fetchManagedAppInventoryWithClusterIdentity(ctx, false)
}

func (s *Server) fetchManagedAppInventoryWithClusterIdentity(ctx context.Context, requireClusterIdentity bool) (entry managedAppStatusListCacheEntry, err error) {
	started := time.Now()
	timings := serverTimingFromContext(ctx)
	mark := func(name string) {
		timings.Add(name, time.Since(started))
		started = time.Now()
	}
	cacheLayer := "inventory"
	if requireClusterIdentity {
		cacheLayer = "list"
	}
	sequence := managedAppObservationSequence{RefreshStarted: s.managedAppStatusCache.NextSequence()}
	s.logManagedAppRefreshEvent(cacheLayer, "start", "started", sequence.RefreshStarted, nil)
	defer func() {
		if sequence.RefreshCompleted == 0 {
			failedSequence := s.managedAppStatusCache.NextSequence()
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
	mark("observation_cluster_identity")
	var items map[string]runtime.ManagedAppObject
	if requireClusterIdentity {
		items, err = client.listObservedManagedAppsByAppID(refreshCtx)
	} else {
		items, err = client.listManagedAppsByAppID(refreshCtx)
	}
	if err != nil {
		return managedAppStatusListCacheEntry{}, err
	}
	mark("observation_managed_objects")
	sequence.ManagedAppsRead = s.managedAppStatusCache.NextSequence()
	if requireClusterIdentity {
		confirmedClusterID, err := client.getClusterID(refreshCtx)
		if err != nil {
			return managedAppStatusListCacheEntry{}, err
		}
		if confirmedClusterID != clusterID {
			return managedAppStatusListCacheEntry{}, fmt.Errorf("kubernetes cluster identity changed during managed app inventory observation")
		}
	}
	mark("observation_identity_confirmation")
	evidenceByAppID := make(map[string]managedAppRuntimeEvidence)
	if requireClusterIdentity && s != nil && s.store != nil {
		snapshot, snapshotErr := client.readRuntimeSnapshot(refreshCtx)
		if snapshotErr != nil {
			return managedAppStatusListCacheEntry{}, snapshotErr
		}
		mark("observation_kubernetes_snapshot")
		sequence.KubeSnapshotRead = s.managedAppStatusCache.NextSequence()
		finalClusterID, finalErr := client.getClusterID(refreshCtx)
		if finalErr != nil {
			return managedAppStatusListCacheEntry{}, finalErr
		}
		if finalClusterID != clusterID {
			return managedAppStatusListCacheEntry{}, fmt.Errorf("kubernetes cluster identity changed during runtime evidence observation")
		}
		mark("observation_identity_final")
		var apps []model.App
		if s != nil && s.store != nil {
			apps, err = s.listAppSummariesWithTiming(refreshCtx, "", true, true)
			if err != nil {
				return managedAppStatusListCacheEntry{}, fmt.Errorf("list apps for runtime evidence: %w", err)
			}
			sequence.DurableAppsRead = s.managedAppStatusCache.NextSequence()
		}
		mark("observation_durable_apps")
		appsByID := make(map[string]model.App, len(apps))
		appIDs := make([]string, 0, len(apps))
		for _, app := range apps {
			appIDs = append(appIDs, strings.TrimSpace(app.ID))
		}
		storeSnapshot, snapshotErr := s.loadManagedAppStoreSnapshotScoped(refreshCtx, apps, items, appIDs...)
		if snapshotErr != nil {
			return managedAppStatusListCacheEntry{}, snapshotErr
		}
		mark("observation_durable_evidence")
		for _, app := range apps {
			appsByID[strings.TrimSpace(app.ID)] = app
		}
		for appID, managed := range items {
			app, ok := appsByID[strings.TrimSpace(appID)]
			if !ok {
				continue
			}
			evidence, evidenceErr := s.buildManagedAppRuntimeEvidenceWithStoreSnapshot(app, managed, true, snapshot, storeSnapshot)
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
			evidence, evidenceErr := s.buildManagedAppRuntimeEvidenceWithStoreSnapshot(app, runtime.ManagedAppObject{}, false, snapshot, storeSnapshot)
			if evidenceErr != nil {
				return managedAppStatusListCacheEntry{}, evidenceErr
			}
			evidenceByAppID[appID] = evidence
		}
		mark("observation_evidence_compute")
	}

	now := time.Now().UTC()
	sequence.RefreshCompleted = s.managedAppStatusCache.NextSequence()
	entry = newManagedAppInventory(managedAppStatusListInput{
		Items:       items,
		Evidence:    evidenceByAppID,
		Ok:          true,
		ClusterID:   clusterID,
		RefreshedAt: now,
		ExpiresAt:   now.Add(s.managedAppStatusCache.TTL()),
		Sequence:    sequence,
	})
	// Only a cluster-identified inventory may populate the observed-status
	// cache.  The backing-service inventory path intentionally predates the
	// observed contract and may be used with a lightweight list-only client;
	// allowing it to overwrite this cache would erase cluster identity and
	// fresh runtime evidence, making Console/Edge report unknown or (worse)
	// reuse an unrelated inventory as observed state.
	if requireClusterIdentity {
		s.managedAppStatusCache.PublishInventory(entry)
	}
	s.logManagedAppRefreshEvent(cacheLayer, "end", "success", sequence.RefreshCompleted, nil)
	return entry, nil
}

func (s *Server) refreshManagedAppInventory(ctx context.Context) (managedAppStatusListCacheEntry, error) {
	return s.managedAppStatusCache.ReadInventory(func() (managedAppStatusListCacheEntry, error) {
		return s.fetchManagedAppInventory(ctx)
	})
}

func (s *Server) refreshManagedAppStatuses(ctx context.Context) (managedAppStatusListCacheEntry, error) {
	return s.managedAppStatusCache.RefreshInventory(func() (managedAppStatusListCacheEntry, error) {
		return s.fetchManagedAppStatuses(ctx)
	})
}

func (s *Server) refreshManagedAppStatusesAsync() {
	s.managedAppStatusCache.RefreshInventoryAsync(func() (managedAppStatusListCacheEntry, error) {
		return s.fetchManagedAppStatuses(context.Background())
	}, func(err error) {
		if s.shouldLogManagedAppStatusError(err) && s.log != nil {
			s.log.Printf("managed app status background list refresh error: %v", err)
		}
	})
}
