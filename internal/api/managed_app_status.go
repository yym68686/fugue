package api

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

type managedAppStatusClient struct {
	client      *http.Client
	baseURL     string
	bearerToken string
}

type managedAppList struct {
	Items []map[string]any `json:"items"`
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

	clientFactory := s.newManagedAppStatusClient
	if clientFactory == nil {
		clientFactory = newManagedAppStatusClient
	}
	client, err := clientFactory()
	if err != nil {
		return apps
	}

	managedByAppID, err := client.listManagedAppsByAppID(ctx)
	if err != nil {
		s.log.Printf("managed app status overlay list error: %v", err)
		return apps
	}

	out := make([]model.App, 0, len(apps))
	for _, app := range apps {
		managed, ok := managedByAppID[strings.TrimSpace(app.ID)]
		if ok {
			app = runtime.OverlayAppStatusFromManagedApp(app, managed)
		}
		out = append(out, app)
	}
	return out
}

func (s *Server) overlayManagedAppStatus(ctx context.Context, app model.App) model.App {
	clientFactory := s.newManagedAppStatusClient
	if clientFactory == nil {
		clientFactory = newManagedAppStatusClient
	}
	client, err := clientFactory()
	if err != nil {
		return app
	}

	managed, found, err := client.getManagedApp(ctx, app)
	if err != nil {
		s.log.Printf("managed app status overlay get error for app %s: %v", app.ID, err)
		return app
	}
	if !found {
		return app
	}
	return runtime.OverlayAppStatusFromManagedApp(app, managed)
}
