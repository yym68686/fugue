package runtime

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"fugue/internal/model"
)

const (
	serviceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	serviceAccountCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

func ApplyManagedApp(app model.App, constraints ...SchedulingConstraints) error {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return fmt.Errorf("kubernetes service host/port is not available in the environment")
	}
	token, err := os.ReadFile(serviceAccountTokenPath)
	if err != nil {
		return fmt.Errorf("read service account token: %w", err)
	}
	caData, err := os.ReadFile(serviceAccountCAPath)
	if err != nil {
		return fmt.Errorf("read service account CA: %w", err)
	}

	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caData) {
		return fmt.Errorf("load service account CA")
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: rootCAs},
		},
	}
	baseURL := "https://" + host + ":" + port

	var scheduling SchedulingConstraints
	if len(constraints) > 0 {
		scheduling = constraints[0]
	}

	for _, obj := range buildAppObjects(app, scheduling) {
		apiPath, err := ObjectAPIPath(NamespaceForTenant(app.TenantID), obj)
		if err != nil {
			return err
		}
		if err := applyObject(client, baseURL, string(token), apiPath, obj); err != nil {
			return err
		}
	}
	return nil
}

func applyObject(client *http.Client, baseURL, bearerToken, apiPath string, obj map[string]any) error {
	payload, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("marshal object: %w", err)
	}

	u, err := url.Parse(baseURL + apiPath)
	if err != nil {
		return fmt.Errorf("parse apply url: %w", err)
	}
	query := u.Query()
	query.Set("fieldManager", "fugue")
	query.Set("force", "true")
	u.RawQuery = query.Encode()

	req, err := http.NewRequest(http.MethodPatch, u.String(), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create patch request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(bearerToken))
	req.Header.Set("Content-Type", "application/apply-patch+yaml")
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("patch object: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func ObjectAPIPath(defaultNamespace string, obj map[string]any) (string, error) {
	apiVersion, _ := obj["apiVersion"].(string)
	kind, _ := obj["kind"].(string)
	metadata, _ := obj["metadata"].(map[string]any)
	name, _ := metadata["name"].(string)
	namespace, _ := metadata["namespace"].(string)
	if namespace == "" {
		namespace = defaultNamespace
	}

	switch {
	case apiVersion == "v1" && kind == "Namespace":
		return "/api/v1/namespaces/" + name, nil
	case apiVersion == "v1" && kind == "Secret":
		return "/api/v1/namespaces/" + namespace + "/secrets/" + name, nil
	case apiVersion == "v1" && kind == "Service":
		return "/api/v1/namespaces/" + namespace + "/services/" + name, nil
	case apiVersion == "apps/v1" && kind == "Deployment":
		return "/apis/apps/v1/namespaces/" + namespace + "/deployments/" + name, nil
	case apiVersion == ManagedAppAPIVersion && kind == ManagedAppKind:
		return "/apis/" + ManagedAppAPIGroup + "/v1alpha1/namespaces/" + namespace + "/" + ManagedAppPlural + "/" + name, nil
	default:
		return "", fmt.Errorf("unsupported object %s %s", apiVersion, kind)
	}
}

func objectAPIPath(defaultNamespace string, obj map[string]any) (string, error) {
	return ObjectAPIPath(defaultNamespace, obj)
}
