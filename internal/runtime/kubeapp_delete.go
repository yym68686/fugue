package runtime

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"fugue/internal/model"
)

func DeleteManagedApp(app model.App, constraints ...SchedulingConstraints) error {
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

	objects := buildAppObjects(app, scheduling)
	for index := len(objects) - 1; index >= 0; index-- {
		obj := objects[index]
		if kind, _ := obj["kind"].(string); kind == "Namespace" {
			continue
		}
		apiPath, err := objectAPIPath(NamespaceForTenant(app.TenantID), obj)
		if err != nil {
			return err
		}
		if err := deleteObject(client, baseURL, string(token), apiPath); err != nil {
			return err
		}
	}
	return nil
}

func deleteObject(client *http.Client, baseURL, bearerToken, apiPath string) error {
	req, err := http.NewRequest(http.MethodDelete, baseURL+apiPath, nil)
	if err != nil {
		return fmt.Errorf("create delete request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(bearerToken))
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
