package runtime

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

func DeleteTenantNamespace(tenantID string) error {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return fmt.Errorf("tenant id is required")
	}

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

	req, err := http.NewRequest(http.MethodDelete, "https://"+host+":"+port+"/api/v1/namespaces/"+NamespaceForTenant(tenantID), nil)
	if err != nil {
		return fmt.Errorf("create namespace delete request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("delete namespace: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("delete namespace: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
