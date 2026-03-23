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
	"sort"
	"strconv"
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
	namespace := NamespaceForTenant(app.TenantID)
	labels := map[string]string{
		"app.kubernetes.io/name":       sanitizeName(app.Name),
		"app.kubernetes.io/managed-by": "fugue",
	}

	if err := applyObject(client, baseURL, string(token), "/api/v1/namespaces/"+namespace, map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata": map[string]any{
			"name": namespace,
		},
	}); err != nil {
		return fmt.Errorf("apply namespace: %w", err)
	}

	var scheduling SchedulingConstraints
	if len(constraints) > 0 {
		scheduling = constraints[0]
	}

	if err := applyObject(client, baseURL, string(token), "/apis/apps/v1/namespaces/"+namespace+"/deployments/"+sanitizeName(app.Name), buildDeploymentObject(namespace, app, labels, scheduling)); err != nil {
		return fmt.Errorf("apply deployment: %w", err)
	}
	if err := applyObject(client, baseURL, string(token), "/api/v1/namespaces/"+namespace+"/services/"+sanitizeName(app.Name), buildServiceObject(namespace, app, labels)); err != nil {
		return fmt.Errorf("apply service: %w", err)
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

func buildDeploymentObject(namespace string, app model.App, labels map[string]string, scheduling SchedulingConstraints) map[string]any {
	container := map[string]any{
		"name":  sanitizeName(app.Name),
		"image": app.Spec.Image,
	}
	if len(app.Spec.Command) > 0 {
		container["command"] = app.Spec.Command
	}
	if len(app.Spec.Args) > 0 {
		container["args"] = app.Spec.Args
	}
	if len(app.Spec.Ports) > 0 {
		ports := make([]map[string]any, 0, len(app.Spec.Ports))
		for _, port := range app.Spec.Ports {
			ports = append(ports, map[string]any{
				"containerPort": port,
				"protocol":      "TCP",
			})
		}
		container["ports"] = ports
	}
	if len(app.Spec.Env) > 0 {
		envKeys := make([]string, 0, len(app.Spec.Env))
		for key := range app.Spec.Env {
			envKeys = append(envKeys, key)
		}
		sort.Strings(envKeys)
		env := make([]map[string]string, 0, len(envKeys))
		for _, key := range envKeys {
			env = append(env, map[string]string{
				"name":  key,
				"value": app.Spec.Env[key],
			})
		}
		container["env"] = env
	}

	podSpec := map[string]any{
		"containers": []map[string]any{container},
	}
	if len(scheduling.NodeSelector) > 0 {
		nodeSelector := make(map[string]string, len(scheduling.NodeSelector))
		for key, value := range scheduling.NodeSelector {
			nodeSelector[key] = value
		}
		podSpec["nodeSelector"] = nodeSelector
	}
	if len(scheduling.Tolerations) > 0 {
		tolerations := make([]map[string]any, 0, len(scheduling.Tolerations))
		for _, toleration := range scheduling.Tolerations {
			tolerations = append(tolerations, map[string]any{
				"key":      toleration.Key,
				"operator": toleration.Operator,
				"value":    toleration.Value,
				"effect":   toleration.Effect,
			})
		}
		podSpec["tolerations"] = tolerations
	}

	return map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      sanitizeName(app.Name),
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": map[string]any{
			"replicas": app.Spec.Replicas,
			"selector": map[string]any{
				"matchLabels": labels,
			},
			"template": map[string]any{
				"metadata": map[string]any{
					"labels": labels,
				},
				"spec": podSpec,
			},
		},
	}
}

func buildServiceObject(namespace string, app model.App, labels map[string]string) map[string]any {
	servicePorts := make([]map[string]any, 0, len(app.Spec.Ports))
	for _, port := range app.Spec.Ports {
		servicePorts = append(servicePorts, map[string]any{
			"name":       "tcp-" + strconv.Itoa(port),
			"port":       port,
			"targetPort": port,
			"protocol":   "TCP",
		})
	}
	if len(servicePorts) == 0 {
		servicePorts = append(servicePorts, map[string]any{
			"name":       "tcp-80",
			"port":       80,
			"targetPort": 80,
			"protocol":   "TCP",
		})
	}

	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      sanitizeName(app.Name),
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": map[string]any{
			"selector": labels,
			"ports":    servicePorts,
		},
	}
}
