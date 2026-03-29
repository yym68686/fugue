package controller

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
)

const serviceAccountNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

var (
	errKubeNotFound = errors.New("kubernetes resource not found")
	errKubeConflict = errors.New("kubernetes resource conflict")
)

type kubeClient struct {
	client      *http.Client
	baseURL     string
	bearerToken string
	namespace   string
}

type kubeLease struct {
	APIVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Metadata   struct {
		Name            string `json:"name,omitempty"`
		Namespace       string `json:"namespace,omitempty"`
		ResourceVersion string `json:"resourceVersion,omitempty"`
	} `json:"metadata"`
	Spec kubeLeaseSpec `json:"spec"`
}

type kubeLeaseSpec struct {
	HolderIdentity       string `json:"holderIdentity,omitempty"`
	LeaseDurationSeconds int    `json:"leaseDurationSeconds,omitempty"`
	AcquireTime          string `json:"acquireTime,omitempty"`
	RenewTime            string `json:"renewTime,omitempty"`
	LeaseTransitions     int    `json:"leaseTransitions,omitempty"`
}

type kubePodList struct {
	Items []kubePod `json:"items"`
}

type kubeJobList struct {
	Items []kubeJobInfo `json:"items"`
}

type kubeJobInfo struct {
	Metadata struct {
		Name   string            `json:"name"`
		Labels map[string]string `json:"labels,omitempty"`
	} `json:"metadata"`
	Status kubeJobStatus `json:"status"`
}

type kubeJobStatus struct {
	Active int `json:"active,omitempty"`
}

type kubePod struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		Containers []struct {
			Name string `json:"name"`
		} `json:"containers"`
	} `json:"spec"`
	Status struct {
		Phase string `json:"phase"`
	} `json:"status"`
}

func newKubeClient(namespace string) (*kubeClient, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
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

	if strings.TrimSpace(namespace) == "" {
		if namespaceData, err := os.ReadFile(serviceAccountNamespacePath); err == nil {
			namespace = strings.TrimSpace(string(namespaceData))
		}
	}
	if strings.TrimSpace(namespace) == "" {
		return nil, fmt.Errorf("resolve kubernetes namespace")
	}

	return &kubeClient{
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: rootCAs},
			},
			Timeout: 10 * time.Second,
		},
		baseURL:     "https://" + host + ":" + port,
		bearerToken: strings.TrimSpace(string(token)),
		namespace:   strings.TrimSpace(namespace),
	}, nil
}

func (c *kubeClient) effectiveNamespace(namespace string) string {
	namespace = strings.TrimSpace(namespace)
	if namespace != "" {
		return namespace
	}
	return c.namespace
}

func (c *kubeClient) getLease(ctx context.Context, namespace, name string) (kubeLease, bool, error) {
	var lease kubeLease
	status, err := c.doJSON(ctx, http.MethodGet, "/apis/coordination.k8s.io/v1/namespaces/"+c.effectiveNamespace(namespace)+"/leases/"+url.PathEscape(name), nil, &lease)
	if err != nil {
		if status == http.StatusNotFound {
			return kubeLease{}, false, nil
		}
		return kubeLease{}, false, err
	}
	return lease, true, nil
}

func (c *kubeClient) listJobsBySelector(ctx context.Context, namespace, selector string) ([]kubeJobInfo, error) {
	query := url.Values{}
	if strings.TrimSpace(selector) != "" {
		query.Set("labelSelector", selector)
	}

	var jobList kubeJobList
	apiPath := "/apis/batch/v1/namespaces/" + c.effectiveNamespace(namespace) + "/jobs"
	if encoded := query.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	_, err := c.doJSON(ctx, http.MethodGet, apiPath, nil, &jobList)
	if err != nil {
		return nil, err
	}
	return jobList.Items, nil
}

func (c *kubeClient) deleteJob(ctx context.Context, namespace, name string) error {
	apiPath := "/apis/batch/v1/namespaces/" + c.effectiveNamespace(namespace) + "/jobs/" + url.PathEscape(strings.TrimSpace(name)) + "?propagationPolicy=Background"
	status, err := c.doJSON(ctx, http.MethodDelete, apiPath, nil, nil)
	if status == http.StatusNotFound {
		return nil
	}
	return err
}

func (c *kubeClient) createLease(ctx context.Context, namespace string, lease kubeLease) error {
	status, err := c.doJSON(ctx, http.MethodPost, "/apis/coordination.k8s.io/v1/namespaces/"+c.effectiveNamespace(namespace)+"/leases", lease, nil)
	if status == http.StatusConflict {
		return errKubeConflict
	}
	return err
}

func (c *kubeClient) updateLease(ctx context.Context, namespace string, lease kubeLease) error {
	status, err := c.doJSON(ctx, http.MethodPut, "/apis/coordination.k8s.io/v1/namespaces/"+c.effectiveNamespace(namespace)+"/leases/"+url.PathEscape(lease.Metadata.Name), lease, nil)
	if status == http.StatusConflict {
		return errKubeConflict
	}
	if status == http.StatusNotFound {
		return errKubeNotFound
	}
	return err
}

func (c *kubeClient) podWithContainerExists(ctx context.Context, namespace, labelSelector, containerName string) (bool, error) {
	query := url.Values{}
	if strings.TrimSpace(labelSelector) != "" {
		query.Set("labelSelector", labelSelector)
	}

	var podList kubePodList
	apiPath := "/api/v1/namespaces/" + c.effectiveNamespace(namespace) + "/pods"
	if encoded := query.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	_, err := c.doJSON(ctx, http.MethodGet, apiPath, nil, &podList)
	if err != nil {
		return false, err
	}

	for _, pod := range podList.Items {
		if pod.Status.Phase == "Succeeded" || pod.Status.Phase == "Failed" {
			continue
		}
		for _, container := range pod.Spec.Containers {
			if container.Name == containerName {
				return true, nil
			}
		}
	}
	return false, nil
}

func (c *kubeClient) doJSON(ctx context.Context, method, apiPath string, body any, out any) (int, error) {
	var payload io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, fmt.Errorf("marshal kubernetes request: %w", err)
		}
		payload = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+apiPath, payload)
	if err != nil {
		return 0, fmt.Errorf("create kubernetes request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("kubernetes request %s %s: %w", method, apiPath, err)
	}
	defer resp.Body.Close()

	responseBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("kubernetes request %s %s failed: status=%d body=%s", method, apiPath, resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	if out != nil && len(responseBody) > 0 {
		if err := json.Unmarshal(responseBody, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode kubernetes response: %w", err)
		}
	}
	return resp.StatusCode, nil
}

func parseKubeTimestamp(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed.UTC()
}

func formatKubeTimestamp(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format("2006-01-02T15:04:05.000000Z07:00")
}
