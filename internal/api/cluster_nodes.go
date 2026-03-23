package api

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

type clusterNodeClient struct {
	client      *http.Client
	baseURL     string
	bearerToken string
}

type kubeNodeList struct {
	Items []kubeNode `json:"items"`
}

type kubeNode struct {
	Metadata struct {
		Name              string            `json:"name"`
		CreationTimestamp string            `json:"creationTimestamp"`
		Labels            map[string]string `json:"labels"`
	} `json:"metadata"`
	Status struct {
		Addresses []struct {
			Type    string `json:"type"`
			Address string `json:"address"`
		} `json:"addresses"`
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
		NodeInfo struct {
			KubeletVersion   string `json:"kubeletVersion"`
			OSImage          string `json:"osImage"`
			KernelVersion    string `json:"kernelVersion"`
			ContainerRuntime string `json:"containerRuntimeVersion"`
		} `json:"nodeInfo"`
	} `json:"status"`
}

func (s *Server) handleListClusterNodes(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)

	client, err := newClusterNodeClient()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	nodes, err := client.listClusterNodes(r.Context())
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	runtimes, err := s.store.ListNodes(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	runtimeByClusterNode := make(map[string]model.Runtime, len(runtimes))
	for _, runtime := range runtimes {
		name := strings.TrimSpace(runtime.ClusterNodeName)
		if name == "" {
			continue
		}
		if existing, ok := runtimeByClusterNode[name]; ok && existing.UpdatedAt.After(runtime.UpdatedAt) {
			continue
		}
		runtimeByClusterNode[name] = runtime
	}

	filtered := make([]model.ClusterNode, 0, len(nodes))
	for _, node := range nodes {
		runtime, ok := runtimeByClusterNode[node.Name]
		if !principal.IsPlatformAdmin() && !ok {
			continue
		}
		if ok {
			node.RuntimeID = runtime.ID
			node.TenantID = runtime.TenantID
		}
		filtered = append(filtered, node)
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].CreatedAt != nil && filtered[j].CreatedAt != nil && !filtered[i].CreatedAt.Equal(*filtered[j].CreatedAt) {
			return filtered[i].CreatedAt.Before(*filtered[j].CreatedAt)
		}
		return filtered[i].Name < filtered[j].Name
	})

	httpx.WriteJSON(w, http.StatusOK, map[string]any{"cluster_nodes": filtered})
}

func newClusterNodeClient() (*clusterNodeClient, error) {
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

	return &clusterNodeClient{
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

func (c *clusterNodeClient) listClusterNodes(ctx context.Context) ([]model.ClusterNode, error) {
	var nodeList kubeNodeList
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/nodes", &nodeList); err != nil {
		return nil, err
	}

	nodes := make([]model.ClusterNode, 0, len(nodeList.Items))
	for _, item := range nodeList.Items {
		node := model.ClusterNode{
			Name:             strings.TrimSpace(item.Metadata.Name),
			Status:           kubeNodeReadyStatus(item),
			Roles:            kubeNodeRoles(item.Metadata.Labels),
			InternalIP:       kubeNodeAddress(item, "InternalIP"),
			ExternalIP:       kubeNodeAddress(item, "ExternalIP"),
			KubeletVersion:   strings.TrimSpace(item.Status.NodeInfo.KubeletVersion),
			OSImage:          strings.TrimSpace(item.Status.NodeInfo.OSImage),
			KernelVersion:    strings.TrimSpace(item.Status.NodeInfo.KernelVersion),
			ContainerRuntime: strings.TrimSpace(item.Status.NodeInfo.ContainerRuntime),
		}
		if createdAt := parseClusterNodeTimestamp(item.Metadata.CreationTimestamp); createdAt != nil {
			node.CreatedAt = createdAt
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (c *clusterNodeClient) doJSON(ctx context.Context, method, apiPath string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+apiPath, nil)
	if err != nil {
		return fmt.Errorf("create kubernetes request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("kubernetes request %s %s: %w", method, apiPath, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("kubernetes request %s %s failed: status=%d body=%s", method, apiPath, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if out != nil && len(body) > 0 {
		if err := json.Unmarshal(body, out); err != nil {
			return fmt.Errorf("decode kubernetes response: %w", err)
		}
	}
	return nil
}

func kubeNodeReadyStatus(node kubeNode) string {
	for _, condition := range node.Status.Conditions {
		if strings.EqualFold(strings.TrimSpace(condition.Type), "Ready") {
			switch strings.ToLower(strings.TrimSpace(condition.Status)) {
			case "true":
				return "ready"
			case "false":
				return "not-ready"
			default:
				return "unknown"
			}
		}
	}
	return "unknown"
}

func kubeNodeRoles(labels map[string]string) []string {
	roles := make([]string, 0, 2)
	for key := range labels {
		if strings.HasPrefix(key, "node-role.kubernetes.io/") {
			role := strings.TrimPrefix(key, "node-role.kubernetes.io/")
			role = strings.TrimSpace(role)
			if role == "" {
				role = "worker"
			}
			roles = append(roles, role)
		}
	}
	if legacyRole := strings.TrimSpace(labels["kubernetes.io/role"]); legacyRole != "" {
		roles = append(roles, legacyRole)
	}
	if len(roles) == 0 {
		return nil
	}
	sort.Strings(roles)
	deduped := roles[:0]
	var prev string
	for _, role := range roles {
		if role == prev {
			continue
		}
		deduped = append(deduped, role)
		prev = role
	}
	return deduped
}

func kubeNodeAddress(node kubeNode, addressType string) string {
	for _, address := range node.Status.Addresses {
		if strings.EqualFold(strings.TrimSpace(address.Type), addressType) {
			return strings.TrimSpace(address.Address)
		}
	}
	return ""
}

func parseClusterNodeTimestamp(value string) *time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil
	}
	parsed = parsed.UTC()
	return &parsed
}
