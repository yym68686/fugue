package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"fugue/internal/model"
)

type clusterNodeFilesystemUsage struct {
	Filesystem     string   `json:"filesystem,omitempty"`
	MountPath      string   `json:"mount_path"`
	SizeBytes      *int64   `json:"size_bytes,omitempty"`
	UsedBytes      *int64   `json:"used_bytes,omitempty"`
	AvailableBytes *int64   `json:"available_bytes,omitempty"`
	UsedPercent    *float64 `json:"used_percent,omitempty"`
}

type clusterNodePathUsage struct {
	Path  string `json:"path"`
	Bytes int64  `json:"bytes"`
}

type clusterNodeJournalEntry struct {
	Timestamp *time.Time `json:"timestamp,omitempty"`
	Unit      string     `json:"unit,omitempty"`
	Message   string     `json:"message"`
}

type clusterNodeMetricsDiagnosis struct {
	Status   string   `json:"status"`
	Summary  string   `json:"summary"`
	Evidence []string `json:"evidence"`
	Warnings []string `json:"warnings"`
}

type clusterNodeDiagnosis struct {
	Node             *model.ClusterNode           `json:"node,omitempty"`
	Summary          string                       `json:"summary"`
	JanitorNamespace string                       `json:"janitor_namespace,omitempty"`
	JanitorPod       string                       `json:"janitor_pod,omitempty"`
	Filesystems      []clusterNodeFilesystemUsage `json:"filesystems"`
	HotPaths         []clusterNodePathUsage       `json:"hot_paths"`
	Journal          []clusterNodeJournalEntry    `json:"journal"`
	Events           []model.ClusterEvent         `json:"events"`
	Metrics          *clusterNodeMetricsDiagnosis `json:"metrics,omitempty"`
	Warnings         []string                     `json:"warnings"`
}

type appDiagnosis struct {
	Category       string               `json:"category"`
	Summary        string               `json:"summary"`
	Hint           string               `json:"hint,omitempty"`
	Component      string               `json:"component,omitempty"`
	Namespace      string               `json:"namespace,omitempty"`
	Selector       string               `json:"selector,omitempty"`
	ImplicatedNode string               `json:"implicated_node,omitempty"`
	ImplicatedPod  string               `json:"implicated_pod,omitempty"`
	LivePods       int                  `json:"live_pods,omitempty"`
	ReadyPods      int                  `json:"ready_pods,omitempty"`
	Evidence       []string             `json:"evidence"`
	Warnings       []string             `json:"warnings"`
	Events         []model.ClusterEvent `json:"events"`
}

func (c *Client) GetClusterNodeDiagnosis(name string) (clusterNodeDiagnosis, error) {
	var response struct {
		Diagnosis clusterNodeDiagnosis `json:"diagnosis"`
	}
	if err := c.doJSON(http.MethodGet, path.Join("/v1/cluster/nodes", strings.TrimSpace(name), "diagnosis"), nil, &response); err != nil {
		return clusterNodeDiagnosis{}, err
	}
	return response.Diagnosis, nil
}

func (c *Client) GetAppDiagnosis(id, component string) (appDiagnosis, error) {
	query := url.Values{}
	if value := strings.TrimSpace(component); value != "" {
		query.Set("component", value)
	}
	relative := path.Join("/v1/apps", strings.TrimSpace(id), "diagnosis")
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response struct {
		Diagnosis appDiagnosis `json:"diagnosis"`
	}
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return appDiagnosis{}, err
	}
	return response.Diagnosis, nil
}

func (c *Client) TryGetAppDiagnosis(id, component string) (*appDiagnosis, error) {
	query := url.Values{}
	if value := strings.TrimSpace(component); value != "" {
		query.Set("component", value)
	}
	relative := path.Join("/v1/apps", strings.TrimSpace(id), "diagnosis")
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}

	httpReq, err := http.NewRequest(http.MethodGet, c.resolveURL(relative), nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.token)

	result, err := c.doPrepared(httpReq)
	if err != nil {
		return nil, err
	}
	if result.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if result.StatusCode < 200 || result.StatusCode >= 300 {
		var apiErr apiError
		if err := json.Unmarshal(result.Payload, &apiErr); err == nil && strings.TrimSpace(apiErr.Error) != "" {
			return nil, fmt.Errorf("%s", apiErr.Error)
		}
		if trimmed := strings.TrimSpace(string(result.Payload)); trimmed != "" {
			return nil, fmt.Errorf("request failed: status=%d body=%s", result.StatusCode, trimmed)
		}
		return nil, fmt.Errorf("request failed: status=%d", result.StatusCode)
	}

	var response struct {
		Diagnosis appDiagnosis `json:"diagnosis"`
	}
	if err := json.Unmarshal(result.Payload, &response); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &response.Diagnosis, nil
}
