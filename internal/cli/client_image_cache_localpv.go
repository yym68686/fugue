package cli

import (
	"net/http"
	"net/url"
	"strings"

	"fugue/internal/model"
)

type imageCacheInventoryListResponse struct {
	Nodes     []model.ImageCacheNodeInventory `json:"nodes"`
	Manifests []model.ImageCacheManifest      `json:"manifests"`
}

type imageCachePrunePlanResponse struct {
	Plan model.ImageCachePrunePlan `json:"plan"`
}

type imageCachePrunePlanTaskResponse struct {
	Plan model.ImageCachePrunePlan `json:"plan"`
	Task model.NodeUpdateTask      `json:"task,omitempty"`
}

type createImageCachePrunePlanTaskRequest struct {
	NodeID          string `json:"node_id,omitempty"`
	ClusterNodeName string `json:"cluster_node_name,omitempty"`
	RuntimeID       string `json:"runtime_id,omitempty"`
	Mode            string `json:"mode,omitempty"`
	AllowDelete     bool   `json:"allow_delete,omitempty"`
	MaxDeleteBytes  int64  `json:"max_delete_bytes,omitempty"`
	DryRun          *bool  `json:"dry_run,omitempty"`
}

type localPVInventoryListResponse struct {
	Inventories []model.LocalPVInventory `json:"inventories"`
}

func (c *Client) ListImageCacheInventory(nodeID, clusterNodeName, runtimeID string) ([]model.ImageCacheNodeInventory, []model.ImageCacheManifest, error) {
	query := url.Values{}
	if strings.TrimSpace(nodeID) != "" {
		query.Set("node_id", strings.TrimSpace(nodeID))
	}
	if strings.TrimSpace(clusterNodeName) != "" {
		query.Set("cluster_node_name", strings.TrimSpace(clusterNodeName))
	}
	if strings.TrimSpace(runtimeID) != "" {
		query.Set("runtime_id", strings.TrimSpace(runtimeID))
	}
	relative := "/v1/admin/image-cache/inventory"
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response imageCacheInventoryListResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return nil, nil, err
	}
	return response.Nodes, response.Manifests, nil
}

func (c *Client) GetImageCachePrunePlan(nodeID, clusterNodeName, runtimeID, mode string, persist bool) (model.ImageCachePrunePlan, error) {
	query := url.Values{}
	if strings.TrimSpace(nodeID) != "" {
		query.Set("node_id", strings.TrimSpace(nodeID))
	}
	if strings.TrimSpace(clusterNodeName) != "" {
		query.Set("cluster_node_name", strings.TrimSpace(clusterNodeName))
	}
	if strings.TrimSpace(runtimeID) != "" {
		query.Set("runtime_id", strings.TrimSpace(runtimeID))
	}
	if strings.TrimSpace(mode) != "" {
		query.Set("mode", strings.TrimSpace(mode))
	}
	if persist {
		query.Set("persist", "true")
	}
	relative := "/v1/admin/image-cache/prune-plan"
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response imageCachePrunePlanResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return model.ImageCachePrunePlan{}, err
	}
	return response.Plan, nil
}

func (c *Client) CreateImageCachePrunePlanTask(request createImageCachePrunePlanTaskRequest) (model.ImageCachePrunePlan, model.NodeUpdateTask, error) {
	var response imageCachePrunePlanTaskResponse
	if err := c.doJSON(http.MethodPost, "/v1/admin/image-cache/prune-plan", request, &response); err != nil {
		return model.ImageCachePrunePlan{}, model.NodeUpdateTask{}, err
	}
	return response.Plan, response.Task, nil
}

func (c *Client) ListLocalPVInventories(nodeID, clusterNodeName, runtimeID string) ([]model.LocalPVInventory, error) {
	query := url.Values{}
	if strings.TrimSpace(nodeID) != "" {
		query.Set("node_id", strings.TrimSpace(nodeID))
	}
	if strings.TrimSpace(clusterNodeName) != "" {
		query.Set("cluster_node_name", strings.TrimSpace(clusterNodeName))
	}
	if strings.TrimSpace(runtimeID) != "" {
		query.Set("runtime_id", strings.TrimSpace(runtimeID))
	}
	relative := "/v1/admin/localpv/inventory"
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response localPVInventoryListResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return nil, err
	}
	return response.Inventories, nil
}
