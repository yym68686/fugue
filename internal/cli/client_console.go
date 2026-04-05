package cli

import (
	"net/http"
	"path"

	"fugue/internal/model"
)

type consoleProjectBadge struct {
	Kind  string `json:"kind"`
	Label string `json:"label"`
	Meta  string `json:"meta"`
}

type consoleProjectLifecycle struct {
	Label    string `json:"label"`
	Live     bool   `json:"live"`
	SyncMode string `json:"sync_mode"`
	Tone     string `json:"tone"`
}

type consoleProjectSummary struct {
	AppCount              int                     `json:"app_count"`
	ID                    string                  `json:"id"`
	Lifecycle             consoleProjectLifecycle `json:"lifecycle"`
	Name                  string                  `json:"name"`
	ResourceUsageSnapshot model.ResourceUsage     `json:"resource_usage_snapshot"`
	ServiceBadges         []consoleProjectBadge   `json:"service_badges"`
	ServiceCount          int                     `json:"service_count"`
}

type consoleGalleryResponse struct {
	Projects []consoleProjectSummary `json:"projects"`
}

type consoleProjectDetailResponse struct {
	Apps         []model.App         `json:"apps"`
	ClusterNodes []model.ClusterNode `json:"cluster_nodes"`
	Operations   []model.Operation   `json:"operations"`
	Project      *model.Project      `json:"project,omitempty"`
	ProjectID    string              `json:"project_id"`
	ProjectName  string              `json:"project_name"`
}

type appPatchResponse struct {
	App            model.App `json:"app"`
	AlreadyCurrent bool      `json:"already_current,omitempty"`
}

func (c *Client) GetConsoleGallery() (consoleGalleryResponse, error) {
	var response consoleGalleryResponse
	if err := c.doJSON(http.MethodGet, "/v1/console/gallery", nil, &response); err != nil {
		return consoleGalleryResponse{}, err
	}
	return response, nil
}

func (c *Client) GetConsoleProject(id string) (consoleProjectDetailResponse, error) {
	var response consoleProjectDetailResponse
	if err := c.doJSON(http.MethodGet, path.Join("/v1/console/projects", id), nil, &response); err != nil {
		return consoleProjectDetailResponse{}, err
	}
	return response, nil
}

func (c *Client) SetAppImageMirrorLimit(id string, limit int) (appPatchResponse, error) {
	var response appPatchResponse
	request := map[string]int{"image_mirror_limit": limit}
	if err := c.doJSON(http.MethodPatch, path.Join("/v1/apps", id), request, &response); err != nil {
		return appPatchResponse{}, err
	}
	return response, nil
}
