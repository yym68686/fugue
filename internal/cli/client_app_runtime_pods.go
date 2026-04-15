package cli

import (
	"net/http"
	"net/url"
	"path"
	"strings"

	"fugue/internal/model"
)

func (c *Client) GetAppRuntimePods(id, component string) (model.AppRuntimePodInventory, error) {
	relative := path.Join("/v1/apps", strings.TrimSpace(id), "runtime-pods")
	query := url.Values{}
	if value := strings.TrimSpace(component); value != "" {
		query.Set("component", value)
	}
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response model.AppRuntimePodInventory
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return model.AppRuntimePodInventory{}, err
	}
	if response.Groups == nil {
		response.Groups = []model.AppRuntimePodGroup{}
	}
	if response.Warnings == nil {
		response.Warnings = []string{}
	}
	return response, nil
}
