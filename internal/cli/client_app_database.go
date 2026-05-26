package cli

import (
	"net/url"
	"path"
	"strings"

	"fugue/internal/model"
)

type appDatabaseImportResponse struct {
	App model.App                   `json:"app"`
	Job *model.AppDatabaseImportJob `json:"job,omitempty"`
}

type appDatabaseAccessResponse struct {
	App    model.App                      `json:"app"`
	Grants []model.AppDatabaseAccessGrant `json:"grants"`
}

type appDatabaseAccessGrantCreateResponse struct {
	Grant  model.AppDatabaseAccessGrant `json:"grant"`
	Secret string                       `json:"secret"`
}

type appDatabaseAccessRevokeResponse struct {
	Removed bool `json:"removed"`
}

func (c *Client) ImportAppDatabase(id string, req model.AppDatabaseImportRequest, dumpName string, dumpBytes []byte) (appDatabaseImportResponse, error) {
	var response appDatabaseImportResponse
	if err := c.doMultipartJSONWithFileField(path.Join("/v1/apps", strings.TrimSpace(id), "database", "import"), req, "dump", dumpName, dumpBytes, &response); err != nil {
		return appDatabaseImportResponse{}, err
	}
	return response, nil
}

func (c *Client) GetAppDatabaseImport(id string) (appDatabaseImportResponse, error) {
	var response appDatabaseImportResponse
	if err := c.doJSON("GET", path.Join("/v1/apps", strings.TrimSpace(id), "database", "import"), nil, &response); err != nil {
		return appDatabaseImportResponse{}, err
	}
	return response, nil
}

func (c *Client) RetryAppDatabaseImport(id string, req model.AppDatabaseImportRetryRequest) (appDatabaseImportResponse, error) {
	var response appDatabaseImportResponse
	if err := c.doJSON("POST", path.Join("/v1/apps", strings.TrimSpace(id), "database", "import", "retry"), req, &response); err != nil {
		return appDatabaseImportResponse{}, err
	}
	return response, nil
}

func (c *Client) ListAppDatabaseAccessGrants(id string) (appDatabaseAccessResponse, error) {
	var response appDatabaseAccessResponse
	if err := c.doJSON("GET", path.Join("/v1/apps", strings.TrimSpace(id), "database", "access"), nil, &response); err != nil {
		return appDatabaseAccessResponse{}, err
	}
	return response, nil
}

func (c *Client) CreateAppDatabaseAccessGrant(id string, req model.AppDatabaseAccessGrantCreateRequest) (appDatabaseAccessGrantCreateResponse, error) {
	var response appDatabaseAccessGrantCreateResponse
	if err := c.doJSON("POST", path.Join("/v1/apps", strings.TrimSpace(id), "database", "access"), req, &response); err != nil {
		return appDatabaseAccessGrantCreateResponse{}, err
	}
	return response, nil
}

func (c *Client) RevokeAppDatabaseAccessGrant(id, grantID string) (appDatabaseAccessRevokeResponse, error) {
	var response appDatabaseAccessRevokeResponse
	if err := c.doJSON("DELETE", path.Join("/v1/apps", strings.TrimSpace(id), "database", "access", strings.TrimSpace(grantID)), nil, &response); err != nil {
		return appDatabaseAccessRevokeResponse{}, err
	}
	return response, nil
}

func (c *Client) AppDatabaseTunnelWebSocketURL(id, grantID, token string) string {
	relative := path.Join("/v1/apps", strings.TrimSpace(id), "database", "access", strings.TrimSpace(grantID), "tunnel")
	if strings.TrimSpace(token) != "" {
		relative += "?token=" + url.QueryEscape(strings.TrimSpace(token))
	}
	base := c.resolveURL(relative)
	parsed, err := url.Parse(base)
	if err != nil {
		return base
	}
	switch parsed.Scheme {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	}
	return parsed.String()
}
