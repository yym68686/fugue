package cli

import (
	"net/http"
	"path"

	"fugue/internal/model"
)

type appSSHResponse struct {
	App            model.App             `json:"app"`
	SSH            model.AppSSHStatus    `json:"ssh"`
	Endpoint       *model.AppSSHEndpoint `json:"endpoint,omitempty"`
	Operation      *model.Operation      `json:"operation,omitempty"`
	AlreadyCurrent bool                  `json:"already_current,omitempty"`
}

type appSSHDiagnosisCheck struct {
	Name    string `json:"name"`
	Pass    bool   `json:"pass"`
	Message string `json:"message"`
}

type appSSHDiagnosisResponse struct {
	App      model.App              `json:"app"`
	SSH      model.AppSSHStatus     `json:"ssh"`
	Endpoint *model.AppSSHEndpoint  `json:"endpoint,omitempty"`
	Checks   []appSSHDiagnosisCheck `json:"checks"`
}

type patchAppSSHRequest struct {
	Enabled            *bool    `json:"enabled,omitempty"`
	TargetPort         int      `json:"target_port,omitempty"`
	User               string   `json:"user,omitempty"`
	AuthorizedKeyIDs   []string `json:"authorized_key_ids,omitempty"`
	AuthorizedKeys     []string `json:"authorized_keys,omitempty"`
	AllowTCPForwarding bool     `json:"allow_tcp_forwarding,omitempty"`
}

type createSSHKeyRequest struct {
	TenantID  string `json:"tenant_id,omitempty"`
	Label     string `json:"label,omitempty"`
	PublicKey string `json:"public_key"`
}

func (c *Client) ListSSHKeys() ([]model.SSHKey, error) {
	var response struct {
		SSHKeys []model.SSHKey `json:"ssh_keys"`
	}
	if err := c.doJSON(http.MethodGet, "/v1/ssh/keys", nil, &response); err != nil {
		return nil, err
	}
	return response.SSHKeys, nil
}

func (c *Client) CreateSSHKey(request createSSHKeyRequest) (model.SSHKey, error) {
	var response struct {
		SSHKey model.SSHKey `json:"ssh_key"`
	}
	if err := c.doJSON(http.MethodPost, "/v1/ssh/keys", request, &response); err != nil {
		return model.SSHKey{}, err
	}
	return response.SSHKey, nil
}

func (c *Client) DeleteSSHKey(id string) (model.SSHKey, error) {
	var response struct {
		SSHKey model.SSHKey `json:"ssh_key"`
	}
	if err := c.doJSON(http.MethodDelete, path.Join("/v1/ssh/keys", id), nil, &response); err != nil {
		return model.SSHKey{}, err
	}
	return response.SSHKey, nil
}

func (c *Client) GetAppSSH(appID string) (appSSHResponse, error) {
	var response appSSHResponse
	if err := c.doJSON(http.MethodGet, path.Join("/v1/apps", appID, "ssh"), nil, &response); err != nil {
		return appSSHResponse{}, err
	}
	return response, nil
}

func (c *Client) PatchAppSSH(appID string, request patchAppSSHRequest) (appSSHResponse, error) {
	var response appSSHResponse
	if err := c.doJSON(http.MethodPatch, path.Join("/v1/apps", appID, "ssh"), request, &response); err != nil {
		return appSSHResponse{}, err
	}
	return response, nil
}

func (c *Client) RotateAppSSHPort(appID string) (appSSHResponse, error) {
	var response appSSHResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", appID, "ssh", "rotate-port"), nil, &response); err != nil {
		return appSSHResponse{}, err
	}
	return response, nil
}

func (c *Client) DiagnoseAppSSH(appID string) (appSSHDiagnosisResponse, error) {
	var response appSSHDiagnosisResponse
	if err := c.doJSON(http.MethodGet, path.Join("/v1/apps", appID, "ssh", "diagnose"), nil, &response); err != nil {
		return appSSHDiagnosisResponse{}, err
	}
	return response, nil
}
