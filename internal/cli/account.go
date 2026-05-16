package cli

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

type adminWorkspaceResolveResponse struct {
	Email     string                    `json:"email"`
	Workspace adminWorkspaceResolveView `json:"workspace"`
}

type adminWorkspaceResolveView struct {
	TenantID           string `json:"tenantId"`
	TenantName         string `json:"tenantName"`
	DefaultProjectID   string `json:"defaultProjectId,omitempty"`
	DefaultProjectName string `json:"defaultProjectName,omitempty"`
	FirstAppID         string `json:"firstAppId,omitempty"`
}

func (c *Client) ResolveAdminWorkspace(email string) (adminWorkspaceResolveResponse, error) {
	email = strings.TrimSpace(email)
	if email == "" {
		return adminWorkspaceResolveResponse{}, fmt.Errorf("account email is required")
	}
	query := url.Values{}
	query.Set("email", email)

	var response adminWorkspaceResolveResponse
	if err := c.doJSON(http.MethodGet, "/api/admin/workspaces/resolve?"+query.Encode(), nil, &response); err != nil {
		return adminWorkspaceResolveResponse{}, err
	}
	return response, nil
}

func (c *CLI) resolveAccountWorkspaceTarget(client *Client) (*adminWorkspaceResolveResponse, bool, error) {
	email := strings.TrimSpace(c.effectiveAccount())
	if email == "" {
		return nil, false, nil
	}
	if strings.TrimSpace(c.effectiveTenantID()) != "" || strings.TrimSpace(c.effectiveTenantName()) != "" {
		return nil, false, fmt.Errorf("--account cannot be combined with --tenant or --tenant-id")
	}
	if c.account != nil && strings.EqualFold(c.account.Email, email) {
		return c.account, true, nil
	}

	context, err := client.GetAuthContext()
	if err != nil {
		return nil, false, fmt.Errorf("verify admin account targeting: %w", err)
	}
	if !context.Principal.PlatformAdmin {
		return nil, false, fmt.Errorf("--account requires a platform-admin or bootstrap key")
	}

	webClient, err := c.newWebClient("")
	if err != nil {
		return nil, false, err
	}
	resolved, err := webClient.ResolveAdminWorkspace(email)
	if err != nil {
		return nil, false, fmt.Errorf("resolve account workspace: %w", err)
	}
	if strings.TrimSpace(resolved.Workspace.TenantID) == "" {
		return nil, false, fmt.Errorf("account %s resolved without a tenant id", email)
	}

	c.account = &resolved
	return c.account, true, nil
}

func (c *CLI) resolveTenantSelection(client *Client, tenantID, tenantName string) (string, error) {
	if account, ok, err := c.resolveAccountWorkspaceTarget(client); err != nil {
		return "", err
	} else if ok {
		return account.Workspace.TenantID, nil
	}
	return resolveTenantSelection(client, tenantID, tenantName)
}
