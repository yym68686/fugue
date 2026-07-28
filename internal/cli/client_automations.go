package cli

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"fugue/internal/model"
)

func (c *Client) ListAutomationPolicies() (model.AutomationPolicyListResponse, error) {
	var response model.AutomationPolicyListResponse
	if err := c.doJSON(http.MethodGet, "/v1/admin/automations", nil, &response); err != nil {
		return model.AutomationPolicyListResponse{}, err
	}
	return response, nil
}

func (c *Client) GetAutomationPolicy(policyID string) (model.AutomationPolicy, error) {
	var response model.AutomationPolicyResponse
	if err := c.doJSON(
		http.MethodGet,
		"/v1/admin/automations/"+url.PathEscape(strings.TrimSpace(policyID)),
		nil,
		&response,
	); err != nil {
		return model.AutomationPolicy{}, err
	}
	return response.Policy, nil
}

// ListUserAutomationPolicies lists only persisted, user-owned policies. The
// server still applies the credential boundary; the optional selectors are
// useful for platform administrators and for keys that can see more than one
// workspace.
func (c *Client) ListUserAutomationPolicies(tenantID, projectID string) (model.AutomationPolicyListResponse, error) {
	values := url.Values{}
	if tenantID = strings.TrimSpace(tenantID); tenantID != "" {
		values.Set("tenant_id", tenantID)
	}
	if projectID = strings.TrimSpace(projectID); projectID != "" {
		values.Set("project_id", projectID)
	}
	relative := "/v1/automations"
	if encoded := values.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response model.AutomationPolicyListResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return model.AutomationPolicyListResponse{}, err
	}
	return response, nil
}

func (c *Client) GetUserAutomationPolicy(policyID string) (model.AutomationPolicy, error) {
	policyID = strings.TrimSpace(policyID)
	if policyID == "" {
		return model.AutomationPolicy{}, fmt.Errorf("policy id is required")
	}
	var response model.AutomationPolicyResponse
	if err := c.doJSON(
		http.MethodGet,
		"/v1/automations/"+url.PathEscape(policyID),
		nil,
		&response,
	); err != nil {
		return model.AutomationPolicy{}, err
	}
	return response.Policy, nil
}

func (c *Client) CreateUserAutomationPolicy(request model.CreateAutomationPolicyRequest) (model.AutomationPolicy, error) {
	var response model.AutomationPolicyResponse
	if err := c.doJSON(http.MethodPost, "/v1/automations", request, &response); err != nil {
		return model.AutomationPolicy{}, err
	}
	return response.Policy, nil
}

func (c *Client) UpdateUserAutomationPolicy(policyID string, request model.UpdateAutomationPolicyRequest) (model.AutomationPolicy, error) {
	policyID = strings.TrimSpace(policyID)
	if policyID == "" {
		return model.AutomationPolicy{}, fmt.Errorf("policy id is required")
	}
	var response model.AutomationPolicyResponse
	if err := c.doJSON(
		http.MethodPut,
		"/v1/automations/"+url.PathEscape(policyID),
		request,
		&response,
	); err != nil {
		return model.AutomationPolicy{}, err
	}
	return response.Policy, nil
}

func (c *Client) DeleteUserAutomationPolicy(policyID string, expectedGeneration int64) (model.DeleteAutomationPolicyResponse, error) {
	policyID = strings.TrimSpace(policyID)
	if policyID == "" {
		return model.DeleteAutomationPolicyResponse{}, fmt.Errorf("policy id is required")
	}
	if expectedGeneration <= 0 {
		return model.DeleteAutomationPolicyResponse{}, fmt.Errorf("expected generation must be a positive integer")
	}
	relative := "/v1/automations/" + url.PathEscape(policyID) +
		"?expected_generation=" + url.QueryEscape(strconv.FormatInt(expectedGeneration, 10))
	var response model.DeleteAutomationPolicyResponse
	if err := c.doJSON(http.MethodDelete, relative, nil, &response); err != nil {
		return model.DeleteAutomationPolicyResponse{}, err
	}
	return response, nil
}
