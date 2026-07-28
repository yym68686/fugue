package cli

import (
	"net/http"
	"net/url"
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
