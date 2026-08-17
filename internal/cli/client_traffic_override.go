package cli

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
)

type trafficOverrideListResponse struct {
	Overrides []model.TrafficOverride `json:"overrides"`
}

type trafficOverrideResponse struct {
	Override model.TrafficOverride `json:"override"`
}

type trafficOverrideSigningKeyResponse struct {
	SigningKey model.TrafficOverrideSigningKeyStatus `json:"signing_key"`
}

type trafficOverridePutRequest struct {
	Answers            []string  `json:"answers"`
	RequiredHostRoutes []string  `json:"required_host_routes"`
	RouteGeneration    string    `json:"route_generation"`
	RouteDigest        string    `json:"route_digest"`
	ExpiresAt          time.Time `json:"expires_at"`
	Reason             string    `json:"reason"`
	ExpectedGeneration uint64    `json:"expected_generation"`
}

func (c *Client) ListTrafficOverrides() ([]model.TrafficOverride, error) {
	var response trafficOverrideListResponse
	if err := c.doJSON(http.MethodGet, "/v1/admin/traffic-overrides", nil, &response); err != nil {
		return nil, err
	}
	return response.Overrides, nil
}

func (c *Client) GetTrafficOverride(hostname string) (model.TrafficOverride, error) {
	var response trafficOverrideResponse
	if err := c.doJSON(http.MethodGet, trafficOverridePath(hostname), nil, &response); err != nil {
		return model.TrafficOverride{}, err
	}
	return response.Override, nil
}

func (c *Client) PutTrafficOverride(hostname string, request trafficOverridePutRequest) (model.TrafficOverride, error) {
	var response trafficOverrideResponse
	if err := c.doJSON(http.MethodPut, trafficOverridePath(hostname), request, &response); err != nil {
		return model.TrafficOverride{}, err
	}
	return response.Override, nil
}

func (c *Client) RevokeTrafficOverride(hostname, reason string, expectedGeneration uint64) (model.TrafficOverride, error) {
	var response trafficOverrideResponse
	body := map[string]any{"reason": strings.TrimSpace(reason), "expected_generation": expectedGeneration}
	if err := c.doJSON(http.MethodPost, trafficOverridePath(hostname)+"/revoke", body, &response); err != nil {
		return model.TrafficOverride{}, err
	}
	return response.Override, nil
}

func (c *Client) GetTrafficOverrideSigningKey() (model.TrafficOverrideSigningKeyStatus, error) {
	var response trafficOverrideSigningKeyResponse
	if err := c.doJSON(http.MethodGet, "/v1/admin/traffic-override-signing-key", nil, &response); err != nil {
		return model.TrafficOverrideSigningKeyStatus{}, err
	}
	return response.SigningKey, nil
}

func (c *Client) RotateTrafficOverrideSigningKey(expectedGeneration uint64) (model.TrafficOverrideSigningKeyStatus, error) {
	var response trafficOverrideSigningKeyResponse
	if err := c.doJSON(http.MethodPost, "/v1/admin/traffic-override-signing-key/rotate", map[string]any{"expected_generation": expectedGeneration}, &response); err != nil {
		return model.TrafficOverrideSigningKeyStatus{}, err
	}
	return response.SigningKey, nil
}

func trafficOverridePath(hostname string) string {
	return "/v1/admin/traffic-overrides/" + url.PathEscape(strings.TrimSpace(hostname))
}
