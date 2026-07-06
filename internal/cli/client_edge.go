package cli

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
)

type edgeRoutePolicyListResponse struct {
	Policies []model.EdgeRoutePolicy `json:"policies"`
}

type edgeRoutePolicyResponse struct {
	Policy model.EdgeRoutePolicy `json:"policy"`
}

type edgeRoutePolicyDeleteResponse struct {
	Policy  model.EdgeRoutePolicy `json:"policy"`
	Deleted bool                  `json:"deleted"`
}

type platformDomainBindingListResponse struct {
	Bindings []model.PlatformDomainBinding `json:"bindings"`
}

type platformDomainBindingResponse struct {
	Binding model.PlatformDomainBinding `json:"binding"`
}

type platformDomainBindingDeleteResponse struct {
	Binding model.PlatformDomainBinding `json:"binding"`
	Deleted bool                        `json:"deleted"`
}

type edgeNodeListResponse struct {
	Nodes  []model.EdgeNode  `json:"nodes"`
	Groups []model.EdgeGroup `json:"groups"`
}

type edgeNodeResponse struct {
	Node  model.EdgeNode  `json:"node"`
	Group model.EdgeGroup `json:"group"`
}

type createEdgeNodeTokenRequest struct {
	EdgeGroupID    string `json:"edge_group_id"`
	WorkloadMode   string `json:"workload_mode,omitempty"`
	CanaryState    string `json:"canary_state,omitempty"`
	CanaryWeight   int    `json:"canary_weight,omitempty"`
	Region         string `json:"region,omitempty"`
	Country        string `json:"country,omitempty"`
	PublicHostname string `json:"public_hostname,omitempty"`
	PublicIPv4     string `json:"public_ipv4,omitempty"`
	PublicIPv6     string `json:"public_ipv6,omitempty"`
	MeshIP         string `json:"mesh_ip,omitempty"`
	Draining       bool   `json:"draining,omitempty"`
}

type createEdgeNodeTokenResponse struct {
	Node  model.EdgeNode `json:"node"`
	Token string         `json:"token"`
}

type edgeNodeDesiredState struct {
	EdgeID            string     `json:"edge_id"`
	EdgeGroupID       string     `json:"edge_group_id"`
	WorkloadMode      string     `json:"workload_mode"`
	CanaryState       string     `json:"canary_state"`
	CanaryWeight      int        `json:"canary_weight"`
	PublicProbeStatus string     `json:"public_probe_status"`
	DNSEligible       bool       `json:"dns_eligible"`
	Draining          bool       `json:"draining"`
	RouteReady        bool       `json:"route_ready"`
	TLSReady          bool       `json:"tls_ready"`
	TokenPrefix       string     `json:"token_prefix,omitempty"`
	LastHeartbeatAt   *time.Time `json:"last_heartbeat_at,omitempty"`
}

type edgeNodeDesiredStateResponse struct {
	DesiredState edgeNodeDesiredState `json:"desired_state"`
}

type edgeNodeControlResponse struct {
	Node         model.EdgeNode       `json:"node"`
	Group        model.EdgeGroup      `json:"group"`
	DesiredState edgeNodeDesiredState `json:"desired_state"`
}

type setEdgeNodeCanaryRequest struct {
	State  string `json:"state,omitempty"`
	Weight int    `json:"weight,omitempty"`
}

type putEdgeRoutePolicyRequest struct {
	EdgeGroupID          string     `json:"edge_group_id,omitempty"`
	ExcludedEdgeIDs      []string   `json:"excluded_edge_ids,omitempty"`
	ExcludedEdgeGroupIDs []string   `json:"excluded_edge_group_ids,omitempty"`
	ExclusionReason      string     `json:"exclusion_reason,omitempty"`
	ExclusionExpiresAt   *time.Time `json:"exclusion_expires_at,omitempty"`
	RoutePolicy          string     `json:"route_policy"`
}

type edgeRoutePolicyUpdate struct {
	EdgeGroupID          string
	ExcludedEdgeIDs      []string
	ExcludedEdgeGroupIDs []string
	ExclusionReason      string
	ExclusionExpiresAt   *time.Time
	RoutePolicy          string
}

type putPlatformDomainBindingRequest struct {
	AppID       string `json:"app_id"`
	RoutePolicy string `json:"route_policy,omitempty"`
	EdgeGroupID string `json:"edge_group_id,omitempty"`
}

func (c *Client) ListEdgeRoutePolicies() ([]model.EdgeRoutePolicy, error) {
	var response edgeRoutePolicyListResponse
	if err := c.doJSON(http.MethodGet, "/v1/edge/route-policies", nil, &response); err != nil {
		return nil, err
	}
	return response.Policies, nil
}

func (c *Client) GetEdgeRoutePolicy(hostname string) (model.EdgeRoutePolicy, error) {
	var response edgeRoutePolicyResponse
	if err := c.doJSON(http.MethodGet, edgeRoutePolicyPath(hostname), nil, &response); err != nil {
		return model.EdgeRoutePolicy{}, err
	}
	return response.Policy, nil
}

func (c *Client) PutEdgeRoutePolicy(hostname, edgeGroupID, routePolicy string) (model.EdgeRoutePolicy, error) {
	return c.PutEdgeRoutePolicyUpdate(hostname, edgeRoutePolicyUpdate{
		EdgeGroupID: strings.TrimSpace(edgeGroupID),
		RoutePolicy: strings.TrimSpace(routePolicy),
	})
}

func (c *Client) PutEdgeRoutePolicyUpdate(hostname string, update edgeRoutePolicyUpdate) (model.EdgeRoutePolicy, error) {
	request := putEdgeRoutePolicyRequest{
		EdgeGroupID:          strings.TrimSpace(update.EdgeGroupID),
		ExcludedEdgeIDs:      append([]string(nil), update.ExcludedEdgeIDs...),
		ExcludedEdgeGroupIDs: append([]string(nil), update.ExcludedEdgeGroupIDs...),
		ExclusionReason:      strings.TrimSpace(update.ExclusionReason),
		ExclusionExpiresAt:   update.ExclusionExpiresAt,
		RoutePolicy:          strings.TrimSpace(update.RoutePolicy),
	}
	var response edgeRoutePolicyResponse
	if err := c.doJSON(http.MethodPut, edgeRoutePolicyPath(hostname), request, &response); err != nil {
		return model.EdgeRoutePolicy{}, err
	}
	return response.Policy, nil
}

func (c *Client) DeleteEdgeRoutePolicy(hostname string) (edgeRoutePolicyDeleteResponse, error) {
	var response edgeRoutePolicyDeleteResponse
	if err := c.doJSON(http.MethodDelete, edgeRoutePolicyPath(hostname), nil, &response); err != nil {
		return edgeRoutePolicyDeleteResponse{}, err
	}
	return response, nil
}

func (c *Client) ListPlatformDomainBindings(zone string) ([]model.PlatformDomainBinding, error) {
	apiPath := "/v1/admin/domains"
	if strings.TrimSpace(zone) != "" {
		values := url.Values{}
		values.Set("zone", strings.TrimSpace(zone))
		apiPath += "?" + values.Encode()
	}
	var response platformDomainBindingListResponse
	if err := c.doJSON(http.MethodGet, apiPath, nil, &response); err != nil {
		return nil, err
	}
	return response.Bindings, nil
}

func (c *Client) GetPlatformDomainBinding(hostname string) (model.PlatformDomainBinding, error) {
	var response platformDomainBindingResponse
	if err := c.doJSON(http.MethodGet, platformDomainBindingPath(hostname), nil, &response); err != nil {
		return model.PlatformDomainBinding{}, err
	}
	return response.Binding, nil
}

func (c *Client) PutPlatformDomainBinding(hostname string, request putPlatformDomainBindingRequest) (model.PlatformDomainBinding, error) {
	var response platformDomainBindingResponse
	if err := c.doJSON(http.MethodPut, platformDomainBindingPath(hostname), request, &response); err != nil {
		return model.PlatformDomainBinding{}, err
	}
	return response.Binding, nil
}

func (c *Client) DeletePlatformDomainBinding(hostname string) (platformDomainBindingDeleteResponse, error) {
	var response platformDomainBindingDeleteResponse
	if err := c.doJSON(http.MethodDelete, platformDomainBindingPath(hostname), nil, &response); err != nil {
		return platformDomainBindingDeleteResponse{}, err
	}
	return response, nil
}

func (c *Client) ListEdgeNodes(edgeGroupID string) (edgeNodeListResponse, error) {
	apiPath := "/v1/edge/nodes"
	if strings.TrimSpace(edgeGroupID) != "" {
		values := url.Values{}
		values.Set("edge_group_id", strings.TrimSpace(edgeGroupID))
		apiPath += "?" + values.Encode()
	}
	var response edgeNodeListResponse
	if err := c.doJSON(http.MethodGet, apiPath, nil, &response); err != nil {
		return edgeNodeListResponse{}, err
	}
	return response, nil
}

func (c *Client) GetEdgeNode(edgeID string) (edgeNodeResponse, error) {
	var response edgeNodeResponse
	if err := c.doJSON(http.MethodGet, edgeNodePath(edgeID), nil, &response); err != nil {
		return edgeNodeResponse{}, err
	}
	return response, nil
}

func (c *Client) GetEdgeNodeQuality(edgeID, since string) (model.EdgeNodeQualityResponse, error) {
	apiPath := edgeNodePath(edgeID) + "/quality"
	if strings.TrimSpace(since) != "" {
		values := url.Values{}
		values.Set("since", strings.TrimSpace(since))
		apiPath += "?" + values.Encode()
	}
	var response model.EdgeNodeQualityResponse
	if err := c.doJSON(http.MethodGet, apiPath, nil, &response); err != nil {
		return model.EdgeNodeQualityResponse{}, err
	}
	return response, nil
}

func (c *Client) GetEdgeQualityRank(hostname, trafficClass, method, pathPrefix, scope, window, since string) (model.EdgeQualityRankResponse, error) {
	apiPath := "/v1/edge/quality-rank/" + url.PathEscape(strings.TrimSpace(hostname))
	values := url.Values{}
	if strings.TrimSpace(trafficClass) != "" {
		values.Set("traffic_class", strings.TrimSpace(trafficClass))
	}
	if strings.TrimSpace(method) != "" {
		values.Set("method", strings.TrimSpace(method))
	}
	if strings.TrimSpace(pathPrefix) != "" {
		values.Set("path_prefix", strings.TrimSpace(pathPrefix))
	}
	if strings.TrimSpace(scope) != "" {
		values.Set("scope", strings.TrimSpace(scope))
	}
	if strings.TrimSpace(window) != "" {
		values.Set("window", strings.TrimSpace(window))
	}
	if strings.TrimSpace(since) != "" {
		values.Set("since", strings.TrimSpace(since))
	}
	if encoded := values.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	var response model.EdgeQualityRankResponse
	if err := c.doJSON(http.MethodGet, apiPath, nil, &response); err != nil {
		return model.EdgeQualityRankResponse{}, err
	}
	return response, nil
}

func (c *Client) CreateEdgeNodeToken(edgeID string, request createEdgeNodeTokenRequest) (createEdgeNodeTokenResponse, error) {
	var response createEdgeNodeTokenResponse
	if err := c.doJSON(http.MethodPost, edgeNodePath(edgeID)+"/token", request, &response); err != nil {
		return createEdgeNodeTokenResponse{}, err
	}
	return response, nil
}

func (c *Client) GetAdminEdgeNodeDesiredState(edgeID string) (edgeNodeDesiredStateResponse, error) {
	var response edgeNodeDesiredStateResponse
	if err := c.doJSON(http.MethodGet, adminEdgeNodePath(edgeID)+"/desired-state", nil, &response); err != nil {
		return edgeNodeDesiredStateResponse{}, err
	}
	return response, nil
}

func (c *Client) ProbeEdgeNode(edgeID string) (edgeNodeControlResponse, error) {
	var response edgeNodeControlResponse
	if err := c.doJSON(http.MethodPost, adminEdgeNodePath(edgeID)+"/probe", nil, &response); err != nil {
		return edgeNodeControlResponse{}, err
	}
	return response, nil
}

func (c *Client) SetEdgeNodeCanary(edgeID string, request setEdgeNodeCanaryRequest) (edgeNodeControlResponse, error) {
	var response edgeNodeControlResponse
	if err := c.doJSON(http.MethodPost, adminEdgeNodePath(edgeID)+"/canary", request, &response); err != nil {
		return edgeNodeControlResponse{}, err
	}
	return response, nil
}

func (c *Client) DrainEdgeNode(edgeID string) (edgeNodeControlResponse, error) {
	var response edgeNodeControlResponse
	if err := c.doJSON(http.MethodPost, adminEdgeNodePath(edgeID)+"/drain", nil, &response); err != nil {
		return edgeNodeControlResponse{}, err
	}
	return response, nil
}

func (c *Client) UndrainEdgeNode(edgeID string) (edgeNodeControlResponse, error) {
	var response edgeNodeControlResponse
	if err := c.doJSON(http.MethodPost, adminEdgeNodePath(edgeID)+"/undrain", nil, &response); err != nil {
		return edgeNodeControlResponse{}, err
	}
	return response, nil
}

func edgeRoutePolicyPath(hostname string) string {
	return "/v1/edge/route-policies/" + url.PathEscape(strings.TrimSpace(hostname))
}

func platformDomainBindingPath(hostname string) string {
	return "/v1/admin/domains/" + url.PathEscape(strings.TrimSpace(hostname))
}

func edgeNodePath(edgeID string) string {
	return "/v1/edge/nodes/" + url.PathEscape(strings.TrimSpace(edgeID))
}

func adminEdgeNodePath(edgeID string) string {
	return "/v1/admin/edge/nodes/" + url.PathEscape(strings.TrimSpace(edgeID))
}
