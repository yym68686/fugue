package cli

import (
	"net/http"
	"net/url"
	"strings"

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

type putEdgeRoutePolicyRequest struct {
	EdgeGroupID string `json:"edge_group_id,omitempty"`
	RoutePolicy string `json:"route_policy"`
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
	request := putEdgeRoutePolicyRequest{
		EdgeGroupID: strings.TrimSpace(edgeGroupID),
		RoutePolicy: strings.TrimSpace(routePolicy),
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

func (c *Client) CreateEdgeNodeToken(edgeID string, request createEdgeNodeTokenRequest) (createEdgeNodeTokenResponse, error) {
	var response createEdgeNodeTokenResponse
	if err := c.doJSON(http.MethodPost, edgeNodePath(edgeID)+"/token", request, &response); err != nil {
		return createEdgeNodeTokenResponse{}, err
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
