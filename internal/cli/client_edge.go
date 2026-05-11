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

type putEdgeRoutePolicyRequest struct {
	EdgeGroupID string `json:"edge_group_id,omitempty"`
	RoutePolicy string `json:"route_policy"`
}

func (c *Client) ListEdgeRoutePolicies() ([]model.EdgeRoutePolicy, error) {
	var response edgeRoutePolicyListResponse
	if err := c.doJSON(http.MethodGet, "/v1/edge/route-policies", nil, &response); err != nil {
		return nil, err
	}
	return response.Policies, nil
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

func edgeRoutePolicyPath(hostname string) string {
	return "/v1/edge/route-policies/" + url.PathEscape(strings.TrimSpace(hostname))
}
