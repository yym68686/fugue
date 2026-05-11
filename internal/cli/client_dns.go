package cli

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"fugue/internal/model"
)

type dnsNodeListResponse struct {
	Nodes []model.DNSNode `json:"nodes"`
}

type dnsNodeResponse struct {
	Node model.DNSNode `json:"node"`
}

type dnsDelegationPreflightOptions struct {
	Zone            string
	ProbeName       string
	EdgeGroupID     string
	MinHealthyNodes int
}

func (c *Client) ListDNSNodes(edgeGroupID string) (dnsNodeListResponse, error) {
	apiPath := "/v1/dns/nodes"
	if strings.TrimSpace(edgeGroupID) != "" {
		values := url.Values{}
		values.Set("edge_group_id", strings.TrimSpace(edgeGroupID))
		apiPath += "?" + values.Encode()
	}
	var response dnsNodeListResponse
	if err := c.doJSON(http.MethodGet, apiPath, nil, &response); err != nil {
		return dnsNodeListResponse{}, err
	}
	return response, nil
}

func (c *Client) GetDNSNode(dnsNodeID string) (dnsNodeResponse, error) {
	var response dnsNodeResponse
	if err := c.doJSON(http.MethodGet, dnsNodePath(dnsNodeID), nil, &response); err != nil {
		return dnsNodeResponse{}, err
	}
	return response, nil
}

func (c *Client) DNSDelegationPreflight(opts dnsDelegationPreflightOptions) (model.DNSDelegationPreflightResponse, error) {
	values := url.Values{}
	if strings.TrimSpace(opts.Zone) != "" {
		values.Set("zone", strings.TrimSpace(opts.Zone))
	}
	if strings.TrimSpace(opts.ProbeName) != "" {
		values.Set("probe_name", strings.TrimSpace(opts.ProbeName))
	}
	if strings.TrimSpace(opts.EdgeGroupID) != "" {
		values.Set("edge_group_id", strings.TrimSpace(opts.EdgeGroupID))
	}
	if opts.MinHealthyNodes > 0 {
		values.Set("min_healthy_nodes", strconv.Itoa(opts.MinHealthyNodes))
	}
	apiPath := "/v1/dns/delegation/preflight"
	if encoded := values.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	var response model.DNSDelegationPreflightResponse
	if err := c.doJSON(http.MethodGet, apiPath, nil, &response); err != nil {
		return model.DNSDelegationPreflightResponse{}, err
	}
	return response, nil
}

func dnsNodePath(dnsNodeID string) string {
	return "/v1/dns/nodes/" + url.PathEscape(strings.TrimSpace(dnsNodeID))
}
