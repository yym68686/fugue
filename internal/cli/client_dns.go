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

type dnsACMEChallengeListResponse struct {
	Challenges []model.DNSACMEChallenge `json:"challenges"`
}

type dnsACMEChallengeResponse struct {
	Challenge model.DNSACMEChallenge `json:"challenge"`
}

type deleteDNSACMEChallengeResponse struct {
	Deleted   bool                   `json:"deleted"`
	Challenge model.DNSACMEChallenge `json:"challenge"`
}

type upsertDNSACMEChallengeClientRequest struct {
	Zone             string `json:"zone,omitempty"`
	Name             string `json:"name"`
	Value            string `json:"value"`
	TTL              int    `json:"ttl,omitempty"`
	ExpiresInSeconds int    `json:"expires_in_seconds,omitempty"`
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

func (c *Client) ListDNSACMEChallenges(zone string, includeExpired bool) (dnsACMEChallengeListResponse, error) {
	values := url.Values{}
	if strings.TrimSpace(zone) != "" {
		values.Set("zone", strings.TrimSpace(zone))
	}
	if includeExpired {
		values.Set("include_expired", "true")
	}
	apiPath := "/v1/dns/acme-challenges"
	if encoded := values.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	var response dnsACMEChallengeListResponse
	if err := c.doJSON(http.MethodGet, apiPath, nil, &response); err != nil {
		return dnsACMEChallengeListResponse{}, err
	}
	return response, nil
}

func (c *Client) UpsertDNSACMEChallenge(req upsertDNSACMEChallengeClientRequest) (dnsACMEChallengeResponse, error) {
	var response dnsACMEChallengeResponse
	if err := c.doJSON(http.MethodPost, "/v1/dns/acme-challenges", req, &response); err != nil {
		return dnsACMEChallengeResponse{}, err
	}
	return response, nil
}

func (c *Client) DeleteDNSACMEChallenge(challengeID string) (deleteDNSACMEChallengeResponse, error) {
	var response deleteDNSACMEChallengeResponse
	if err := c.doJSON(http.MethodDelete, dnsACMEChallengePath(challengeID), nil, &response); err != nil {
		return deleteDNSACMEChallengeResponse{}, err
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

func dnsACMEChallengePath(challengeID string) string {
	return "/v1/dns/acme-challenges/" + url.PathEscape(strings.TrimSpace(challengeID))
}
