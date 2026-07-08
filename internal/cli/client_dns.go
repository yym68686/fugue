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

type hostedDNSZoneListResponse struct {
	Zones []model.HostedZone `json:"zones"`
}

type hostedDNSZoneResponse struct {
	Zone model.HostedZone `json:"zone"`
}

type hostedDNSZonePreflightResponse struct {
	Zone      model.HostedZone                     `json:"zone"`
	Preflight model.DNSDelegationPreflightResponse `json:"preflight"`
}

type deleteHostedDNSZoneResponse struct {
	Deleted bool             `json:"deleted"`
	Zone    model.HostedZone `json:"zone"`
}

type hostedDNSRecordListResponse struct {
	Records []model.DNSRecord `json:"records"`
}

type hostedDNSRecordResponse struct {
	Record model.DNSRecord `json:"record"`
}

type deleteHostedDNSRecordResponse struct {
	Deleted bool            `json:"deleted"`
	Record  model.DNSRecord `json:"record"`
}

type createHostedDNSZoneClientRequest struct {
	ZoneName  string `json:"zone_name"`
	TenantID  string `json:"tenant_id,omitempty"`
	ProjectID string `json:"project_id,omitempty"`
}

type createHostedDNSRecordClientRequest struct {
	Name                  string   `json:"name"`
	Type                  string   `json:"type"`
	Values                []string `json:"values"`
	TTL                   int      `json:"ttl,omitempty"`
	Flatten               bool     `json:"flatten,omitempty"`
	FlattenMode           string   `json:"flatten_mode,omitempty"`
	FlattenTarget         string   `json:"flatten_target,omitempty"`
	FlattenIPv4Policy     string   `json:"flatten_ipv4_policy,omitempty"`
	FlattenIPv6Policy     string   `json:"flatten_ipv6_policy,omitempty"`
	FlattenTTLPolicy      string   `json:"flatten_ttl_policy,omitempty"`
	FlattenFallbackPolicy string   `json:"flatten_fallback_policy,omitempty"`
	Overwrite             bool     `json:"overwrite,omitempty"`
}

type patchHostedDNSRecordClientRequest struct {
	Values                []string `json:"values,omitempty"`
	TTL                   int      `json:"ttl,omitempty"`
	Flatten               *bool    `json:"flatten,omitempty"`
	FlattenMode           string   `json:"flatten_mode,omitempty"`
	FlattenTarget         string   `json:"flatten_target,omitempty"`
	FlattenIPv4Policy     string   `json:"flatten_ipv4_policy,omitempty"`
	FlattenIPv6Policy     string   `json:"flatten_ipv6_policy,omitempty"`
	FlattenTTLPolicy      string   `json:"flatten_ttl_policy,omitempty"`
	FlattenFallbackPolicy string   `json:"flatten_fallback_policy,omitempty"`
	Status                string   `json:"status,omitempty"`
	Overwrite             bool     `json:"overwrite,omitempty"`
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

func (c *Client) ListHostedDNSZones() ([]model.HostedZone, error) {
	var response hostedDNSZoneListResponse
	if err := c.doJSON(http.MethodGet, "/v1/dns/zones", nil, &response); err != nil {
		return nil, err
	}
	return response.Zones, nil
}

func (c *Client) CreateHostedDNSZone(req createHostedDNSZoneClientRequest) (model.HostedZone, error) {
	var response hostedDNSZoneResponse
	if err := c.doJSON(http.MethodPost, "/v1/dns/zones", req, &response); err != nil {
		return model.HostedZone{}, err
	}
	return response.Zone, nil
}

func (c *Client) GetHostedDNSZone(zone string) (model.HostedZone, error) {
	var response hostedDNSZoneResponse
	if err := c.doJSON(http.MethodGet, hostedDNSZonePath(zone), nil, &response); err != nil {
		return model.HostedZone{}, err
	}
	return response.Zone, nil
}

func (c *Client) DeleteHostedDNSZone(zone string) (deleteHostedDNSZoneResponse, error) {
	var response deleteHostedDNSZoneResponse
	if err := c.doJSON(http.MethodDelete, hostedDNSZonePath(zone), nil, &response); err != nil {
		return deleteHostedDNSZoneResponse{}, err
	}
	return response, nil
}

func (c *Client) HostedDNSZonePreflight(zone string, minHealthyNodes int) (hostedDNSZonePreflightResponse, error) {
	values := url.Values{}
	if minHealthyNodes > 0 {
		values.Set("min_healthy_nodes", strconv.Itoa(minHealthyNodes))
	}
	apiPath := hostedDNSZonePath(zone) + "/preflight"
	if encoded := values.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	var response hostedDNSZonePreflightResponse
	if err := c.doJSON(http.MethodGet, apiPath, nil, &response); err != nil {
		return hostedDNSZonePreflightResponse{}, err
	}
	return response, nil
}

func (c *Client) ListHostedDNSRecords(zone string) ([]model.DNSRecord, error) {
	var response hostedDNSRecordListResponse
	if err := c.doJSON(http.MethodGet, hostedDNSZonePath(zone)+"/records", nil, &response); err != nil {
		return nil, err
	}
	return response.Records, nil
}

func (c *Client) CreateHostedDNSRecord(zone string, req createHostedDNSRecordClientRequest) (model.DNSRecord, error) {
	var response hostedDNSRecordResponse
	if err := c.doJSON(http.MethodPost, hostedDNSZonePath(zone)+"/records", req, &response); err != nil {
		return model.DNSRecord{}, err
	}
	return response.Record, nil
}

func (c *Client) PatchHostedDNSRecord(zone, recordID string, req patchHostedDNSRecordClientRequest) (model.DNSRecord, error) {
	var response hostedDNSRecordResponse
	if err := c.doJSON(http.MethodPatch, hostedDNSRecordPath(zone, recordID), req, &response); err != nil {
		return model.DNSRecord{}, err
	}
	return response.Record, nil
}

func (c *Client) DeleteHostedDNSRecord(zone, recordID string) (deleteHostedDNSRecordResponse, error) {
	var response deleteHostedDNSRecordResponse
	if err := c.doJSON(http.MethodDelete, hostedDNSRecordPath(zone, recordID), nil, &response); err != nil {
		return deleteHostedDNSRecordResponse{}, err
	}
	return response, nil
}

func dnsNodePath(dnsNodeID string) string {
	return "/v1/dns/nodes/" + url.PathEscape(strings.TrimSpace(dnsNodeID))
}

func dnsACMEChallengePath(challengeID string) string {
	return "/v1/dns/acme-challenges/" + url.PathEscape(strings.TrimSpace(challengeID))
}

func hostedDNSZonePath(zone string) string {
	return "/v1/dns/zones/" + url.PathEscape(strings.TrimSpace(zone))
}

func hostedDNSRecordPath(zone, recordID string) string {
	return hostedDNSZonePath(zone) + "/records/" + url.PathEscape(strings.TrimSpace(recordID))
}
