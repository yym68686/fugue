package cli

import (
	"net/http"
	"net/url"
	"path"
	"strings"

	"fugue/internal/model"
)

type createStaticEdgeRegistrationRequest struct {
	TenantID               string `json:"tenant_id,omitempty"`
	ProjectID              string `json:"project_id"`
	Name                   string `json:"name"`
	EdgeID                 string `json:"edge_id"`
	Transport              string `json:"transport"`
	ManagerURL             string `json:"manager_url,omitempty"`
	CertificateFingerprint string `json:"certificate_fingerprint,omitempty"`
	SigningKeyID           string `json:"signing_key_id"`
	PossessionProofDigest  string `json:"possession_proof_digest"`
}

type updateStaticEdgePossessionProofRequest struct {
	PossessionProofDigest string `json:"possession_proof_digest"`
	SigningKeyID          string `json:"signing_key_id"`
	Ready                 bool   `json:"ready,omitempty"`
}

func (c *Client) ListStaticEdgeRegistrations(tenantID, projectID string) (model.StaticEdgeRegistrationListResponse, error) {
	query := url.Values{}
	if strings.TrimSpace(tenantID) != "" {
		query.Set("tenant_id", strings.TrimSpace(tenantID))
	}
	if strings.TrimSpace(projectID) != "" {
		query.Set("project_id", strings.TrimSpace(projectID))
	}
	relative := "/v1/static-edges"
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response model.StaticEdgeRegistrationListResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return model.StaticEdgeRegistrationListResponse{}, err
	}
	return response, nil
}

func (c *Client) CreateStaticEdgeRegistration(request createStaticEdgeRegistrationRequest) (model.StaticEdgeRegistration, error) {
	var response model.StaticEdgeRegistrationResponse
	if err := c.doJSON(http.MethodPost, "/v1/static-edges", request, &response); err != nil {
		return model.StaticEdgeRegistration{}, err
	}
	return response.Registration, nil
}

func (c *Client) GetStaticEdgeRegistration(id string) (model.StaticEdgeRegistration, error) {
	var response model.StaticEdgeRegistrationResponse
	if err := c.doJSON(http.MethodGet, path.Join("/v1/static-edges", strings.TrimSpace(id)), nil, &response); err != nil {
		return model.StaticEdgeRegistration{}, err
	}
	return response.Registration, nil
}

func (c *Client) UpdateStaticEdgePossessionProof(id string, request updateStaticEdgePossessionProofRequest) (model.StaticEdgeRegistration, error) {
	var response model.StaticEdgeRegistrationResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/static-edges", strings.TrimSpace(id), "proof"), request, &response); err != nil {
		return model.StaticEdgeRegistration{}, err
	}
	return response.Registration, nil
}

func (c *Client) RevokeStaticEdgeRegistration(id string) (model.StaticEdgeRegistration, error) {
	var response model.StaticEdgeRegistrationResponse
	if err := c.doJSON(http.MethodDelete, path.Join("/v1/static-edges", strings.TrimSpace(id)), nil, &response); err != nil {
		return model.StaticEdgeRegistration{}, err
	}
	return response.Registration, nil
}
