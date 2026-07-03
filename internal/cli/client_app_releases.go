package cli

import (
	"net/http"
	"path"
	"strings"

	"fugue/internal/model"
)

type appReleaseCreateCLIRequest struct {
	Role             string         `json:"role,omitempty"`
	SourceRef        string         `json:"source_ref,omitempty"`
	ResolvedImageRef string         `json:"resolved_image_ref,omitempty"`
	UpstreamURL      string         `json:"upstream_url,omitempty"`
	RuntimeID        string         `json:"runtime_id,omitempty"`
	DeploymentName   string         `json:"deployment_name,omitempty"`
	ServiceName      string         `json:"service_name,omitempty"`
	Status           string         `json:"status,omitempty"`
	StatusReason     string         `json:"status_reason,omitempty"`
	SpecSnapshot     *model.AppSpec `json:"spec_snapshot,omitempty"`
}

type appReleaseListCLIResponse struct {
	AppID    string                  `json:"app_id"`
	Releases []model.AppRelease      `json:"releases"`
	Traffic  *model.AppTrafficPolicy `json:"traffic,omitempty"`
}

type appReleaseCLIResponse struct {
	AppID   string           `json:"app_id"`
	Release model.AppRelease `json:"release"`
}

type appTrafficPatchCLIRequest struct {
	Mode               string `json:"mode,omitempty"`
	StableReleaseID    string `json:"stable_release_id,omitempty"`
	CandidateReleaseID string `json:"candidate_release_id,omitempty"`
	StableWeight       *int   `json:"stable_weight,omitempty"`
	CandidateWeight    *int   `json:"candidate_weight,omitempty"`
	StickyHeader       string `json:"sticky_header,omitempty"`
	StickyCookie       string `json:"sticky_cookie,omitempty"`
}

type appTrafficCLIResponse struct {
	AppID    string                 `json:"app_id"`
	Traffic  model.AppTrafficPolicy `json:"traffic"`
	Releases []model.AppRelease     `json:"releases,omitempty"`
}

type appReleasePromoteCLIRequest struct {
	CandidateWeight *int `json:"candidate_weight,omitempty"`
}

type appReleaseAbortCLIRequest struct {
	MarkFailed bool   `json:"mark_failed,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

type appReleaseProbeCLIRequest struct {
	Probes []model.AppReleaseProbe `json:"probes,omitempty"`
}

type appReleaseProbeCLIResponse struct {
	AppID   string                        `json:"app_id"`
	Release model.AppRelease              `json:"release"`
	Results []model.AppReleaseProbeResult `json:"results"`
	Status  string                        `json:"status"`
}

type appReleaseGateCLIRequest struct {
	Policy *model.AppReleaseGatePolicy `json:"policy,omitempty"`
}

type appReleaseGateCLIResponse struct {
	AppID  string                     `json:"app_id"`
	Gate   model.AppReleaseGateResult `json:"gate"`
	Policy model.AppReleaseGatePolicy `json:"policy"`
}

func (c *Client) ListAppReleases(appID string) (appReleaseListCLIResponse, error) {
	var response appReleaseListCLIResponse
	err := c.doJSON(http.MethodGet, path.Join("/v1/apps", strings.TrimSpace(appID), "releases"), nil, &response)
	return response, err
}

func (c *Client) CreateAppRelease(appID string, req appReleaseCreateCLIRequest) (appReleaseCLIResponse, error) {
	var response appReleaseCLIResponse
	err := c.doJSON(http.MethodPost, path.Join("/v1/apps", strings.TrimSpace(appID), "releases"), req, &response)
	return response, err
}

func (c *Client) GetAppTrafficPolicy(appID string) (appTrafficCLIResponse, error) {
	var response appTrafficCLIResponse
	err := c.doJSON(http.MethodGet, path.Join("/v1/apps", strings.TrimSpace(appID), "traffic"), nil, &response)
	return response, err
}

func (c *Client) PatchAppTrafficPolicy(appID string, req appTrafficPatchCLIRequest) (appTrafficCLIResponse, error) {
	var response appTrafficCLIResponse
	err := c.doJSON(http.MethodPatch, path.Join("/v1/apps", strings.TrimSpace(appID), "traffic"), req, &response)
	return response, err
}

func (c *Client) ProbeAppRelease(appID, releaseID string, probes []model.AppReleaseProbe) (appReleaseProbeCLIResponse, error) {
	var response appReleaseProbeCLIResponse
	req := appReleaseProbeCLIRequest{Probes: probes}
	err := c.doJSON(http.MethodPost, path.Join("/v1/apps", strings.TrimSpace(appID), "releases", strings.TrimSpace(releaseID), "probe"), req, &response)
	return response, err
}

func (c *Client) EvaluateAppReleaseGate(appID, releaseID string, policy model.AppReleaseGatePolicy) (appReleaseGateCLIResponse, error) {
	var response appReleaseGateCLIResponse
	req := appReleaseGateCLIRequest{Policy: &policy}
	err := c.doJSON(http.MethodPost, path.Join("/v1/apps", strings.TrimSpace(appID), "releases", strings.TrimSpace(releaseID), "gate", "evaluate"), req, &response)
	return response, err
}

func (c *Client) PromoteAppRelease(appID, releaseID string, weight int) (appTrafficCLIResponse, error) {
	var response appTrafficCLIResponse
	req := appReleasePromoteCLIRequest{CandidateWeight: &weight}
	err := c.doJSON(http.MethodPost, path.Join("/v1/apps", strings.TrimSpace(appID), "releases", strings.TrimSpace(releaseID), "promote"), req, &response)
	return response, err
}

func (c *Client) AbortAppRelease(appID, releaseID string, markFailed bool, reason string) (appTrafficCLIResponse, error) {
	var response appTrafficCLIResponse
	req := appReleaseAbortCLIRequest{MarkFailed: markFailed, Reason: strings.TrimSpace(reason)}
	err := c.doJSON(http.MethodPost, path.Join("/v1/apps", strings.TrimSpace(appID), "releases", strings.TrimSpace(releaseID), "abort"), req, &response)
	return response, err
}

func (c *Client) ListAppReleaseAttempts(appID string) ([]model.ReleaseAttempt, error) {
	var response struct {
		ReleaseAttempts []model.ReleaseAttempt `json:"release_attempts"`
	}
	err := c.doJSON(http.MethodGet, path.Join("/v1/apps", strings.TrimSpace(appID), "release-attempts"), nil, &response)
	return response.ReleaseAttempts, err
}

func (c *Client) GetAppReleaseAttempt(appID, attemptID string) (model.ReleaseAttempt, error) {
	var response struct {
		ReleaseAttempt model.ReleaseAttempt `json:"release_attempt"`
	}
	err := c.doJSON(http.MethodGet, path.Join("/v1/apps", strings.TrimSpace(appID), "release-attempts", strings.TrimSpace(attemptID)), nil, &response)
	return response.ReleaseAttempt, err
}

func (c *Client) GetAppReleaseAttemptTimeline(appID, attemptID string) ([]model.ReleaseTimelineEntry, error) {
	var response struct {
		Timeline []model.ReleaseTimelineEntry `json:"timeline"`
	}
	err := c.doJSON(http.MethodGet, path.Join("/v1/apps", strings.TrimSpace(appID), "release-attempts", strings.TrimSpace(attemptID), "timeline"), nil, &response)
	return response.Timeline, err
}

func (c *Client) GetAppReleaseAttemptEvidence(appID, attemptID string, includePayload bool) ([]model.OperationEvidence, error) {
	var response struct {
		Evidence []model.OperationEvidence `json:"evidence"`
	}
	apiPath := path.Join("/v1/apps", strings.TrimSpace(appID), "release-attempts", strings.TrimSpace(attemptID), "evidence")
	if includePayload {
		apiPath += "?include_payload=true"
	}
	err := c.doJSON(http.MethodGet, apiPath, nil, &response)
	return response.Evidence, err
}

func (c *Client) GetAppReleaseAttemptDebugBundleZip(appID, attemptID string) ([]byte, error) {
	return c.doJSONRaw(http.MethodGet, path.Join("/v1/apps", strings.TrimSpace(appID), "release-attempts", strings.TrimSpace(attemptID), "debug-bundle")+"?format=zip", nil)
}

func (c *Client) GetAppReleaseAttemptDebugBundle(appID, attemptID string) (model.ReleaseDebugBundle, error) {
	var response struct {
		Bundle model.ReleaseDebugBundle `json:"bundle"`
	}
	err := c.doJSON(http.MethodGet, path.Join("/v1/apps", strings.TrimSpace(appID), "release-attempts", strings.TrimSpace(attemptID), "debug-bundle"), nil, &response)
	return response.Bundle, err
}
