package cli

import (
	"net/http"
	"net/url"
	"strings"

	"fugue/internal/model"
)

type controlPlaneStoreStatusResponse struct {
	Status model.ControlPlaneStoreStatus `json:"status"`
}

type cliStorePromoteRequest struct {
	SourceKind        string `json:"source_kind,omitempty"`
	SourceFingerprint string `json:"source_fingerprint,omitempty"`
	TargetStore       string `json:"target_store,omitempty"`
	Generation        string `json:"generation,omitempty"`
	BackupRef         string `json:"backup_ref,omitempty"`
	RollbackRef       string `json:"rollback_ref,omitempty"`
	DryRun            bool   `json:"dry_run,omitempty"`
	Confirm           bool   `json:"confirm,omitempty"`
}

type storePromotionResponse struct {
	Promotion model.StorePromotion          `json:"promotion"`
	Status    model.ControlPlaneStoreStatus `json:"status"`
}

type routeExplainEnvelope struct {
	Explain model.RouteExplainResponse `json:"explain"`
}

type routeServingModeListEnvelope struct {
	Routes []model.RouteServingMode `json:"routes"`
}

type platformAutonomyStatusResponse struct {
	Status model.PlatformAutonomyStatus `json:"status"`
}

type dnsFullZonePreflightEnvelope struct {
	Preflight model.DNSFullZonePreflightResponse `json:"preflight"`
}

type platformFailureDrillEnvelope struct {
	Report model.PlatformFailureDrillReport `json:"report"`
}

type releaseGuardStatusEnvelope struct {
	Status model.ReleaseGuardStatus `json:"status"`
}

type gatePolicyListEnvelope struct {
	Policies []model.GatePolicy `json:"policies"`
}

type gatePolicyEnvelope struct {
	Policy model.GatePolicy `json:"policy"`
}

type gatePolicyPromotionEnvelope struct {
	Policy   model.GatePolicy              `json:"policy"`
	Artifact model.PlatformArtifact        `json:"artifact"`
	Release  model.PlatformArtifactRelease `json:"release"`
	Message  model.PlatformReleaseMessage  `json:"message"`
	LKG      *model.PlatformLKGSnapshot    `json:"lkg,omitempty"`
}

type trafficSafetyExplainEnvelope struct {
	State model.ServiceTrafficSafetyState `json:"state"`
}

type requestExplainEnvelope struct {
	Explain model.RequestExplainResponse `json:"explain"`
}

type keyRotationPreflightEnvelope struct {
	Preflight model.KeyRotationPreflight `json:"preflight"`
}

type robustnessStatusEnvelope struct {
	Status model.RobustnessStatus `json:"status"`
}

type robustnessIncidentListEnvelope struct {
	Incidents   []model.RobustnessIncident `json:"incidents"`
	GeneratedAt string                     `json:"generated_at,omitempty"`
}

type robustnessIncidentEnvelope struct {
	Incident model.RobustnessIncident `json:"incident"`
	Status   model.RobustnessStatus   `json:"status"`
}

type robustnessRepairPlanEnvelope struct {
	Plan model.RobustnessRepairPlan `json:"plan"`
}

type platformArtifactListEnvelope struct {
	Artifacts []model.PlatformArtifact `json:"artifacts"`
}

type platformArtifactEnvelope struct {
	Artifact model.PlatformArtifact `json:"artifact"`
}

type platformArtifactValidationEnvelope struct {
	Artifact model.PlatformArtifact                   `json:"artifact"`
	Results  []model.PlatformArtifactValidationResult `json:"results"`
	Pass     bool                                     `json:"pass"`
	DryRun   bool                                     `json:"dry_run"`
}

type platformArtifactReleaseEnvelope struct {
	Artifact model.PlatformArtifact        `json:"artifact"`
	Release  model.PlatformArtifactRelease `json:"release"`
	Message  model.PlatformReleaseMessage  `json:"message"`
	LKG      *model.PlatformLKGSnapshot    `json:"lkg,omitempty"`
}

type platformArtifactConsumersEnvelope struct {
	Consumers []model.PlatformConsumerInstance `json:"consumers"`
}

type platformArtifactLKGEnvelope struct {
	LKG *model.PlatformLKGSnapshot `json:"lkg,omitempty"`
}

type platformStateArtifactEnvelope struct {
	Artifact   *model.PlatformArtifact        `json:"artifact,omitempty"`
	Release    *model.PlatformArtifactRelease `json:"release,omitempty"`
	Messages   []model.PlatformReleaseMessage `json:"messages,omitempty"`
	LKG        *model.PlatformLKGSnapshot     `json:"lkg,omitempty"`
	Generation string                         `json:"generation,omitempty"`
	Waited     bool                           `json:"waited"`
}

type subsystemFailureContractListEnvelope struct {
	Contracts []model.SubsystemFailureContract `json:"contracts"`
}

type subsystemFailureContractEnvelope struct {
	Contract model.SubsystemFailureContract `json:"contract"`
}

func (c *Client) GetControlPlaneStoreStatus() (model.ControlPlaneStoreStatus, error) {
	var response controlPlaneStoreStatusResponse
	if err := c.doJSON(http.MethodGet, "/v1/admin/control-plane/store/status", nil, &response); err != nil {
		return model.ControlPlaneStoreStatus{}, err
	}
	return response.Status, nil
}

func (c *Client) PromoteControlPlaneStore(request cliStorePromoteRequest) (storePromotionResponse, error) {
	var response storePromotionResponse
	if err := c.doJSON(http.MethodPost, "/v1/admin/control-plane/store/promote", request, &response); err != nil {
		return storePromotionResponse{}, err
	}
	return response, nil
}

func (c *Client) ExplainRoute(hostname string) (model.RouteExplainResponse, error) {
	var response routeExplainEnvelope
	if err := c.doJSON(http.MethodGet, "/v1/admin/routes/explain/"+url.PathEscape(strings.TrimSpace(hostname)), nil, &response); err != nil {
		return model.RouteExplainResponse{}, err
	}
	return response.Explain, nil
}

func (c *Client) ListRouteServingModes() ([]model.RouteServingMode, error) {
	var response routeServingModeListEnvelope
	if err := c.doJSON(http.MethodGet, "/v1/admin/routes", nil, &response); err != nil {
		return nil, err
	}
	return response.Routes, nil
}

func (c *Client) GetPlatformAutonomyStatus() (model.PlatformAutonomyStatus, error) {
	var response platformAutonomyStatusResponse
	if err := c.doJSON(http.MethodGet, "/v1/admin/platform/autonomy/status", nil, &response); err != nil {
		return model.PlatformAutonomyStatus{}, err
	}
	return response.Status, nil
}

func (c *Client) RunPlatformFailureDrill(request model.PlatformFailureDrillRequest) (model.PlatformFailureDrillReport, error) {
	var response platformFailureDrillEnvelope
	if err := c.doJSON(http.MethodPost, "/v1/admin/platform/failure-drills", request, &response); err != nil {
		return model.PlatformFailureDrillReport{}, err
	}
	return response.Report, nil
}

func (c *Client) GetReleaseGuardStatus(subject string) (model.ReleaseGuardStatus, error) {
	path := "/v1/admin/release-guard/status"
	values := url.Values{}
	if strings.TrimSpace(subject) != "" {
		values.Set("subject", strings.TrimSpace(subject))
	}
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var response releaseGuardStatusEnvelope
	if err := c.doJSON(http.MethodGet, path, nil, &response); err != nil {
		return model.ReleaseGuardStatus{}, err
	}
	return response.Status, nil
}

func (c *Client) ListGatePolicies() ([]model.GatePolicy, error) {
	var response gatePolicyListEnvelope
	if err := c.doJSON(http.MethodGet, "/v1/admin/gates", nil, &response); err != nil {
		return nil, err
	}
	return response.Policies, nil
}

func (c *Client) GetGatePolicy(gateID string) (model.GatePolicy, error) {
	var response gatePolicyEnvelope
	if err := c.doJSON(http.MethodGet, "/v1/admin/gates/"+url.PathEscape(strings.TrimSpace(gateID)), nil, &response); err != nil {
		return model.GatePolicy{}, err
	}
	return response.Policy, nil
}

func (c *Client) PromoteGatePolicy(gateID string, request model.GatePolicyPromoteRequest) (model.GatePolicyPromotionResponse, error) {
	var response gatePolicyPromotionEnvelope
	if err := c.doJSON(http.MethodPost, "/v1/admin/gates/"+url.PathEscape(strings.TrimSpace(gateID))+"/promote", request, &response); err != nil {
		return model.GatePolicyPromotionResponse{}, err
	}
	return model.GatePolicyPromotionResponse{
		Policy:   response.Policy,
		Artifact: response.Artifact,
		Release:  response.Release,
		Message:  response.Message,
		LKG:      response.LKG,
	}, nil
}

func (c *Client) ExplainTrafficSafety(hostname string, minHealthyEdges int) (model.ServiceTrafficSafetyState, error) {
	values := url.Values{}
	if minHealthyEdges > 0 {
		values.Set("min_healthy_edges", formatInt(minHealthyEdges))
	}
	path := "/v1/admin/traffic-safety/explain/" + url.PathEscape(strings.TrimSpace(hostname))
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var response trafficSafetyExplainEnvelope
	if err := c.doJSON(http.MethodGet, path, nil, &response); err != nil {
		return model.ServiceTrafficSafetyState{}, err
	}
	return response.State, nil
}

func (c *Client) ExplainRequest(requestID, since string) (model.RequestExplainResponse, error) {
	values := url.Values{}
	if strings.TrimSpace(since) != "" {
		values.Set("since", strings.TrimSpace(since))
	}
	path := "/v1/admin/requests/" + url.PathEscape(strings.TrimSpace(requestID)) + "/explain"
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var response requestExplainEnvelope
	if err := c.doJSON(http.MethodGet, path, nil, &response); err != nil {
		return model.RequestExplainResponse{}, err
	}
	return response.Explain, nil
}

func (c *Client) PreflightKeyRotation(request model.KeyRotationPreflightRequest) (model.KeyRotationPreflight, error) {
	var response keyRotationPreflightEnvelope
	if err := c.doJSON(http.MethodPost, "/v1/admin/security/key-rotation", request, &response); err != nil {
		return model.KeyRotationPreflight{}, err
	}
	return response.Preflight, nil
}

func (c *Client) DNSFullZonePreflight(zone, dnssecStatus string, minHealthyNodes int) (model.DNSFullZonePreflightResponse, error) {
	values := url.Values{}
	if strings.TrimSpace(zone) != "" {
		values.Set("zone", strings.TrimSpace(zone))
	}
	if strings.TrimSpace(dnssecStatus) != "" {
		values.Set("dnssec_status", strings.TrimSpace(dnssecStatus))
	}
	if minHealthyNodes > 0 {
		values.Set("min_healthy_nodes", formatInt(minHealthyNodes))
	}
	path := "/v1/admin/dns/full-zone/preflight"
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var response dnsFullZonePreflightEnvelope
	if err := c.doJSON(http.MethodGet, path, nil, &response); err != nil {
		return model.DNSFullZonePreflightResponse{}, err
	}
	return response.Preflight, nil
}

func (c *Client) GetRobustnessStatus(subject string) (model.RobustnessStatus, error) {
	path := "/v1/admin/robustness/status"
	values := url.Values{}
	if strings.TrimSpace(subject) != "" {
		values.Set("subject", strings.TrimSpace(subject))
	}
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var response robustnessStatusEnvelope
	if err := c.doJSON(http.MethodGet, path, nil, &response); err != nil {
		return model.RobustnessStatus{}, err
	}
	return response.Status, nil
}

func (c *Client) CheckRobustnessSubject(subject string) (model.RobustnessStatus, error) {
	var response robustnessStatusEnvelope
	if err := c.doJSON(http.MethodGet, "/v1/admin/robustness/check/"+url.PathEscape(strings.TrimSpace(subject)), nil, &response); err != nil {
		return model.RobustnessStatus{}, err
	}
	return response.Status, nil
}

func (c *Client) ListRobustnessIncidents(subject string) ([]model.RobustnessIncident, error) {
	path := "/v1/admin/robustness/incidents"
	values := url.Values{}
	if strings.TrimSpace(subject) != "" {
		values.Set("subject", strings.TrimSpace(subject))
	}
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var response robustnessIncidentListEnvelope
	if err := c.doJSON(http.MethodGet, path, nil, &response); err != nil {
		return nil, err
	}
	return response.Incidents, nil
}

func (c *Client) GetRobustnessIncident(id, subject string) (model.RobustnessIncident, model.RobustnessStatus, error) {
	path := "/v1/admin/robustness/incidents/" + url.PathEscape(strings.TrimSpace(id))
	values := url.Values{}
	if strings.TrimSpace(subject) != "" {
		values.Set("subject", strings.TrimSpace(subject))
	}
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var response robustnessIncidentEnvelope
	if err := c.doJSON(http.MethodGet, path, nil, &response); err != nil {
		return model.RobustnessIncident{}, model.RobustnessStatus{}, err
	}
	return response.Incident, response.Status, nil
}

func (c *Client) PlanRobustnessRepair(id, subject string) (model.RobustnessRepairPlan, error) {
	path := "/v1/admin/robustness/incidents/" + url.PathEscape(strings.TrimSpace(id)) + "/repair-plan"
	values := url.Values{}
	if strings.TrimSpace(subject) != "" {
		values.Set("subject", strings.TrimSpace(subject))
	}
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var response robustnessRepairPlanEnvelope
	if err := c.doJSON(http.MethodPost, path, nil, &response); err != nil {
		return model.RobustnessRepairPlan{}, err
	}
	return response.Plan, nil
}

func (c *Client) RunRobustnessRepair(id, subject string, request model.RobustnessRepairRequest) (model.RobustnessRepairPlan, error) {
	path := "/v1/admin/robustness/incidents/" + url.PathEscape(strings.TrimSpace(id)) + "/repair"
	values := url.Values{}
	if strings.TrimSpace(subject) != "" {
		values.Set("subject", strings.TrimSpace(subject))
	}
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var response robustnessRepairPlanEnvelope
	if err := c.doJSON(http.MethodPost, path, request, &response); err != nil {
		return model.RobustnessRepairPlan{}, err
	}
	return response.Plan, nil
}

func (c *Client) ListPlatformArtifacts(kind, scope, status string, limit int) ([]model.PlatformArtifact, error) {
	values := url.Values{}
	if strings.TrimSpace(kind) != "" {
		values.Set("kind", strings.TrimSpace(kind))
	}
	if strings.TrimSpace(scope) != "" {
		values.Set("scope", strings.TrimSpace(scope))
	}
	if strings.TrimSpace(status) != "" {
		values.Set("status", strings.TrimSpace(status))
	}
	if limit > 0 {
		values.Set("limit", formatInt(limit))
	}
	path := "/v1/admin/artifacts"
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var response platformArtifactListEnvelope
	if err := c.doJSON(http.MethodGet, path, nil, &response); err != nil {
		return nil, err
	}
	return response.Artifacts, nil
}

func (c *Client) CreatePlatformArtifact(req model.PlatformArtifactCreateRequest) (model.PlatformArtifact, error) {
	var response platformArtifactEnvelope
	if err := c.doJSON(http.MethodPost, "/v1/admin/artifacts", req, &response); err != nil {
		return model.PlatformArtifact{}, err
	}
	return response.Artifact, nil
}

func (c *Client) GetPlatformArtifact(id string) (model.PlatformArtifact, error) {
	var response platformArtifactEnvelope
	if err := c.doJSON(http.MethodGet, "/v1/admin/artifacts/"+url.PathEscape(strings.TrimSpace(id)), nil, &response); err != nil {
		return model.PlatformArtifact{}, err
	}
	return response.Artifact, nil
}

func (c *Client) ValidatePlatformArtifact(id string, dryRun bool) (platformArtifactValidationEnvelope, error) {
	var response platformArtifactValidationEnvelope
	err := c.doJSON(http.MethodPost, "/v1/admin/artifacts/"+url.PathEscape(strings.TrimSpace(id))+"/validate", model.PlatformArtifactValidateRequest{DryRun: dryRun}, &response)
	return response, err
}

func (c *Client) ReleasePlatformArtifact(id string, req model.PlatformArtifactReleaseRequest) (platformArtifactReleaseEnvelope, error) {
	var response platformArtifactReleaseEnvelope
	err := c.doJSON(http.MethodPost, "/v1/admin/artifacts/"+url.PathEscape(strings.TrimSpace(id))+"/release", req, &response)
	return response, err
}

func (c *Client) VerifyPlatformArtifactReleaseLKG(releaseID string, req model.PlatformArtifactVerifyLKGRequest) (platformArtifactReleaseEnvelope, error) {
	var response platformArtifactReleaseEnvelope
	err := c.doJSON(http.MethodPost, "/v1/admin/artifact-releases/"+url.PathEscape(strings.TrimSpace(releaseID))+"/verify-lkg", req, &response)
	return response, err
}

func (c *Client) RollbackPlatformArtifact(id string, req model.PlatformArtifactRollbackRequest) (platformArtifactReleaseEnvelope, error) {
	var response platformArtifactReleaseEnvelope
	err := c.doJSON(http.MethodPost, "/v1/admin/artifacts/"+url.PathEscape(strings.TrimSpace(id))+"/rollback", req, &response)
	return response, err
}

func (c *Client) ListPlatformArtifactConsumers(id string) ([]model.PlatformConsumerInstance, error) {
	var response platformArtifactConsumersEnvelope
	if err := c.doJSON(http.MethodGet, "/v1/admin/artifacts/"+url.PathEscape(strings.TrimSpace(id))+"/consumers", nil, &response); err != nil {
		return nil, err
	}
	return response.Consumers, nil
}

func (c *Client) GetPlatformArtifactLKG(id string) (*model.PlatformLKGSnapshot, error) {
	var response platformArtifactLKGEnvelope
	if err := c.doJSON(http.MethodGet, "/v1/admin/artifacts/"+url.PathEscape(strings.TrimSpace(id))+"/lkg", nil, &response); err != nil {
		return nil, err
	}
	return response.LKG, nil
}

func (c *Client) GetPlatformStateArtifact(kind, scopeKey, channel string) (platformStateArtifactEnvelope, error) {
	values := url.Values{}
	if strings.TrimSpace(scopeKey) != "" {
		values.Set("scope_key", strings.TrimSpace(scopeKey))
	}
	if strings.TrimSpace(channel) != "" {
		values.Set("channel", strings.TrimSpace(channel))
	}
	path := "/v1/platform-state/artifacts/" + url.PathEscape(strings.TrimSpace(kind))
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var response platformStateArtifactEnvelope
	err := c.doJSON(http.MethodGet, path, nil, &response)
	return response, err
}

func (c *Client) ListSubsystemFailureContracts() ([]model.SubsystemFailureContract, error) {
	var response subsystemFailureContractListEnvelope
	if err := c.doJSON(http.MethodGet, "/v1/admin/failure-contracts", nil, &response); err != nil {
		return nil, err
	}
	return response.Contracts, nil
}

func (c *Client) GetSubsystemFailureContract(subsystem string) (model.SubsystemFailureContract, error) {
	var response subsystemFailureContractEnvelope
	if err := c.doJSON(http.MethodGet, "/v1/admin/failure-contracts/"+url.PathEscape(strings.TrimSpace(subsystem)), nil, &response); err != nil {
		return model.SubsystemFailureContract{}, err
	}
	return response.Contract, nil
}
