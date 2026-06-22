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
