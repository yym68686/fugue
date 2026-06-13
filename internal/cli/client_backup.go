package cli

import (
	"net/http"
	"net/url"
	"path"

	"fugue/internal/model"
)

type backupBackendTestResponse struct {
	Status  string              `json:"status"`
	Message string              `json:"message"`
	Backend model.BackupBackend `json:"backend"`
}

type backupRunEnvelope struct {
	Run       model.BackupRun        `json:"run"`
	Artifacts []model.BackupArtifact `json:"artifacts,omitempty"`
}

type adminBackupStatusResponse struct {
	Policies []model.BackupPolicy  `json:"policies"`
	Runs     []model.BackupRun     `json:"runs"`
	Usage    model.BackupUsage     `json:"usage"`
	Posture  []model.BackupPosture `json:"posture"`
}

type appBackupStatusResponse struct {
	App       model.App              `json:"app"`
	Policies  []model.BackupPolicy   `json:"policies"`
	Artifacts []model.BackupArtifact `json:"artifacts"`
	Posture   []model.BackupPosture  `json:"posture"`
}

func (c *Client) ListBackupBackends() ([]model.BackupBackend, error) {
	var resp struct {
		Backends []model.BackupBackend `json:"backends"`
	}
	if err := c.doJSON(http.MethodGet, "/v1/backups/backends", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Backends, nil
}

func (c *Client) CreateBackupBackend(req map[string]any) (model.BackupBackend, error) {
	var resp struct {
		Backend model.BackupBackend `json:"backend"`
	}
	if err := c.doJSON(http.MethodPost, "/v1/backups/backends", req, &resp); err != nil {
		return model.BackupBackend{}, err
	}
	return resp.Backend, nil
}

func (c *Client) GetBackupBackend(id string) (model.BackupBackend, error) {
	var resp struct {
		Backend model.BackupBackend `json:"backend"`
	}
	if err := c.doJSON(http.MethodGet, "/v1/backups/backends/"+url.PathEscape(id), nil, &resp); err != nil {
		return model.BackupBackend{}, err
	}
	return resp.Backend, nil
}

func (c *Client) DeleteBackupBackend(id string) (model.BackupBackend, error) {
	var resp struct {
		Backend model.BackupBackend `json:"backend"`
	}
	if err := c.doJSON(http.MethodDelete, "/v1/backups/backends/"+url.PathEscape(id), nil, &resp); err != nil {
		return model.BackupBackend{}, err
	}
	return resp.Backend, nil
}

func (c *Client) TestBackupBackend(id string) (backupBackendTestResponse, error) {
	var resp backupBackendTestResponse
	err := c.doJSON(http.MethodPost, "/v1/backups/backends/"+url.PathEscape(id)+"/test", nil, &resp)
	return resp, err
}

func (c *Client) ListBackupPolicies(values url.Values) ([]model.BackupPolicy, error) {
	var resp struct {
		Policies []model.BackupPolicy `json:"policies"`
	}
	relative := "/v1/backups/policies"
	if len(values) > 0 {
		relative += "?" + values.Encode()
	}
	if err := c.doJSON(http.MethodGet, relative, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Policies, nil
}

func (c *Client) UpsertBackupPolicy(req map[string]any) (model.BackupPolicy, error) {
	var resp struct {
		Policy model.BackupPolicy `json:"policy"`
	}
	if err := c.doJSON(http.MethodPost, "/v1/backups/policies", req, &resp); err != nil {
		return model.BackupPolicy{}, err
	}
	return resp.Policy, nil
}

func (c *Client) PatchBackupPolicy(id string, req map[string]any) (model.BackupPolicy, error) {
	var resp struct {
		Policy model.BackupPolicy `json:"policy"`
	}
	if err := c.doJSON(http.MethodPatch, "/v1/backups/policies/"+url.PathEscape(id), req, &resp); err != nil {
		return model.BackupPolicy{}, err
	}
	return resp.Policy, nil
}

func (c *Client) GetBackupPolicy(id string) (model.BackupPolicy, error) {
	var resp struct {
		Policy model.BackupPolicy `json:"policy"`
	}
	if err := c.doJSON(http.MethodGet, "/v1/backups/policies/"+url.PathEscape(id), nil, &resp); err != nil {
		return model.BackupPolicy{}, err
	}
	return resp.Policy, nil
}

func (c *Client) ListBackupRuns(values url.Values) ([]model.BackupRun, error) {
	var resp struct {
		Runs []model.BackupRun `json:"runs"`
	}
	relative := "/v1/backups/runs"
	if len(values) > 0 {
		relative += "?" + values.Encode()
	}
	if err := c.doJSON(http.MethodGet, relative, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Runs, nil
}

func (c *Client) CreateBackupRun(req map[string]any) (backupRunEnvelope, error) {
	var resp backupRunEnvelope
	err := c.doJSON(http.MethodPost, "/v1/backups/runs", req, &resp)
	return resp, err
}

func (c *Client) GetBackupRun(id string) (backupRunEnvelope, error) {
	var resp backupRunEnvelope
	err := c.doJSON(http.MethodGet, "/v1/backups/runs/"+url.PathEscape(id), nil, &resp)
	return resp, err
}

func (c *Client) ListBackupArtifacts(values url.Values) ([]model.BackupArtifact, error) {
	var resp struct {
		Artifacts []model.BackupArtifact `json:"artifacts"`
	}
	relative := "/v1/backups/artifacts"
	if len(values) > 0 {
		relative += "?" + values.Encode()
	}
	if err := c.doJSON(http.MethodGet, relative, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Artifacts, nil
}

func (c *Client) GetBackupArtifact(id string) (model.BackupArtifact, error) {
	var resp struct {
		Artifact model.BackupArtifact `json:"artifact"`
	}
	if err := c.doJSON(http.MethodGet, "/v1/backups/artifacts/"+url.PathEscape(id), nil, &resp); err != nil {
		return model.BackupArtifact{}, err
	}
	return resp.Artifact, nil
}

func (c *Client) DeleteBackupArtifact(id string) (model.BackupArtifact, error) {
	var resp struct {
		Artifact model.BackupArtifact `json:"artifact"`
	}
	if err := c.doJSON(http.MethodDelete, "/v1/backups/artifacts/"+url.PathEscape(id), nil, &resp); err != nil {
		return model.BackupArtifact{}, err
	}
	return resp.Artifact, nil
}

func (c *Client) CreateBackupRestorePlan(req map[string]any) (model.BackupRestorePlan, error) {
	var resp struct {
		Plan model.BackupRestorePlan `json:"plan"`
	}
	if err := c.doJSON(http.MethodPost, "/v1/backups/restore-plans", req, &resp); err != nil {
		return model.BackupRestorePlan{}, err
	}
	return resp.Plan, nil
}

func (c *Client) ListBackupRestorePlans() ([]model.BackupRestorePlan, error) {
	var resp struct {
		Plans []model.BackupRestorePlan `json:"plans"`
	}
	if err := c.doJSON(http.MethodGet, "/v1/backups/restore-plans", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Plans, nil
}

func (c *Client) CreateBackupRestoreRun(req map[string]any) (model.BackupRestoreRun, error) {
	var resp struct {
		Run model.BackupRestoreRun `json:"run"`
	}
	if err := c.doJSON(http.MethodPost, "/v1/backups/restore-runs", req, &resp); err != nil {
		return model.BackupRestoreRun{}, err
	}
	return resp.Run, nil
}

func (c *Client) GetBackupUsage() (model.BackupUsage, error) {
	var resp struct {
		Usage model.BackupUsage `json:"usage"`
	}
	if err := c.doJSON(http.MethodGet, "/v1/backups/usage", nil, &resp); err != nil {
		return model.BackupUsage{}, err
	}
	return resp.Usage, nil
}

func (c *Client) GetAdminBackupStatus() (adminBackupStatusResponse, error) {
	var resp adminBackupStatusResponse
	err := c.doJSON(http.MethodGet, "/v1/admin/backups/status", nil, &resp)
	return resp, err
}

func (c *Client) GetAppBackupStatus(appID string) (appBackupStatusResponse, error) {
	var resp appBackupStatusResponse
	err := c.doJSON(http.MethodGet, "/v1/apps/"+url.PathEscape(appID)+"/backups/status", nil, &resp)
	return resp, err
}

func (c *Client) CreateAppBackupPolicy(appID string, req map[string]any) (model.BackupPolicy, error) {
	var resp struct {
		Policy model.BackupPolicy `json:"policy"`
	}
	err := c.doJSON(http.MethodPost, path.Join("/v1/apps", url.PathEscape(appID), "backups/policies"), req, &resp)
	return resp.Policy, err
}

func (c *Client) CreateAppBackupRun(appID string, req map[string]any) (backupRunEnvelope, error) {
	var resp backupRunEnvelope
	err := c.doJSON(http.MethodPost, path.Join("/v1/apps", url.PathEscape(appID), "backups/runs"), req, &resp)
	return resp, err
}
