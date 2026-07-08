package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"fugue/internal/model"
)

type Client struct {
	baseURL              string
	token                string
	cookie               string
	httpClient           *http.Client
	dataObjectHTTPClient *http.Client
	observer             requestObserver
	readRetryCount       int
	readRetryDelay       time.Duration
}

type clientOptions struct {
	Cookie         string
	Observer       requestObserver
	RequireToken   bool
	RequestTimeout time.Duration
	ReadRetryCount int
	ReadRetryDelay time.Duration
}

type importProjectRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type createAppRequest struct {
	TenantID    string           `json:"tenant_id,omitempty"`
	ProjectID   string           `json:"project_id,omitempty"`
	Name        string           `json:"name,omitempty"`
	Description string           `json:"description,omitempty"`
	Spec        model.AppSpec    `json:"spec"`
	Source      *model.AppSource `json:"source,omitempty"`
}

type importGitHubPersistentStorageSeedFile struct {
	Service     string `json:"service"`
	Path        string `json:"path"`
	SeedContent string `json:"seed_content"`
}

type importUploadRequest struct {
	AppID                    string                                            `json:"app_id,omitempty"`
	TenantID                 string                                            `json:"tenant_id,omitempty"`
	ProjectID                string                                            `json:"project_id,omitempty"`
	Project                  *importProjectRequest                             `json:"project,omitempty"`
	SourceDir                string                                            `json:"source_dir,omitempty"`
	Name                     string                                            `json:"name,omitempty"`
	Description              string                                            `json:"description,omitempty"`
	BuildStrategy            string                                            `json:"build_strategy,omitempty"`
	RuntimeID                string                                            `json:"runtime_id,omitempty"`
	Replicas                 int                                               `json:"replicas,omitempty"`
	NetworkMode              string                                            `json:"network_mode,omitempty"`
	ServicePort              int                                               `json:"service_port,omitempty"`
	DockerfilePath           string                                            `json:"dockerfile_path,omitempty"`
	BuildContextDir          string                                            `json:"build_context_dir,omitempty"`
	Env                      map[string]string                                 `json:"env,omitempty"`
	ServiceEnv               map[string]map[string]string                      `json:"service_env,omitempty"`
	ServicePersistentStorage map[string]model.ServicePersistentStorageOverride `json:"service_persistent_storage,omitempty"`
	ConfigContent            string                                            `json:"config_content,omitempty"`
	Files                    []model.AppFile                                   `json:"files,omitempty"`
	StartupCommand           *string                                           `json:"startup_command,omitempty"`
	PersistentStorage        *model.AppPersistentStorageSpec                   `json:"persistent_storage,omitempty"`
	Postgres                 *model.AppPostgresSpec                            `json:"postgres,omitempty"`
	ClearFiles               bool                                              `json:"clear_files,omitempty"`
	ReplaceSource            bool                                              `json:"replace_source,omitempty"`
	UpdateExisting           bool                                              `json:"update_existing,omitempty"`
	DeleteMissing            bool                                              `json:"delete_missing,omitempty"`
	DryRun                   bool                                              `json:"dry_run,omitempty"`
}

type importGitHubRequest struct {
	TenantID                   string                                            `json:"tenant_id,omitempty"`
	ProjectID                  string                                            `json:"project_id,omitempty"`
	Project                    *importProjectRequest                             `json:"project,omitempty"`
	RepoURL                    string                                            `json:"repo_url,omitempty"`
	RepoVisibility             string                                            `json:"repo_visibility,omitempty"`
	RepoAuthToken              string                                            `json:"repo_auth_token,omitempty"`
	Branch                     string                                            `json:"branch,omitempty"`
	SourceDir                  string                                            `json:"source_dir,omitempty"`
	Name                       string                                            `json:"name,omitempty"`
	Description                string                                            `json:"description,omitempty"`
	BuildStrategy              string                                            `json:"build_strategy,omitempty"`
	RuntimeID                  string                                            `json:"runtime_id,omitempty"`
	Replicas                   int                                               `json:"replicas,omitempty"`
	NetworkMode                string                                            `json:"network_mode,omitempty"`
	ServicePort                int                                               `json:"service_port,omitempty"`
	DockerfilePath             string                                            `json:"dockerfile_path,omitempty"`
	BuildContextDir            string                                            `json:"build_context_dir,omitempty"`
	Env                        map[string]string                                 `json:"env,omitempty"`
	ServiceEnv                 map[string]map[string]string                      `json:"service_env,omitempty"`
	ServicePersistentStorage   map[string]model.ServicePersistentStorageOverride `json:"service_persistent_storage,omitempty"`
	ConfigContent              string                                            `json:"config_content,omitempty"`
	Files                      []model.AppFile                                   `json:"files,omitempty"`
	StartupCommand             *string                                           `json:"startup_command,omitempty"`
	PersistentStorage          *model.AppPersistentStorageSpec                   `json:"persistent_storage,omitempty"`
	PersistentStorageSeedFiles []importGitHubPersistentStorageSeedFile           `json:"persistent_storage_seed_files,omitempty"`
	Postgres                   *model.AppPostgresSpec                            `json:"postgres,omitempty"`
	IdempotencyKey             string                                            `json:"idempotency_key,omitempty"`
	UpdateExisting             bool                                              `json:"update_existing,omitempty"`
	DeleteMissing              bool                                              `json:"delete_missing,omitempty"`
	DryRun                     bool                                              `json:"dry_run,omitempty"`
}

type importGitHubIdempotency struct {
	Key      string `json:"key"`
	Status   string `json:"status"`
	Replayed bool   `json:"replayed,omitempty"`
}

type importGitHubResponse struct {
	App               *model.App                `json:"app,omitempty"`
	Operation         *model.Operation          `json:"operation,omitempty"`
	Apps              []model.App               `json:"apps,omitempty"`
	Operations        []model.Operation         `json:"operations,omitempty"`
	Plan              *model.TopologyDeployPlan `json:"plan,omitempty"`
	ComposeStack      map[string]any            `json:"compose_stack,omitempty"`
	FugueManifest     map[string]any            `json:"fugue_manifest,omitempty"`
	Idempotency       *importGitHubIdempotency  `json:"idempotency,omitempty"`
	RequestInProgress bool                      `json:"request_in_progress,omitempty"`
}

type importUploadResponse struct {
	App           *model.App                `json:"app,omitempty"`
	Operation     *model.Operation          `json:"operation,omitempty"`
	Apps          []model.App               `json:"apps,omitempty"`
	Operations    []model.Operation         `json:"operations,omitempty"`
	Plan          *model.TopologyDeployPlan `json:"plan,omitempty"`
	ComposeStack  map[string]any            `json:"compose_stack,omitempty"`
	FugueManifest map[string]any            `json:"fugue_manifest,omitempty"`
}

type importImageRequest struct {
	TenantID          string                          `json:"tenant_id,omitempty"`
	ProjectID         string                          `json:"project_id,omitempty"`
	Project           *importProjectRequest           `json:"project,omitempty"`
	ImageRef          string                          `json:"image_ref,omitempty"`
	Name              string                          `json:"name,omitempty"`
	Description       string                          `json:"description,omitempty"`
	RuntimeID         string                          `json:"runtime_id,omitempty"`
	Replicas          int                             `json:"replicas,omitempty"`
	NetworkMode       string                          `json:"network_mode,omitempty"`
	ServicePort       int                             `json:"service_port,omitempty"`
	Env               map[string]string               `json:"env,omitempty"`
	ConfigContent     string                          `json:"config_content,omitempty"`
	Files             []model.AppFile                 `json:"files,omitempty"`
	StartupCommand    *string                         `json:"startup_command,omitempty"`
	PersistentStorage *model.AppPersistentStorageSpec `json:"persistent_storage,omitempty"`
	Postgres          *model.AppPostgresSpec          `json:"postgres,omitempty"`
}

type importImageResponse struct {
	App       model.App       `json:"app"`
	Operation model.Operation `json:"operation"`
}

type appCreateResponse struct {
	App model.App `json:"app"`
}

type buildLogsResponse struct {
	OperationID     string                  `json:"operation_id"`
	OperationStatus string                  `json:"operation_status"`
	JobName         string                  `json:"job_name"`
	Available       bool                    `json:"available"`
	Source          string                  `json:"source"`
	Logs            string                  `json:"logs"`
	Summary         string                  `json:"summary,omitempty"`
	BuildStrategy   string                  `json:"build_strategy"`
	ErrorMessage    string                  `json:"error_message"`
	ResultMessage   string                  `json:"result_message"`
	LastUpdatedAt   time.Time               `json:"last_updated_at"`
	CompletedAt     *time.Time              `json:"completed_at,omitempty"`
	StartedAt       *time.Time              `json:"started_at,omitempty"`
	ArtifactSummary *appBuildArtifactReport `json:"artifact_summary,omitempty"`
}

type runtimeLogsOptions struct {
	Component string
	Pod       string
	TailLines int
	Previous  bool
	Grep      string
}

type runtimeLogsResponse struct {
	Component string   `json:"component"`
	Namespace string   `json:"namespace"`
	Selector  string   `json:"selector"`
	Container string   `json:"container"`
	Pods      []string `json:"pods"`
	Logs      string   `json:"logs"`
	Warnings  []string `json:"warnings"`
}

type restartAppResponse struct {
	Operation    model.Operation `json:"operation"`
	RestartToken string          `json:"restart_token"`
}

type operationResponse struct {
	Operation model.Operation `json:"operation"`
}

type appResourceRecommendationResponse struct {
	Recommendation model.AppRightSizingRecommendation `json:"recommendation"`
}

type appResourceRecommendationApplyResponse struct {
	Recommendation model.AppRightSizingRecommendation `json:"recommendation"`
	AlreadyCurrent bool                               `json:"already_current"`
	Operation      *model.Operation                   `json:"operation,omitempty"`
}

type appEnvResponse struct {
	Env            map[string]string     `json:"env"`
	Entries        []model.AppEnvEntry   `json:"entries,omitempty"`
	AlreadyCurrent bool                  `json:"already_current,omitempty"`
	Operation      *model.Operation      `json:"operation,omitempty"`
	ReleaseAttempt *model.ReleaseAttempt `json:"release_attempt,omitempty"`
}

type appFilesResponse struct {
	Files          []model.AppFile  `json:"files"`
	AlreadyCurrent bool             `json:"already_current,omitempty"`
	Operation      *model.Operation `json:"operation,omitempty"`
}

type appImageTrackingResponse struct {
	AppID    string                  `json:"app_id"`
	Tracking *model.AppImageTracking `json:"tracking,omitempty"`
}

type appImageTrackingHistoryResponse struct {
	AppID    string                        `json:"app_id"`
	Tracking *model.AppImageTracking       `json:"tracking,omitempty"`
	Checks   []model.AppImageTrackingCheck `json:"checks"`
}

type appImageTrackingDiagnosis struct {
	Category         string                        `json:"category"`
	Summary          string                        `json:"summary"`
	Hint             string                        `json:"hint,omitempty"`
	AppID            string                        `json:"app_id"`
	Tracking         *model.AppImageTracking       `json:"tracking,omitempty"`
	LatestCheck      *model.AppImageTrackingCheck  `json:"latest_check,omitempty"`
	RecentChecks     []model.AppImageTrackingCheck `json:"recent_checks"`
	RemoteDigest     string                        `json:"remote_digest,omitempty"`
	CurrentAppDigest string                        `json:"current_app_digest,omitempty"`
	ActiveOperation  *model.Operation              `json:"active_operation,omitempty"`
	Evidence         []string                      `json:"evidence"`
	Warnings         []string                      `json:"warnings"`
}

type appImageTrackingDiagnosisResponse struct {
	AppID     string                    `json:"app_id"`
	Diagnosis appImageTrackingDiagnosis `json:"diagnosis"`
}

type appImageSyncResponse struct {
	AppID          string                  `json:"app_id"`
	Tracking       *model.AppImageTracking `json:"tracking,omitempty"`
	Operation      *model.Operation        `json:"operation,omitempty"`
	ReleaseAttempt *model.ReleaseAttempt   `json:"release_attempt,omitempty"`
	Digest         string                  `json:"digest,omitempty"`
	Changed        bool                    `json:"changed"`
	AlreadyCurrent bool                    `json:"already_current"`
	RolloutPending bool                    `json:"rollout_pending,omitempty"`
	AppPhase       string                  `json:"app_phase,omitempty"`
	Message        string                  `json:"message,omitempty"`
}

type appDomainAvailability struct {
	Input     string `json:"input,omitempty"`
	Hostname  string `json:"hostname,omitempty"`
	Valid     bool   `json:"valid"`
	Available bool   `json:"available"`
	Current   bool   `json:"current"`
	Reason    string `json:"reason,omitempty"`
}

type appDomainPutResponse struct {
	Domain         model.AppDomain       `json:"domain"`
	Availability   appDomainAvailability `json:"availability"`
	AlreadyCurrent bool                  `json:"already_current"`
}

type appDomainVerifyResponse struct {
	Domain   model.AppDomain `json:"domain"`
	Verified bool            `json:"verified"`
}

type appDomainDNSObservation struct {
	Verified      bool     `json:"verified"`
	RecordKind    string   `json:"record_kind,omitempty"`
	CNAME         string   `json:"cname,omitempty"`
	MatchedTarget string   `json:"matched_target,omitempty"`
	HostIPs       []string `json:"host_ips,omitempty"`
	TargetIPs     []string `json:"target_ips,omitempty"`
	Message       string   `json:"message,omitempty"`
}

type appDomainTLSCertificateSummary struct {
	Present               bool       `json:"present"`
	CertificateSHA256     string     `json:"certificate_sha256,omitempty"`
	NotAfter              *time.Time `json:"not_after,omitempty"`
	IssuerStorage         string     `json:"issuer_storage,omitempty"`
	UploadedByEdgeID      string     `json:"uploaded_by_edge_id,omitempty"`
	UploadedByEdgeGroupID string     `json:"uploaded_by_edge_group_id,omitempty"`
	UpdatedAt             *time.Time `json:"updated_at,omitempty"`
}

type appDomainDiagnosticCheck struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Message    string `json:"message,omitempty"`
	Repairable bool   `json:"repairable,omitempty"`
}

type appDomainDiagnosis struct {
	Domain               model.AppDomain                `json:"domain"`
	DNSTargets           []string                       `json:"dns_targets"`
	DNSObservation       appDomainDNSObservation        `json:"dns_observation"`
	SharedTLSCertificate appDomainTLSCertificateSummary `json:"shared_tls_certificate"`
	Checks               []appDomainDiagnosticCheck     `json:"checks"`
	RecommendedActions   []string                       `json:"recommended_actions,omitempty"`
}

type appDomainDiagnosisResponse struct {
	Diagnosis appDomainDiagnosis `json:"diagnosis"`
}

type appDomainRepairResponse struct {
	Domain    model.AppDomain    `json:"domain"`
	Diagnosis appDomainDiagnosis `json:"diagnosis"`
}

type appDomainRepairRequest struct {
	Hostname    string `json:"hostname"`
	DNSMode     string `json:"dns_mode,omitempty"`
	DNSZoneID   string `json:"dns_zone_id,omitempty"`
	DNSRecordID string `json:"dns_record_id,omitempty"`
	Overwrite   bool   `json:"overwrite,omitempty"`
}

type appDomainMutationRequest struct {
	Hostname    string `json:"hostname"`
	DNSMode     string `json:"dns_mode,omitempty"`
	DNSZoneID   string `json:"dns_zone_id,omitempty"`
	DNSRecordID string `json:"dns_record_id,omitempty"`
	Overwrite   bool   `json:"overwrite,omitempty"`
}

type edgeTLSCertificateBundleInput struct {
	CertificatePEM string `json:"certificate_pem"`
	PrivateKeyPEM  string `json:"private_key_pem"`
	MetadataJSON   string `json:"metadata_json,omitempty"`
	IssuerStorage  string `json:"issuer_storage,omitempty"`
}

type edgeTLSCertificateBundleResponse struct {
	Certificate model.EdgeTLSCertificate `json:"certificate"`
}

type edgeTLSCertificateBundlePutResponse struct {
	Certificate model.EdgeTLSCertificate `json:"certificate"`
	Domain      model.AppDomain          `json:"domain"`
}

type appFilesystemEntry struct {
	Name        string    `json:"name"`
	Path        string    `json:"path"`
	Kind        string    `json:"kind"`
	Size        int64     `json:"size"`
	Mode        int32     `json:"mode,omitempty"`
	ModifiedAt  time.Time `json:"modified_at"`
	HasChildren bool      `json:"has_children"`
}

type appFilesystemTreeResponse struct {
	Component     string               `json:"component"`
	Pod           string               `json:"pod"`
	Path          string               `json:"path"`
	Depth         int                  `json:"depth"`
	WorkspaceRoot string               `json:"workspace_root"`
	Entries       []appFilesystemEntry `json:"entries"`
}

type appFilesystemFileResponse struct {
	Component     string    `json:"component"`
	Pod           string    `json:"pod"`
	Path          string    `json:"path"`
	WorkspaceRoot string    `json:"workspace_root"`
	Content       string    `json:"content"`
	Encoding      string    `json:"encoding"`
	Size          int64     `json:"size"`
	Mode          int32     `json:"mode"`
	ModifiedAt    time.Time `json:"modified_at"`
	Truncated     bool      `json:"truncated"`
}

type appFilesystemMutationResponse struct {
	Component     string     `json:"component"`
	Pod           string     `json:"pod"`
	Path          string     `json:"path"`
	WorkspaceRoot string     `json:"workspace_root"`
	Kind          string     `json:"kind,omitempty"`
	Size          int64      `json:"size,omitempty"`
	Mode          int32      `json:"mode,omitempty"`
	ModifiedAt    *time.Time `json:"modified_at,omitempty"`
	Deleted       bool       `json:"deleted,omitempty"`
}

type appDeleteResponse struct {
	Operation       *model.Operation `json:"operation,omitempty"`
	AlreadyDeleting bool             `json:"already_deleting,omitempty"`
}

type apiError struct {
	Error     string         `json:"error"`
	Code      string         `json:"code,omitempty"`
	Category  string         `json:"category,omitempty"`
	Retryable bool           `json:"retryable,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
}

type apiServerError struct {
	StatusCode int
	Response   apiError
}

func (e *apiServerError) Error() string {
	if e == nil {
		return ""
	}
	if message := strings.TrimSpace(e.Response.Error); message != "" {
		return message
	}
	return fmt.Sprintf("request failed: status=%d", e.StatusCode)
}

func (e *apiServerError) IsRetryable() bool {
	return e != nil && e.Response.Retryable
}

type authContextResponse struct {
	Principal authPrincipalContext `json:"principal"`
}

type authPrincipalContext struct {
	ActorType     string   `json:"actor_type"`
	ActorID       string   `json:"actor_id"`
	TenantID      string   `json:"tenant_id,omitempty"`
	ProjectID     string   `json:"project_id,omitempty"`
	AppID         string   `json:"app_id,omitempty"`
	Scopes        []string `json:"scopes"`
	PlatformAdmin bool     `json:"platform_admin"`
}

func NewClient(baseURL, token string) (*Client, error) {
	return newClientWithOptions(baseURL, token, clientOptions{RequireToken: true})
}

func newClientWithOptions(baseURL, token string, opts clientOptions) (*Client, error) {
	baseURL = strings.TrimSpace(baseURL)
	token = strings.TrimSpace(token)
	if baseURL == "" {
		return nil, fmt.Errorf("base url is required; pass --base-url or set FUGUE_BASE_URL/FUGUE_API_URL")
	}
	if opts.RequireToken && token == "" {
		return nil, fmt.Errorf("API key is required; run `fugue auth login --token <key>`, pass --token, or set FUGUE_API_KEY, FUGUE_TOKEN, or FUGUE_BOOTSTRAP_KEY")
	}
	if !opts.RequireToken && token == "" && strings.TrimSpace(opts.Cookie) == "" {
		return nil, fmt.Errorf("either an API key or cookie is required")
	}
	if _, err := url.Parse(baseURL); err != nil {
		return nil, fmt.Errorf("parse base url %q: %w", baseURL, err)
	}
	timeout := opts.RequestTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	readRetryCount := opts.ReadRetryCount
	if readRetryCount == 0 {
		readRetryCount = 2
	}
	if readRetryCount < 0 {
		readRetryCount = 0
	}
	readRetryDelay := opts.ReadRetryDelay
	if readRetryDelay <= 0 {
		readRetryDelay = 250 * time.Millisecond
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		cookie:  strings.TrimSpace(opts.Cookie),
		httpClient: &http.Client{
			Timeout: timeout,
		},
		dataObjectHTTPClient: newDataObjectHTTPClient(),
		observer:             opts.Observer,
		readRetryCount:       readRetryCount,
		readRetryDelay:       readRetryDelay,
	}, nil
}

func (c *Client) ListTenants() ([]model.Tenant, error) {
	var response struct {
		Tenants []model.Tenant `json:"tenants"`
	}
	if err := c.doJSON(http.MethodGet, "/v1/tenants", nil, &response); err != nil {
		return nil, err
	}
	return response.Tenants, nil
}

func (c *Client) GetAuthContext() (authContextResponse, error) {
	var response authContextResponse
	if err := c.doJSON(http.MethodGet, "/v1/auth/context", nil, &response); err != nil {
		return authContextResponse{}, err
	}
	return response, nil
}

func (c *Client) ListProjects(tenantID string) ([]model.Project, error) {
	var response struct {
		Projects []model.Project `json:"projects"`
	}
	relative := "/v1/projects"
	if trimmed := strings.TrimSpace(tenantID); trimmed != "" {
		relative += "?tenant_id=" + url.QueryEscape(trimmed)
	}
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return nil, err
	}
	return response.Projects, nil
}

func (c *Client) ListApps() ([]model.App, error) {
	return c.ListAppsWithLiveStatus(true)
}

func (c *Client) ListAppsWithLiveStatus(includeLiveStatus bool) ([]model.App, error) {
	return c.ListAppsWithOptions(listAppsOptions{
		IncludeLiveStatus:    includeLiveStatus,
		IncludeResourceUsage: true,
	})
}

type listAppsOptions struct {
	IncludeLiveStatus    bool
	IncludeResourceUsage bool
	TenantID             string
	ProjectID            string
	Query                string
	Domain               string
	SourceRef            string
}

func (c *Client) ListAppsWithOptions(options listAppsOptions) ([]model.App, error) {
	var response struct {
		Apps []model.App `json:"apps"`
	}
	values := url.Values{}
	values.Set("include_live_status", fmt.Sprintf("%t", options.IncludeLiveStatus))
	values.Set("include_resource_usage", fmt.Sprintf("%t", options.IncludeResourceUsage))
	if value := strings.TrimSpace(options.TenantID); value != "" {
		values.Set("tenant_id", value)
	}
	if value := strings.TrimSpace(options.ProjectID); value != "" {
		values.Set("project_id", value)
	}
	if value := strings.TrimSpace(options.Query); value != "" {
		values.Set("q", value)
	}
	if value := strings.TrimSpace(options.Domain); value != "" {
		values.Set("domain", value)
	}
	if value := strings.TrimSpace(options.SourceRef); value != "" {
		values.Set("source_ref", value)
	}
	relative := "/v1/apps?" + values.Encode()
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return nil, err
	}
	return response.Apps, nil
}

func (c *Client) SearchResources(query string, types []string, limit int) (model.SearchResponse, error) {
	var response model.SearchResponse
	values := url.Values{}
	values.Set("q", strings.TrimSpace(query))
	if len(types) > 0 {
		normalized := make([]string, 0, len(types))
		for _, value := range types {
			value = strings.TrimSpace(value)
			if value != "" {
				normalized = append(normalized, value)
			}
		}
		if len(normalized) > 0 {
			values.Set("types", strings.Join(normalized, ","))
		}
	}
	if limit > 0 {
		values.Set("limit", fmt.Sprintf("%d", limit))
	}
	if err := c.doJSON(http.MethodGet, "/v1/search?"+values.Encode(), nil, &response); err != nil {
		return model.SearchResponse{}, err
	}
	return response, nil
}

func (c *Client) CreateApp(request createAppRequest) (model.App, error) {
	var response appCreateResponse
	if err := c.doJSON(http.MethodPost, "/v1/apps", request, &response); err != nil {
		return model.App{}, err
	}
	return response.App, nil
}

func (c *Client) ListRuntimes() ([]model.Runtime, error) {
	var response struct {
		Runtimes []model.Runtime `json:"runtimes"`
	}
	if err := c.doJSON(http.MethodGet, "/v1/runtimes", nil, &response); err != nil {
		return nil, err
	}
	return response.Runtimes, nil
}

func (c *Client) ImportUpload(req importUploadRequest, archiveName string, archiveBytes []byte) (importUploadResponse, error) {
	var response importUploadResponse
	if err := c.doMultipartJSON("/v1/apps/import-upload", req, archiveName, archiveBytes, &response); err != nil {
		return importUploadResponse{}, err
	}
	return response, nil
}

func (c *Client) InspectUploadTemplate(req importUploadRequest, archiveName string, archiveBytes []byte) (inspectUploadTemplateResponse, error) {
	var response inspectUploadTemplateResponse
	if err := c.doMultipartJSON("/v1/templates/inspect-upload", req, archiveName, archiveBytes, &response); err != nil {
		return inspectUploadTemplateResponse{}, err
	}
	return response, nil
}

func (c *Client) doMultipartJSON(relativePath string, requestBody any, archiveName string, archiveBytes []byte, responseBody any) error {
	return c.doMultipartJSONWithFileField(relativePath, requestBody, "archive", archiveName, archiveBytes, responseBody)
}

func (c *Client) doMultipartJSONWithFileField(relativePath string, requestBody any, fileFieldName, fileName string, fileBytes []byte, responseBody any) error {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	requestJSON, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	if err := writer.WriteField("request", string(requestJSON)); err != nil {
		return fmt.Errorf("write request field: %w", err)
	}
	part, err := writer.CreateFormFile(strings.TrimSpace(fileFieldName), fileName)
	if err != nil {
		return fmt.Errorf("create %s field: %w", strings.TrimSpace(fileFieldName), err)
	}
	if _, err := part.Write(fileBytes); err != nil {
		return fmt.Errorf("write %s field: %w", strings.TrimSpace(fileFieldName), err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close multipart writer: %w", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, c.resolveURL(relativePath), &body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	httpReq.Header.Set("Content-Type", writer.FormDataContentType())

	payload, err := c.do(httpReq)
	if err != nil {
		return err
	}
	if responseBody == nil {
		return nil
	}
	if err := json.Unmarshal(payload, responseBody); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (c *Client) ImportGitHub(req importGitHubRequest) (importGitHubResponse, error) {
	var response importGitHubResponse
	if err := c.doJSON(http.MethodPost, "/v1/apps/import-github", req, &response); err != nil {
		return importGitHubResponse{}, err
	}
	return response, nil
}

func (c *Client) ImportImage(req importImageRequest) (importImageResponse, error) {
	var response importImageResponse
	if err := c.doJSON(http.MethodPost, "/v1/apps/import-image", req, &response); err != nil {
		return importImageResponse{}, err
	}
	return response, nil
}

func (c *Client) GetOperation(id string) (model.Operation, error) {
	var response struct {
		Operation model.Operation `json:"operation"`
	}
	if err := c.doJSON(http.MethodGet, path.Join("/v1/operations", id), nil, &response); err != nil {
		return model.Operation{}, err
	}
	return response.Operation, nil
}

func (c *Client) GetOperationDiagnosis(id string) (model.OperationDiagnosis, error) {
	var response struct {
		Diagnosis model.OperationDiagnosis `json:"diagnosis"`
	}
	if err := c.doJSON(http.MethodGet, path.Join("/v1/operations", id, "diagnosis"), nil, &response); err != nil {
		return model.OperationDiagnosis{}, err
	}
	return response.Diagnosis, nil
}

func (c *Client) GetOperationEvidence(id string, includePayload bool) ([]model.OperationEvidence, error) {
	var response struct {
		Evidence []model.OperationEvidence `json:"evidence"`
	}
	apiPath := path.Join("/v1/operations", id, "evidence")
	if includePayload {
		apiPath += "?include_payload=true"
	}
	if err := c.doJSON(http.MethodGet, apiPath, nil, &response); err != nil {
		return nil, err
	}
	return response.Evidence, nil
}

func (c *Client) GetOperationTimeline(id string, includePayload bool) ([]model.OperationTimelineEntry, error) {
	var response struct {
		Timeline []model.OperationTimelineEntry `json:"timeline"`
	}
	apiPath := path.Join("/v1/operations", id, "timeline")
	if includePayload {
		apiPath += "?include_payload=true"
	}
	if err := c.doJSON(http.MethodGet, apiPath, nil, &response); err != nil {
		return nil, err
	}
	return response.Timeline, nil
}

func (c *Client) GetOperationDebugBundleZip(id string) ([]byte, error) {
	return c.doJSONRaw(http.MethodGet, path.Join("/v1/operations", id, "debug-bundle")+"?format=zip", nil)
}

func (c *Client) GetOperationDebugBundle(id string) (model.OperationDebugBundle, error) {
	var response struct {
		Bundle model.OperationDebugBundle `json:"bundle"`
	}
	if err := c.doJSON(http.MethodGet, path.Join("/v1/operations", id, "debug-bundle"), nil, &response); err != nil {
		return model.OperationDebugBundle{}, err
	}
	return response.Bundle, nil
}

func (c *Client) TryGetOperationDiagnosis(id string) (*model.OperationDiagnosis, error) {
	httpReq, err := http.NewRequest(http.MethodGet, c.resolveURL(path.Join("/v1/operations", id, "diagnosis")), nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.token)

	result, err := c.doPrepared(httpReq)
	if err != nil {
		return nil, err
	}
	if result.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if result.StatusCode < 200 || result.StatusCode >= 300 {
		var apiErr apiError
		if err := json.Unmarshal(result.Payload, &apiErr); err == nil && strings.TrimSpace(apiErr.Error) != "" {
			return nil, &apiServerError{StatusCode: result.StatusCode, Response: apiErr}
		}
		if trimmed := strings.TrimSpace(string(result.Payload)); trimmed != "" {
			return nil, fmt.Errorf("request failed: status=%d body=%s", result.StatusCode, trimmed)
		}
		return nil, fmt.Errorf("request failed: status=%d", result.StatusCode)
	}

	var response struct {
		Diagnosis model.OperationDiagnosis `json:"diagnosis"`
	}
	if err := json.Unmarshal(result.Payload, &response); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &response.Diagnosis, nil
}

func (c *Client) GetApp(id string) (model.App, error) {
	var response struct {
		App model.App `json:"app"`
	}
	if err := c.doJSON(http.MethodGet, path.Join("/v1/apps", id), nil, &response); err != nil {
		return model.App{}, err
	}
	return response.App, nil
}

func (c *Client) TryGetApp(id string) (*model.App, error) {
	httpReq, err := http.NewRequest(http.MethodGet, c.resolveURL(path.Join("/v1/apps", strings.TrimSpace(id))), nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.token)

	result, err := c.doPrepared(httpReq)
	if err != nil {
		return nil, err
	}
	if result.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if result.StatusCode < 200 || result.StatusCode >= 300 {
		var apiErr apiError
		if err := json.Unmarshal(result.Payload, &apiErr); err == nil && strings.TrimSpace(apiErr.Error) != "" {
			return nil, &apiServerError{StatusCode: result.StatusCode, Response: apiErr}
		}
		if trimmed := strings.TrimSpace(string(result.Payload)); trimmed != "" {
			return nil, fmt.Errorf("request failed: status=%d body=%s", result.StatusCode, trimmed)
		}
		return nil, fmt.Errorf("request failed: status=%d", result.StatusCode)
	}

	var response struct {
		App model.App `json:"app"`
	}
	if err := json.Unmarshal(result.Payload, &response); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &response.App, nil
}

func (c *Client) RestartApp(id string) (restartAppResponse, error) {
	var response restartAppResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "restart"), nil, &response); err != nil {
		return restartAppResponse{}, err
	}
	return response, nil
}

func (c *Client) ScaleApp(id string, replicas int) (operationResponse, error) {
	var response operationResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "scale"), map[string]int{"replicas": replicas}, &response); err != nil {
		return operationResponse{}, err
	}
	return response, nil
}

func (c *Client) MigrateApp(id, targetRuntimeID string) (operationResponse, error) {
	var response operationResponse
	req := map[string]string{"target_runtime_id": strings.TrimSpace(targetRuntimeID)}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "migrate"), req, &response); err != nil {
		return operationResponse{}, err
	}
	return response, nil
}

func (c *Client) MigrateAppDryRun(id, targetRuntimeID string) (model.AppMoveDryRunResponse, error) {
	var response model.AppMoveDryRunResponse
	req := map[string]any{
		"target_runtime_id": strings.TrimSpace(targetRuntimeID),
		"dry_run":           true,
	}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "migrate"), req, &response); err != nil {
		return model.AppMoveDryRunResponse{}, err
	}
	return response, nil
}

func (c *Client) GetAppDatabaseStatus(id string) (model.ManagedPostgresStatusResponse, error) {
	var response model.ManagedPostgresStatusResponse
	if err := c.doJSON(http.MethodGet, path.Join("/v1/apps", id, "database", "status"), nil, &response); err != nil {
		return model.ManagedPostgresStatusResponse{}, err
	}
	return response, nil
}

func (c *Client) DeleteApp(id string) (appDeleteResponse, error) {
	return c.deleteApp(id, false)
}

func (c *Client) DeleteAppForce(id string) (appDeleteResponse, error) {
	return c.deleteApp(id, true)
}

func (c *Client) GetAppResourceRecommendation(id string, windowHours, minSamples int) (appResourceRecommendationResponse, error) {
	query := url.Values{}
	if windowHours > 0 {
		query.Set("window_hours", fmt.Sprintf("%d", windowHours))
	}
	if minSamples > 0 {
		query.Set("min_samples", fmt.Sprintf("%d", minSamples))
	}
	relative := path.Join("/v1/apps", id, "resources", "recommendation")
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response appResourceRecommendationResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return appResourceRecommendationResponse{}, err
	}
	return response, nil
}

func (c *Client) ApplyAppResourceRecommendation(id string, windowHours, minSamples int) (appResourceRecommendationApplyResponse, error) {
	req := map[string]int{}
	if windowHours > 0 {
		req["window_hours"] = windowHours
	}
	if minSamples > 0 {
		req["min_samples"] = minSamples
	}
	var response appResourceRecommendationApplyResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "resources", "apply-recommendation"), req, &response); err != nil {
		return appResourceRecommendationApplyResponse{}, err
	}
	return response, nil
}

func (c *Client) deleteApp(id string, force bool) (appDeleteResponse, error) {
	var response appDeleteResponse
	relative := path.Join("/v1/apps", id)
	if force {
		relative += "?force=true"
	}
	if err := c.doJSON(http.MethodDelete, relative, nil, &response); err != nil {
		return appDeleteResponse{}, err
	}
	return response, nil
}

func (c *Client) GetAppEnv(id string) (appEnvResponse, error) {
	var response appEnvResponse
	if err := c.doJSON(http.MethodGet, path.Join("/v1/apps", id, "env"), nil, &response); err != nil {
		return appEnvResponse{}, err
	}
	return response, nil
}

func (c *Client) PatchAppEnv(id string, set map[string]string, deleted []string) (appEnvResponse, error) {
	req := map[string]any{}
	if len(set) > 0 {
		req["set"] = set
	}
	if len(deleted) > 0 {
		req["delete"] = deleted
	}
	var response appEnvResponse
	if err := c.doJSON(http.MethodPatch, path.Join("/v1/apps", id, "env"), req, &response); err != nil {
		return appEnvResponse{}, err
	}
	return response, nil
}

func (c *Client) GetAppFiles(id string) (appFilesResponse, error) {
	var response appFilesResponse
	if err := c.doJSON(http.MethodGet, path.Join("/v1/apps", id, "files"), nil, &response); err != nil {
		return appFilesResponse{}, err
	}
	return response, nil
}

func (c *Client) UpsertAppFiles(id string, files []model.AppFile) (appFilesResponse, error) {
	req := map[string]any{
		"files": files,
	}
	var response appFilesResponse
	if err := c.doJSON(http.MethodPut, path.Join("/v1/apps", id, "files"), req, &response); err != nil {
		return appFilesResponse{}, err
	}
	return response, nil
}

func (c *Client) DeleteAppFiles(id string, paths []string) (appFilesResponse, error) {
	query := url.Values{}
	for _, filePath := range paths {
		filePath = strings.TrimSpace(filePath)
		if filePath == "" {
			continue
		}
		query.Add("path", filePath)
	}
	relative := path.Join("/v1/apps", id, "files")
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response appFilesResponse
	if err := c.doJSON(http.MethodDelete, relative, nil, &response); err != nil {
		return appFilesResponse{}, err
	}
	return response, nil
}

func (c *Client) ListAppDomains(id string) ([]model.AppDomain, error) {
	var response struct {
		Domains []model.AppDomain `json:"domains"`
	}
	if err := c.doJSON(http.MethodGet, path.Join("/v1/apps", id, "domains"), nil, &response); err != nil {
		return nil, err
	}
	return response.Domains, nil
}

func (c *Client) GetAppDomainAvailability(id, hostname string) (appDomainAvailability, error) {
	query := url.Values{}
	query.Set("hostname", strings.TrimSpace(hostname))
	relative := path.Join("/v1/apps", id, "domains", "availability") + "?" + query.Encode()
	var response struct {
		Availability appDomainAvailability `json:"availability"`
	}
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return appDomainAvailability{}, err
	}
	return response.Availability, nil
}

func (c *Client) PutAppDomain(id, hostname string, req appDomainMutationRequest) (appDomainPutResponse, error) {
	var response appDomainPutResponse
	req.Hostname = strings.TrimSpace(hostname)
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "domains"), req, &response); err != nil {
		return appDomainPutResponse{}, err
	}
	return response, nil
}

func (c *Client) VerifyAppDomain(id, hostname string, req appDomainMutationRequest) (appDomainVerifyResponse, error) {
	var response appDomainVerifyResponse
	req.Hostname = strings.TrimSpace(hostname)
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "domains", "verify"), req, &response); err != nil {
		return appDomainVerifyResponse{}, err
	}
	return response, nil
}

func (c *Client) GetAppDomainDiagnosis(id, hostname string) (appDomainDiagnosisResponse, error) {
	query := url.Values{}
	query.Set("hostname", strings.TrimSpace(hostname))
	relative := path.Join("/v1/apps", id, "domains", "diagnosis") + "?" + query.Encode()
	var response appDomainDiagnosisResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return appDomainDiagnosisResponse{}, err
	}
	return response, nil
}

func (c *Client) RepairAppDomain(id, hostname string, req appDomainRepairRequest) (appDomainRepairResponse, error) {
	var response appDomainRepairResponse
	req.Hostname = strings.TrimSpace(hostname)
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "domains", "repair"), req, &response); err != nil {
		return appDomainRepairResponse{}, err
	}
	return response, nil
}

func (c *Client) DeleteAppDomain(id, hostname string) (model.AppDomain, error) {
	query := url.Values{}
	query.Set("hostname", strings.TrimSpace(hostname))
	relative := path.Join("/v1/apps", id, "domains") + "?" + query.Encode()
	var response struct {
		Domain model.AppDomain `json:"domain"`
	}
	if err := c.doJSON(http.MethodDelete, relative, nil, &response); err != nil {
		return model.AppDomain{}, err
	}
	return response.Domain, nil
}

func (c *Client) GetEdgeTLSCertificateBundle(hostname string) (edgeTLSCertificateBundleResponse, error) {
	query := url.Values{}
	query.Set("token", c.token)
	relative := path.Join("/v1/edge/domains", strings.TrimSpace(hostname), "tls-bundle")
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response edgeTLSCertificateBundleResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return edgeTLSCertificateBundleResponse{}, err
	}
	return response, nil
}

func (c *Client) PutEdgeTLSCertificateBundle(hostname string, bundle edgeTLSCertificateBundleInput) (edgeTLSCertificateBundlePutResponse, error) {
	query := url.Values{}
	query.Set("token", c.token)
	relative := path.Join("/v1/edge/domains", strings.TrimSpace(hostname), "tls-bundle")
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response edgeTLSCertificateBundlePutResponse
	if err := c.doJSON(http.MethodPut, relative, bundle, &response); err != nil {
		return edgeTLSCertificateBundlePutResponse{}, err
	}
	return response, nil
}

func (c *Client) GetAppFilesystemTree(id, component, requestPath, pod string) (appFilesystemTreeResponse, error) {
	query := url.Values{}
	if strings.TrimSpace(component) != "" {
		query.Set("component", strings.TrimSpace(component))
	}
	if strings.TrimSpace(requestPath) != "" {
		query.Set("path", strings.TrimSpace(requestPath))
	}
	if strings.TrimSpace(pod) != "" {
		query.Set("pod", strings.TrimSpace(pod))
	}
	relative := path.Join("/v1/apps", id, "filesystem", "tree")
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response appFilesystemTreeResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return appFilesystemTreeResponse{}, err
	}
	return response, nil
}

func (c *Client) GetAppFilesystemFile(id, component, requestPath, pod string, maxBytes int) (appFilesystemFileResponse, error) {
	query := url.Values{}
	if strings.TrimSpace(component) != "" {
		query.Set("component", strings.TrimSpace(component))
	}
	if strings.TrimSpace(requestPath) != "" {
		query.Set("path", strings.TrimSpace(requestPath))
	}
	if strings.TrimSpace(pod) != "" {
		query.Set("pod", strings.TrimSpace(pod))
	}
	if maxBytes > 0 {
		query.Set("max_bytes", fmt.Sprintf("%d", maxBytes))
	}
	relative := path.Join("/v1/apps", id, "filesystem", "file")
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response appFilesystemFileResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return appFilesystemFileResponse{}, err
	}
	return response, nil
}

func (c *Client) PutAppFilesystemFile(id, component, requestPath, content, encoding, pod string, mode int32, mkdirParents bool) (appFilesystemMutationResponse, error) {
	req := map[string]any{
		"path":          strings.TrimSpace(requestPath),
		"content":       content,
		"mkdir_parents": mkdirParents,
	}
	if strings.TrimSpace(encoding) != "" {
		req["encoding"] = strings.TrimSpace(encoding)
	}
	if mode > 0 {
		req["mode"] = mode
	}
	query := url.Values{}
	if strings.TrimSpace(component) != "" {
		query.Set("component", strings.TrimSpace(component))
	}
	if strings.TrimSpace(pod) != "" {
		query.Set("pod", strings.TrimSpace(pod))
	}
	relative := path.Join("/v1/apps", id, "filesystem", "file")
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response appFilesystemMutationResponse
	if err := c.doJSON(http.MethodPut, relative, req, &response); err != nil {
		return appFilesystemMutationResponse{}, err
	}
	return response, nil
}

func (c *Client) CreateAppFilesystemDirectory(id, component, requestPath, pod string, mode int32, parents bool) (appFilesystemMutationResponse, error) {
	req := map[string]any{
		"path":    strings.TrimSpace(requestPath),
		"parents": parents,
	}
	if mode > 0 {
		req["mode"] = mode
	}
	query := url.Values{}
	if strings.TrimSpace(component) != "" {
		query.Set("component", strings.TrimSpace(component))
	}
	if strings.TrimSpace(pod) != "" {
		query.Set("pod", strings.TrimSpace(pod))
	}
	relative := path.Join("/v1/apps", id, "filesystem", "directory")
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response appFilesystemMutationResponse
	if err := c.doJSON(http.MethodPost, relative, req, &response); err != nil {
		return appFilesystemMutationResponse{}, err
	}
	return response, nil
}

func (c *Client) DeleteAppFilesystemPath(id, component, requestPath, pod string, recursive bool) (appFilesystemMutationResponse, error) {
	query := url.Values{}
	if strings.TrimSpace(component) != "" {
		query.Set("component", strings.TrimSpace(component))
	}
	if strings.TrimSpace(requestPath) != "" {
		query.Set("path", strings.TrimSpace(requestPath))
	}
	if strings.TrimSpace(pod) != "" {
		query.Set("pod", strings.TrimSpace(pod))
	}
	if recursive {
		query.Set("recursive", "true")
	}
	relative := path.Join("/v1/apps", id, "filesystem")
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response appFilesystemMutationResponse
	if err := c.doJSON(http.MethodDelete, relative, nil, &response); err != nil {
		return appFilesystemMutationResponse{}, err
	}
	return response, nil
}

func (c *Client) GetBuildLogs(appID, operationID string, tailLines int) (buildLogsResponse, error) {
	query := url.Values{}
	if strings.TrimSpace(operationID) != "" {
		query.Set("operation_id", strings.TrimSpace(operationID))
	}
	if tailLines <= 0 {
		tailLines = 200
	}
	query.Set("tail_lines", fmt.Sprintf("%d", tailLines))
	relative := path.Join("/v1/apps", appID, "build-logs")
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	payload, err := c.doJSONRaw(http.MethodGet, relative, nil)
	if err != nil {
		return buildLogsResponse{}, err
	}
	var response buildLogsResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		return buildLogsResponse{}, fmt.Errorf("decode build logs response: %w", err)
	}
	return response, nil
}

func (c *Client) GetRuntimeLogs(appID string, opts runtimeLogsOptions) (runtimeLogsResponse, error) {
	query := url.Values{}
	if strings.TrimSpace(opts.Component) != "" {
		query.Set("component", strings.TrimSpace(opts.Component))
	}
	if strings.TrimSpace(opts.Pod) != "" {
		query.Set("pod", strings.TrimSpace(opts.Pod))
	}
	if opts.TailLines > 0 {
		query.Set("tail_lines", fmt.Sprintf("%d", opts.TailLines))
	}
	if opts.Previous {
		query.Set("previous", "true")
	}
	relative := path.Join("/v1/apps", appID, "runtime-logs")
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response runtimeLogsResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return runtimeLogsResponse{}, err
	}
	return response, nil
}

func (c *Client) doJSON(method, relativePath string, requestBody any, responseBody any) error {
	payload, err := c.doJSONRaw(method, relativePath, requestBody)
	if err != nil {
		return err
	}
	if responseBody == nil {
		return nil
	}
	if err := json.Unmarshal(payload, responseBody); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (c *Client) doJSONWithTimeout(method, relativePath string, requestBody any, responseBody any, timeout time.Duration) error {
	if timeout <= 0 || c == nil || c.httpClient == nil {
		return c.doJSON(method, relativePath, requestBody, responseBody)
	}
	scoped := *c
	httpClient := *c.httpClient
	httpClient.Timeout = timeout
	scoped.httpClient = &httpClient
	return scoped.doJSON(method, relativePath, requestBody, responseBody)
}

func (c *Client) doJSONRaw(method, relativePath string, requestBody any) ([]byte, error) {
	var body io.Reader
	if requestBody != nil {
		raw, err := json.Marshal(requestBody)
		if err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}
		body = bytes.NewReader(raw)
	}
	httpReq, err := http.NewRequest(method, c.resolveURL(relativePath), body)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	if requestBody != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	return c.do(httpReq)
}

func (c *Client) do(httpReq *http.Request) ([]byte, error) {
	result, err := c.doPrepared(httpReq)
	if err != nil {
		return nil, err
	}
	if result.StatusCode < 200 || result.StatusCode >= 300 {
		var apiErr apiError
		if err := json.Unmarshal(result.Payload, &apiErr); err == nil && strings.TrimSpace(apiErr.Error) != "" {
			return nil, &apiServerError{StatusCode: result.StatusCode, Response: apiErr}
		}
		if trimmed := strings.TrimSpace(string(result.Payload)); trimmed != "" {
			return nil, fmt.Errorf("request failed: status=%d body=%s", result.StatusCode, trimmed)
		}
		return nil, fmt.Errorf("request failed: status=%d", result.StatusCode)
	}
	return result.Payload, nil
}

func (c *Client) resolveURL(relativePath string) string {
	trimmed := strings.TrimSpace(relativePath)
	if trimmed == "" {
		return c.baseURL
	}
	if parsed, err := url.Parse(trimmed); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		return parsed.String()
	}
	if !strings.HasPrefix(trimmed, "/") {
		trimmed = "/" + trimmed
	}
	return c.baseURL + trimmed
}
