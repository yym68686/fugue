package cli

import (
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"fugue/internal/model"
)

type appRouteAvailability struct {
	Input      string `json:"input,omitempty"`
	Label      string `json:"label,omitempty"`
	Hostname   string `json:"hostname,omitempty"`
	BaseDomain string `json:"base_domain,omitempty"`
	PublicURL  string `json:"public_url,omitempty"`
	Valid      bool   `json:"valid"`
	Available  bool   `json:"available"`
	Current    bool   `json:"current"`
	Reason     string `json:"reason,omitempty"`
}

type appRoutePatchResponse struct {
	App            model.App            `json:"app"`
	Availability   appRouteAvailability `json:"availability"`
	AlreadyCurrent bool                 `json:"already_current"`
}

type appBindingsResponse struct {
	Bindings        []model.ServiceBinding `json:"bindings"`
	BackingServices []model.BackingService `json:"backing_services"`
}

type appBindingMutationResponse struct {
	Binding        model.ServiceBinding `json:"binding"`
	BackingService model.BackingService `json:"backing_service"`
	Operation      model.Operation      `json:"operation"`
}

type appImageSummary struct {
	VersionCount         int   `json:"version_count"`
	CurrentVersionCount  int   `json:"current_version_count"`
	StaleVersionCount    int   `json:"stale_version_count"`
	TotalSizeBytes       int64 `json:"total_size_bytes"`
	CurrentSizeBytes     int64 `json:"current_size_bytes"`
	StaleSizeBytes       int64 `json:"stale_size_bytes"`
	ReclaimableSizeBytes int64 `json:"reclaimable_size_bytes"`
}

type appImageVersion struct {
	ImageRef             string           `json:"image_ref"`
	RuntimeImageRef      string           `json:"runtime_image_ref,omitempty"`
	Digest               string           `json:"digest,omitempty"`
	Status               string           `json:"status"`
	Current              bool             `json:"current"`
	SizeBytes            int64            `json:"size_bytes,omitempty"`
	ReclaimableSizeBytes int64            `json:"reclaimable_size_bytes,omitempty"`
	DeleteSupported      bool             `json:"delete_supported"`
	RedeploySupported    bool             `json:"redeploy_supported"`
	LastDeployedAt       *time.Time       `json:"last_deployed_at,omitempty"`
	Source               *model.AppSource `json:"source,omitempty"`
}

type appImageInventoryResponse struct {
	AppID              string            `json:"app_id"`
	RegistryConfigured bool              `json:"registry_configured"`
	ReclaimRequiresGC  bool              `json:"reclaim_requires_gc"`
	ReclaimNote        string            `json:"reclaim_note,omitempty"`
	Summary            appImageSummary   `json:"summary"`
	Versions           []appImageVersion `json:"versions"`
}

type appImageDeleteResponse struct {
	Image              *appImageVersion `json:"image,omitempty"`
	Deleted            bool             `json:"deleted"`
	AlreadyMissing     bool             `json:"already_missing"`
	RegistryConfigured bool             `json:"registry_configured"`
	ReclaimRequiresGC  bool             `json:"reclaim_requires_gc"`
	ReclaimNote        string           `json:"reclaim_note,omitempty"`
	ReclaimedSizeBytes int64            `json:"reclaimed_size_bytes,omitempty"`
}

type appImageRedeployResponse struct {
	Image     *appImageVersion `json:"image,omitempty"`
	Operation model.Operation  `json:"operation"`
}

type rebuildPlan struct {
	SourceType       string `json:"source_type,omitempty"`
	ImageRef         string `json:"image_ref,omitempty"`
	ResolvedImageRef string `json:"resolved_image_ref,omitempty"`
	Branch           string `json:"branch,omitempty"`
	UploadID         string `json:"upload_id,omitempty"`
	SourceDir        string `json:"source_dir,omitempty"`
	DockerfilePath   string `json:"dockerfile_path,omitempty"`
	BuildContextDir  string `json:"build_context_dir,omitempty"`
	BuildStrategy    string `json:"build_strategy,omitempty"`
	ComposeService   string `json:"compose_service,omitempty"`
	ClearFiles       bool   `json:"clear_files,omitempty"`
}

type rebuildPlanRequest struct {
	Branch          string `json:"branch,omitempty"`
	ImageRef        string `json:"image_ref,omitempty"`
	SourceDir       string `json:"source_dir,omitempty"`
	DockerfilePath  string `json:"dockerfile_path,omitempty"`
	BuildContextDir string `json:"build_context_dir,omitempty"`
	RepoAuthToken   string `json:"repo_auth_token,omitempty"`
	ClearFiles      bool   `json:"clear_files,omitempty"`
}

type appRebuildResponse struct {
	Operation model.Operation `json:"operation"`
	Build     rebuildPlan     `json:"build"`
}

type appDisableResponse struct {
	App             *model.App       `json:"app,omitempty"`
	Operation       *model.Operation `json:"operation,omitempty"`
	AlreadyDisabled bool             `json:"already_disabled,omitempty"`
}

type appContinuityAppFailoverRequest struct {
	Enabled         bool   `json:"enabled"`
	TargetRuntimeID string `json:"target_runtime_id,omitempty"`
}

type appContinuityDatabaseFailoverRequest struct {
	Enabled         bool   `json:"enabled"`
	TargetRuntimeID string `json:"target_runtime_id,omitempty"`
	RebalanceNow    bool   `json:"rebalance_now,omitempty"`
}

type patchAppContinuityRequest struct {
	AppFailover      *appContinuityAppFailoverRequest      `json:"app_failover,omitempty"`
	DatabaseFailover *appContinuityDatabaseFailoverRequest `json:"database_failover,omitempty"`
}

type appContinuityResponse struct {
	AppFailover    *model.AppFailoverSpec `json:"app_failover,omitempty"`
	Database       *model.AppPostgresSpec `json:"database,omitempty"`
	AlreadyCurrent bool                   `json:"already_current,omitempty"`
	Operation      *model.Operation       `json:"operation,omitempty"`
}

type projectImageUsageAppSummary struct {
	AppID                string `json:"app_id"`
	AppName              string `json:"app_name"`
	VersionCount         int    `json:"version_count"`
	CurrentVersionCount  int    `json:"current_version_count"`
	StaleVersionCount    int    `json:"stale_version_count"`
	TotalSizeBytes       int64  `json:"total_size_bytes"`
	CurrentSizeBytes     int64  `json:"current_size_bytes"`
	StaleSizeBytes       int64  `json:"stale_size_bytes"`
	ReclaimableSizeBytes int64  `json:"reclaimable_size_bytes"`
}

type projectImageUsageSummary struct {
	ProjectID            string                        `json:"project_id"`
	VersionCount         int                           `json:"version_count"`
	CurrentVersionCount  int                           `json:"current_version_count"`
	StaleVersionCount    int                           `json:"stale_version_count"`
	TotalSizeBytes       int64                         `json:"total_size_bytes"`
	CurrentSizeBytes     int64                         `json:"current_size_bytes"`
	StaleSizeBytes       int64                         `json:"stale_size_bytes"`
	ReclaimableSizeBytes int64                         `json:"reclaimable_size_bytes"`
	Apps                 []projectImageUsageAppSummary `json:"apps"`
}

type projectImageUsageResponse struct {
	RegistryConfigured bool                       `json:"registry_configured"`
	ReclaimRequiresGC  bool                       `json:"reclaim_requires_gc"`
	ReclaimNote        string                     `json:"reclaim_note,omitempty"`
	Projects           []projectImageUsageSummary `json:"projects"`
}

type backingServiceResponse struct {
	BackingService model.BackingService `json:"backing_service"`
}

type backingServiceMigrateResponse struct {
	BackingService model.BackingService `json:"backing_service"`
	Operation      *model.Operation     `json:"operation,omitempty"`
	AlreadyCurrent bool                 `json:"already_current,omitempty"`
}

type createBackingServiceRequest struct {
	TenantID    string                   `json:"tenant_id,omitempty"`
	ProjectID   string                   `json:"project_id,omitempty"`
	Name        string                   `json:"name"`
	Description string                   `json:"description,omitempty"`
	Spec        model.BackingServiceSpec `json:"spec"`
}

type apiKeyListResponse struct {
	APIKeys []model.APIKey `json:"api_keys"`
}

type apiKeySecretResponse struct {
	APIKey model.APIKey `json:"api_key"`
	Secret string       `json:"secret"`
}

type deleteAPIKeyResponse struct {
	Deleted bool         `json:"deleted"`
	APIKey  model.APIKey `json:"api_key"`
}

type patchAPIKeyRequest struct {
	Label  *string   `json:"label,omitempty"`
	Scopes *[]string `json:"scopes,omitempty"`
}

type rotateAPIKeyRequest struct {
	Label  *string   `json:"label,omitempty"`
	Scopes *[]string `json:"scopes,omitempty"`
}

type nodeKeyListResponse struct {
	NodeKeys []model.NodeKey `json:"node_keys"`
}

type nodeKeySecretResponse struct {
	NodeKey model.NodeKey `json:"node_key"`
	Secret  string        `json:"secret"`
}

type nodeKeyUsagesResponse struct {
	NodeKey    model.NodeKey   `json:"node_key"`
	UsageCount int             `json:"usage_count"`
	Runtimes   []model.Runtime `json:"runtimes"`
}

type revokeNodeKeyResponse struct {
	NodeKey model.NodeKey  `json:"node_key"`
	Cleanup nodeKeyCleanup `json:"cleanup"`
}

type nodeKeyCleanup struct {
	DeletedClusterNodes      []string `json:"deleted_cluster_nodes,omitempty"`
	DeletedBootstrapTokenIDs []string `json:"deleted_bootstrap_token_ids,omitempty"`
	DetachedRuntimeIDs       []string `json:"detached_runtime_ids,omitempty"`
	Warnings                 []string `json:"warnings,omitempty"`
}

type runtimeResponse struct {
	Runtime model.Runtime `json:"runtime"`
}

type runtimeCreateResponse struct {
	Runtime    model.Runtime `json:"runtime"`
	RuntimeKey string        `json:"runtime_key"`
}

type createRuntimeRequest struct {
	TenantID string            `json:"tenant_id,omitempty"`
	Name     string            `json:"name"`
	Type     string            `json:"type"`
	Endpoint string            `json:"endpoint,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
}

type runtimeSharingResponse struct {
	Runtime model.Runtime              `json:"runtime"`
	Grants  []model.RuntimeAccessGrant `json:"grants"`
}

type runtimeAccessGrantResponse struct {
	Grant model.RuntimeAccessGrant `json:"grant"`
}

type runtimeAccessRevokeResponse struct {
	Removed bool `json:"removed"`
}

type runtimeAccessModeResponse struct {
	Runtime model.Runtime `json:"runtime"`
}

type runtimePoolModeResponse struct {
	Runtime        model.Runtime `json:"runtime"`
	NodeReconciled bool          `json:"node_reconciled"`
}

type runtimeDeleteResponse struct {
	Deleted bool          `json:"deleted"`
	Runtime model.Runtime `json:"runtime"`
}

type setRuntimePublicOfferRequest struct {
	ReferenceBundle                 model.BillingResourceSpec `json:"reference_bundle"`
	ReferenceMonthlyPriceMicroCents int64                     `json:"reference_monthly_price_microcents,omitempty"`
	Free                            bool                      `json:"free,omitempty"`
	FreeCPU                         bool                      `json:"free_cpu,omitempty"`
	FreeMemory                      bool                      `json:"free_memory,omitempty"`
	FreeStorage                     bool                      `json:"free_storage,omitempty"`
}

type enrollmentTokenListResponse struct {
	EnrollmentTokens []model.EnrollmentToken `json:"enrollment_tokens"`
}

type enrollmentTokenSecretResponse struct {
	EnrollmentToken model.EnrollmentToken `json:"enrollment_token"`
	Secret          string                `json:"secret"`
}

type clusterNodeListResponse struct {
	ClusterNodes []model.ClusterNode `json:"cluster_nodes"`
}

type clusterNodePolicyListResponse struct {
	NodePolicies []model.ClusterNodePolicyStatus `json:"node_policies"`
}

type clusterNodePolicyResponse struct {
	NodePolicy model.ClusterNodePolicyStatus `json:"node_policy"`
}

type clusterNodePolicyStatusResponse struct {
	Summary      model.ClusterNodePolicyStatusSummary `json:"summary"`
	NodePolicies []model.ClusterNodePolicyStatus      `json:"node_policies"`
}

type controlPlaneStatusResponse struct {
	ControlPlane model.ControlPlaneStatus `json:"control_plane"`
}

type sourceUploadInspectionResponse struct {
	SourceUpload model.SourceUploadInspection `json:"source_upload"`
}

type billingResponse struct {
	Billing model.TenantBillingSummary `json:"billing"`
}

type deleteTenantCleanup struct {
	Namespace                string   `json:"namespace"`
	NamespaceDeleteRequested bool     `json:"namespace_delete_requested"`
	OwnedNodes               int      `json:"owned_nodes"`
	ManagedOwnedNodes        int      `json:"managed_owned_nodes"`
	Warnings                 []string `json:"warnings,omitempty"`
}

type deleteTenantResponse struct {
	Tenant  model.Tenant        `json:"tenant"`
	Cleanup deleteTenantCleanup `json:"cleanup"`
}

type inspectGitHubTemplateRequest struct {
	RepoURL        string `json:"repo_url"`
	RepoVisibility string `json:"repo_visibility,omitempty"`
	RepoAuthToken  string `json:"repo_auth_token,omitempty"`
	Branch         string `json:"branch,omitempty"`
}

type inspectGitHubTemplateRepository struct {
	RepoURL           string `json:"repo_url"`
	RepoVisibility    string `json:"repo_visibility"`
	RepoOwner         string `json:"repo_owner"`
	RepoName          string `json:"repo_name"`
	Branch            string `json:"branch"`
	CommitSHA         string `json:"commit_sha"`
	CommitCommittedAt string `json:"commit_committed_at"`
	DefaultAppName    string `json:"default_app_name"`
}

type templateTopologyInference struct {
	Level    string `json:"level"`
	Category string `json:"category"`
	Service  string `json:"service"`
	Message  string `json:"message"`
}

type persistentStorageSeedFile struct {
	Path        string `json:"path"`
	Mode        int32  `json:"mode"`
	SeedContent string `json:"seed_content"`
}

type inspectGitHubTemplateManifestService struct {
	Service                    string                      `json:"service"`
	Kind                       string                      `json:"kind"`
	ServiceType                string                      `json:"service_type,omitempty"`
	BackingService             bool                        `json:"backing_service,omitempty"`
	BuildStrategy              string                      `json:"build_strategy"`
	InternalPort               int                         `json:"internal_port"`
	ComposeService             string                      `json:"compose_service"`
	Published                  bool                        `json:"published"`
	SourceDir                  string                      `json:"source_dir"`
	DockerfilePath             string                      `json:"dockerfile_path"`
	BuildContextDir            string                      `json:"build_context_dir"`
	BindingTargets             []string                    `json:"binding_targets,omitempty"`
	PersistentStorageSeedFiles []persistentStorageSeedFile `json:"persistent_storage_seed_files"`
}

type inspectGitHubTemplateManifest struct {
	ManifestPath    string                                 `json:"manifest_path"`
	PrimaryService  string                                 `json:"primary_service"`
	Services        []inspectGitHubTemplateManifestService `json:"services"`
	Warnings        []string                               `json:"warnings"`
	InferenceReport []templateTopologyInference            `json:"inference_report"`
}

type inspectGitHubTemplateComposeStack struct {
	ComposePath     string                                 `json:"compose_path"`
	PrimaryService  string                                 `json:"primary_service"`
	Services        []inspectGitHubTemplateManifestService `json:"services"`
	Warnings        []string                               `json:"warnings"`
	InferenceReport []templateTopologyInference            `json:"inference_report"`
}

type templateVariable struct {
	Key          string `json:"key"`
	Label        string `json:"label"`
	Description  string `json:"description"`
	DefaultValue string `json:"default_value"`
	Secret       bool   `json:"secret"`
	Required     bool   `json:"required"`
	Generate     string `json:"generate"`
}

type templateMetadata struct {
	Slug           string             `json:"slug"`
	Name           string             `json:"name"`
	Description    string             `json:"description"`
	DemoURL        string             `json:"demo_url"`
	DocsURL        string             `json:"docs_url"`
	SourceMode     string             `json:"source_mode"`
	DefaultRuntime string             `json:"default_runtime"`
	Variables      []templateVariable `json:"variables"`
}

type inspectGitHubTemplateResponse struct {
	Repository    inspectGitHubTemplateRepository    `json:"repository"`
	FugueManifest *inspectGitHubTemplateManifest     `json:"fugue_manifest,omitempty"`
	ComposeStack  *inspectGitHubTemplateComposeStack `json:"compose_stack,omitempty"`
	Template      *templateMetadata                  `json:"template,omitempty"`
}

type inspectUploadTemplateUpload struct {
	ArchiveFilename  string `json:"archive_filename"`
	ArchiveSHA256    string `json:"archive_sha256"`
	ArchiveSizeBytes int64  `json:"archive_size_bytes"`
	DefaultAppName   string `json:"default_app_name"`
	SourceKind       string `json:"source_kind,omitempty"`
	SourcePath       string `json:"source_path,omitempty"`
}

type inspectUploadTemplateResponse struct {
	Upload        inspectUploadTemplateUpload        `json:"upload"`
	FugueManifest *inspectGitHubTemplateManifest     `json:"fugue_manifest,omitempty"`
	ComposeStack  *inspectGitHubTemplateComposeStack `json:"compose_stack,omitempty"`
}

type projectDeleteResponse struct {
	Project                model.Project     `json:"project"`
	Deleted                bool              `json:"deleted"`
	DeleteRequested        bool              `json:"delete_requested"`
	Operations             []model.Operation `json:"operations,omitempty"`
	QueuedOperations       int               `json:"queued_operations,omitempty"`
	AlreadyDeletingApps    int               `json:"already_deleting_apps,omitempty"`
	DeletedBackingServices int               `json:"deleted_backing_services,omitempty"`
}

func (c *Client) CreateTenant(name string) (model.Tenant, error) {
	var response struct {
		Tenant model.Tenant `json:"tenant"`
	}
	request := map[string]string{"name": strings.TrimSpace(name)}
	if err := c.doJSON(http.MethodPost, "/v1/tenants", request, &response); err != nil {
		return model.Tenant{}, err
	}
	return response.Tenant, nil
}

func (c *Client) CreateProject(tenantID, name, description string, defaultRuntimeID ...string) (model.Project, error) {
	var response struct {
		Project model.Project `json:"project"`
	}
	request := map[string]any{
		"name": name,
	}
	if strings.TrimSpace(tenantID) != "" {
		request["tenant_id"] = strings.TrimSpace(tenantID)
	}
	if strings.TrimSpace(description) != "" {
		request["description"] = strings.TrimSpace(description)
	}
	if len(defaultRuntimeID) > 0 && strings.TrimSpace(defaultRuntimeID[0]) != "" {
		request["default_runtime_id"] = strings.TrimSpace(defaultRuntimeID[0])
	}
	if err := c.doJSON(http.MethodPost, "/v1/projects", request, &response); err != nil {
		return model.Project{}, err
	}
	return response.Project, nil
}

func (c *Client) PatchProject(id string, name, description *string) (model.Project, error) {
	return c.PatchProjectFields(id, name, description, nil, false)
}

func (c *Client) PatchProjectFields(id string, name, description, defaultRuntimeID *string, clearDefaultRuntimeID bool) (model.Project, error) {
	request := map[string]any{}
	if name != nil {
		request["name"] = strings.TrimSpace(*name)
	}
	if description != nil {
		request["description"] = *description
	}
	if defaultRuntimeID != nil {
		request["default_runtime_id"] = strings.TrimSpace(*defaultRuntimeID)
	}
	if clearDefaultRuntimeID {
		request["clear_default_runtime_id"] = true
	}
	var response struct {
		Project model.Project `json:"project"`
	}
	if err := c.doJSON(http.MethodPatch, path.Join("/v1/projects", id), request, &response); err != nil {
		return model.Project{}, err
	}
	return response.Project, nil
}

func (c *Client) DeleteProject(id string) (model.Project, error) {
	response, err := c.DeleteProjectDetailed(id, false)
	if err != nil {
		return model.Project{}, err
	}
	return response.Project, nil
}

func (c *Client) DeleteProjectDetailed(id string, cascade bool) (projectDeleteResponse, error) {
	relative := path.Join("/v1/projects", id)
	if cascade {
		relative += "?cascade=true"
	}
	var response projectDeleteResponse
	if err := c.doJSON(http.MethodDelete, relative, nil, &response); err != nil {
		return projectDeleteResponse{}, err
	}
	return response, nil
}

func (c *Client) ListProjectRuntimeReservations(projectID string) ([]model.ProjectRuntimeReservation, error) {
	var response struct {
		RuntimeReservations []model.ProjectRuntimeReservation `json:"runtime_reservations"`
	}
	if err := c.doJSON(http.MethodGet, path.Join("/v1/projects", projectID, "runtime-reservations"), nil, &response); err != nil {
		return nil, err
	}
	return response.RuntimeReservations, nil
}

func (c *Client) ReserveProjectRuntime(projectID, runtimeID string) (model.ProjectRuntimeReservation, error) {
	var response struct {
		RuntimeReservation model.ProjectRuntimeReservation `json:"runtime_reservation"`
	}
	request := map[string]string{"runtime_id": strings.TrimSpace(runtimeID)}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/projects", projectID, "runtime-reservations"), request, &response); err != nil {
		return model.ProjectRuntimeReservation{}, err
	}
	return response.RuntimeReservation, nil
}

func (c *Client) DeleteProjectRuntimeReservation(projectID, runtimeID string) (model.ProjectRuntimeReservation, error) {
	var response struct {
		RuntimeReservation model.ProjectRuntimeReservation `json:"runtime_reservation"`
	}
	if err := c.doJSON(http.MethodDelete, path.Join("/v1/projects", projectID, "runtime-reservations", runtimeID), nil, &response); err != nil {
		return model.ProjectRuntimeReservation{}, err
	}
	return response.RuntimeReservation, nil
}

func (c *Client) ListProjectImageUsage() (projectImageUsageResponse, error) {
	var response projectImageUsageResponse
	if err := c.doJSON(http.MethodGet, "/v1/projects/image-usage", nil, &response); err != nil {
		return projectImageUsageResponse{}, err
	}
	return response, nil
}

func (c *Client) GetAppRouteAvailability(id, hostname string) (appRouteAvailability, error) {
	query := url.Values{}
	query.Set("hostname", strings.TrimSpace(hostname))
	relative := path.Join("/v1/apps", id, "route", "availability") + "?" + query.Encode()
	var response struct {
		Availability appRouteAvailability `json:"availability"`
	}
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return appRouteAvailability{}, err
	}
	return response.Availability, nil
}

func (c *Client) PatchAppRoute(id, hostname string) (appRoutePatchResponse, error) {
	var response appRoutePatchResponse
	request := map[string]string{"hostname": strings.TrimSpace(hostname)}
	if err := c.doJSON(http.MethodPatch, path.Join("/v1/apps", id, "route"), request, &response); err != nil {
		return appRoutePatchResponse{}, err
	}
	return response, nil
}

func (c *Client) ListAppBindings(id string) (appBindingsResponse, error) {
	var response appBindingsResponse
	if err := c.doJSON(http.MethodGet, path.Join("/v1/apps", id, "bindings"), nil, &response); err != nil {
		return appBindingsResponse{}, err
	}
	return response, nil
}

func (c *Client) CreateAppBinding(id, serviceID, alias string, env map[string]string) (appBindingMutationResponse, error) {
	request := map[string]any{
		"service_id": strings.TrimSpace(serviceID),
	}
	if strings.TrimSpace(alias) != "" {
		request["alias"] = strings.TrimSpace(alias)
	}
	if len(env) > 0 {
		request["env"] = env
	}
	var response appBindingMutationResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "bindings"), request, &response); err != nil {
		return appBindingMutationResponse{}, err
	}
	return response, nil
}

func (c *Client) DeleteAppBinding(id, bindingID string) (appBindingMutationResponse, error) {
	var response appBindingMutationResponse
	if err := c.doJSON(http.MethodDelete, path.Join("/v1/apps", id, "bindings", bindingID), nil, &response); err != nil {
		return appBindingMutationResponse{}, err
	}
	return response, nil
}

func (c *Client) GetAppImages(id string) (appImageInventoryResponse, error) {
	var response appImageInventoryResponse
	if err := c.doJSON(http.MethodGet, path.Join("/v1/apps", id, "images"), nil, &response); err != nil {
		return appImageInventoryResponse{}, err
	}
	return response, nil
}

func (c *Client) RedeployAppImage(id, imageRef string) (appImageRedeployResponse, error) {
	var response appImageRedeployResponse
	request := map[string]string{"image_ref": strings.TrimSpace(imageRef)}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "images", "redeploy"), request, &response); err != nil {
		return appImageRedeployResponse{}, err
	}
	return response, nil
}

func (c *Client) DeleteAppImage(id, imageRef string) (appImageDeleteResponse, error) {
	var response appImageDeleteResponse
	request := map[string]string{"image_ref": strings.TrimSpace(imageRef)}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "images", "delete"), request, &response); err != nil {
		return appImageDeleteResponse{}, err
	}
	return response, nil
}

func (c *Client) RebuildApp(id string, request rebuildPlanRequest) (appRebuildResponse, error) {
	var response appRebuildResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "rebuild"), request, &response); err != nil {
		return appRebuildResponse{}, err
	}
	return response, nil
}

func (c *Client) DeployApp(id string, spec *model.AppSpec) (operationResponse, error) {
	return c.DeployAppWithWorkspace(id, spec, nil)
}

func (c *Client) DeployAppWithWorkspace(id string, spec *model.AppSpec, workspace *model.AppWorkspaceSpec) (operationResponse, error) {
	request := map[string]any{}
	if spec != nil {
		request["spec"] = spec
	}
	if workspace != nil {
		request["workspace"] = workspace
	}
	var body any
	if len(request) > 0 {
		body = request
	}
	var response operationResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "deploy"), body, &response); err != nil {
		return operationResponse{}, err
	}
	return response, nil
}

func (c *Client) PatchAppStartupCommand(id string, startupCommand *string) (appPatchResponse, error) {
	request := map[string]any{}
	if startupCommand != nil {
		request["startup_command"] = strings.TrimSpace(*startupCommand)
	}
	var response appPatchResponse
	if err := c.doJSON(http.MethodPatch, path.Join("/v1/apps", id), request, &response); err != nil {
		return appPatchResponse{}, err
	}
	return response, nil
}

func (c *Client) PatchAppPersistentStorage(id string, storage *model.AppPersistentStorageSpec) (appPatchResponse, error) {
	request := map[string]any{}
	if storage != nil {
		request["persistent_storage"] = storage
	}
	var response appPatchResponse
	if err := c.doJSON(http.MethodPatch, path.Join("/v1/apps", id), request, &response); err != nil {
		return appPatchResponse{}, err
	}
	return response, nil
}

func (c *Client) PatchAppRightSizing(id string, rightSizing *model.AppRightSizingSpec) (appPatchResponse, error) {
	request := map[string]any{}
	if rightSizing != nil {
		request["right_sizing"] = rightSizing
	}
	var response appPatchResponse
	if err := c.doJSON(http.MethodPatch, path.Join("/v1/apps", id), request, &response); err != nil {
		return appPatchResponse{}, err
	}
	return response, nil
}

func (c *Client) PatchAppOriginSource(id string, originSource *model.AppSource) (appPatchResponse, error) {
	request := map[string]any{}
	if originSource != nil {
		request["origin_source"] = originSource
	}
	var response appPatchResponse
	if err := c.doJSON(http.MethodPatch, path.Join("/v1/apps", id, "source"), request, &response); err != nil {
		return appPatchResponse{}, err
	}
	return response, nil
}

func (c *Client) DisableApp(id string) (appDisableResponse, error) {
	var response appDisableResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "disable"), nil, &response); err != nil {
		return appDisableResponse{}, err
	}
	return response, nil
}

func (c *Client) FailoverApp(id, targetRuntimeID string) (operationResponse, error) {
	request := map[string]any{}
	if strings.TrimSpace(targetRuntimeID) != "" {
		request["target_runtime_id"] = strings.TrimSpace(targetRuntimeID)
	}
	var body any
	if len(request) > 0 {
		body = request
	}
	var response operationResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "failover"), body, &response); err != nil {
		return operationResponse{}, err
	}
	return response, nil
}

func (c *Client) PatchAppContinuity(id string, request patchAppContinuityRequest) (appContinuityResponse, error) {
	var response appContinuityResponse
	if err := c.doJSON(http.MethodPatch, path.Join("/v1/apps", id, "continuity"), request, &response); err != nil {
		return appContinuityResponse{}, err
	}
	return response, nil
}

func (c *Client) SwitchoverAppDatabase(id, targetRuntimeID string) (operationResponse, error) {
	request := map[string]string{
		"target_runtime_id": strings.TrimSpace(targetRuntimeID),
	}
	var response operationResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "database", "switchover"), request, &response); err != nil {
		return operationResponse{}, err
	}
	return response, nil
}

func (c *Client) LocalizeAppDatabase(id, targetNodeName, targetRuntimeID string) (operationResponse, error) {
	request := map[string]string{}
	if strings.TrimSpace(targetNodeName) != "" {
		request["target_node_name"] = strings.TrimSpace(targetNodeName)
	}
	if strings.TrimSpace(targetRuntimeID) != "" {
		request["target_runtime_id"] = strings.TrimSpace(targetRuntimeID)
	}
	var response operationResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "database", "localize"), request, &response); err != nil {
		return operationResponse{}, err
	}
	return response, nil
}

func (c *Client) ListBackingServices() ([]model.BackingService, error) {
	var response struct {
		BackingServices []model.BackingService `json:"backing_services"`
	}
	if err := c.doJSON(http.MethodGet, "/v1/backing-services", nil, &response); err != nil {
		return nil, err
	}
	return response.BackingServices, nil
}

func (c *Client) GetBackingService(id string) (model.BackingService, error) {
	var response backingServiceResponse
	if err := c.doJSON(http.MethodGet, path.Join("/v1/backing-services", id), nil, &response); err != nil {
		return model.BackingService{}, err
	}
	return response.BackingService, nil
}

func (c *Client) CreateBackingService(request createBackingServiceRequest) (model.BackingService, error) {
	var response backingServiceResponse
	if err := c.doJSON(http.MethodPost, "/v1/backing-services", request, &response); err != nil {
		return model.BackingService{}, err
	}
	return response.BackingService, nil
}

func (c *Client) DeleteBackingService(id string) (model.BackingService, error) {
	var response backingServiceResponse
	if err := c.doJSON(http.MethodDelete, path.Join("/v1/backing-services", id), nil, &response); err != nil {
		return model.BackingService{}, err
	}
	return response.BackingService, nil
}

func (c *Client) MigrateBackingService(id, targetRuntimeID string) (backingServiceMigrateResponse, error) {
	var response backingServiceMigrateResponse
	request := map[string]string{"target_runtime_id": strings.TrimSpace(targetRuntimeID)}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/backing-services", id, "migrate"), request, &response); err != nil {
		return backingServiceMigrateResponse{}, err
	}
	return response, nil
}

func (c *Client) LocalizeBackingService(id, targetRuntimeID, targetNodeName string) (backingServiceMigrateResponse, error) {
	var response backingServiceMigrateResponse
	request := map[string]string{"target_runtime_id": strings.TrimSpace(targetRuntimeID)}
	if strings.TrimSpace(targetNodeName) != "" {
		request["target_node_name"] = strings.TrimSpace(targetNodeName)
	}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/backing-services", id, "localize"), request, &response); err != nil {
		return backingServiceMigrateResponse{}, err
	}
	return response, nil
}

func (c *Client) ListOperations(appID string) ([]model.Operation, error) {
	return c.listOperations(appID, false)
}

func (c *Client) ListOperationsWithDesiredState(appID string) ([]model.Operation, error) {
	return c.listOperations(appID, true)
}

func (c *Client) listOperations(appID string, includeDesired bool) ([]model.Operation, error) {
	relative := "/v1/operations"
	query := url.Values{}
	if strings.TrimSpace(appID) != "" {
		query.Set("app_id", strings.TrimSpace(appID))
	}
	if includeDesired {
		query.Set("include_desired", "true")
	}
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response struct {
		Operations []model.Operation `json:"operations"`
	}
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return nil, err
	}
	return response.Operations, nil
}

func (c *Client) ListAuditEvents() ([]model.AuditEvent, error) {
	var response struct {
		AuditEvents []model.AuditEvent `json:"audit_events"`
	}
	if err := c.doJSON(http.MethodGet, "/v1/audit-events", nil, &response); err != nil {
		return nil, err
	}
	return response.AuditEvents, nil
}

func (c *Client) ListAPIKeys() ([]model.APIKey, error) {
	var response apiKeyListResponse
	if err := c.doJSON(http.MethodGet, "/v1/api-keys", nil, &response); err != nil {
		return nil, err
	}
	return response.APIKeys, nil
}

func (c *Client) CreateAPIKey(tenantID, label string, scopes []string) (apiKeySecretResponse, error) {
	var response apiKeySecretResponse
	request := map[string]any{
		"label":  strings.TrimSpace(label),
		"scopes": scopes,
	}
	if strings.TrimSpace(tenantID) != "" {
		request["tenant_id"] = strings.TrimSpace(tenantID)
	}
	if err := c.doJSON(http.MethodPost, "/v1/api-keys", request, &response); err != nil {
		return apiKeySecretResponse{}, err
	}
	return response, nil
}

func (c *Client) PatchAPIKey(id string, request patchAPIKeyRequest) (model.APIKey, error) {
	var response struct {
		APIKey model.APIKey `json:"api_key"`
	}
	if err := c.doJSON(http.MethodPatch, path.Join("/v1/api-keys", id), request, &response); err != nil {
		return model.APIKey{}, err
	}
	return response.APIKey, nil
}

func (c *Client) RotateAPIKey(id string, request *rotateAPIKeyRequest) (apiKeySecretResponse, error) {
	var body any
	if request != nil {
		body = request
	}
	var response apiKeySecretResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/api-keys", id, "rotate"), body, &response); err != nil {
		return apiKeySecretResponse{}, err
	}
	return response, nil
}

func (c *Client) DisableAPIKey(id string) (model.APIKey, error) {
	var response struct {
		APIKey model.APIKey `json:"api_key"`
	}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/api-keys", id, "disable"), nil, &response); err != nil {
		return model.APIKey{}, err
	}
	return response.APIKey, nil
}

func (c *Client) EnableAPIKey(id string) (model.APIKey, error) {
	var response struct {
		APIKey model.APIKey `json:"api_key"`
	}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/api-keys", id, "enable"), nil, &response); err != nil {
		return model.APIKey{}, err
	}
	return response.APIKey, nil
}

func (c *Client) DeleteAPIKey(id string) (deleteAPIKeyResponse, error) {
	var response deleteAPIKeyResponse
	if err := c.doJSON(http.MethodDelete, path.Join("/v1/api-keys", id), nil, &response); err != nil {
		return deleteAPIKeyResponse{}, err
	}
	return response, nil
}

func (c *Client) ListNodeKeys() ([]model.NodeKey, error) {
	var response nodeKeyListResponse
	if err := c.doJSON(http.MethodGet, "/v1/node-keys", nil, &response); err != nil {
		return nil, err
	}
	return response.NodeKeys, nil
}

func (c *Client) CreateNodeKey(tenantID, label string) (nodeKeySecretResponse, error) {
	request := map[string]any{}
	if strings.TrimSpace(tenantID) != "" {
		request["tenant_id"] = strings.TrimSpace(tenantID)
	}
	if strings.TrimSpace(label) != "" {
		request["label"] = strings.TrimSpace(label)
	}
	var body any
	if len(request) > 0 {
		body = request
	}
	var response nodeKeySecretResponse
	if err := c.doJSON(http.MethodPost, "/v1/node-keys", body, &response); err != nil {
		return nodeKeySecretResponse{}, err
	}
	return response, nil
}

func (c *Client) GetNodeKeyUsages(id string) (nodeKeyUsagesResponse, error) {
	var response nodeKeyUsagesResponse
	if err := c.doJSON(http.MethodGet, path.Join("/v1/node-keys", id, "usages"), nil, &response); err != nil {
		return nodeKeyUsagesResponse{}, err
	}
	return response, nil
}

func (c *Client) RevokeNodeKey(id string) (revokeNodeKeyResponse, error) {
	var response revokeNodeKeyResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/node-keys", id, "revoke"), nil, &response); err != nil {
		return revokeNodeKeyResponse{}, err
	}
	return response, nil
}

func (c *Client) GetRuntime(id string) (model.Runtime, error) {
	var response runtimeResponse
	if err := c.doJSON(http.MethodGet, path.Join("/v1/runtimes", id), nil, &response); err != nil {
		return model.Runtime{}, err
	}
	return response.Runtime, nil
}

func (c *Client) CreateRuntime(request createRuntimeRequest) (runtimeCreateResponse, error) {
	var response runtimeCreateResponse
	if err := c.doJSON(http.MethodPost, "/v1/runtimes", request, &response); err != nil {
		return runtimeCreateResponse{}, err
	}
	return response, nil
}

func (c *Client) DeleteRuntime(id string) (runtimeDeleteResponse, error) {
	var response runtimeDeleteResponse
	if err := c.doJSON(http.MethodDelete, path.Join("/v1/runtimes", id), nil, &response); err != nil {
		return runtimeDeleteResponse{}, err
	}
	return response, nil
}

func (c *Client) GetRuntimeSharing(id string) (runtimeSharingResponse, error) {
	var response runtimeSharingResponse
	if err := c.doJSON(http.MethodGet, path.Join("/v1/runtimes", id, "sharing"), nil, &response); err != nil {
		return runtimeSharingResponse{}, err
	}
	return response, nil
}

func (c *Client) GrantRuntimeAccess(id, tenantID string) (model.RuntimeAccessGrant, error) {
	var response runtimeAccessGrantResponse
	request := map[string]string{"tenant_id": strings.TrimSpace(tenantID)}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/runtimes", id, "sharing", "grants"), request, &response); err != nil {
		return model.RuntimeAccessGrant{}, err
	}
	return response.Grant, nil
}

func (c *Client) RevokeRuntimeAccess(id, tenantID string) (bool, error) {
	var response runtimeAccessRevokeResponse
	if err := c.doJSON(http.MethodDelete, path.Join("/v1/runtimes", id, "sharing", "grants", strings.TrimSpace(tenantID)), nil, &response); err != nil {
		return false, err
	}
	return response.Removed, nil
}

func (c *Client) SetRuntimeAccessMode(id, accessMode string) (model.Runtime, error) {
	var response runtimeAccessModeResponse
	request := map[string]string{"access_mode": strings.TrimSpace(accessMode)}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/runtimes", id, "sharing", "mode"), request, &response); err != nil {
		return model.Runtime{}, err
	}
	return response.Runtime, nil
}

func (c *Client) SetRuntimePublicOffer(id string, request setRuntimePublicOfferRequest) (model.Runtime, error) {
	var response runtimeResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/runtimes", id, "public-offer"), request, &response); err != nil {
		return model.Runtime{}, err
	}
	return response.Runtime, nil
}

func (c *Client) SetRuntimePoolMode(id, poolMode string) (runtimePoolModeResponse, error) {
	var response runtimePoolModeResponse
	request := map[string]string{"pool_mode": strings.TrimSpace(poolMode)}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/runtimes", id, "pool-mode"), request, &response); err != nil {
		return runtimePoolModeResponse{}, err
	}
	return response, nil
}

func (c *Client) ListEnrollmentTokens(tenantID string) ([]model.EnrollmentToken, error) {
	relative := "/v1/runtimes/enroll-tokens"
	if strings.TrimSpace(tenantID) != "" {
		relative += "?tenant_id=" + url.QueryEscape(strings.TrimSpace(tenantID))
	}
	var response enrollmentTokenListResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return nil, err
	}
	return response.EnrollmentTokens, nil
}

func (c *Client) CreateEnrollmentToken(tenantID, label string, ttlSeconds int) (enrollmentTokenSecretResponse, error) {
	request := map[string]any{
		"ttl_seconds": ttlSeconds,
	}
	if strings.TrimSpace(tenantID) != "" {
		request["tenant_id"] = strings.TrimSpace(tenantID)
	}
	if strings.TrimSpace(label) != "" {
		request["label"] = strings.TrimSpace(label)
	}
	var response enrollmentTokenSecretResponse
	if err := c.doJSON(http.MethodPost, "/v1/runtimes/enroll-tokens", request, &response); err != nil {
		return enrollmentTokenSecretResponse{}, err
	}
	return response, nil
}

func (c *Client) ListClusterNodes() ([]model.ClusterNode, error) {
	var response clusterNodeListResponse
	if err := c.doJSON(http.MethodGet, "/v1/cluster/nodes", nil, &response); err != nil {
		return nil, err
	}
	return response.ClusterNodes, nil
}

func (c *Client) ListClusterNodePolicies() ([]model.ClusterNodePolicyStatus, error) {
	var response clusterNodePolicyListResponse
	if err := c.doJSON(http.MethodGet, "/v1/cluster/node-policies", nil, &response); err != nil {
		return nil, err
	}
	return response.NodePolicies, nil
}

func (c *Client) GetClusterNodePolicy(nodeName string) (model.ClusterNodePolicyStatus, error) {
	var response clusterNodePolicyResponse
	if err := c.doJSON(http.MethodGet, path.Join("/v1/cluster/node-policies", strings.TrimSpace(nodeName)), nil, &response); err != nil {
		return model.ClusterNodePolicyStatus{}, err
	}
	return response.NodePolicy, nil
}

func (c *Client) GetClusterNodePolicyStatus() (model.ClusterNodePolicyStatusSummary, []model.ClusterNodePolicyStatus, error) {
	var response clusterNodePolicyStatusResponse
	if err := c.doJSON(http.MethodGet, "/v1/cluster/node-policies/status", nil, &response); err != nil {
		return model.ClusterNodePolicyStatusSummary{}, nil, err
	}
	return response.Summary, response.NodePolicies, nil
}

func (c *Client) GetControlPlaneStatus() (model.ControlPlaneStatus, error) {
	var response controlPlaneStatusResponse
	if err := c.doJSON(http.MethodGet, "/v1/cluster/control-plane", nil, &response); err != nil {
		return model.ControlPlaneStatus{}, err
	}
	return response.ControlPlane, nil
}

func (c *Client) GetSourceUpload(id string) (model.SourceUploadInspection, error) {
	var response sourceUploadInspectionResponse
	if err := c.doJSON(http.MethodGet, path.Join("/v1/source-uploads", strings.TrimSpace(id)), nil, &response); err != nil {
		return model.SourceUploadInspection{}, err
	}
	return response.SourceUpload, nil
}

func (c *Client) GetBilling(tenantID string) (model.TenantBillingSummary, error) {
	relative := "/v1/billing"
	if strings.TrimSpace(tenantID) != "" {
		relative += "?tenant_id=" + url.QueryEscape(strings.TrimSpace(tenantID))
	}
	var response billingResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return model.TenantBillingSummary{}, err
	}
	return response.Billing, nil
}

func (c *Client) UpdateBilling(tenantID string, managedCap model.BillingResourceSpec) (model.TenantBillingSummary, error) {
	request := map[string]any{
		"managed_cap": managedCap,
	}
	if strings.TrimSpace(tenantID) != "" {
		request["tenant_id"] = strings.TrimSpace(tenantID)
	}
	var response billingResponse
	if err := c.doJSON(http.MethodPatch, "/v1/billing", request, &response); err != nil {
		return model.TenantBillingSummary{}, err
	}
	return response.Billing, nil
}

func (c *Client) SetBillingBalance(tenantID string, balanceCents int64, note string) (model.TenantBillingSummary, error) {
	request := map[string]any{
		"balance_cents": balanceCents,
	}
	if strings.TrimSpace(tenantID) != "" {
		request["tenant_id"] = strings.TrimSpace(tenantID)
	}
	if strings.TrimSpace(note) != "" {
		request["note"] = strings.TrimSpace(note)
	}
	var response billingResponse
	if err := c.doJSON(http.MethodPatch, "/v1/billing/balance", request, &response); err != nil {
		return model.TenantBillingSummary{}, err
	}
	return response.Billing, nil
}

func (c *Client) TopUpBilling(tenantID string, amountCents int64, note string) (model.TenantBillingSummary, error) {
	request := map[string]any{
		"amount_cents": amountCents,
	}
	if strings.TrimSpace(tenantID) != "" {
		request["tenant_id"] = strings.TrimSpace(tenantID)
	}
	if strings.TrimSpace(note) != "" {
		request["note"] = strings.TrimSpace(note)
	}
	var response billingResponse
	if err := c.doJSON(http.MethodPost, "/v1/billing/top-ups", request, &response); err != nil {
		return model.TenantBillingSummary{}, err
	}
	return response.Billing, nil
}

func (c *Client) DeleteTenant(id string) (deleteTenantResponse, error) {
	var response deleteTenantResponse
	if err := c.doJSON(http.MethodDelete, path.Join("/v1/tenants", id), nil, &response); err != nil {
		return deleteTenantResponse{}, err
	}
	return response, nil
}

func (c *Client) InspectGitHubTemplate(request inspectGitHubTemplateRequest) (inspectGitHubTemplateResponse, error) {
	var response inspectGitHubTemplateResponse
	if err := c.doJSON(http.MethodPost, "/v1/templates/inspect-github", request, &response); err != nil {
		return inspectGitHubTemplateResponse{}, err
	}
	return response, nil
}

func (c *Client) GetJoinClusterScript() (string, error) {
	httpReq, err := http.NewRequest(http.MethodGet, c.resolveURL("/install/join-cluster.sh"), nil)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	if strings.TrimSpace(c.token) != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.token)
	}
	payload, err := c.do(httpReq)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}
