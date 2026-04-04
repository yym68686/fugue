package api

import (
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"fugue/internal/auth"
	"fugue/internal/failover"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

type Server struct {
	store                        *store.Store
	auth                         *auth.Authenticator
	log                          *log.Logger
	controlPlaneNamespace        string
	controlPlaneReleaseInstance  string
	appBaseDomain                string
	customDomainBaseDomain       string
	apiPublicDomain              string
	edgeTLSAskToken              string
	registryPushBase             string
	registryPullBase             string
	clusterJoinRegistryEndpoint  string
	reservedAppHosts             map[string]struct{}
	clusterJoinServer            string
	clusterJoinCAHash            string
	clusterJoinBootstrapTokenTTL time.Duration
	clusterJoinMeshProvider      string
	clusterJoinMeshLoginServer   string
	clusterJoinMeshAuthKey       string
	importer                     *sourceimport.Importer
	appImageRegistry             appImageRegistry
	newClusterNodeClient         func() (*clusterNodeClient, error)
	newManagedAppStatusClient    func() (*managedAppStatusClient, error)
	newLogsClient                func(namespace string) (appLogsClient, error)
	newFilesystemPodLister       func(namespace string) (filesystemPodLister, error)
	filesystemExecRunner         filesystemPodExecRunner
	dnsResolver                  appDomainDNSResolver
	logStreamTuning              logStreamTuning
	ready                        atomic.Bool
}

func NewServer(store *store.Store, authn *auth.Authenticator, logger *log.Logger, cfg ServerConfig) *Server {
	if logger == nil {
		logger = log.Default()
	}
	server := &Server{
		store:                        store,
		auth:                         authn,
		log:                          logger,
		controlPlaneNamespace:        strings.TrimSpace(cfg.ControlPlaneNamespace),
		controlPlaneReleaseInstance:  strings.TrimSpace(cfg.ControlPlaneReleaseInstance),
		appBaseDomain:                strings.TrimSpace(strings.ToLower(cfg.AppBaseDomain)),
		customDomainBaseDomain:       defaultCustomDomainBaseDomain(cfg.AppBaseDomain),
		apiPublicDomain:              strings.TrimSpace(strings.ToLower(cfg.APIPublicDomain)),
		edgeTLSAskToken:              strings.TrimSpace(cfg.EdgeTLSAskToken),
		registryPushBase:             strings.TrimSpace(cfg.RegistryPushBase),
		registryPullBase:             strings.TrimSpace(cfg.RegistryPullBase),
		clusterJoinRegistryEndpoint:  strings.TrimSpace(cfg.ClusterJoinRegistryEndpoint),
		clusterJoinServer:            strings.TrimSpace(cfg.ClusterJoinServer),
		clusterJoinCAHash:            normalizeClusterJoinCAHash(cfg.ClusterJoinCAHash),
		clusterJoinBootstrapTokenTTL: cfg.ClusterJoinBootstrapTokenTTL,
		clusterJoinMeshProvider:      strings.TrimSpace(strings.ToLower(cfg.ClusterJoinMeshProvider)),
		clusterJoinMeshLoginServer:   strings.TrimSpace(cfg.ClusterJoinMeshLoginServer),
		clusterJoinMeshAuthKey:       strings.TrimSpace(cfg.ClusterJoinMeshAuthKey),
		importer:                     sourceimport.NewImporter(cfg.ImportWorkDir, logger, sourceimport.BuilderPodPolicy{}),
		appImageRegistry:             newRemoteAppImageRegistry(),
		newClusterNodeClient:         newClusterNodeClient,
		newManagedAppStatusClient:    newManagedAppStatusClient,
		newLogsClient: func(namespace string) (appLogsClient, error) {
			return newKubeLogsClient(namespace)
		},
		newFilesystemPodLister: func(namespace string) (filesystemPodLister, error) {
			return newKubeLogsClient(namespace)
		},
		filesystemExecRunner: kubeFilesystemExecRunner{},
		dnsResolver:          netAppDomainResolver{},
		logStreamTuning:      defaultLogStreamTuning(),
	}
	if server.registryPullBase == "" {
		server.registryPullBase = server.registryPushBase
	}
	if server.clusterJoinRegistryEndpoint == "" {
		server.clusterJoinRegistryEndpoint = server.registryPullBase
	}
	server.reservedAppHosts = reservedAppHosts(server.apiPublicDomain, server.registryPushBase, server.registryPullBase)
	server.ready.Store(true)
	return server
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if !s.ready.Load() {
		httpx.WriteError(w, http.StatusServiceUnavailable, "shutting down")
		return
	}
	results := s.readinessCheckResults(r.Context())
	statusCode := http.StatusOK
	status := "ok"
	if readinessHasFailure(results) {
		statusCode = http.StatusServiceUnavailable
		status = "degraded"
	}
	httpx.WriteJSON(w, statusCode, map[string]any{
		"status": status,
		"checks": results,
	})
}

func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

func (s *Server) handleListTenants(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if principal.IsPlatformAdmin() {
		tenants, err := s.store.ListTenants()
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"tenants": tenants})
		return
	}
	if principal.TenantID == "" {
		httpx.WriteError(w, http.StatusForbidden, "tenant context required")
		return
	}
	tenant, err := s.store.GetTenant(principal.TenantID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"tenants": []model.Tenant{tenant}})
}

func (s *Server) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tenant, err := s.store.CreateTenant(req.Name)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "tenant.create", "tenant", tenant.ID, tenant.ID, map[string]string{"name": tenant.Name})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"tenant": tenant})
}

func (s *Server) handleListProjects(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	tenantID := principal.TenantID
	if principal.IsPlatformAdmin() {
		tenantID = strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	}
	if tenantID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "tenant_id is required")
		return
	}
	projects, err := s.store.ListProjects(tenantID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"projects": projects})
}

func (s *Server) handleCreateProject(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("project.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing project.write scope")
		return
	}
	var req struct {
		TenantID    string `json:"tenant_id"`
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tenantID, ok := s.resolveTenantID(principal, req.TenantID)
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot write project for another tenant")
		return
	}
	project, err := s.store.CreateProject(tenantID, req.Name, req.Description)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "project.create", "project", project.ID, tenantID, map[string]string{"name": project.Name})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"project": project})
}

func (s *Server) handlePatchProject(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("project.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing project.write scope")
		return
	}
	project, allowed := s.loadAuthorizedProject(w, r, principal)
	if !allowed {
		return
	}
	var req struct {
		Name        *string `json:"name"`
		Description *string `json:"description"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	project, err := s.store.UpdateProject(project.ID, req.Name, req.Description)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "project.update", "project", project.ID, project.TenantID, map[string]string{"name": project.Name})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"project": project})
}

func (s *Server) handleDeleteProject(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("project.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing project.write scope")
		return
	}
	project, allowed := s.loadAuthorizedProject(w, r, principal)
	if !allowed {
		return
	}
	project, err := s.store.DeleteProject(project.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "project.delete", "project", project.ID, project.TenantID, map[string]string{"name": project.Name})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"project": project})
}

func (s *Server) handleListAPIKeys(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	keys, err := s.store.ListAPIKeys(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"api_keys": keys})
}

func (s *Server) handleCreateAPIKey(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("apikey.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing apikey.write scope")
		return
	}
	var req struct {
		TenantID string   `json:"tenant_id"`
		Label    string   `json:"label"`
		Scopes   []string `json:"scopes"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tenantID, ok := s.resolveTenantID(principal, req.TenantID)
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot create key for another tenant")
		return
	}
	if !principal.IsPlatformAdmin() && !scopesSubset(req.Scopes, principal) {
		httpx.WriteError(w, http.StatusForbidden, "cannot mint scopes you do not hold")
		return
	}
	key, secret, err := s.store.CreateAPIKey(tenantID, req.Label, req.Scopes)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "apikey.create", "api_key", key.ID, tenantID, map[string]string{"label": key.Label})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"api_key": key, "secret": secret})
}

func (s *Server) handlePatchAPIKey(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("apikey.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing apikey.write scope")
		return
	}
	key, allowed := s.loadAuthorizedAPIKey(w, r, principal)
	if !allowed {
		return
	}

	var req struct {
		Label  *string   `json:"label"`
		Scopes *[]string `json:"scopes"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Scopes != nil {
		normalized := model.NormalizeScopes(*req.Scopes)
		req.Scopes = &normalized
	}
	if !principal.IsPlatformAdmin() && req.Scopes != nil && !scopesSubset(*req.Scopes, principal) {
		httpx.WriteError(w, http.StatusForbidden, "cannot mint scopes you do not hold")
		return
	}

	key, err := s.store.UpdateAPIKey(key.ID, req.Label, req.Scopes)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "apikey.update", "api_key", key.ID, key.TenantID, map[string]string{"label": key.Label})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"api_key": key})
}

func (s *Server) handleRotateAPIKey(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("apikey.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing apikey.write scope")
		return
	}
	key, allowed := s.loadAuthorizedAPIKey(w, r, principal)
	if !allowed {
		return
	}

	var req struct {
		Label  *string   `json:"label"`
		Scopes *[]string `json:"scopes"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		if !errors.Is(err, io.EOF) {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		req = struct {
			Label  *string   `json:"label"`
			Scopes *[]string `json:"scopes"`
		}{}
	}
	if req.Scopes != nil {
		normalized := model.NormalizeScopes(*req.Scopes)
		req.Scopes = &normalized
	}

	finalScopes := key.Scopes
	if req.Scopes != nil {
		finalScopes = *req.Scopes
	}
	if !principal.IsPlatformAdmin() && !scopesSubset(finalScopes, principal) {
		httpx.WriteError(w, http.StatusForbidden, "cannot mint scopes you do not hold")
		return
	}

	key, secret, err := s.store.RotateAPIKey(key.ID, req.Label, req.Scopes)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "apikey.rotate", "api_key", key.ID, key.TenantID, map[string]string{"label": key.Label})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"api_key": key, "secret": secret})
}

func (s *Server) handleDisableAPIKey(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("apikey.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing apikey.write scope")
		return
	}
	key, allowed := s.loadAuthorizedAPIKey(w, r, principal)
	if !allowed {
		return
	}

	key, err := s.store.DisableAPIKey(key.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "apikey.disable", "api_key", key.ID, key.TenantID, map[string]string{"label": key.Label})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"api_key": key})
}

func (s *Server) handleEnableAPIKey(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("apikey.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing apikey.write scope")
		return
	}
	key, allowed := s.loadAuthorizedAPIKey(w, r, principal)
	if !allowed {
		return
	}

	key, err := s.store.EnableAPIKey(key.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "apikey.enable", "api_key", key.ID, key.TenantID, map[string]string{"label": key.Label})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"api_key": key})
}

func (s *Server) handleDeleteAPIKey(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("apikey.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing apikey.write scope")
		return
	}
	key, allowed := s.loadAuthorizedAPIKey(w, r, principal)
	if !allowed {
		return
	}

	deleted, err := s.store.DeleteAPIKey(key.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "apikey.delete", "api_key", deleted.ID, deleted.TenantID, map[string]string{"label": deleted.Label})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"deleted": true,
		"api_key": deleted,
	})
}

func (s *Server) handleListNodeKeys(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	keys, err := s.store.ListNodeKeys(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"node_keys": keys})
}

func (s *Server) handleCreateNodeKey(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("runtime.attach") {
		httpx.WriteError(w, http.StatusForbidden, "missing runtime.attach scope")
		return
	}
	var req struct {
		TenantID string `json:"tenant_id"`
		Label    string `json:"label"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		if errors.Is(err, io.EOF) {
			req = struct {
				TenantID string `json:"tenant_id"`
				Label    string `json:"label"`
			}{}
		} else {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	tenantID, ok := s.resolveTenantID(principal, req.TenantID)
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot create node key for another tenant")
		return
	}
	key, secret, err := s.store.CreateNodeKey(tenantID, req.Label)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "node_key.create", "node_key", key.ID, tenantID, map[string]string{"label": key.Label})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"node_key": key, "secret": secret})
}

func (s *Server) handleRevokeNodeKey(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("runtime.attach") {
		httpx.WriteError(w, http.StatusForbidden, "missing runtime.attach scope")
		return
	}
	key, err := s.store.GetNodeKey(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() && key.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "node key is not visible to this tenant")
		return
	}
	key, err = s.store.RevokeNodeKey(key.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	cleanup := s.cleanupRevokedNodeKey(r.Context(), key)
	s.appendAudit(principal, "node_key.revoke", "node_key", key.ID, key.TenantID, map[string]string{"label": key.Label})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"node_key": key,
		"cleanup":  cleanup,
	})
}

func (s *Server) handleListEnrollmentTokens(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	tenantID, ok := s.resolveTenantID(principal, strings.TrimSpace(r.URL.Query().Get("tenant_id")))
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot list tokens for another tenant")
		return
	}
	tokens, err := s.store.ListEnrollmentTokens(tenantID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"enrollment_tokens": tokens})
}

func (s *Server) handleCreateEnrollmentToken(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("runtime.attach") {
		httpx.WriteError(w, http.StatusForbidden, "missing runtime.attach scope")
		return
	}
	var req struct {
		TenantID   string `json:"tenant_id"`
		Label      string `json:"label"`
		TTLSeconds int    `json:"ttl_seconds"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tenantID, ok := s.resolveTenantID(principal, req.TenantID)
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot create token for another tenant")
		return
	}
	if req.TTLSeconds <= 0 {
		req.TTLSeconds = 3600
	}
	token, secret, err := s.store.CreateEnrollmentToken(tenantID, req.Label, time.Duration(req.TTLSeconds)*time.Second)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "runtime.enroll_token.create", "enrollment_token", token.ID, tenantID, map[string]string{"label": token.Label})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"enrollment_token": token, "secret": secret})
}

func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	markDeprecatedNodesView(w)
	nodes, err := s.store.ListNodes(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"nodes": nodes})
}

func (s *Server) handleGetNode(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	markDeprecatedNodesView(w)
	runtimeID := r.PathValue("id")
	node, err := s.store.GetRuntime(runtimeID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if node.Type != model.RuntimeTypeExternalOwned && node.Type != model.RuntimeTypeManagedOwned {
		httpx.WriteError(w, http.StatusNotFound, "resource not found")
		return
	}
	visible, err := s.store.RuntimeVisibleToTenant(runtimeID, principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !visible {
		httpx.WriteError(w, http.StatusForbidden, "node is not visible to this tenant")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"node": node})
}

func (s *Server) handleGetNodeKeyUsages(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	nodeKey, err := s.store.GetNodeKey(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() && nodeKey.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "node key is not visible to this tenant")
		return
	}
	nodeKey.Hash = ""

	runtimes, err := s.store.ListRuntimesByNodeKey(nodeKey.ID, principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"node_key":    nodeKey,
		"usage_count": len(runtimes),
		"runtimes":    runtimes,
	})
}

func (s *Server) handleListRuntimes(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	timings := serverTimingFromContext(r.Context())

	syncLocations, err := readBoolQuery(r, "sync_locations", true)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	if syncLocations {
		syncStartedAt := time.Now()
		s.trySyncManagedSharedLocationRuntimes(r.Context())
		timings.Add("runtime_sync", time.Since(syncStartedAt))
	}

	storeStartedAt := time.Now()
	runtimes, err := s.store.ListRuntimes(principal.TenantID, principal.IsPlatformAdmin())
	timings.Add("store_runtimes", time.Since(storeStartedAt))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"runtimes": runtimes})
}

func (s *Server) handleGetRuntime(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	runtimeID := r.PathValue("id")
	runtimeObj, err := s.store.GetRuntime(runtimeID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	visible, err := s.store.RuntimeVisibleToTenant(runtimeID, principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !visible {
		httpx.WriteError(w, http.StatusForbidden, "runtime is not visible to this tenant")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"runtime": runtimeObj})
}

func (s *Server) handleCreateRuntime(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("runtime.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing runtime.write scope")
		return
	}
	var req struct {
		TenantID string            `json:"tenant_id"`
		Name     string            `json:"name"`
		Type     string            `json:"type"`
		Endpoint string            `json:"endpoint"`
		Labels   map[string]string `json:"labels"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Type == model.RuntimeTypeManagedShared {
		if !principal.IsPlatformAdmin() {
			httpx.WriteError(w, http.StatusForbidden, "only platform admin can create managed-shared runtimes")
			return
		}
		if strings.TrimSpace(req.TenantID) != "" {
			httpx.WriteError(w, http.StatusBadRequest, "managed-shared runtime must not set tenant_id")
			return
		}
	}
	if req.Type == model.RuntimeTypeManagedOwned {
		httpx.WriteError(w, http.StatusBadRequest, "managed-owned runtime must be created through the node join flow")
		return
	}
	tenantID, ok := s.resolveTenantID(principal, req.TenantID)
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot create runtime for another tenant")
		return
	}
	runtimeObj, runtimeKey, err := s.store.CreateRuntime(tenantID, req.Name, req.Type, req.Endpoint, req.Labels)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "runtime.create", "runtime", runtimeObj.ID, tenantID, map[string]string{"name": runtimeObj.Name, "type": runtimeObj.Type})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"runtime": runtimeObj, "runtime_key": runtimeKey})
}

func (s *Server) handleListApps(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	timings := serverTimingFromContext(r.Context())

	includeLiveStatus, err := readBoolQuery(r, "include_live_status", true)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	includeResourceUsage, err := readBoolQuery(r, "include_resource_usage", true)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	storeStartedAt := time.Now()
	apps, err := s.store.ListApps(principal.TenantID, principal.IsPlatformAdmin())
	timings.Add("store_apps", time.Since(storeStartedAt))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	visibleApps := make([]model.App, 0, len(apps))
	for _, app := range apps {
		if strings.EqualFold(strings.TrimSpace(app.Status.Phase), "deleting") {
			continue
		}
		visibleApps = append(visibleApps, app)
	}
	if includeLiveStatus {
		liveStatusStartedAt := time.Now()
		visibleApps = s.overlayManagedAppStatuses(r.Context(), visibleApps)
		timings.Add("live_status", time.Since(liveStatusStartedAt))
	}
	if includeResourceUsage {
		resourceUsageStartedAt := time.Now()
		visibleApps = s.overlayCurrentResourceUsageOnApps(r.Context(), visibleApps)
		timings.Add("resource_usage", time.Since(resourceUsageStartedAt))
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"apps": sanitizeAppsForAPI(visibleApps)})
}

func (s *Server) handleGetApp(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, err := s.store.GetApp(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() && app.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "app is not visible to this tenant")
		return
	}
	app = s.overlayManagedAppStatus(r.Context(), app)
	app = s.overlayCurrentResourceUsageOnApp(r.Context(), app)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"app": sanitizeAppForAPI(app)})
}

func (s *Server) handleCreateApp(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write scope")
		return
	}
	var req struct {
		TenantID    string        `json:"tenant_id"`
		ProjectID   string        `json:"project_id"`
		Name        string        `json:"name"`
		Description string        `json:"description"`
		Spec        model.AppSpec `json:"spec"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tenantID, ok := s.resolveTenantID(principal, req.TenantID)
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot create app for another tenant")
		return
	}

	var (
		app model.App
		err error
	)
	if strings.TrimSpace(s.appBaseDomain) != "" {
		app, err = s.createAppWithAutoRoute(tenantID, req.ProjectID, req.Name, req.Description, req.Spec)
	} else {
		app, err = s.store.CreateApp(tenantID, req.ProjectID, req.Name, req.Description, req.Spec)
	}
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.create", "app", app.ID, tenantID, map[string]string{"name": app.Name})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"app": sanitizeAppForAPI(app)})
}

func (s *Server) handleDeployApp(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	var req struct {
		Spec *model.AppSpec `json:"spec"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	spec := app.Spec
	if req.Spec != nil {
		spec = *req.Spec
	}
	op, err := s.store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		AppID:           app.ID,
		DesiredSpec:     &spec,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.deploy", "operation", op.ID, app.TenantID, map[string]string{"app_id": app.ID})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"operation": sanitizeOperationForAPI(op)})
}

func (s *Server) handleScaleApp(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.scale") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.scale scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	var req struct {
		Replicas int `json:"replicas"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	op, err := s.store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeScale,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		AppID:           app.ID,
		DesiredReplicas: &req.Replicas,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.scale", "operation", op.ID, app.TenantID, map[string]string{"app_id": app.ID})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"operation": sanitizeOperationForAPI(op)})
}

func (s *Server) handleDisableApp(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.scale") && !principal.HasScope("app.disable") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.scale or app.disable scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	if app.Spec.Replicas == 0 && app.Status.CurrentReplicas == 0 {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"app":              sanitizeAppForAPI(app),
			"already_disabled": true,
		})
		return
	}
	replicas := 0
	op, err := s.store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeScale,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		AppID:           app.ID,
		DesiredReplicas: &replicas,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.disable", "operation", op.ID, app.TenantID, map[string]string{"app_id": app.ID})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"operation": sanitizeOperationForAPI(op)})
}

func (s *Server) handleMigrateApp(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.migrate") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.migrate scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	if blockerMessage := failover.MigrationBlockerMessage(app); blockerMessage != "" {
		httpx.WriteError(w, http.StatusBadRequest, blockerMessage)
		return
	}
	var req struct {
		TargetRuntimeID string `json:"target_runtime_id"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	op, err := s.store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeMigrate,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		AppID:           app.ID,
		TargetRuntimeID: req.TargetRuntimeID,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.migrate", "operation", op.ID, app.TenantID, map[string]string{"app_id": app.ID, "target_runtime_id": req.TargetRuntimeID})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"operation": sanitizeOperationForAPI(op)})
}

func (s *Server) handleFailoverApp(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.failover") && !principal.HasScope("app.migrate") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.failover or app.migrate scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	var req struct {
		TargetRuntimeID string `json:"target_runtime_id"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	targetRuntimeID := strings.TrimSpace(req.TargetRuntimeID)
	if targetRuntimeID == "" && app.Spec.Failover != nil {
		targetRuntimeID = strings.TrimSpace(app.Spec.Failover.TargetRuntimeID)
	}
	if targetRuntimeID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "target_runtime_id is required when app.spec.failover.target_runtime_id is not configured")
		return
	}
	op, err := s.store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeFailover,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		AppID:           app.ID,
		TargetRuntimeID: targetRuntimeID,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.failover", "operation", op.ID, app.TenantID, map[string]string{"app_id": app.ID, "target_runtime_id": targetRuntimeID})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"operation": sanitizeOperationForAPI(op)})
}

func (s *Server) handleDeleteApp(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.delete") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.delete scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	if strings.EqualFold(strings.TrimSpace(app.Status.Phase), "deleting") {
		response := map[string]any{
			"already_deleting": true,
		}
		if operationID := strings.TrimSpace(app.Status.LastOperationID); operationID != "" {
			op, err := s.store.GetOperation(operationID)
			if err == nil && op.Type == model.OperationTypeDelete {
				response["operation"] = sanitizeOperationForAPI(op)
				httpx.WriteJSON(w, http.StatusAccepted, response)
				return
			}
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				s.writeStoreError(w, err)
				return
			}
		}
		httpx.WriteJSON(w, http.StatusAccepted, response)
		return
	}
	op, err := s.store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDelete,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
		AppID:           app.ID,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.delete", "operation", op.ID, app.TenantID, map[string]string{"app_id": app.ID})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"operation": sanitizeOperationForAPI(op)})
}

func (s *Server) handleListOperations(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	timings := serverTimingFromContext(r.Context())
	appID := readOptionalStringQuery(r, "app_id")

	storeStartedAt := time.Now()
	var ops []model.Operation
	var err error
	if appID != "" {
		ops, err = s.store.ListOperationsByApp(principal.TenantID, principal.IsPlatformAdmin(), appID)
		timings.Add("store_operations_app", time.Since(storeStartedAt))
	} else {
		ops, err = s.store.ListOperations(principal.TenantID, principal.IsPlatformAdmin())
		timings.Add("store_operations", time.Since(storeStartedAt))
	}
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"operations": sanitizeOperationsForAPI(ops)})
}

func (s *Server) handleGetOperation(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	op, err := s.store.GetOperation(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() && op.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "operation is not visible to this tenant")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"operation": sanitizeOperationForAPI(op)})
}

func (s *Server) handleListAuditEvents(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	events, err := s.store.ListAuditEvents(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"audit_events": events})
}

func (s *Server) handleBootstrapNode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeKey            string            `json:"node_key"`
		NodeName           string            `json:"node_name"`
		RuntimeName        string            `json:"runtime_name"`
		MachineName        string            `json:"machine_name"`
		MachineFingerprint string            `json:"machine_fingerprint"`
		Endpoint           string            `json:"endpoint"`
		Labels             map[string]string `json:"labels"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	nodeName := strings.TrimSpace(req.NodeName)
	if nodeName == "" {
		nodeName = strings.TrimSpace(req.RuntimeName)
	}

	key, node, runtimeKey, err := s.store.BootstrapNode(req.NodeKey, nodeName, req.Endpoint, req.Labels, req.MachineName, req.MachineFingerprint)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	actor := model.Principal{
		ActorType: model.ActorTypeNodeKey,
		ActorID:   key.ID,
		TenantID:  key.TenantID,
	}
	s.appendAudit(actor, "node.bootstrap", "node", node.ID, key.TenantID, map[string]string{"name": node.Name, "node_key_id": key.ID})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"node": node, "runtime_key": runtimeKey})
}

func (s *Server) handleAgentEnroll(w http.ResponseWriter, r *http.Request) {
	var req struct {
		EnrollToken        string            `json:"enroll_token"`
		RuntimeName        string            `json:"runtime_name"`
		MachineName        string            `json:"machine_name"`
		MachineFingerprint string            `json:"machine_fingerprint"`
		Endpoint           string            `json:"endpoint"`
		Labels             map[string]string `json:"labels"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	runtimeObj, runtimeKey, err := s.store.ConsumeEnrollmentToken(req.EnrollToken, req.RuntimeName, req.Endpoint, req.Labels, req.MachineName, req.MachineFingerprint)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	actor := model.Principal{
		ActorType: model.ActorTypeRuntime,
		ActorID:   runtimeObj.ID,
		TenantID:  runtimeObj.TenantID,
	}
	s.appendAudit(actor, "runtime.enroll", "runtime", runtimeObj.ID, runtimeObj.TenantID, map[string]string{"name": runtimeObj.Name})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"runtime": runtimeObj, "runtime_key": runtimeKey})
}

func (s *Server) handleAgentHeartbeat(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	var req struct {
		Endpoint string `json:"endpoint"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	runtimeObj, err := s.store.UpdateRuntimeHeartbeat(principal.ActorID, req.Endpoint)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"runtime": runtimeObj})
}

func (s *Server) handleAgentOperations(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	ops, err := s.store.ListAssignedOperations(principal.ActorID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	tasks := make([]runtime.AgentTask, 0, len(ops))
	for _, op := range ops {
		app, err := s.store.GetApp(op.AppID)
		if err != nil {
			continue
		}
		tasks = append(tasks, runtime.AgentTask{
			Operation: op,
			App:       app,
		})
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func (s *Server) handleAgentCompleteOperation(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	var req struct {
		ManifestPath string `json:"manifest_path"`
		Message      string `json:"message"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	var deletingApp model.App
	opBeforeComplete, err := s.store.GetOperation(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if opBeforeComplete.Type == model.OperationTypeDelete {
		deletingApp, err = s.store.GetApp(opBeforeComplete.AppID)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
	}
	op, err := s.store.CompleteAgentOperation(r.PathValue("id"), principal.ActorID, req.ManifestPath, req.Message)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if op.Type == model.OperationTypeDelete {
		if err := s.cleanupDeletedAppImages(r.Context(), deletingApp); err != nil && s.log != nil {
			s.log.Printf("cleanup deleted app images for app=%s failed: %v", deletingApp.ID, err)
		}
		s.refreshTenantBillingImageStorage(r.Context(), deletingApp.TenantID, true)
	}
	s.appendAudit(principal, "operation.complete", "operation", op.ID, op.TenantID, map[string]string{"mode": op.ExecutionMode})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"operation": op})
}

func (s *Server) loadAuthorizedApp(w http.ResponseWriter, r *http.Request, principal model.Principal) (model.App, bool) {
	app, err := s.store.GetApp(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return model.App{}, false
	}
	if !principal.IsPlatformAdmin() && app.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "app is not visible to this tenant")
		return model.App{}, false
	}
	return app, true
}

func (s *Server) loadAuthorizedProject(w http.ResponseWriter, r *http.Request, principal model.Principal) (model.Project, bool) {
	project, err := s.store.GetProject(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return model.Project{}, false
	}
	if !principal.IsPlatformAdmin() && project.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "project is not visible to this tenant")
		return model.Project{}, false
	}
	return project, true
}

func (s *Server) loadAuthorizedAPIKey(w http.ResponseWriter, r *http.Request, principal model.Principal) (model.APIKey, bool) {
	key, err := s.store.GetAPIKey(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return model.APIKey{}, false
	}
	if !principal.IsPlatformAdmin() && key.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "api key is not visible to this tenant")
		return model.APIKey{}, false
	}
	return key, true
}

func (s *Server) resolveTenantID(principal model.Principal, requested string) (string, bool) {
	requested = strings.TrimSpace(requested)
	if principal.IsPlatformAdmin() {
		return requested, true
	}
	if requested == "" {
		return principal.TenantID, principal.TenantID != ""
	}
	return principal.TenantID, requested == principal.TenantID
}

func (s *Server) writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		httpx.WriteError(w, http.StatusNotFound, "resource not found")
	case errors.Is(err, store.ErrConflict):
		httpx.WriteError(w, http.StatusConflict, "resource conflict")
	case errors.Is(err, store.ErrInvalidInput):
		httpx.WriteError(w, http.StatusBadRequest, "invalid input")
	case errors.Is(err, store.ErrIdempotencyMismatch):
		httpx.WriteError(w, http.StatusConflict, "idempotency key does not match the original request")
	case errors.Is(err, store.ErrBillingCapExceeded):
		httpx.WriteError(w, http.StatusConflict, err.Error())
	case errors.Is(err, store.ErrBillingBalanceDepleted):
		httpx.WriteError(w, http.StatusPaymentRequired, err.Error())
	default:
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
	}
}

func markDeprecatedNodesView(w http.ResponseWriter) {
	w.Header().Set("Deprecation", "true")
	w.Header().Set("Link", "</v1/runtimes>; rel=\"successor-version\"")
	w.Header().Add("Warning", `299 - "/v1/nodes is a compatibility runtime view; use /v1/runtimes and /v1/cluster/nodes"`)
}

func (s *Server) appendAudit(principal model.Principal, action, targetType, targetID, tenantID string, metadata map[string]string) {
	if err := s.store.AppendAuditEvent(model.AuditEvent{
		TenantID:   tenantID,
		ActorType:  principal.ActorType,
		ActorID:    principal.ActorID,
		Action:     action,
		TargetType: targetType,
		TargetID:   targetID,
		Metadata:   metadata,
	}); err != nil {
		s.log.Printf("append audit failed for %s %s: %v", action, targetID, err)
	}
}

func mustPrincipal(r *http.Request) model.Principal {
	principal, ok := auth.PrincipalFromContext(r.Context())
	if !ok {
		panic("principal missing from request context")
	}
	return principal
}

func scopesSubset(scopes []string, principal model.Principal) bool {
	for _, scope := range scopes {
		if !principal.HasScope(scope) {
			return false
		}
	}
	return true
}

func loggingMiddleware(logger *log.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		logger.Printf("%s %s status=%d duration=%s", r.Method, r.URL.Path, recorder.status, time.Since(start))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Flush() {
	flusher, ok := r.ResponseWriter.(http.Flusher)
	if !ok {
		return
	}
	flusher.Flush()
}
