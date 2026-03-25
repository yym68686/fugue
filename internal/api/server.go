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
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

type Server struct {
	store                       *store.Store
	auth                        *auth.Authenticator
	log                         *log.Logger
	appBaseDomain               string
	apiPublicDomain             string
	registryPushBase            string
	registryPullBase            string
	clusterJoinRegistryEndpoint string
	reservedAppHosts            map[string]struct{}
	clusterJoinServer           string
	clusterJoinToken            string
	clusterJoinMeshProvider     string
	clusterJoinMeshLoginServer  string
	clusterJoinMeshAuthKey      string
	importer                    *sourceimport.Importer
	newClusterNodeClient        func() (*clusterNodeClient, error)
	newWorkspacePodLister       func(namespace string) (workspacePodLister, error)
	workspaceExecRunner         workspacePodExecRunner
	ready                       atomic.Bool
}

func NewServer(store *store.Store, authn *auth.Authenticator, logger *log.Logger, cfg ServerConfig) *Server {
	if logger == nil {
		logger = log.Default()
	}
	server := &Server{
		store:                       store,
		auth:                        authn,
		log:                         logger,
		appBaseDomain:               strings.TrimSpace(strings.ToLower(cfg.AppBaseDomain)),
		apiPublicDomain:             strings.TrimSpace(strings.ToLower(cfg.APIPublicDomain)),
		registryPushBase:            strings.TrimSpace(cfg.RegistryPushBase),
		registryPullBase:            strings.TrimSpace(cfg.RegistryPullBase),
		clusterJoinRegistryEndpoint: strings.TrimSpace(cfg.ClusterJoinRegistryEndpoint),
		clusterJoinServer:           strings.TrimSpace(cfg.ClusterJoinServer),
		clusterJoinToken:            strings.TrimSpace(cfg.ClusterJoinToken),
		clusterJoinMeshProvider:     strings.TrimSpace(strings.ToLower(cfg.ClusterJoinMeshProvider)),
		clusterJoinMeshLoginServer:  strings.TrimSpace(cfg.ClusterJoinMeshLoginServer),
		clusterJoinMeshAuthKey:      strings.TrimSpace(cfg.ClusterJoinMeshAuthKey),
		importer:                    sourceimport.NewImporter(cfg.ImportWorkDir, logger),
		newClusterNodeClient:        newClusterNodeClient,
		newWorkspacePodLister: func(namespace string) (workspacePodLister, error) {
			return newKubeLogsClient(namespace)
		},
		workspaceExecRunner: kubectlWorkspaceExecRunner{},
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

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	mux.Handle("GET /v1/tenants", s.auth.RequireAPI(http.HandlerFunc(s.handleListTenants)))
	mux.Handle("POST /v1/tenants", s.auth.RequireAPI(http.HandlerFunc(s.handleCreateTenant)))
	mux.Handle("DELETE /v1/tenants/{id}", s.auth.RequireAPI(http.HandlerFunc(s.handleDeleteTenant)))

	mux.Handle("GET /v1/projects", s.auth.RequireAPI(http.HandlerFunc(s.handleListProjects)))
	mux.Handle("POST /v1/projects", s.auth.RequireAPI(http.HandlerFunc(s.handleCreateProject)))
	mux.Handle("PATCH /v1/projects/{id}", s.auth.RequireAPI(http.HandlerFunc(s.handlePatchProject)))
	mux.Handle("DELETE /v1/projects/{id}", s.auth.RequireAPI(http.HandlerFunc(s.handleDeleteProject)))

	mux.Handle("GET /v1/api-keys", s.auth.RequireAPI(http.HandlerFunc(s.handleListAPIKeys)))
	mux.Handle("POST /v1/api-keys", s.auth.RequireAPI(http.HandlerFunc(s.handleCreateAPIKey)))
	mux.Handle("PATCH /v1/api-keys/{id}", s.auth.RequireAPI(http.HandlerFunc(s.handlePatchAPIKey)))
	mux.Handle("POST /v1/api-keys/{id}/rotate", s.auth.RequireAPI(http.HandlerFunc(s.handleRotateAPIKey)))
	mux.Handle("POST /v1/api-keys/{id}/disable", s.auth.RequireAPI(http.HandlerFunc(s.handleDisableAPIKey)))
	mux.Handle("POST /v1/api-keys/{id}/enable", s.auth.RequireAPI(http.HandlerFunc(s.handleEnableAPIKey)))
	mux.Handle("DELETE /v1/api-keys/{id}", s.auth.RequireAPI(http.HandlerFunc(s.handleDeleteAPIKey)))

	mux.Handle("GET /v1/node-keys", s.auth.RequireAPI(http.HandlerFunc(s.handleListNodeKeys)))
	mux.Handle("POST /v1/node-keys", s.auth.RequireAPI(http.HandlerFunc(s.handleCreateNodeKey)))
	mux.Handle("GET /v1/node-keys/{id}/usages", s.auth.RequireAPI(http.HandlerFunc(s.handleGetNodeKeyUsages)))
	mux.Handle("POST /v1/node-keys/{id}/revoke", s.auth.RequireAPI(http.HandlerFunc(s.handleRevokeNodeKey)))

	mux.Handle("GET /v1/cluster/nodes", s.auth.RequireAPI(http.HandlerFunc(s.handleListClusterNodes)))
	mux.Handle("GET /v1/nodes", s.auth.RequireAPI(http.HandlerFunc(s.handleListNodes)))
	mux.Handle("GET /v1/nodes/{id}", s.auth.RequireAPI(http.HandlerFunc(s.handleGetNode)))

	mux.Handle("GET /v1/runtimes", s.auth.RequireAPI(http.HandlerFunc(s.handleListRuntimes)))
	mux.Handle("POST /v1/runtimes", s.auth.RequireAPI(http.HandlerFunc(s.handleCreateRuntime)))
	mux.Handle("GET /v1/runtimes/{id}", s.auth.RequireAPI(http.HandlerFunc(s.handleGetRuntime)))
	mux.Handle("GET /v1/runtimes/enroll-tokens", s.auth.RequireAPI(http.HandlerFunc(s.handleListEnrollmentTokens)))
	mux.Handle("POST /v1/runtimes/enroll-tokens", s.auth.RequireAPI(http.HandlerFunc(s.handleCreateEnrollmentToken)))
	mux.Handle("GET /v1/backing-services", s.auth.RequireAPI(http.HandlerFunc(s.handleListBackingServices)))
	mux.Handle("POST /v1/backing-services", s.auth.RequireAPI(http.HandlerFunc(s.handleCreateBackingService)))
	mux.Handle("GET /v1/backing-services/{id}", s.auth.RequireAPI(http.HandlerFunc(s.handleGetBackingService)))
	mux.Handle("DELETE /v1/backing-services/{id}", s.auth.RequireAPI(http.HandlerFunc(s.handleDeleteBackingService)))

	mux.Handle("GET /v1/apps", s.auth.RequireAPI(http.HandlerFunc(s.handleListApps)))
	mux.Handle("POST /v1/apps", s.auth.RequireAPI(http.HandlerFunc(s.handleCreateApp)))
	mux.Handle("POST /v1/apps/import-github", s.auth.RequireAPI(http.HandlerFunc(s.handleImportGitHubApp)))
	mux.Handle("POST /v1/apps/import-upload", s.auth.RequireAPI(http.HandlerFunc(s.handleImportUploadApp)))
	mux.Handle("GET /v1/apps/{id}", s.auth.RequireAPI(http.HandlerFunc(s.handleGetApp)))
	mux.Handle("GET /v1/apps/{id}/bindings", s.auth.RequireAPI(http.HandlerFunc(s.handleListAppBindings)))
	mux.Handle("POST /v1/apps/{id}/bindings", s.auth.RequireAPI(http.HandlerFunc(s.handleCreateAppBinding)))
	mux.Handle("DELETE /v1/apps/{id}/bindings/{binding_id}", s.auth.RequireAPI(http.HandlerFunc(s.handleDeleteAppBinding)))
	mux.Handle("GET /v1/apps/{id}/build-logs", s.auth.RequireAPI(http.HandlerFunc(s.handleGetAppBuildLogs)))
	mux.Handle("GET /v1/apps/{id}/runtime-logs", s.auth.RequireAPI(http.HandlerFunc(s.handleGetAppRuntimeLogs)))
	mux.Handle("GET /v1/apps/{id}/env", s.auth.RequireAPI(http.HandlerFunc(s.handleGetAppEnv)))
	mux.Handle("PATCH /v1/apps/{id}/env", s.auth.RequireAPI(http.HandlerFunc(s.handlePatchAppEnv)))
	mux.Handle("GET /v1/apps/{id}/files", s.auth.RequireAPI(http.HandlerFunc(s.handleGetAppFiles)))
	mux.Handle("PUT /v1/apps/{id}/files", s.auth.RequireAPI(http.HandlerFunc(s.handleUpsertAppFiles)))
	mux.Handle("DELETE /v1/apps/{id}/files", s.auth.RequireAPI(http.HandlerFunc(s.handleDeleteAppFiles)))
	mux.Handle("GET /v1/apps/{id}/filesystem/tree", s.auth.RequireAPI(http.HandlerFunc(s.handleGetAppFilesystemTree)))
	mux.Handle("GET /v1/apps/{id}/filesystem/file", s.auth.RequireAPI(http.HandlerFunc(s.handleGetAppFilesystemFile)))
	mux.Handle("PUT /v1/apps/{id}/filesystem/file", s.auth.RequireAPI(http.HandlerFunc(s.handlePutAppFilesystemFile)))
	mux.Handle("POST /v1/apps/{id}/filesystem/directory", s.auth.RequireAPI(http.HandlerFunc(s.handleCreateAppFilesystemDirectory)))
	mux.Handle("DELETE /v1/apps/{id}/filesystem", s.auth.RequireAPI(http.HandlerFunc(s.handleDeleteAppFilesystemPath)))
	mux.Handle("POST /v1/apps/{id}/rebuild", s.auth.RequireAPI(http.HandlerFunc(s.handleRebuildApp)))
	mux.Handle("POST /v1/apps/{id}/deploy", s.auth.RequireAPI(http.HandlerFunc(s.handleDeployApp)))
	mux.Handle("POST /v1/apps/{id}/restart", s.auth.RequireAPI(http.HandlerFunc(s.handleRestartApp)))
	mux.Handle("POST /v1/apps/{id}/scale", s.auth.RequireAPI(http.HandlerFunc(s.handleScaleApp)))
	mux.Handle("POST /v1/apps/{id}/disable", s.auth.RequireAPI(http.HandlerFunc(s.handleDisableApp)))
	mux.Handle("POST /v1/apps/{id}/migrate", s.auth.RequireAPI(http.HandlerFunc(s.handleMigrateApp)))
	mux.Handle("DELETE /v1/apps/{id}", s.auth.RequireAPI(http.HandlerFunc(s.handleDeleteApp)))

	mux.Handle("GET /v1/operations", s.auth.RequireAPI(http.HandlerFunc(s.handleListOperations)))
	mux.Handle("GET /v1/operations/{id}", s.auth.RequireAPI(http.HandlerFunc(s.handleGetOperation)))

	mux.Handle("GET /v1/audit-events", s.auth.RequireAPI(http.HandlerFunc(s.handleListAuditEvents)))

	mux.HandleFunc("GET /install/join-cluster.sh", s.handleJoinClusterInstallScript)
	mux.HandleFunc("GET /v1/source-uploads/{id}/archive", s.handleGetSourceUploadArchive)
	mux.HandleFunc("POST /v1/agent/enroll", s.handleAgentEnroll)
	mux.HandleFunc("POST /v1/nodes/bootstrap", s.handleBootstrapNode)
	mux.HandleFunc("POST /v1/nodes/join-cluster", s.handleJoinClusterNode)
	mux.HandleFunc("POST /v1/nodes/join-cluster/env", s.handleJoinClusterNodeEnv)
	mux.Handle("POST /v1/agent/heartbeat", s.auth.RequireRuntime(http.HandlerFunc(s.handleAgentHeartbeat)))
	mux.Handle("GET /v1/agent/operations", s.auth.RequireRuntime(http.HandlerFunc(s.handleAgentOperations)))
	mux.Handle("POST /v1/agent/operations/{id}/complete", s.auth.RequireRuntime(http.HandlerFunc(s.handleAgentCompleteOperation)))

	return loggingMiddleware(s.log, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.maybeHandleAppProxy(w, r) {
			return
		}
		mux.ServeHTTP(w, r)
	}))
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if !s.ready.Load() {
		httpx.WriteError(w, http.StatusServiceUnavailable, "shutting down")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
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
	s.appendAudit(principal, "node_key.revoke", "node_key", key.ID, key.TenantID, map[string]string{"label": key.Label})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"node_key": key})
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
	node, err := s.store.GetRuntime(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if node.Type != model.RuntimeTypeExternalOwned && node.Type != model.RuntimeTypeManagedOwned {
		httpx.WriteError(w, http.StatusNotFound, "resource not found")
		return
	}
	if !principal.IsPlatformAdmin() && node.TenantID != principal.TenantID {
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
	runtimes, err := s.store.ListRuntimes(principal.TenantID, principal.IsPlatformAdmin())
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
	if !principal.IsPlatformAdmin() && runtimeObj.Type != model.RuntimeTypeManagedShared && runtimeObj.TenantID != principal.TenantID {
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
	apps, err := s.store.ListApps(principal.TenantID, principal.IsPlatformAdmin())
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
	if hasManagedStatefulBinding(app) || app.Spec.Workspace != nil {
		httpx.WriteError(w, http.StatusBadRequest, "stateful apps with managed backing services or persistent workspaces are not migratable yet")
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
	ops, err := s.store.ListOperations(principal.TenantID, principal.IsPlatformAdmin())
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
	op, err := s.store.CompleteAgentOperation(r.PathValue("id"), principal.ActorID, req.ManifestPath, req.Message)
	if err != nil {
		s.writeStoreError(w, err)
		return
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
