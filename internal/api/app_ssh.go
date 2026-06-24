package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

type appSSHDiagnosisCheck struct {
	Name    string `json:"name"`
	Pass    bool   `json:"pass"`
	Message string `json:"message"`
}

func (s *Server) handleListSSHKeys(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("ssh.key.read") && !principal.HasScope("ssh.key.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing ssh.key.read scope")
		return
	}
	keys, err := s.store.ListSSHKeys(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"ssh_keys": keys})
}

func (s *Server) handleCreateSSHKey(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("ssh.key.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing ssh.key.write scope")
		return
	}
	var req struct {
		TenantID  string `json:"tenant_id"`
		Label     string `json:"label"`
		PublicKey string `json:"public_key"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tenantID, ok := s.resolveTenantID(principal, req.TenantID)
	if !ok {
		httpx.WriteError(w, http.StatusForbidden, "cannot create ssh key for another tenant")
		return
	}
	key, err := s.store.CreateSSHKey(tenantID, req.Label, req.PublicKey)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "ssh.key.create", "ssh_key", key.ID, tenantID, map[string]string{"label": key.Label, "fingerprint": key.Fingerprint})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"ssh_key": key})
}

func (s *Server) handleDeleteSSHKey(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("ssh.key.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing ssh.key.write scope")
		return
	}
	key, err := s.store.GetSSHKey(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() && key.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "ssh key is not visible to this tenant")
		return
	}
	deleted, err := s.store.DeleteSSHKey(key.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "ssh.key.delete", "ssh_key", deleted.ID, deleted.TenantID, map[string]string{"label": deleted.Label, "fingerprint": deleted.Fingerprint})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"deleted": true, "ssh_key": deleted})
}

func (s *Server) handleGetAppSSH(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.ssh.read") && !principal.HasScope("app.ssh.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.ssh.read scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	endpoint, found, err := s.appSSHEndpoint(app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, appSSHResponse(app, endpoint, found, nil, false))
}

func (s *Server) handlePatchAppSSH(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.ssh.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.ssh.write scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	var req struct {
		Enabled            *bool    `json:"enabled"`
		TargetPort         int      `json:"target_port"`
		User               string   `json:"user"`
		AuthorizedKeyIDs   []string `json:"authorized_key_ids"`
		AuthorizedKeys     []string `json:"authorized_keys"`
		AllowTCPForwarding bool     `json:"allow_tcp_forwarding"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	if req.TargetPort < 0 || req.TargetPort > 65535 {
		httpx.WriteError(w, http.StatusBadRequest, "target_port must be between 1 and 65535")
		return
	}
	for _, publicKey := range req.AuthorizedKeys {
		if _, err := model.NormalizeSSHPublicKey(publicKey); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	baselineSpec, source, err := s.recoverAppDeployBaseline(app)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	var (
		updatedApp model.App
		endpoint   model.AppSSHEndpoint
	)
	if enabled {
		updatedApp, endpoint, err = s.store.UpsertAppSSHConfig(app.ID, store.AppSSHUpdate{
			Enabled:            true,
			TargetPort:         req.TargetPort,
			User:               req.User,
			AuthorizedKeyIDs:   req.AuthorizedKeyIDs,
			AuthorizedKeys:     req.AuthorizedKeys,
			AllowTCPForwarding: req.AllowTCPForwarding,
			Hostname:           s.sshPublicHostname(),
			PublicPortStart:    s.sshPublicPortStart,
			PublicPortEnd:      s.sshPublicPortEnd,
		})
	} else {
		updatedApp, endpoint, err = s.store.DisableAppSSH(app.ID)
	}
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	spec := cloneAppSpec(baselineSpec)
	spec.SSH = updatedApp.Spec.SSH
	op, err := s.store.CreateOperation(model.Operation{
		TenantID:            updatedApp.TenantID,
		Type:                model.OperationTypeDeploy,
		RequestedByType:     principal.ActorType,
		RequestedByID:       principal.ActorID,
		AppID:               updatedApp.ID,
		DesiredSpec:         &spec,
		DesiredSource:       source,
		DesiredOriginSource: model.AppOriginSource(app),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	action := "app.ssh.enable"
	if !enabled {
		action = "app.ssh.disable"
	}
	s.appendAudit(principal, action, "operation", op.ID, updatedApp.TenantID, map[string]string{"app_id": updatedApp.ID})
	httpx.WriteJSON(w, http.StatusAccepted, appSSHResponse(updatedApp, endpoint, endpoint.ID != "", &op, false))
}

func (s *Server) handleRotateAppSSHPort(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.ssh.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.ssh.write scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	endpoint, err := s.store.RotateAppSSHPort(app.ID, s.sshPublicPortStart, s.sshPublicPortEnd)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.ssh.rotate_port", "app", app.ID, app.TenantID, map[string]string{"public_port": httpxValue(endpoint.PublicPort)})
	httpx.WriteJSON(w, http.StatusOK, appSSHResponse(app, endpoint, true, nil, false))
}

func (s *Server) handleDiagnoseAppSSH(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.ssh.read") && !principal.HasScope("app.ssh.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.ssh.read scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	endpoint, found, err := s.appSSHEndpoint(app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	checks := appSSHDiagnosisChecks(app, endpoint, found)
	response := appSSHResponse(app, endpoint, found, nil, false)
	response["checks"] = checks
	httpx.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) handleEdgeSSHRoutes(w http.ResponseWriter, r *http.Request) {
	authContext, ok := s.authorizeEdgeRequest(w, r)
	if !ok {
		return
	}
	options := store.AppSSHRouteOptions{
		EdgeID:      strings.TrimSpace(r.URL.Query().Get("edge_id")),
		EdgeGroupID: strings.TrimSpace(r.URL.Query().Get("edge_group_id")),
	}
	if err := authContext.constrain(&options.EdgeID, &options.EdgeGroupID); err != nil {
		httpx.WriteError(w, http.StatusForbidden, err.Error())
		return
	}
	bundle, err := s.deriveEdgeSSHRouteBundle(options)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	w.Header().Set("ETag", edgeRouteBundleETag(bundle.Version))
	w.Header().Set("Cache-Control", "private, no-cache")
	w.Header().Set("X-Fugue-SSH-Route-Bundle-Version", bundle.Version)
	httpx.WriteJSON(w, http.StatusOK, bundle)
}

func (s *Server) deriveEdgeSSHRouteBundle(options store.AppSSHRouteOptions) (model.EdgeSSHRouteBundle, error) {
	routes, err := s.store.ListEdgeSSHRoutes(options)
	if err != nil {
		return model.EdgeSSHRouteBundle{}, err
	}
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].PublicPort == routes[j].PublicPort {
			return routes[i].AppID < routes[j].AppID
		}
		return routes[i].PublicPort < routes[j].PublicPort
	})
	now := time.Now().UTC()
	bundle := model.EdgeSSHRouteBundle{
		SchemaVersion: model.BundleSchemaVersionV1,
		GeneratedAt:   now,
		Issuer:        model.BundleIssuerFugue,
		EdgeID:        strings.TrimSpace(options.EdgeID),
		EdgeGroupID:   strings.TrimSpace(options.EdgeGroupID),
		Routes:        routes,
	}
	bundle.Version = edgeSSHRouteBundleVersion(bundle)
	return signEdgeSSHRouteBundle(bundle, s.bundleKeyring(), s.discoveryBundleTTL()), nil
}

func edgeSSHRouteBundleVersion(bundle model.EdgeSSHRouteBundle) string {
	material := struct {
		Routes []model.EdgeSSHRoute `json:"routes"`
	}{
		Routes: append([]model.EdgeSSHRoute(nil), bundle.Routes...),
	}
	payload, _ := json.Marshal(material)
	sum := sha256.Sum256(payload)
	return "sshroutegen_" + hex.EncodeToString(sum[:])[:16]
}

func (s *Server) appSSHEndpoint(appID string) (model.AppSSHEndpoint, bool, error) {
	endpoint, err := s.store.GetAppSSHEndpoint(appID)
	if err != nil {
		if err == store.ErrNotFound {
			return model.AppSSHEndpoint{}, false, nil
		}
		return model.AppSSHEndpoint{}, false, err
	}
	return endpoint, true, nil
}

func appSSHResponse(app model.App, endpoint model.AppSSHEndpoint, found bool, op *model.Operation, alreadyCurrent bool) map[string]any {
	response := map[string]any{
		"app": sanitizeAppForAPI(app),
		"ssh": appSSHStatus(app, endpoint, found),
	}
	if found {
		response["endpoint"] = endpoint
	}
	if op != nil {
		response["operation"] = sanitizeOperationForAPI(*op)
	}
	if alreadyCurrent {
		response["already_current"] = true
	}
	return response
}

func appSSHStatus(app model.App, endpoint model.AppSSHEndpoint, found bool) model.AppSSHStatus {
	ssh := model.NormalizeAppSSHSpec(app.Spec.SSH)
	status := model.AppSSHStatus{Supported: true}
	if ssh == nil {
		status.Ready = false
		status.Message = "ssh disabled"
		return status
	}
	status.TargetPort = ssh.TargetPort
	status.User = ssh.User
	if !found {
		status.Ready = false
		status.Message = "ssh endpoint has not been allocated"
		return status
	}
	status.Hostname = endpoint.Hostname
	status.PublicPort = endpoint.PublicPort
	status.TargetPort = endpoint.TargetPort
	status.User = endpoint.User
	status.HostKeyFingerprint = endpoint.HostKeyFingerprint
	switch model.NormalizeAppSSHEndpointStatus(endpoint.Status) {
	case model.AppSSHEndpointStatusUnsupported:
		status.Supported = false
		status.Ready = false
	case model.AppSSHEndpointStatusUnavailable, model.AppSSHEndpointStatusDisabled, model.AppSSHEndpointStatusReleased:
		status.Ready = false
	default:
		status.Ready = endpoint.PublicPort > 0 && strings.TrimSpace(endpoint.TargetHost) != ""
	}
	status.Message = endpoint.StatusReason
	return status
}

func appSSHDiagnosisChecks(app model.App, endpoint model.AppSSHEndpoint, found bool) []appSSHDiagnosisCheck {
	ssh := model.NormalizeAppSSHSpec(app.Spec.SSH)
	checks := []appSSHDiagnosisCheck{
		{
			Name:    "ssh_enabled",
			Pass:    ssh != nil,
			Message: appSSHCheckMessage(ssh != nil, "ssh is enabled in app spec", "ssh is disabled in app spec"),
		},
	}
	if ssh != nil {
		hasKeys := len(ssh.AuthorizedKeys) > 0 || len(ssh.AuthorizedKeyIDs) > 0
		checks = append(checks, appSSHDiagnosisCheck{
			Name:    "authorized_keys",
			Pass:    hasKeys,
			Message: appSSHCheckMessage(hasKeys, "authorized keys are configured", "no authorized keys are configured"),
		})
	}
	checks = append(checks, appSSHDiagnosisCheck{
		Name:    "endpoint_allocated",
		Pass:    found && endpoint.PublicPort > 0,
		Message: appSSHCheckMessage(found && endpoint.PublicPort > 0, "public ssh endpoint is allocated", "public ssh endpoint is missing"),
	})
	if found {
		supported := model.NormalizeAppSSHEndpointStatus(endpoint.Status) != model.AppSSHEndpointStatusUnsupported
		checks = append(checks, appSSHDiagnosisCheck{
			Name:    "runtime_supported",
			Pass:    supported,
			Message: appSSHCheckMessage(supported, "runtime supports native ssh routes", endpoint.StatusReason),
		})
		routeReady := endpoint.PublicPort > 0 && strings.TrimSpace(endpoint.TargetHost) != "" &&
			(model.NormalizeAppSSHEndpointStatus(endpoint.Status) == model.AppSSHEndpointStatusReady ||
				model.NormalizeAppSSHEndpointStatus(endpoint.Status) == model.AppSSHEndpointStatusPending)
		checks = append(checks, appSSHDiagnosisCheck{
			Name:    "edge_route",
			Pass:    routeReady,
			Message: appSSHCheckMessage(routeReady, "edge ssh route is publishable", "edge ssh route is not publishable"),
		})
	}
	return checks
}

func appSSHCheckMessage(pass bool, okMessage, failMessage string) string {
	if pass {
		return okMessage
	}
	if strings.TrimSpace(failMessage) == "" {
		return "check failed"
	}
	return failMessage
}

func (s *Server) sshPublicHostname() string {
	return defaultSSHPublicHost(s.sshPublicHost, s.appBaseDomain)
}

func defaultSSHPublicHost(rawHost, appBaseDomain string) string {
	host := strings.TrimSpace(strings.ToLower(rawHost))
	if host == "" && strings.TrimSpace(appBaseDomain) != "" {
		host = "ssh." + strings.TrimSpace(strings.ToLower(appBaseDomain))
	}
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	if before, _, ok := strings.Cut(host, "/"); ok {
		host = before
	}
	return strings.TrimSpace(host)
}
