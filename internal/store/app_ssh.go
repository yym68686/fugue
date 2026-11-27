package store

import (
	"fmt"
	"strings"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

type AppSSHUpdate struct {
	Enabled            bool
	TargetPort         int
	User               string
	AuthorizedKeyIDs   []string
	AuthorizedKeys     []string
	AllowTCPForwarding bool
	Hostname           string
	PublicPortStart    int
	PublicPortEnd      int
}

type AppSSHRouteOptions struct {
	EdgeID      string
	EdgeGroupID string
}

func (s *Store) ListSSHKeys(tenantID string, platformAdmin bool) ([]model.SSHKey, error) {
	return s.pgListSSHKeys(tenantID, platformAdmin)
}

func (s *Store) GetSSHKey(id string) (model.SSHKey, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.SSHKey{}, ErrInvalidInput
	}
	return s.pgGetSSHKey(id)
}

func (s *Store) CreateSSHKey(tenantID, label, publicKey string) (model.SSHKey, error) {
	tenantID = strings.TrimSpace(tenantID)
	label = strings.TrimSpace(label)
	if label == "" {
		label = "default"
	}
	normalizedKey, err := model.NormalizeSSHPublicKey(publicKey)
	if err != nil {
		return model.SSHKey{}, ErrInvalidInput
	}
	fingerprint, err := model.SSHPublicKeyFingerprint(normalizedKey)
	if err != nil {
		return model.SSHKey{}, ErrInvalidInput
	}
	if tenantID == "" {
		return model.SSHKey{}, ErrInvalidInput
	}
	return s.pgCreateSSHKey(tenantID, label, normalizedKey, fingerprint)
}

func (s *Store) DeleteSSHKey(id string) (model.SSHKey, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.SSHKey{}, ErrInvalidInput
	}
	return s.pgDeleteSSHKey(id)
}

func (s *Store) GetAppSSHEndpoint(appID string) (model.AppSSHEndpoint, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return model.AppSSHEndpoint{}, ErrInvalidInput
	}
	return s.pgGetAppSSHEndpoint(appID)
}

func (s *Store) UpsertAppSSHConfig(appID string, update AppSSHUpdate) (model.App, model.AppSSHEndpoint, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return model.App{}, model.AppSSHEndpoint{}, ErrInvalidInput
	}
	update = normalizeAppSSHUpdate(update)
	if !update.Enabled {
		return s.DisableAppSSH(appID)
	}
	return s.pgUpsertAppSSHConfig(appID, update)
}

func (s *Store) DisableAppSSH(appID string) (model.App, model.AppSSHEndpoint, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return model.App{}, model.AppSSHEndpoint{}, ErrInvalidInput
	}
	return s.pgDisableAppSSH(appID)
}

func (s *Store) RotateAppSSHPort(appID string, start, end int) (model.AppSSHEndpoint, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return model.AppSSHEndpoint{}, ErrInvalidInput
	}
	start, end = normalizeAppSSHPortRange(start, end)
	return s.pgRotateAppSSHPort(appID, start, end)
}

func (s *Store) ListEdgeSSHRoutes(options AppSSHRouteOptions) ([]model.EdgeSSHRoute, error) {
	return s.pgListEdgeSSHRoutes(options)
}

func normalizeAppSSHUpdate(update AppSSHUpdate) AppSSHUpdate {
	update.TargetPort = normalizePort(update.TargetPort, model.DefaultAppSSHPort)
	update.User = strings.TrimSpace(update.User)
	if update.User == "" {
		update.User = model.DefaultAppSSHUser
	}
	update.AuthorizedKeyIDs = uniqueTrimmedStrings(update.AuthorizedKeyIDs)
	update.AuthorizedKeys = model.NormalizeSSHPublicKeys(update.AuthorizedKeys)
	update.PublicPortStart, update.PublicPortEnd = normalizeAppSSHPortRange(update.PublicPortStart, update.PublicPortEnd)
	return update
}

func normalizeAppSSHPortRange(start, end int) (int, int) {
	if start <= 0 {
		start = model.DefaultAppSSHPublicPortStart
	}
	if end <= 0 {
		end = model.DefaultAppSSHPublicPortEnd
	}
	if end < start {
		start, end = end, start
	}
	if start < 1 {
		start = 1
	}
	if end > 65535 {
		end = 65535
	}
	return start, end
}

func normalizePort(port int, fallback int) int {
	if port <= 0 || port > 65535 {
		return fallback
	}
	return port
}

func edgeSSHRouteFromEndpoint(endpoint model.AppSSHEndpoint, options AppSSHRouteOptions) (model.EdgeSSHRoute, bool) {
	endpoint = normalizeAppSSHEndpointForRead(endpoint)
	if endpoint.Status != model.AppSSHEndpointStatusPending && endpoint.Status != model.AppSSHEndpointStatusReady {
		return model.EdgeSSHRoute{}, false
	}
	if endpoint.PublicPort <= 0 || endpoint.TargetPort <= 0 || strings.TrimSpace(endpoint.TargetHost) == "" {
		return model.EdgeSSHRoute{}, false
	}
	if edgeGroupID := strings.TrimSpace(options.EdgeGroupID); edgeGroupID != "" && strings.TrimSpace(endpoint.EdgeGroupID) != "" && endpoint.EdgeGroupID != edgeGroupID {
		return model.EdgeSSHRoute{}, false
	}
	status := model.AppSSHEndpointStatusReady
	statusReason := endpoint.StatusReason
	if endpoint.Status == model.AppSSHEndpointStatusPending {
		status = model.AppSSHEndpointStatusPending
		if statusReason == "" {
			statusReason = "app ssh endpoint is not confirmed ready yet"
		}
	}
	route := model.EdgeSSHRoute{
		AppID:           endpoint.AppID,
		TenantID:        endpoint.TenantID,
		RuntimeID:       endpoint.RuntimeID,
		RuntimeType:     endpoint.RuntimeType,
		EdgeGroupID:     endpoint.EdgeGroupID,
		Hostname:        endpoint.Hostname,
		PublicPort:      endpoint.PublicPort,
		TargetHost:      endpoint.TargetHost,
		TargetPort:      endpoint.TargetPort,
		User:            endpoint.User,
		Status:          status,
		StatusReason:    statusReason,
		RouteGeneration: fmt.Sprintf("%s:%d:%s:%d", endpoint.AppID, endpoint.PublicPort, endpoint.TargetHost, endpoint.TargetPort),
	}
	return route, true
}

func normalizeAppSSHEndpointForRead(endpoint model.AppSSHEndpoint) model.AppSSHEndpoint {
	endpoint.Status = model.NormalizeAppSSHEndpointStatus(endpoint.Status)
	if endpoint.Status == "" {
		endpoint.Status = model.AppSSHEndpointStatusPending
	}
	if endpoint.User == "" {
		endpoint.User = model.DefaultAppSSHUser
	}
	if endpoint.TargetPort <= 0 {
		endpoint.TargetPort = model.DefaultAppSSHPort
	}
	return endpoint
}

func removeStringValue(values []string, removed string) []string {
	out := values[:0]
	for _, value := range values {
		if value == removed {
			continue
		}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func uniqueTrimmedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func sshPublicKeyComment(publicKey string) string {
	fields := strings.Fields(publicKey)
	if len(fields) < 3 {
		return ""
	}
	return strings.Join(fields[2:], " ")
}

func edgeGroupIDForRuntime(runtimeObj model.Runtime) string {
	if edgeGroupID := firstRuntimeLabelValue(runtimeObj.Labels, runtimepkg.EdgeGroupIDLabelKey, "edge_group_id", "edgeGroupID"); edgetopology.ValidAuthorityID(edgeGroupID) {
		return edgeGroupID
	}
	return ""
}

func firstRuntimeLabelValue(labels map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(labels[key]); value != "" {
			return value
		}
	}
	return ""
}
