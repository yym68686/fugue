package store

import (
	"fmt"
	"sort"
	"strings"
	"time"

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
	if s.usingDatabase() {
		return s.pgListSSHKeys(tenantID, platformAdmin)
	}
	var keys []model.SSHKey
	err := s.withLockedState(false, func(state *model.State) error {
		for _, key := range state.SSHKeys {
			if !platformAdmin && key.TenantID != tenantID {
				continue
			}
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].CreatedAt.Before(keys[j].CreatedAt)
		})
		return nil
	})
	return keys, err
}

func (s *Store) GetSSHKey(id string) (model.SSHKey, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.SSHKey{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetSSHKey(id)
	}
	var key model.SSHKey
	err := s.withLockedState(false, func(state *model.State) error {
		index := findSSHKey(state, id)
		if index < 0 {
			return ErrNotFound
		}
		key = state.SSHKeys[index]
		return nil
	})
	return key, err
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
	if s.usingDatabase() {
		return s.pgCreateSSHKey(tenantID, label, normalizedKey, fingerprint)
	}
	var key model.SSHKey
	err = s.withLockedState(true, func(state *model.State) error {
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		for _, existing := range state.SSHKeys {
			if existing.TenantID == tenantID && existing.Fingerprint == fingerprint && existing.Status == model.SSHKeyStatusActive {
				return ErrConflict
			}
		}
		now := time.Now().UTC()
		key = model.SSHKey{
			ID:          model.NewID("sshkey"),
			TenantID:    tenantID,
			Label:       label,
			PublicKey:   normalizedKey,
			Fingerprint: fingerprint,
			Status:      model.SSHKeyStatusActive,
			Comment:     sshPublicKeyComment(normalizedKey),
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		state.SSHKeys = append(state.SSHKeys, key)
		return nil
	})
	return key, err
}

func (s *Store) DeleteSSHKey(id string) (model.SSHKey, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.SSHKey{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteSSHKey(id)
	}
	var key model.SSHKey
	err := s.withLockedState(true, func(state *model.State) error {
		index := findSSHKey(state, id)
		if index < 0 {
			return ErrNotFound
		}
		key = state.SSHKeys[index]
		state.SSHKeys = append(state.SSHKeys[:index], state.SSHKeys[index+1:]...)
		for appIndex := range state.Apps {
			if state.Apps[appIndex].Spec.SSH == nil {
				continue
			}
			state.Apps[appIndex].Spec.SSH.AuthorizedKeyIDs = removeStringValue(state.Apps[appIndex].Spec.SSH.AuthorizedKeyIDs, id)
			state.Apps[appIndex].Spec.SSH = model.NormalizeAppSSHSpec(state.Apps[appIndex].Spec.SSH)
		}
		return nil
	})
	return key, err
}

func (s *Store) GetAppSSHEndpoint(appID string) (model.AppSSHEndpoint, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return model.AppSSHEndpoint{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetAppSSHEndpoint(appID)
	}
	var endpoint model.AppSSHEndpoint
	err := s.withLockedState(false, func(state *model.State) error {
		index := findAppSSHEndpointByApp(state, appID)
		if index < 0 {
			return ErrNotFound
		}
		endpoint = normalizeAppSSHEndpointForRead(state.AppSSHEndpoints[index])
		return nil
	})
	return endpoint, err
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
	if s.usingDatabase() {
		return s.pgUpsertAppSSHConfig(appID, update)
	}
	var (
		app      model.App
		endpoint model.AppSSHEndpoint
	)
	err := s.withLockedState(true, func(state *model.State) error {
		index := findApp(state, appID)
		if index < 0 {
			return ErrNotFound
		}
		app = state.Apps[index]
		if isDeletedApp(app) {
			return ErrNotFound
		}
		if err := validateAppSSHKeysState(state, app.TenantID, update.AuthorizedKeyIDs); err != nil {
			return err
		}
		authorizedKeys, err := resolveAppSSHAuthorizedKeysState(state, app.TenantID, update.AuthorizedKeyIDs, update.AuthorizedKeys)
		if err != nil {
			return err
		}
		runtimeObj, runtimeFound := runtimeByID(state, app.Spec.RuntimeID)
		status := model.AppSSHEndpointStatusPending
		statusReason := ""
		runtimeType := ""
		edgeGroupID := ""
		if runtimeFound {
			runtimeType = runtimeObj.Type
			edgeGroupID = edgeGroupIDForRuntime(runtimeObj)
			if runtimeObj.Type == model.RuntimeTypeExternalOwned {
				status = model.AppSSHEndpointStatusUnsupported
				statusReason = "external-owned runtimes do not support native ssh routes yet"
			}
		} else {
			status = model.AppSSHEndpointStatusUnavailable
			statusReason = "runtime is missing"
		}
		ssh := &model.AppSSHSpec{
			Enabled:            true,
			TargetPort:         update.TargetPort,
			User:               update.User,
			AuthorizedKeyIDs:   append([]string(nil), update.AuthorizedKeyIDs...),
			AuthorizedKeys:     authorizedKeys,
			AllowTCPForwarding: update.AllowTCPForwarding,
		}
		app.Spec.SSH = model.NormalizeAppSSHSpec(ssh)
		app.UpdatedAt = time.Now().UTC()
		state.Apps[index] = app

		endpointIndex := findAppSSHEndpointByApp(state, appID)
		if endpointIndex >= 0 {
			endpoint = state.AppSSHEndpoints[endpointIndex]
		} else {
			port, err := allocateAppSSHPublicPortState(state, update.PublicPortStart, update.PublicPortEnd, 0)
			if err != nil {
				return err
			}
			now := time.Now().UTC()
			endpoint = model.AppSSHEndpoint{
				ID:         model.NewID("sshend"),
				TenantID:   app.TenantID,
				ProjectID:  app.ProjectID,
				AppID:      app.ID,
				PublicPort: port,
				CreatedAt:  now,
			}
			state.AppSSHEndpoints = append(state.AppSSHEndpoints, endpoint)
			endpointIndex = len(state.AppSSHEndpoints) - 1
		}
		endpoint.TenantID = app.TenantID
		endpoint.ProjectID = app.ProjectID
		endpoint.RuntimeID = app.Spec.RuntimeID
		endpoint.RuntimeType = runtimeType
		endpoint.EdgeGroupID = edgeGroupID
		endpoint.Hostname = strings.TrimSpace(update.Hostname)
		endpoint.TargetNamespace = runtimepkg.NamespaceForTenant(app.TenantID)
		endpoint.TargetService = runtimepkg.RuntimeAppServiceName(app)
		endpoint.TargetHost = endpoint.TargetService + "." + endpoint.TargetNamespace + ".svc.cluster.local"
		endpoint.TargetPort = app.Spec.SSH.TargetPort
		endpoint.User = app.Spec.SSH.User
		endpoint.Status = status
		endpoint.StatusReason = statusReason
		endpoint.ReleasedAt = nil
		endpoint.UpdatedAt = time.Now().UTC()
		state.AppSSHEndpoints[endpointIndex] = endpoint
		normalizeAppStatusForRead(&app)
		hydrateAppBackingServices(state, &app)
		return nil
	})
	return app, normalizeAppSSHEndpointForRead(endpoint), err
}

func (s *Store) DisableAppSSH(appID string) (model.App, model.AppSSHEndpoint, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return model.App{}, model.AppSSHEndpoint{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDisableAppSSH(appID)
	}
	var (
		app      model.App
		endpoint model.AppSSHEndpoint
	)
	err := s.withLockedState(true, func(state *model.State) error {
		index := findApp(state, appID)
		if index < 0 {
			return ErrNotFound
		}
		app = state.Apps[index]
		if isDeletedApp(app) {
			return ErrNotFound
		}
		app.Spec.SSH = nil
		app.UpdatedAt = time.Now().UTC()
		state.Apps[index] = app
		endpointIndex := findAppSSHEndpointByApp(state, appID)
		if endpointIndex >= 0 {
			now := time.Now().UTC()
			endpoint = state.AppSSHEndpoints[endpointIndex]
			endpoint.Status = model.AppSSHEndpointStatusDisabled
			endpoint.StatusReason = "ssh disabled"
			endpoint.ReleasedAt = &now
			endpoint.UpdatedAt = now
			state.AppSSHEndpoints[endpointIndex] = endpoint
		}
		normalizeAppStatusForRead(&app)
		hydrateAppBackingServices(state, &app)
		return nil
	})
	return app, normalizeAppSSHEndpointForRead(endpoint), err
}

func (s *Store) RotateAppSSHPort(appID string, start, end int) (model.AppSSHEndpoint, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return model.AppSSHEndpoint{}, ErrInvalidInput
	}
	start, end = normalizeAppSSHPortRange(start, end)
	if s.usingDatabase() {
		return s.pgRotateAppSSHPort(appID, start, end)
	}
	var endpoint model.AppSSHEndpoint
	err := s.withLockedState(true, func(state *model.State) error {
		index := findAppSSHEndpointByApp(state, appID)
		if index < 0 {
			return ErrNotFound
		}
		current := state.AppSSHEndpoints[index]
		port, err := allocateAppSSHPublicPortState(state, start, end, current.PublicPort)
		if err != nil {
			return err
		}
		current.PublicPort = port
		current.UpdatedAt = time.Now().UTC()
		state.AppSSHEndpoints[index] = current
		endpoint = current
		return nil
	})
	return normalizeAppSSHEndpointForRead(endpoint), err
}

func (s *Store) ListEdgeSSHRoutes(options AppSSHRouteOptions) ([]model.EdgeSSHRoute, error) {
	if s.usingDatabase() {
		return s.pgListEdgeSSHRoutes(options)
	}
	var routes []model.EdgeSSHRoute
	err := s.withLockedState(false, func(state *model.State) error {
		for _, endpoint := range state.AppSSHEndpoints {
			route, ok := edgeSSHRouteFromEndpoint(endpoint, options)
			if ok {
				routes = append(routes, route)
			}
		}
		sort.Slice(routes, func(i, j int) bool {
			return routes[i].PublicPort < routes[j].PublicPort
		})
		return nil
	})
	return routes, err
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

func validateAppSSHKeysState(state *model.State, tenantID string, keyIDs []string) error {
	for _, id := range keyIDs {
		index := findSSHKey(state, id)
		if index < 0 {
			return ErrNotFound
		}
		key := state.SSHKeys[index]
		if key.TenantID != tenantID || key.Status != model.SSHKeyStatusActive {
			return ErrNotFound
		}
	}
	return nil
}

func resolveAppSSHAuthorizedKeysState(state *model.State, tenantID string, keyIDs, inlineKeys []string) ([]string, error) {
	keys := make([]string, 0, len(keyIDs)+len(inlineKeys))
	for _, id := range keyIDs {
		index := findSSHKey(state, id)
		if index < 0 {
			return nil, ErrNotFound
		}
		key := state.SSHKeys[index]
		if key.TenantID != tenantID || key.Status != model.SSHKeyStatusActive {
			return nil, ErrNotFound
		}
		keys = append(keys, key.PublicKey)
	}
	keys = append(keys, inlineKeys...)
	return model.NormalizeSSHPublicKeys(keys), nil
}

func allocateAppSSHPublicPortState(state *model.State, start, end, exclude int) (int, error) {
	start, end = normalizeAppSSHPortRange(start, end)
	used := map[int]struct{}{}
	for _, endpoint := range state.AppSSHEndpoints {
		if endpoint.PublicPort == exclude {
			continue
		}
		if endpoint.PublicPort <= 0 {
			continue
		}
		switch model.NormalizeAppSSHEndpointStatus(endpoint.Status) {
		case model.AppSSHEndpointStatusReleased:
			if endpoint.ReleasedAt != nil && time.Since(*endpoint.ReleasedAt) > time.Hour {
				continue
			}
		}
		used[endpoint.PublicPort] = struct{}{}
	}
	for port := start; port <= end; port++ {
		if port == exclude {
			continue
		}
		if _, ok := used[port]; !ok {
			return port, nil
		}
	}
	return 0, ErrConflict
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

func findSSHKey(state *model.State, id string) int {
	for idx, key := range state.SSHKeys {
		if key.ID == id {
			return idx
		}
	}
	return -1
}

func findAppSSHEndpointByApp(state *model.State, appID string) int {
	for idx, endpoint := range state.AppSSHEndpoints {
		if endpoint.AppID == appID {
			return idx
		}
	}
	return -1
}

func deleteSSHKeysByTenant(keys []model.SSHKey, tenantID string) []model.SSHKey {
	filtered := keys[:0]
	for _, key := range keys {
		if key.TenantID == tenantID {
			continue
		}
		filtered = append(filtered, key)
	}
	return filtered
}

func deleteAppSSHEndpointsByTenant(endpoints []model.AppSSHEndpoint, tenantID string) []model.AppSSHEndpoint {
	filtered := endpoints[:0]
	for _, endpoint := range endpoints {
		if endpoint.TenantID == tenantID {
			continue
		}
		filtered = append(filtered, endpoint)
	}
	return filtered
}

func deleteAppSSHEndpointsByApp(endpoints []model.AppSSHEndpoint, appID string) []model.AppSSHEndpoint {
	filtered := endpoints[:0]
	for _, endpoint := range endpoints {
		if endpoint.AppID == appID {
			continue
		}
		filtered = append(filtered, endpoint)
	}
	return filtered
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

func runtimeByID(state *model.State, id string) (model.Runtime, bool) {
	index := findRuntime(state, id)
	if index < 0 {
		return model.Runtime{}, false
	}
	return state.Runtimes[index], true
}

func edgeGroupIDForRuntime(runtimeObj model.Runtime) string {
	if country := firstRuntimeLabelValue(runtimeObj.Labels, runtimepkg.LocationCountryCodeLabelKey, "country_code", "countryCode"); country != "" {
		if slug := edgeRouteSlug(country); slug != "" {
			return "edge-group-country-" + slug
		}
	}
	if region := firstRuntimeLabelValue(runtimeObj.Labels, runtimepkg.RegionLabelKey, runtimepkg.LegacyRegionLabelKey, "region"); region != "" {
		if slug := edgeRouteSlug(region); slug != "" {
			return "edge-group-region-" + slug
		}
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

func edgeRouteSlug(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastDash = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		default:
			if b.Len() > 0 && !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}
