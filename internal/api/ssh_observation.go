package api

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/sshidentity"
	"fugue/internal/store"

	"golang.org/x/crypto/ssh"
)

var appSSHObservationSlots = make(chan struct{}, 8)

func (server *Server) handleObserveAppSSHIdentity(writer http.ResponseWriter, request *http.Request) {
	principal := mustPrincipal(request)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.ssh.read") && !principal.HasScope("app.ssh.write") {
		httpx.WriteError(writer, http.StatusForbidden, "missing app.ssh.read scope")
		return
	}
	app, allowed := server.loadAuthorizedApp(writer, request, principal)
	if !allowed {
		return
	}
	var input struct {
		IdentityID string `json:"identity_id"`
	}
	if err := httpx.DecodeJSON(request, &input); err != nil || strings.TrimSpace(input.IdentityID) == "" {
		httpx.WriteError(writer, http.StatusBadRequest, "identity_id is required")
		return
	}
	identity, err := server.store.GetAppSSHIdentity(app.TenantID, app.ID)
	if err != nil {
		server.writeSSHIdentityError(writer, err)
		return
	}
	if identity.ID != input.IdentityID {
		httpx.WriteError(writer, http.StatusConflict, "managed SSH identity changed")
		return
	}
	endpoint, found, err := server.appSSHEndpoint(app.ID)
	if err != nil {
		server.writeStoreError(writer, err)
		return
	}
	status := server.configuredAppSSHStatus(app, endpoint, found)
	if !status.RouteConfigured || endpoint.Hostname == "" {
		httpx.WriteError(writer, http.StatusConflict, "SSH route is not configured")
		return
	}
	if previous, err := server.store.LatestAppSSHObservation(app.TenantID, app.ID); err == nil && time.Since(previous.ObservedAt) < 5*time.Second {
		writer.Header().Set("Retry-After", "5")
		httpx.WriteError(writer, http.StatusTooManyRequests, "wait before probing the same SSH endpoint again")
		return
	}
	select {
	case appSSHObservationSlots <- struct{}{}:
		defer func() { <-appSSHObservationSlots }()
	default:
		httpx.WriteError(writer, http.StatusTooManyRequests, "SSH probe concurrency limit reached")
		return
	}
	publicKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(identity.PublicKey))
	if err != nil {
		server.writeSSHIdentityError(writer, sshidentity.ErrIntegrity)
		return
	}
	target := model.AppSSHPublicAddress(endpoint)
	observed, probeErr := sshidentity.Probe(request.Context(), target, publicKey, 5*time.Second)
	if errors.Is(probeErr, sshidentity.ErrInvalidProbe) {
		httpx.WriteError(writer, http.StatusConflict, "configured SSH endpoint is invalid")
		return
	}
	if errors.Is(request.Context().Err(), context.Canceled) {
		return
	}
	observer, err := os.Hostname()
	if err != nil || observer == "" {
		observer = "api"
	}
	observation := model.AppSSHObservation{
		ID: model.NewID("sshobs"), TenantID: app.TenantID, AppID: app.ID,
		IdentityID: identity.ID, EndpointID: endpoint.ID, RouteDigest: model.AppSSHRouteDigest(endpoint),
		Scope: "public_endpoint", Observer: observer, Target: target,
		Algorithm: observed.Algorithm, Fingerprint: observed.Fingerprint,
		KeyExchangeVerified: observed.KeyExchangeVerified, IdentityMatched: observed.IdentityMatched,
		ObservedAt: observed.ObservedAt, ExpiresAt: observed.ObservedAt.Add(2 * time.Minute), Reason: observed.Reason,
	}
	if err := server.store.RecordAppSSHObservation(observation); err != nil {
		server.writeStoreError(writer, err)
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(writer, http.StatusOK, map[string]any{"identity": identity, "observation": observation})
}

func (server *Server) observedAppSSHStatus(app model.App, endpoint model.AppSSHEndpoint, found bool) model.AppSSHStatus {
	status := server.configuredAppSSHStatus(app, endpoint, found)
	identity, err := server.store.GetAppSSHIdentity(app.TenantID, app.ID)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			status.IdentityState = "unavailable"
			status.Message = "managed SSH identity metadata is unavailable"
		}
		return status
	}
	status.IdentityID = identity.ID
	status.HostKeyFingerprint = identity.Fingerprint
	status.IdentityState = "unobserved"
	observation, err := server.store.LatestAppSSHObservation(app.TenantID, app.ID)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			status.IdentityState = "unavailable"
		}
		return status
	}
	return applyAppSSHObservation(status, endpoint, identity, observation, time.Now().UTC())
}

func (server *Server) configuredAppSSHStatus(app model.App, endpoint model.AppSSHEndpoint, found bool) model.AppSSHStatus {
	status := appSSHStatus(app, endpoint, found)
	if !found {
		return status
	}
	state, err := server.store.GetManagedSSHControl(app.TenantID, app.ID)
	if errors.Is(err, store.ErrNotFound) {
		return status
	}
	if err != nil {
		status.Ready = false
		return status
	}
	if state.Active == nil {
		return status
	}
	binding, err := server.store.GetManagedSSHBinding(app.TenantID, app.ID)
	if err != nil || binding.ID != endpoint.ID || binding.IdentityID != state.IdentityID || endpoint.TargetService != state.Active.ID || endpoint.TargetNamespace != state.Active.Namespace || endpoint.TargetPort != 2222 {
		status.Ready, status.RouteConfigured = false, false
		status.Message = "managed SSH route does not match its committed serving state"
		return status
	}
	intent, err := server.store.GetManagedSSHIntent(app.TenantID, app.ID)
	if err != nil || !intent.Spec.Enabled {
		status.Ready, status.RouteConfigured = false, false
		status.Message = "managed SSH access is disabled or intent is unavailable"
		return status
	}
	app.Spec.SSH = &model.AppSSHSpec{Enabled: true, TargetPort: endpoint.TargetPort, User: endpoint.User}
	return appSSHStatus(app, endpoint, true)
}

func applyAppSSHObservation(status model.AppSSHStatus, endpoint model.AppSSHEndpoint, identity model.AppSSHIdentity, observation model.AppSSHObservation, now time.Time) model.AppSSHStatus {
	status.Ready = false
	status.IdentityVerified = false
	status.Observation = &observation
	if observation.TenantID != identity.TenantID || observation.AppID != identity.AppID || observation.IdentityID != identity.ID || observation.EndpointID != endpoint.ID || observation.RouteDigest != model.AppSSHRouteDigest(endpoint) || observation.Scope != "public_endpoint" || observation.Target != model.AppSSHPublicAddress(endpoint) {
		status.IdentityState = "route_changed"
		return status
	}
	if !observation.ExpiresAt.After(now) || observation.ObservedAt.After(now.Add(5*time.Second)) || observation.ExpiresAt.After(observation.ObservedAt.Add(2*time.Minute)) {
		status.IdentityState = "expired"
		return status
	}
	if !observation.KeyExchangeVerified {
		status.IdentityState = "unreachable"
		return status
	}
	if !observation.IdentityMatched || observation.Fingerprint != identity.Fingerprint || observation.Algorithm != identity.Algorithm || observation.Reason != "identity_verified" {
		status.IdentityState = "mismatch"
		return status
	}
	status.IdentityState = "verified"
	status.IdentityVerified = true
	status.Ready = status.RouteConfigured
	if status.Ready {
		status.Message = "public SSH endpoint host identity verified from the recorded observer; client trust and user authentication are separate"
	}
	return status
}
