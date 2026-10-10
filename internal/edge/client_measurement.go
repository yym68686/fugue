package edge

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/clientmeasurement"
	"fugue/internal/frontnetwork"
	"fugue/internal/model"
	"fugue/internal/routeproof"
)

func (s *Service) reserveClientMeasurement(permit model.EdgeClientProbePermit, now time.Time) bool {
	if !s.clientProbeMu.TryLock() {
		return false
	}
	defer s.clientProbeMu.Unlock()
	if now.Sub(s.clientProbeLast) < 10*time.Second {
		return false
	}
	if s.clientProbePermits == nil {
		s.clientProbePermits = map[string]time.Time{}
	}
	for id, expiry := range s.clientProbePermits {
		if !expiry.After(now) {
			delete(s.clientProbePermits, id)
		}
	}
	if _, seen := s.clientProbePermits[permit.AttemptID]; seen || len(s.clientProbePermits) >= 128 {
		return false
	}
	s.clientProbePermits[permit.AttemptID] = permit.ExpiresAt
	s.clientProbeLast = now
	return true
}

func (s *Service) handleClientMeasurement(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store, no-transform")
	writer.Header().Set("Content-Type", "application/octet-stream")
	if request.Method != http.MethodGet || len(request.Header.Values(clientmeasurement.RequestHeader)) != 1 || request.URL.RawQuery != "" || !edgeRemoteAddressIsLoopback(request.RemoteAddr) || !s.Config.CaddyProxyProtocolEnabled || s.Config.APIURL == "" || s.Config.EdgeToken == "" {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	token := request.Header.Get(clientmeasurement.RequestHeader)
	if len(token) > 8192 {
		writer.WriteHeader(http.StatusRequestHeaderFieldsTooLarge)
		return
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	var permit model.EdgeClientProbePermit
	keys := bundleauth.NewKeyring(s.Config.BundleSigningKey, s.Config.BundleSigningKeyID, s.Config.BundleSigningPreviousKey, s.Config.BundleSigningPreviousKeyID, s.Config.BundleRevokedKeyIDs)
	now := time.Now().UTC()
	if err != nil || json.Unmarshal(raw, &permit) != nil || clientmeasurement.VerifyPermit(permit, keys, now) != nil || permit.Hostname != normalizeRouteHost(request.Host) || permit.Path != request.URL.Path || permit.EdgeID != s.Config.EdgeID || permit.EdgeGroupID != s.Config.EdgeGroupID {
		writer.WriteHeader(http.StatusForbidden)
		return
	}
	index := s.currentRouteIndex()
	route, found, fallback, version, _ := index.routeForRequest(permit.Hostname, permit.Path)
	digest, digestErr := routeproof.Digest(route)
	class := "dynamic_api"
	if route.Streaming {
		class = "streaming"
	}
	if !found || fallback || route.Status != model.EdgeRouteStatusActive || !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) || slices.Contains(route.ExcludedEdgeIDs, s.Config.EdgeID) || slices.Contains(route.ExcludedEdgeGroupIDs, s.Config.EdgeGroupID) ||
		digestErr != nil || digest != permit.RouteDigest || version != permit.BundleVersion || class != permit.TrafficClass || !s.appTrafficProofApplied(index) {
		writer.WriteHeader(http.StatusConflict)
		return
	}
	if !s.reserveClientMeasurement(permit, now) {
		writer.WriteHeader(http.StatusTooManyRequests)
		return
	}
	remote := request.Header.Get(edgeClientRemoteAddrHeader)
	ctx, cancel := context.WithTimeout(request.Context(), 2500*time.Millisecond)
	defer cancel()
	observed, err := frontnetwork.ReadAPI(ctx, s.HTTPClient, s.Config.APIURL, s.Config.EdgeToken, permit.EdgeID, permit.EdgeGroupID, s.Config.EdgeSlot, remote)
	if err != nil || observed.Sample.Backend == nil || observed.Sample.RTTMS == nil || !observed.Sample.TCPInfoAvailable || s.currentRouteIndex() != index || !s.appTrafficProofApplied(index) {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	attestation := model.EdgeClientProbeAttestation{Schema: "fugue.client-probe-attestation/v1", AttemptID: permit.AttemptID, EdgeID: permit.EdgeID, EdgeGroupID: permit.EdgeGroupID,
		RouteDigest: digest, BundleVersion: version, ObservedAt: observed.Sample.ObservedAt, ClientNetwork: observed.Sample}
	attestation, err = clientmeasurement.SignAttestation(attestation, keys)
	if err != nil || clientmeasurement.ValidateAttestation(attestation, permit) != nil {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	body := clientmeasurement.Payload(permit.BodySeed)
	if clientmeasurement.Digest(body) != permit.BodySHA256 {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	encoded, err := json.Marshal(attestation)
	if err != nil {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	writer.Header().Set(clientmeasurement.AttestationHeader, base64.RawURLEncoding.EncodeToString(encoded))
	writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
	controller := http.NewResponseController(writer)
	_ = controller.SetWriteDeadline(time.Now().Add(90 * time.Second))
	defer controller.SetWriteDeadline(time.Time{})
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(body)
}
