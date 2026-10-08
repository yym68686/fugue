package edge

import (
	"context"
	"net/http"
	"time"

	"fugue/internal/frontnetwork"
	"fugue/internal/model"
	"fugue/internal/routeproof"
)

func (s *Service) observePublicClientNetwork(request *http.Request, route model.EdgeRouteBinding, bundleVersion string, now time.Time) {
	reject := func(reason string) {
		if s == nil {
			return
		}
		s.frontNetworkDiagMu.Lock()
		if s.frontNetworkDiag.Rejections == nil {
			s.frontNetworkDiag.Rejections = map[string]uint64{}
		}
		s.frontNetworkDiag.Rejections[reason]++
		s.frontNetworkDiagMu.Unlock()
	}
	if s == nil || request == nil {
		reject("missing_request")
		return
	}
	if s.Config.FrontNetworkSocket == "" {
		reject("socket_disabled")
		return
	}
	if !s.Config.CaddyEnabled {
		reject("caddy_disabled")
		return
	}
	if !s.Config.CaddyProxyProtocolEnabled {
		reject("proxy_protocol_disabled")
		return
	}
	if !edgeRemoteAddressIsLoopback(request.RemoteAddr) {
		reject("worker_peer_not_loopback")
		return
	}
	if bundleVersion == "" {
		reject("bundle_missing")
		return
	}
	remote := request.Header.Get(edgeClientRemoteAddrHeader)
	if remote == "" {
		reject("client_remote_header_missing")
		return
	}
	if now.UnixNano()-s.frontNetworkLast.Load() < int64(time.Second) {
		reject("rate_limited")
		return
	}
	if !s.frontNetworkInFlight.CompareAndSwap(false, true) {
		reject("in_flight")
		return
	}
	if _, err := frontnetwork.ParseRemote(remote); err != nil {
		reject("client_remote_invalid")
		s.frontNetworkInFlight.Store(false)
		return
	}
	if !s.networkSampleMu.TryLock() {
		reject("sample_lock_busy")
		s.frontNetworkInFlight.Store(false)
		return
	}
	for _, previous := range s.networkSamples {
		if previous.Source == "public_front_tcp_info_v1" && previous.Hostname == route.Hostname && previous.PathPrefix == model.NormalizeAppRoutePathPrefix(route.PathPrefix) && now.Sub(previous.ObservedAt) < time.Minute {
			reject("same_route_recent")
			s.networkSampleMu.Unlock()
			s.frontNetworkInFlight.Store(false)
			return
		}
	}
	s.networkSampleMu.Unlock()
	digest, err := routeproof.Digest(route)
	if err != nil {
		reject("route_digest_failed")
		s.frontNetworkInFlight.Store(false)
		return
	}
	s.frontNetworkLast.Store(now.UnixNano())
	sample := model.EdgeNetworkSample{ID: model.NewID("edge_client_net"), EdgeID: s.Config.EdgeID, EdgeGroupID: s.Config.EdgeGroupID,
		Hostname: normalizeRouteHost(route.Hostname), PathPrefix: model.NormalizeAppRoutePathPrefix(route.PathPrefix), TrafficClass: "dynamic_api",
		RouteDigest: digest, BundleVersion: bundleVersion, Source: "public_front_tcp_info_v1"}
	if route.Streaming {
		sample.TrafficClass = "streaming"
	}
	socketPath, slot := s.Config.FrontNetworkSocket, s.Config.EdgeSlot
	go func() {
		defer s.frontNetworkInFlight.Store(false)
		result, err := frontnetwork.Read(context.Background(), socketPath, sample.EdgeID, sample.EdgeGroupID, slot, remote)
		if err != nil {
			reject("front_" + frontnetwork.FailureReason(err))
			return
		}
		if result.Sample.StartedAt.After(now) {
			reject("sample_started_in_future")
			return
		}
		sample.ClientNetwork, sample.ObservedAt = &result.Sample, result.Sample.ObservedAt
		if model.ValidateEdgeNetworkSample(sample) != nil {
			reject("sample_invalid")
			return
		}
		if !s.networkSampleMu.TryLock() {
			reject("sample_lock_busy_after_read")
			return
		}
		defer s.networkSampleMu.Unlock()
		s.appendNetworkSampleLocked(sample)
		s.frontNetworkDiagMu.Lock()
		s.frontNetworkDiag.Successes++
		s.frontNetworkDiag.LastSuccessAt = networkObservationTime(sample.ObservedAt)
		s.frontNetworkDiagMu.Unlock()
	}()
	s.frontNetworkDiagMu.Lock()
	s.frontNetworkDiag.Attempts++
	s.frontNetworkDiag.LastAttemptAt = networkObservationTime(now)
	s.frontNetworkDiagMu.Unlock()
}

func networkObservationTime(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}
