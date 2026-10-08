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
	if s == nil || request == nil || s.Config.FrontNetworkSocket == "" || !s.Config.CaddyEnabled || !s.Config.CaddyProxyProtocolEnabled ||
		!edgeRemoteAddressIsLoopback(request.RemoteAddr) || bundleVersion == "" || request.Header.Get(edgeClientRemoteAddrHeader) == "" ||
		now.UnixNano()-s.frontNetworkLast.Load() < int64(time.Second) || !s.frontNetworkInFlight.CompareAndSwap(false, true) {
		return
	}
	remote := request.Header.Get(edgeClientRemoteAddrHeader)
	if _, err := frontnetwork.ParseRemote(remote); err != nil {
		s.frontNetworkInFlight.Store(false)
		return
	}
	if !s.networkSampleMu.TryLock() {
		s.frontNetworkInFlight.Store(false)
		return
	}
	for _, previous := range s.networkSamples {
		if previous.Source == "public_front_tcp_info_v1" && previous.Hostname == route.Hostname && previous.PathPrefix == model.NormalizeAppRoutePathPrefix(route.PathPrefix) && now.Sub(previous.ObservedAt) < time.Minute {
			s.networkSampleMu.Unlock()
			s.frontNetworkInFlight.Store(false)
			return
		}
	}
	s.networkSampleMu.Unlock()
	digest, err := routeproof.Digest(route)
	if err != nil {
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
		if err != nil || result.Sample.StartedAt.After(now) {
			return
		}
		sample.ClientNetwork, sample.ObservedAt = &result.Sample, result.Sample.ObservedAt
		if model.ValidateEdgeNetworkSample(sample) != nil || !s.networkSampleMu.TryLock() {
			return
		}
		defer s.networkSampleMu.Unlock()
		s.appendNetworkSampleLocked(sample)
	}()
}
