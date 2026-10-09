package edge

import (
	"context"
	"net"
	"net/url"
	"slices"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/routeproof"
	"fugue/internal/tcpdiag"
)

func (s *Service) originNetworkProbeEnabled() bool {
	return s.Config.OriginNetworkProbeInterval >= 30*time.Second && s.Config.OriginNetworkProbeInterval <= 5*time.Minute &&
		len(s.Config.OriginNetworkProbeHostnames) > 0 && len(s.Config.OriginNetworkProbeHostnames) <= 8
}

func (s *Service) runOriginNetworkProbes(ctx context.Context) {
	if !s.originNetworkProbeEnabled() {
		return
	}
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	ticker := time.NewTicker(s.Config.OriginNetworkProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case scheduledAt := <-ticker.C:
			s.probeOriginNetworkOnce(ctx, scheduledAt.UTC(), dialer.DialContext, tcpdiag.SnapshotFromConn)
		}
	}
}

func (s *Service) probeOriginNetworkOnce(ctx context.Context, now time.Time, dial func(context.Context, string, string) (net.Conn, error), inspect func(net.Conn) tcpdiag.Snapshot) {
	if !s.originNetworkProbeEnabled() || ctx.Err() != nil || !s.originNetworkProbeMu.TryLock() {
		return
	}
	defer s.originNetworkProbeMu.Unlock()
	if !s.originNetworkProbeLast.IsZero() && now.Sub(s.originNetworkProbeLast) < s.Config.OriginNetworkProbeInterval {
		return
	}
	index := s.currentRouteIndex()
	active, known := s.servingActiveForHeartbeat()
	if !active || !known || !s.appTrafficProofApplied(index) {
		return
	}
	targets := map[string]model.EdgeNetworkSample{}
	for _, hostname := range s.Config.OriginNetworkProbeHostnames {
		for _, indexed := range index.byHost[normalizeRouteHost(hostname)] {
			route, ok, fallback, _, _ := index.routeForRequest(hostname, indexed.pathPrefix)
			if !ok || fallback {
				continue
			}
			if sample, ok := originNetworkProbeTarget(route, index.bundleVersion, s.Config.EdgeID, s.Config.EdgeGroupID, now); ok {
				targets[sample.Hostname+"\x00"+sample.PathPrefix] = sample
			}
		}
	}
	keys := make([]string, 0, len(targets))
	for key := range targets {
		keys = append(keys, key)
	}
	if len(keys) == 0 || len(keys) > 64 {
		return
	}
	sort.Strings(keys)
	key := keys[0]
	for _, next := range keys {
		if next > s.originNetworkProbeCursor {
			key = next
			break
		}
	}
	s.originNetworkProbeCursor, s.originNetworkProbeLast = key, now
	sample := targets[key]
	probeContext, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	connection, err := dial(probeContext, "tcp", sample.ServiceTarget)
	if connection != nil {
		defer connection.Close()
	}
	if ctx.Err() != nil {
		return
	}
	failed := err != nil || connection == nil
	sample.ServiceConnectFailed = &failed
	sample.ObservedAt = time.Now().UTC()
	if !failed {
		observation := edgeProxyObservation{BundleVersion: sample.BundleVersion}
		route, ok, fallback, version, _ := index.routeForRequest(sample.Hostname, sample.PathPrefix)
		if !ok || fallback || version != sample.BundleVersion {
			return
		}
		observation.Route = route
		measured, ok := originNetworkSample(&observation, connection.RemoteAddr(), inspect(connection), s.Config.EdgeID, s.Config.EdgeGroupID, sample.ObservedAt)
		if !ok {
			return
		}
		sample.ServiceRTTMS = measured.ServiceRTTMS
	}
	active, known = s.servingActiveForHeartbeat()
	if !active || !known || s.currentRouteIndex() != index || !s.appTrafficProofApplied(index) || model.ValidateEdgeNetworkSample(sample) != nil {
		return
	}
	if s.networkSampleMu.TryLock() {
		s.appendNetworkSampleLocked(sample)
		s.networkSampleMu.Unlock()
	}
}

func originNetworkProbeTarget(route model.EdgeRouteBinding, version, edgeID, groupID string, now time.Time) (model.EdgeNetworkSample, bool) {
	if route.Status != model.EdgeRouteStatusActive || !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) || !routeMatchesCurrentEdgeGroup(route, groupID) ||
		slices.Contains(route.ExcludedEdgeIDs, edgeID) || slices.Contains(route.ExcludedEdgeGroupIDs, groupID) || len(route.Upstreams) != 0 ||
		route.UpstreamKind != model.EdgeRouteUpstreamKindKubernetesService || route.UpstreamScope != model.EdgeRouteUpstreamScopeLocalService {
		return model.EdgeNetworkSample{}, false
	}
	upstream, err := url.Parse(route.UpstreamURL)
	if err != nil || upstream.User != nil || (upstream.Scheme != "http" && upstream.Scheme != "https") || !strings.HasSuffix(upstream.Hostname(), ".svc.cluster.local") {
		return model.EdgeNetworkSample{}, false
	}
	port := upstream.Port()
	if port == "" {
		port = "80"
		if upstream.Scheme == "https" {
			port = "443"
		}
	}
	digest, err := routeproof.Digest(route)
	if err != nil {
		return model.EdgeNetworkSample{}, false
	}
	failed := false
	sample := model.EdgeNetworkSample{ID: model.NewID("edge_origin_probe"), EdgeID: edgeID, EdgeGroupID: groupID,
		Hostname: normalizeRouteHost(route.Hostname), PathPrefix: model.NormalizeAppRoutePathPrefix(route.PathPrefix), TrafficClass: "dynamic_api",
		RouteDigest: digest, BundleVersion: version, ServiceTarget: net.JoinHostPort(upstream.Hostname(), port),
		Source: "service_endpoint_tcp_probe_v1", ServiceConnectFailed: &failed, ObservedAt: now}
	if route.Streaming {
		sample.TrafficClass = "streaming"
	}
	return sample, model.ValidateEdgeNetworkSample(sample) == nil
}
