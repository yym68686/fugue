package edge

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/routeproof"
	"fugue/internal/tcpdiag"
)

func (s *Service) observeOriginNetwork(observed *edgeProxyObservation, connection net.Conn, now time.Time) {
	if s == nil || observed == nil || connection == nil || !s.networkSampleMu.TryLock() {
		return
	}
	defer s.networkSampleMu.Unlock()
	if len(s.networkSamples) > 0 && now.Sub(s.networkSamples[len(s.networkSamples)-1].ObservedAt) < time.Second {
		return
	}
	for _, previous := range s.networkSamples {
		if previous.Hostname == observed.Route.Hostname && previous.PathPrefix == model.NormalizeAppRoutePathPrefix(observed.Route.PathPrefix) && now.Sub(previous.ObservedAt) < time.Minute {
			return
		}
	}
	if wrapped, ok := connection.(interface{ NetConn() net.Conn }); ok {
		connection = wrapped.NetConn()
		if connection == nil {
			return
		}
	}
	sample, ok := originNetworkSample(observed, connection.RemoteAddr(), tcpdiag.SnapshotFromConn(connection), s.Config.EdgeID, s.Config.EdgeGroupID, now)
	if !ok {
		return
	}
	if len(s.networkSamples) >= 32 {
		copy(s.networkSamples, s.networkSamples[1:])
		s.networkSamples = s.networkSamples[:31]
	}
	s.networkSamples = append(s.networkSamples, sample)
}

func originNetworkSample(observed *edgeProxyObservation, remote net.Addr, network tcpdiag.Snapshot, edgeID, groupID string, now time.Time) (model.EdgeNetworkSample, bool) {
	if observed == nil || observed.PeerFallback || remote == nil || len(observed.Route.Upstreams) > 0 || observed.Route.UpstreamKind != "kubernetes-service" || observed.Route.UpstreamScope != "local-service" {
		return model.EdgeNetworkSample{}, false
	}
	host, _, err := net.SplitHostPort(remote.String())
	address := net.ParseIP(host)
	if err != nil || address == nil || address.IsLoopback() || address.IsUnspecified() || address.IsLinkLocalUnicast() || address.IsMulticast() {
		return model.EdgeNetworkSample{}, false
	}
	upstream, err := url.Parse(observed.Route.UpstreamURL)
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
	digest, err := routeproof.Digest(observed.Route)
	if err != nil {
		return model.EdgeNetworkSample{}, false
	}
	sample := model.EdgeNetworkSample{ID: model.NewID("edge_net"), EdgeID: edgeID, EdgeGroupID: groupID,
		Hostname: normalizeRouteHost(observed.Route.Hostname), PathPrefix: model.NormalizeAppRoutePathPrefix(observed.Route.PathPrefix),
		TrafficClass: "dynamic_api", RouteDigest: digest, BundleVersion: observed.BundleVersion,
		ServiceTarget: net.JoinHostPort(upstream.Hostname(), port), Source: "service_endpoint_tcp_info_v1", ObservedAt: now}
	if observed.Route.Streaming {
		sample.TrafficClass = "streaming"
	}
	if network.Available && network.RTTUsec > 0 {
		value := float64(network.RTTUsec) / 1000
		sample.ServiceRTTMS = &value
	}
	return sample, model.ValidateEdgeNetworkSample(sample) == nil
}

func (s *Service) originNetworkSamples() []model.EdgeNetworkSample {
	s.networkSampleMu.Lock()
	defer s.networkSampleMu.Unlock()
	return append([]model.EdgeNetworkSample(nil), s.networkSamples...)
}

func (s *Service) sendHeartbeatWithOptionalNetworkSamples(request *http.Request) (*http.Response, error) {
	response, err := s.HTTPClient.Do(request)
	if err != nil || response.StatusCode != http.StatusBadRequest || request.GetBody == nil {
		return response, err
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
	response.Body.Close()
	response.Body = io.NopCloser(bytes.NewReader(body))
	var failure struct {
		Error string `json:"error"`
	}
	if readErr != nil || json.Unmarshal(body, &failure) != nil || failure.Error != `json: unknown field "network_samples"` {
		return response, nil
	}
	original, err := request.GetBody()
	if err != nil {
		return response, nil
	}
	defer original.Close()
	var fields map[string]json.RawMessage
	if json.NewDecoder(original).Decode(&fields) != nil {
		return response, nil
	}
	if _, exists := fields["network_samples"]; !exists {
		return response, nil
	}
	delete(fields, "network_samples")
	payload, err := json.Marshal(fields)
	if err != nil {
		return response, nil
	}
	retry := request.Clone(request.Context())
	retry.Body = io.NopCloser(bytes.NewReader(payload))
	retry.ContentLength = int64(len(payload))
	retry.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(payload)), nil }
	return s.HTTPClient.Do(retry)
}
