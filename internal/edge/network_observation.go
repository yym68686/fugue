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
	for _, previous := range s.networkSamples {
		if previous.Source == "service_endpoint_tcp_info_v1" && (now.Sub(previous.ObservedAt) < time.Second ||
			(previous.Hostname == observed.Route.Hostname && previous.PathPrefix == model.NormalizeAppRoutePathPrefix(observed.Route.PathPrefix) && now.Sub(previous.ObservedAt) < time.Minute)) {
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
	s.appendNetworkSampleLocked(sample)
}

func (s *Service) appendNetworkSampleLocked(sample model.EdgeNetworkSample) {
	if len(s.networkSamples) >= 32 {
		oldest, count := -1, 0
		for index, previous := range s.networkSamples {
			if previous.Source == sample.Source {
				if oldest < 0 {
					oldest = index
				}
				count++
			}
		}
		if count < 16 {
			for index, previous := range s.networkSamples {
				if previous.Source != sample.Source {
					oldest = index
					break
				}
			}
		}
		if oldest < 0 {
			oldest = 0
		}
		s.networkSamples = append(s.networkSamples[:oldest], s.networkSamples[oldest+1:]...)
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
	if readErr != nil || json.Unmarshal(body, &failure) != nil || (failure.Error != `json: unknown field "network_samples"` && failure.Error != `json: unknown field "client_network"` && failure.Error != `json: unknown field "backend"` && failure.Error != `json: unknown field "service_connect_failed"`) {
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
	if failure.Error == `json: unknown field "client_network"` || failure.Error == `json: unknown field "service_connect_failed"` {
		unsupported := "client_network"
		if failure.Error == `json: unknown field "service_connect_failed"` {
			unsupported = "service_connect_failed"
		}
		var samples []map[string]json.RawMessage
		if json.Unmarshal(fields["network_samples"], &samples) != nil {
			return response, nil
		}
		found := false
		for _, sample := range samples {
			if _, exists := sample[unsupported]; exists {
				found = true
			}
		}
		if !found {
			return response, nil
		}
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
