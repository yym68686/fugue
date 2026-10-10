package edge

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/routeproof"
	"fugue/internal/tcpdiag"
)

func networkTestObservation() edgeProxyObservation {
	return edgeProxyObservation{BundleVersion: "bundle-one", Duration: time.Hour, TTFB: time.Minute,
		Route: model.EdgeRouteBinding{Hostname: "app.example.test", PathPrefix: "/", Streaming: true,
			UpstreamKind: "kubernetes-service", UpstreamScope: "local-service", UpstreamURL: "http://app.tenant.svc.cluster.local:3000"}}
}

func TestOriginNetworkSampleSeparatesSocketRTTFromBusinessDuration(t *testing.T) {
	observed := networkTestObservation()
	now := time.Now().UTC()
	remote := &net.TCPAddr{IP: net.ParseIP("10.43.0.20"), Port: 3000}
	sample, ok := originNetworkSample(&observed, remote, tcpdiag.Snapshot{Available: true, RTTUsec: 800}, "edge-a", "group-a", now)
	if !ok || sample.ServiceRTTMS == nil || *sample.ServiceRTTMS != 0.8 || sample.TrafficClass != "streaming" || !sample.ObservedAt.Equal(now) {
		t.Fatal(sample, ok)
	}
	wantDigest, err := routeproof.Digest(observed.Route)
	if err != nil || sample.RouteDigest != wantDigest || sample.BundleVersion != observed.BundleVersion || sample.ServiceTarget != "app.tenant.svc.cluster.local:3000" {
		t.Fatal("route identity or destination lost", sample, err)
	}
	observed.Duration *= 100
	observed.TTFB *= 100
	next, ok := originNetworkSample(&observed, remote, tcpdiag.Snapshot{Available: true, RTTUsec: 800}, "edge-a", "group-a", now)
	if !ok || *next.ServiceRTTMS != *sample.ServiceRTTMS {
		t.Fatal("application wait changed network observation", next)
	}
	unknown, ok := originNetworkSample(&observed, remote, tcpdiag.Snapshot{}, "edge-a", "group-a", now)
	if !ok || unknown.ServiceRTTMS != nil {
		t.Fatal("unavailable TCP_INFO became zero RTT", unknown)
	}
}

func TestOriginNetworkSampleRejectsUnprovedTransport(t *testing.T) {
	for _, mutation := range []func(*edgeProxyObservation){
		func(observed *edgeProxyObservation) { observed.PeerFallback = true },
		func(observed *edgeProxyObservation) { observed.Route.UpstreamKind = "http-proxy" },
		func(observed *edgeProxyObservation) { observed.Route.UpstreamScope = "tunnel" },
		func(observed *edgeProxyObservation) {
			observed.Route.UpstreamURL = "http://user:secret@app.tenant.svc.cluster.local"
		},
		func(observed *edgeProxyObservation) { observed.Route.UpstreamURL = "http://proxy.example.test" },
		func(observed *edgeProxyObservation) {
			observed.Route.Upstreams = []model.EdgeRouteUpstream{{UpstreamURL: "http://canary"}}
		},
	} {
		observed := networkTestObservation()
		mutation(&observed)
		if sample, ok := originNetworkSample(&observed, &net.TCPAddr{IP: net.ParseIP("10.43.0.20"), Port: 3000}, tcpdiag.Snapshot{Available: true, RTTUsec: 1}, "edge-a", "group-a", time.Now()); ok {
			t.Fatal("unproved origin admitted", sample)
		}
	}
	for _, address := range []string{"127.0.0.1", "::1", "0.0.0.0", "169.254.1.2", "224.0.0.1"} {
		observed := networkTestObservation()
		if _, ok := originNetworkSample(&observed, &net.TCPAddr{IP: net.ParseIP(address), Port: 3000}, tcpdiag.Snapshot{Available: true, RTTUsec: 1}, "edge-a", "group-a", time.Now()); ok {
			t.Fatal("proxy or invalid hop accepted", address)
		}
	}
}

func TestOriginSamplingAcceptsOnlyUnambiguousConfiguredRelease(t *testing.T) {
	observed := networkTestObservation()
	observed.Route.Upstreams = []model.EdgeRouteUpstream{{UpstreamURL: observed.Route.UpstreamURL, UpstreamKind: observed.Route.UpstreamKind, UpstreamScope: observed.Route.UpstreamScope, Weight: 100, Status: model.EdgeRouteStatusActive}}
	remote := &net.TCPAddr{IP: net.ParseIP("10.43.0.20"), Port: 3000}
	sample, ok := originNetworkSample(&observed, remote, tcpdiag.Snapshot{Available: true, RTTUsec: 800}, "edge-a", "group-a", time.Now())
	if !ok || sample.ServiceRTTMS == nil || *sample.ServiceRTTMS != 0.8 {
		t.Fatal("single stable release lost network evidence", sample, ok)
	}
	observed.Route.Upstreams = append(observed.Route.Upstreams, observed.Route.Upstreams[0])
	if _, ok := originNetworkSample(&observed, remote, tcpdiag.Snapshot{Available: true, RTTUsec: 800}, "edge-a", "group-a", time.Now()); ok {
		t.Fatal("ambiguous release network treated as one origin")
	}
}

func TestOriginNetworkQueueNonblockingAndBounded(t *testing.T) {
	service := &Service{}
	service.Config.EdgeID, service.Config.EdgeGroupID = "edge-a", "group-a"
	observed := networkTestObservation()
	connection := &networkTestConn{remote: &net.TCPAddr{IP: net.ParseIP("10.43.0.20"), Port: 3000}}
	now := time.Now().UTC()
	service.networkSampleMu.Lock()
	service.observeOriginNetwork(&observed, connection, now)
	service.networkSampleMu.Unlock()
	if len(service.originNetworkSamples()) != 0 {
		t.Fatal("contended sampler performed work")
	}
	service.observeOriginNetwork(&observed, connection, now)
	service.observeOriginNetwork(&observed, connection, now.Add(time.Second))
	if len(service.originNetworkSamples()) != 1 {
		t.Fatal("same route bypassed interval")
	}
	for index := 1; index <= 50; index++ {
		service.observeOriginNetwork(&observed, connection, now.Add(time.Duration(index)*time.Minute))
	}
	samples := service.originNetworkSamples()
	if len(samples) != 32 || !samples[31].ObservedAt.Equal(now.Add(50*time.Minute)) {
		t.Fatal("unbounded or stale queue", samples)
	}
	samples[0].ID = "modified"
	if strings.Contains(service.originNetworkSamples()[0].ID, "modified") {
		t.Fatal("caller changed queue backing array")
	}
}

type networkTestConn struct {
	net.Conn
	remote net.Addr
}

func (connection *networkTestConn) RemoteAddr() net.Addr { return connection.remote }

func TestNetworkExtensionDoesNotBreakHeartbeatAgainstOldAPI(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		attempts++
		var body map[string]json.RawMessage
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if string(body["edge_id"]) != `"edge-a"` || string(body["performance_samples"]) != `[{"id":"ordinary-sample"}]` || request.Header.Get(edgeServingActiveHeader) != "true" || request.URL.Query().Get("token") != "test-token" {
			t.Error("normal heartbeat identity or performance data changed", body)
		}
		if _, exists := body["network_samples"]; exists {
			writer.WriteHeader(http.StatusBadRequest)
			io.WriteString(writer, `{"error":"json: unknown field \"network_samples\""}`)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	service := &Service{HTTPClient: server.Client()}
	payload := []byte(`{"edge_id":"edge-a","performance_samples":[{"id":"ordinary-sample"}],"network_samples":[{"id":"network-sample"}]}`)
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"?token=test-token", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(edgeServingActiveHeader, "true")
	response, err := service.sendHeartbeatWithOptionalNetworkSamples(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || attempts != 2 {
		t.Fatal("old API compatibility failed", response.StatusCode, attempts)
	}
}

func TestProbeExtensionRetriesOnlyItsActualUnsupportedField(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		attempts++
		var body map[string]json.RawMessage
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if string(body["edge_id"]) != `"edge-a"` {
			t.Fatal("ordinary identity lost")
		}
		if _, exists := body["network_samples"]; exists {
			writer.WriteHeader(http.StatusBadRequest)
			io.WriteString(writer, `{"error":"json: unknown field \"service_connect_failed\""}`)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	service := &Service{HTTPClient: server.Client()}
	request, err := http.NewRequest(http.MethodPost, server.URL, strings.NewReader(`{"edge_id":"edge-a","network_samples":[{"service_connect_failed":false}]}`))
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.sendHeartbeatWithOptionalNetworkSamples(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || attempts != 2 {
		t.Fatal("old API compatibility lost", response.StatusCode, attempts)
	}
}

func TestNetworkHeartbeatFallbackOnlyRetriesExactUnsupportedField(t *testing.T) {
	for _, test := range []struct {
		status int
		body   string
	}{
		{http.StatusForbidden, `{"error":"json: unknown field \"network_samples\""}`},
		{http.StatusBadRequest, `{"error":"invalid edge identity"}`},
		{http.StatusBadRequest, `{"error":"json: unknown field \"client_network\""}`},
		{http.StatusBadRequest, `{"error":"json: unknown field \"service_connect_failed\""}`},
		{http.StatusInternalServerError, `{"error":"database unavailable"}`},
	} {
		attempts := 0
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			attempts++
			writer.WriteHeader(test.status)
			io.WriteString(writer, test.body)
		}))
		service := &Service{HTTPClient: server.Client()}
		request, err := http.NewRequest(http.MethodPost, server.URL, bytes.NewReader([]byte(`{"network_samples":[]}`)))
		if err != nil {
			t.Fatal(err)
		}
		response, err := service.sendHeartbeatWithOptionalNetworkSamples(request)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		server.Close()
		if err != nil || attempts != 1 || response.StatusCode != test.status || string(body) != test.body {
			t.Fatal("unrelated failure retried or hidden", attempts, string(body), err)
		}
	}
}

func TestClientNetworkExtensionDoesNotBreakHeartbeatAgainstOriginOnlyAPI(t *testing.T) {
	for _, field := range []string{"client_network", "backend"} {
		t.Run(field, func(t *testing.T) {
			attempts := 0
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				attempts++
				var fields map[string]json.RawMessage
				if err := json.NewDecoder(request.Body).Decode(&fields); err != nil {
					t.Error(err)
				}
				if string(fields["edge_id"]) != `"edge-a"` || string(fields["performance_samples"]) != `[{"id":"ordinary-sample"}]` {
					t.Error("normal heartbeat fields changed")
				}
				if _, exists := fields["network_samples"]; exists {
					writer.WriteHeader(http.StatusBadRequest)
					json.NewEncoder(writer).Encode(map[string]string{"error": `json: unknown field "` + field + `"`})
					return
				}
				writer.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			service := &Service{HTTPClient: server.Client()}
			payload := []byte(`{"edge_id":"edge-a","performance_samples":[{"id":"ordinary-sample"}],"network_samples":[{"id":"network-sample","client_network":{}}]}`)
			request, err := http.NewRequest(http.MethodPost, server.URL, bytes.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			response, err := service.sendHeartbeatWithOptionalNetworkSamples(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK || attempts != 2 {
				t.Fatal("origin-only API compatibility failed", response.StatusCode, attempts)
			}
		})
	}
}
