package edge

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/frontnetwork"
	"fugue/internal/model"
)

func TestPublicClientSamplerNeverBlocksBusinessAndBindsExactRequest(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "edge-client-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	path := filepath.Join(directory, "network.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		var query frontnetwork.Request
		if json.NewDecoder(request.Body).Decode(&query) != nil || query.EdgeID != "edge-a" || query.GroupID != "group-a" || query.Slot != "b" || query.RemoteAddr != "203.0.113.1:41000" {
			t.Error("request connection identity lost")
			return
		}
		close(entered)
		<-release
		now := time.Now().UTC()
		rtt := 180.5
		json.NewEncoder(writer).Encode(frontnetwork.Response{Schema: frontnetwork.Schema, Nonce: query.Nonce, EdgeID: query.EdgeID, GroupID: query.GroupID,
			Sample: model.EdgeClientNetworkSample{ConnectionID: "connection-a", Slot: "b", Scope: "tcp_peer:203.0.113.0/24", StartedAt: now.Add(-time.Minute), ObservedAt: now, TCPInfoAvailable: true, RTTMS: &rtt}})
	})}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	service := &Service{Config: config.EdgeConfig{EdgeID: "edge-a", EdgeGroupID: "group-a", EdgeSlot: "b", CaddyEnabled: true, CaddyProxyProtocolEnabled: true, FrontNetworkSocket: path}}
	request := httptest.NewRequest(http.MethodGet, "https://app.example.test/", nil)
	request.RemoteAddr = "127.0.0.1:32000"
	request.Header.Set(edgeClientRemoteAddrHeader, "203.0.113.1:41000")
	observation := networkTestObservation()
	service.observePublicClientNetwork(request, observation.Route, observation.BundleVersion, time.Now())
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("sampler did not run independently")
	}
	service.observePublicClientNetwork(request, observation.Route, observation.BundleVersion, time.Now().Add(time.Second))
	close(release)
	deadline := time.Now().Add(time.Second)
	for service.frontNetworkInFlight.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	samples := service.originNetworkSamples()
	if calls.Load() != 1 || len(samples) != 1 || samples[0].Source != "public_front_tcp_info_v1" || samples[0].ClientNetwork == nil || *samples[0].ClientNetwork.RTTMS != 180.5 || samples[0].ServiceRTTMS != nil || samples[0].ServiceTarget != "" || samples[0].BundleVersion != observation.BundleVersion {
		t.Fatal(calls.Load(), samples)
	}
	raw, _ := json.Marshal(samples)
	if strings.Contains(string(raw), "203.0.113.1:") {
		t.Fatal("durable sample retained peer endpoint")
	}
	service.observePublicClientNetwork(request, observation.Route, observation.BundleVersion, time.Now().Add(2*time.Second))
	if service.frontNetworkInFlight.Load() || calls.Load() != 1 {
		t.Fatal("same route bypassed passive sample interval")
	}
	status := service.frontNetworkObservationStatus()
	if status.Attempts != 1 || status.Successes != 1 || status.LastAttemptAt == nil || status.LastSuccessAt == nil || status.Rejections["in_flight"] != 1 || status.Rejections["same_route_recent"] != 1 {
		t.Fatal("passive observation accounting incomplete", status)
	}
	status.Rejections["same_route_recent"] = 99
	if service.frontNetworkObservationStatus().Rejections["same_route_recent"] != 1 {
		t.Fatal("diagnostic snapshot aliased mutable state")
	}
}

func TestPublicClientSamplerRejectsUntrustedHeadersAndDisabledConfiguration(t *testing.T) {
	for _, test := range []struct {
		name   string
		reason string
		edit   func(*Service, *http.Request)
	}{
		{"external", "worker_peer_not_loopback", func(_ *Service, request *http.Request) { request.RemoteAddr = "203.0.113.2:80" }},
		{"absent_header", "client_remote_header_missing", func(_ *Service, request *http.Request) { request.Header.Del(edgeClientRemoteAddrHeader) }},
		{"loopback_peer", "client_remote_invalid", func(_ *Service, request *http.Request) {
			request.Header.Set(edgeClientRemoteAddrHeader, "127.0.0.1:80")
		}},
		{"disabled", "socket_disabled", func(service *Service, _ *http.Request) { service.Config.FrontNetworkSocket = "" }},
		{"no_caddy", "caddy_disabled", func(service *Service, _ *http.Request) { service.Config.CaddyEnabled = false }},
		{"no_proxy_protocol", "proxy_protocol_disabled", func(service *Service, _ *http.Request) { service.Config.CaddyProxyProtocolEnabled = false }},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &Service{Config: config.EdgeConfig{EdgeID: "edge-a", EdgeGroupID: "group-a", EdgeSlot: "b", CaddyEnabled: true, CaddyProxyProtocolEnabled: true, FrontNetworkSocket: "/tmp/does-not-exist.sock"}}
			request := httptest.NewRequest(http.MethodGet, "https://app.example.test", nil)
			request.RemoteAddr = "127.0.0.1:1234"
			request.Header.Set(edgeClientRemoteAddrHeader, "203.0.113.1:443")
			test.edit(service, request)
			observation := networkTestObservation()
			service.observePublicClientNetwork(request, observation.Route, observation.BundleVersion, time.Now())
			if service.frontNetworkInFlight.Load() || service.frontNetworkLast.Load() != 0 || len(service.originNetworkSamples()) != 0 {
				t.Fatal("untrusted request entered observation queue")
			}
			if status := service.frontNetworkObservationStatus(); status.Attempts != 0 || status.Successes != 0 || status.Rejections[test.reason] != 1 {
				t.Fatal("missing exact rejection reason", status)
			}
		})
	}
}

func TestNetworkQueueRetainsBothSegmentsUnderOneSidedTraffic(t *testing.T) {
	service := &Service{}
	for index := 0; index < 32; index++ {
		service.appendNetworkSampleLocked(model.EdgeNetworkSample{Source: "service_endpoint_tcp_info_v1"})
	}
	for index := 0; index < 100; index++ {
		service.appendNetworkSampleLocked(model.EdgeNetworkSample{Source: "public_front_tcp_info_v1"})
	}
	counts := map[string]int{}
	for _, sample := range service.originNetworkSamples() {
		counts[sample.Source]++
	}
	if counts["service_endpoint_tcp_info_v1"] != 16 || counts["public_front_tcp_info_v1"] != 16 {
		t.Fatal(counts)
	}
}

func TestDeliveryPairRejectsOtherConnectionOrReplacedFront(t *testing.T) {
	now := time.Now().UTC()
	baseline := model.EdgeClientNetworkSample{ConnectionID: "connection-a", Slot: "b", Scope: "tcp_peer:203.0.113.0/24", StartedAt: now.Add(-time.Minute), ObservedAt: now.Add(-time.Second), TCPInfoAvailable: true,
		Delivery: &model.EdgeClientDeliveryCounters{ObservedAt: now.Add(-time.Second), BytesAcked: 1000, BusyMicroseconds: 100000}}
	current := baseline
	current.ObservedAt = now
	current.Delivery = &model.EdgeClientDeliveryCounters{ObservedAt: now, BytesAcked: 101000, BusyMicroseconds: 600000, DataSegmentsOut: 100, DeliveryRateBytesPerSecond: 200000}
	paired, ok := pairPublicDelivery(baseline, current)
	if !ok || paired.DeliveryBaseline == nil || baseline.DeliveryBaseline != nil || current.DeliveryBaseline != nil {
		t.Fatal("pair mutated its input or lost counters", paired, ok)
	}
	for _, edit := range []func(*model.EdgeClientNetworkSample){
		func(value *model.EdgeClientNetworkSample) { value.ConnectionID = "other" },
		func(value *model.EdgeClientNetworkSample) { value.StartedAt = value.StartedAt.Add(time.Second) },
		func(value *model.EdgeClientNetworkSample) { value.Scope = "tcp_peer:203.0.114.0/24" },
		func(value *model.EdgeClientNetworkSample) { value.Slot = "a" },
		func(value *model.EdgeClientNetworkSample) { value.Delivery = nil },
		func(value *model.EdgeClientNetworkSample) { value.Backend = &model.EdgeClientNetworkBackend{} },
	} {
		changed := current
		edit(&changed)
		if _, ok := pairPublicDelivery(baseline, changed); ok {
			t.Fatal("paired unrelated or unproven socket")
		}
	}
}
