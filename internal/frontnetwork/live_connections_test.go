package frontnetwork

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func liveFixture() (Request, LiveConnections, time.Time) {
	now := time.Now().UTC()
	query := Request{Schema: Schema, Nonce: strings.Repeat("a", 32), EdgeID: "edge-a", GroupID: "group-a", Slot: "b", RemoteAddr: "203.0.113.1:41000"}
	info := map[string]json.RawMessage{"tcp_info_available": json.RawMessage("true"), "tcp_state": json.RawMessage("1"), "tcp_rtt_us": json.RawMessage("170500"), "tcp_min_rtt_us": json.RawMessage("160000"), "tcp_rttvar_us": json.RawMessage("7000"), "tcp_segs_out": json.RawMessage("12"), "tcp_total_retrans": json.RawMessage("1"), "tcp_bytes_sent": json.RawMessage("8000"), "tcp_bytes_retrans": json.RawMessage("1500")}
	return query, LiveConnections{Count: 1, Active: []LiveConnection{{ID: "connection-a", Protocol: "https", Slot: "b", DownstreamRemote: query.RemoteAddr, StartedAt: now.Add(-time.Minute), ProxyProtocol: true, TCPInfo: info}}}, now
}

func TestLiveFrontSocketBindsExactIdentityWithoutPersistingPeer(t *testing.T) {
	query, connections, now := liveFixture()
	sample, err := SampleFromLiveConnections(query, connections, now)
	if err != nil || sample.RTTMS == nil || *sample.RTTMS != 170.5 || sample.RetransmittedSegments != 1 || sample.Scope != "tcp_peer:203.0.113.0/24" {
		t.Fatal(sample, err)
	}
	raw, _ := json.Marshal(sample)
	if strings.Contains(string(raw), query.RemoteAddr) {
		t.Fatal("retained transient peer endpoint")
	}
	for _, edit := range []func(*LiveConnections){
		func(value *LiveConnections) { value.Count++ },
		func(value *LiveConnections) { value.Active[0].Slot = "a" },
		func(value *LiveConnections) { value.Active[0].DownstreamRemote = "203.0.113.2:41000" },
		func(value *LiveConnections) { value.Active[0].Protocol = "http" },
		func(value *LiveConnections) { value.Active[0].ProxyProtocol = false },
		func(value *LiveConnections) { value.Active[0].StartedAt = now.Add(time.Minute) },
		func(value *LiveConnections) { value.Active[0].TCPInfo["tcp_state"] = json.RawMessage("5") },
		func(value *LiveConnections) { delete(value.Active[0].TCPInfo, "tcp_rtt_us") },
		func(value *LiveConnections) { value.Count = 2; value.Active = append(value.Active, value.Active[0]) },
	} {
		query, modified, now := liveFixture()
		edit(&modified)
		if _, err := SampleFromLiveConnections(query, modified, now); err == nil {
			t.Fatal("accepted ambiguous, unavailable or foreign socket")
		}
	}
	connections.Active[0].TCPInfo = map[string]json.RawMessage{"tcp_info_available": json.RawMessage("false")}
	sample, err = SampleFromLiveConnections(query, connections, now)
	if err != nil || sample.RTTMS != nil || sample.SegmentsOut != 0 {
		t.Fatal("unavailable TCP_INFO did not remain unknown", sample, err)
	}
}

func TestReadAPIRequiresTransportWitnessAndRejectsRedirect(t *testing.T) {
	for _, scenario := range []string{"valid", "missing_backend", "redirect"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/v1/edge/network-observation" || request.Header.Get("Authorization") != "Bearer scoped" {
					t.Error("incorrect credential destination")
				}
				if scenario == "redirect" {
					http.Redirect(writer, request, "https://untrusted.example.test", http.StatusTemporaryRedirect)
					return
				}
				var query Request
				json.NewDecoder(request.Body).Decode(&query)
				_, connections, now := liveFixture()
				sample, _ := SampleFromLiveConnections(query, connections, now)
				if scenario == "valid" {
					sample.Backend = &model.EdgeClientNetworkBackend{Namespace: "system", PodName: "front", PodUID: "pod-a", PodVersion: "1", ServiceName: "public", ServiceUID: "service-a", ServiceVersion: "2", EndpointsDigest: "sha256:" + strings.Repeat("b", 64)}
				}
				json.NewEncoder(writer).Encode(Response{Schema: Schema, Nonce: query.Nonce, EdgeID: query.EdgeID, GroupID: query.GroupID, Sample: sample})
			}))
			defer server.Close()
			_, err := ReadAPI(context.Background(), server.Client(), server.URL, "scoped", "edge-a", "group-a", "b", "203.0.113.1:41000")
			if (err == nil) != (scenario == "valid") {
				t.Fatal(scenario, err)
			}
		})
	}
}
