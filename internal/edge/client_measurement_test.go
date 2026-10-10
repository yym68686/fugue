package edge

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/clientmeasurement"
	"fugue/internal/frontnetwork"
	"fugue/internal/model"
	"fugue/internal/routeproof"
)

func TestClientMeasurementRequiresSignedPermitAndExactPublicSocket(t *testing.T) {
	service := originProbeService(t)
	service.Config.EdgeSlot = "a"
	service.Config.CaddyProxyProtocolEnabled = true
	service.Config.BundleSigningKey, service.Config.BundleSigningKeyID = "synthetic-probe-key", "key-a"
	service.Config.EdgeToken = "node-scoped-test-token"
	keys := bundleauth.NewKeyring(service.Config.BundleSigningKey, service.Config.BundleSigningKeyID, "", "", nil)
	index := service.currentRouteIndex()
	route, _, _, version, _ := index.routeForRequest("app.example.test", "/")
	digest, err := routeproof.Digest(route)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	seed := strings.Repeat("ab", 32)
	permit, err := clientmeasurement.SignPermit(model.EdgeClientProbePermit{Schema: "fugue.client-probe-permit/v1", RoundID: "round-a", AttemptID: "attempt-a", ObserverID: "observer-a", Hostname: route.Hostname,
		Path: "/", TrafficClass: "streaming", EdgeID: service.Config.EdgeID, EdgeGroupID: service.Config.EdgeGroupID, Address: "8.8.8.8", RouteDigest: digest, BundleVersion: version,
		IssuedAt: now.Add(-time.Second), ExpiresAt: now.Add(time.Minute), BodyBytes: clientmeasurement.BodyBytes, BodySeed: seed, BodySHA256: clientmeasurement.Digest(clientmeasurement.Payload(seed)), TargetEdgeIDs: []string{"edge-a", "edge-b"}}, keys)
	if err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		reads.Add(1)
		var query frontnetwork.Request
		if json.NewDecoder(request.Body).Decode(&query) != nil || query.RemoteAddr != "203.0.113.5:54321" || query.EdgeID != permit.EdgeID {
			t.Error("measurement queried a different public peer")
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		at, rtt := time.Now().UTC(), 123.0
		sample := model.EdgeClientNetworkSample{ConnectionID: "exact-public-connection", Slot: "a", Scope: "tcp_peer:203.0.113.0/24", StartedAt: now, ObservedAt: at, TCPInfoAvailable: true, RTTMS: &rtt,
			Backend: &model.EdgeClientNetworkBackend{Namespace: "system", PodName: "front", PodUID: "pod-a", PodVersion: "10", ServiceName: "public", ServiceUID: "service-a", ServiceVersion: "20", EndpointsDigest: "sha256:" + strings.Repeat("c", 64)}}
		_ = json.NewEncoder(writer).Encode(frontnetwork.Response{Schema: frontnetwork.Schema, Nonce: query.Nonce, EdgeID: query.EdgeID, GroupID: query.GroupID, Sample: sample})
	}))
	defer api.Close()
	service.Config.APIURL = api.URL
	request := func(value model.EdgeClientProbePermit, peer string) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(value)
		query := httptest.NewRequest(http.MethodGet, "https://app.example.test/", nil)
		query.RemoteAddr = peer
		query.Header.Set(clientmeasurement.RequestHeader, base64.RawURLEncoding.EncodeToString(raw))
		query.Header.Set(edgeClientRemoteAddrHeader, "203.0.113.5:54321")
		writer := httptest.NewRecorder()
		service.handleProxy(writer, query)
		return writer
	}
	for _, edit := range []func(*model.EdgeClientProbePermit){
		func(value *model.EdgeClientProbePermit) { value.Hostname = "other.example.test" },
		func(value *model.EdgeClientProbePermit) { value.EdgeID = "other-edge" },
		func(value *model.EdgeClientProbePermit) { value.Signature = "invalid" },
	} {
		changed := permit
		edit(&changed)
		if response := request(changed, "127.0.0.1:1234"); response.Code != http.StatusForbidden || reads.Load() != 0 {
			t.Fatal("untrusted measurement reached network or origin", response.Code, reads.Load())
		}
	}
	if response := request(permit, "203.0.113.5:54321"); response.Code != http.StatusBadRequest || reads.Load() != 0 {
		t.Fatal("public forwarding header trusted without local proxy", response.Code)
	}
	response := request(permit, "127.0.0.1:1234")
	if response.Code != http.StatusOK || reads.Load() != 1 || len(response.Body.Bytes()) != clientmeasurement.BodyBytes || clientmeasurement.Digest(response.Body.Bytes()) != permit.BodySHA256 {
		t.Fatal("valid probe failed to return exact fixed payload", response.Code, reads.Load(), response.Body.Len())
	}
	raw, err := base64.RawURLEncoding.DecodeString(response.Header().Get(clientmeasurement.AttestationHeader))
	var attestation model.EdgeClientProbeAttestation
	if err != nil || json.Unmarshal(raw, &attestation) != nil || clientmeasurement.VerifyAttestation(attestation, permit, keys) != nil || *attestation.ClientNetwork.RTTMS != 123 {
		t.Fatal("returned network attestation is not reconstructible", err)
	}
	if response := request(permit, "127.0.0.1:1234"); response.Code != http.StatusTooManyRequests || reads.Load() != 1 {
		t.Fatal("signed attempt replay consumed network budget", response.Code, reads.Load())
	}
}
