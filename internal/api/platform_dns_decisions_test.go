package api

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"slices"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestDNSDecisionsBackendBindingAndNoPublicationSubstitution(t *testing.T) {
	filename := t.TempDir() + "/state.json"
	state := store.New(filename)
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	server := NewServer(state, auth.New(state, "decision-admin"), nil, ServerConfig{BundleSigningKey: "synthetic-key", BundleSigningKeyID: "key"})
	fixture := newDNSBackendFixture(t)
	if _, err := state.UpdateDNSHeartbeat(model.DNSNode{ID: fixture.claims.NodeID, EdgeGroupID: "edge-group-a", Zone: "example.test", PublicIPv4: "192.0.2.10"}); err != nil {
		t.Fatal(err)
	}
	seedVerifiedDNSDelegationFixture(t, server, "example.test")
	parent, release, found, err := state.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "gray")
	if err != nil || !found {
		t.Fatal(err)
	}
	sets, err := server.currentReleaseSetExpectations(parent)
	if err != nil {
		t.Fatal(err)
	}
	var expected model.PlatformExpectedConsumerSet
	for _, set := range sets {
		if set.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle {
			expected = set
		}
	}
	child, err := server.consumerAssignmentChild(parent, expected.ArtifactKind)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ring := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "test", Keys: map[string]string{"test": "synthetic-identity"}}
	token, err := platformcontrol.IssuePlatformComponentIdentity(ring, fixture.claims, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := platformcontrol.ParsePlatformComponentIdentity(ring, token, now)
	if err != nil {
		t.Fatal(err)
	}
	heartbeat := trustedPlatformHeartbeatRequest(t, claims, expected, now, 20, child.GenerationSequence, release.FencingToken, "decision-receipt")
	heartbeat.LKGGeneration = child.Generation
	heartbeat.EvidenceHash, _ = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(heartbeat)
	if _, err := state.AcceptTrustedPlatformConsumerHeartbeat(claims, expected.ID, heartbeat, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
		t.Fatal(err)
	}
	fixture.pod.Spec.Containers = []corev1.Container{{Name: "resolver", Ports: []corev1.ContainerPort{{ContainerPort: 53, Protocol: corev1.ProtocolTCP}, {ContainerPort: 53, Protocol: corev1.ProtocolUDP}, {Name: "observation", ContainerPort: 8081, Protocol: corev1.ProtocolTCP}}, ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromString("observation")}}}}}
	snapshot := dnsserver.DNSDecisionSnapshot{NodeID: fixture.claims.NodeID, ProcessID: "process-a", CapturedAt: now, Receipts: []dnsserver.DNSDecisionReceipt{}, Publication: dnsserver.DNSDecisionPublicationState{ObservedAt: now, DesiredKnown: true, Desired: &dnsserver.DNSDecisionPublication{Digest: "desired"}, Loaded: &dnsserver.DNSDecisionPublication{Digest: "loaded-lkg"}, LKG: &dnsserver.DNSDecisionPublication{Digest: "loaded-lkg"}, ServingLKG: true, Rejected: true}}
	reads := 0
	fixture.proxyHandler = func(writer http.ResponseWriter, request *http.Request) {
		reads++
		if request.URL.Query().Get("hostname") != "target.example.test" || request.URL.Query().Get("limit") != "2" {
			t.Error("lost filters")
		}
		json.NewEncoder(writer).Encode(snapshot)
	}
	fixture.install(t, server)
	before, _ := os.ReadFile(filename)
	endpoint := "/v1/admin/platform-state/dns-decisions/" + fixture.claims.NodeID + "?hostname=target.example.test&limit=2"
	response := performJSONRequest(t, server, http.MethodGet, endpoint, "decision-admin", nil)
	var result platformDNSDecisionResponse
	mustDecodeJSON(t, response, &result)
	if response.Code != 200 || reads != 1 || result.Backend.PodUID != string(fixture.pod.UID) || result.Snapshot.Publication.Loaded.Digest != "loaded-lkg" || !result.Snapshot.Publication.Rejected {
		t.Fatal(response.Code, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatal("cacheable private data")
	}
	after, _ := os.ReadFile(filename)
	if string(before) != string(after) {
		t.Fatal("read changed stored publication")
	}
	fixture.proxyHandler = func(writer http.ResponseWriter, request *http.Request) {
		json.NewEncoder(writer).Encode(snapshot)
	}
	quality, err := server.capturePhysicalQuality(context.Background(), "target.example.test", "streaming", edgeQualityRankScope{}, fixture.claims.NodeID, edgequality.DefaultNetworkPolicy())
	if err != nil || !slices.Contains(quality.Snapshot.Blockers, "actual_dns_receipt_not_retained") || slices.Contains(quality.Snapshot.Blockers, "actual_dns_backend_unavailable") {
		t.Fatal("empty real journal mislabeled as backend outage", quality.Snapshot.Blockers, err)
	}
	fixture.proxyHandler = func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}
	quality, err = server.capturePhysicalQuality(context.Background(), "target.example.test", "streaming", edgeQualityRankScope{}, fixture.claims.NodeID, edgequality.DefaultNetworkPolicy())
	if err != nil || !slices.Contains(quality.Snapshot.Blockers, "actual_dns_backend_unavailable") || slices.Contains(quality.Snapshot.Blockers, "actual_dns_receipt_not_retained") {
		t.Fatal("backend outage mislabeled as empty journal", quality.Snapshot.Blockers, err)
	}
	fixture.proxyHandler = func(writer http.ResponseWriter, request *http.Request) {
		json.NewEncoder(writer).Encode(snapshot)
	}
	for _, suffix := range []string{"?limit=0", "?limit=21", "?limit=oops", "?hostname=bad%20name"} {
		response = performJSONRequest(t, server, http.MethodGet, "/v1/admin/platform-state/dns-decisions/"+fixture.claims.NodeID+suffix, "decision-admin", nil)
		if response.Code != 400 {
			t.Fatal(response.Code, suffix)
		}
	}
	if response = performJSONRequest(t, server, http.MethodGet, endpoint, "invalid", nil); response.Code != 401 {
		t.Fatal("unauthorized read", response.Code)
	}
	snapshot.NodeID = "foreign-node"
	if response = performJSONRequest(t, server, http.MethodGet, endpoint, "decision-admin", nil); response.Code != 503 {
		t.Fatal("foreign observations accepted", response.Code)
	}
	snapshot.NodeID = fixture.claims.NodeID
	fixture.changePath = "slice"
	if response = performJSONRequest(t, server, http.MethodGet, endpoint, "decision-admin", nil); response.Code != 503 {
		t.Fatal("changed backend accepted", response.Code)
	}
}
