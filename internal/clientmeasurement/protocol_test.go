package clientmeasurement

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

func reportFixture(t *testing.T) (model.EdgeClientProbeReport, bundleauth.Keyring) {
	t.Helper()
	keys := bundleauth.NewKeyring("synthetic-measurement-key", "key-a", "", "", nil)
	now := time.Now().UTC().Add(-time.Minute)
	seed := strings.Repeat("ab", 32)
	report := model.EdgeClientProbeReport{Schema: "fugue.client-probe-report/v1", Plan: model.EdgeClientProbePlan{Schema: "fugue.client-probe-plan/v1", RoundID: "round-a", ObserverLabel: "test-observer"}}
	for _, edge := range []string{"edge-a", "edge-b"} {
		permit := model.EdgeClientProbePermit{Schema: "fugue.client-probe-permit/v1", RoundID: "round-a", AttemptID: "attempt-" + edge, ObserverID: "observer-a", Hostname: "app.example.test", Path: "/", TrafficClass: "streaming",
			EdgeID: edge, EdgeGroupID: "group-a", Address: "8.8.8.8", RouteDigest: "sha256:" + strings.Repeat("a", 64), BundleVersion: "bundle-a", IssuedAt: now, ExpiresAt: now.Add(2 * time.Minute),
			BodyBytes: BodyBytes, BodySeed: seed, BodySHA256: Digest(Payload(seed)), TargetEdgeIDs: []string{"edge-a", "edge-b"}}
		permit, err := SignPermit(permit, keys)
		if err != nil {
			t.Fatal(err)
		}
		rtt := 50.0
		network := model.EdgeClientNetworkSample{ConnectionID: "connection-" + edge, Slot: "a", Scope: "tcp_peer:203.0.113.0/24", StartedAt: now.Add(time.Second), ObservedAt: now.Add(2 * time.Second), TCPInfoAvailable: true, RTTMS: &rtt,
			Backend: &model.EdgeClientNetworkBackend{Namespace: "system", PodName: "front", PodUID: "pod-a", PodVersion: "10", ServiceName: "public", ServiceUID: "service-a", ServiceVersion: "20", EndpointsDigest: "sha256:" + strings.Repeat("b", 64)}}
		attestation, err := SignAttestation(model.EdgeClientProbeAttestation{Schema: "fugue.client-probe-attestation/v1", AttemptID: permit.AttemptID, EdgeID: edge, EdgeGroupID: permit.EdgeGroupID,
			RouteDigest: permit.RouteDigest, BundleVersion: permit.BundleVersion, ObservedAt: network.ObservedAt, ClientNetwork: network}, keys)
		if err != nil {
			t.Fatal(err)
		}
		report.Plan.Permits = append(report.Plan.Permits, permit)
		report.Outcomes = append(report.Outcomes, model.EdgeClientProbeOutcome{AttemptID: permit.AttemptID, StartedAt: now.Add(time.Second), CompletedAt: now.Add(4 * time.Second), BytesReceived: BodyBytes, BodySHA256: permit.BodySHA256, BodySeconds: 2, Attestation: &attestation})
	}
	return report, keys
}

func TestProbeSignaturesBindTargetsAndExactPublicSocket(t *testing.T) {
	report, keys := reportFixture(t)
	permit := report.Plan.Permits[0]
	if err := VerifyPermit(permit, keys, permit.IssuedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	attestation := *report.Outcomes[0].Attestation
	if err := VerifyAttestation(attestation, permit, keys); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*model.EdgeClientProbePermit){
		func(value *model.EdgeClientProbePermit) { value.Address = "9.9.9.9" },
		func(value *model.EdgeClientProbePermit) { value.Hostname = "other.example.test" },
		func(value *model.EdgeClientProbePermit) { value.BodyBytes *= 2 },
		func(value *model.EdgeClientProbePermit) { value.TargetEdgeIDs = []string{"edge-a"} },
		func(value *model.EdgeClientProbePermit) { value.ObserverID = "other-observer" },
	} {
		changed := permit
		edit(&changed)
		if VerifyPermit(changed, keys, permit.IssuedAt.Add(time.Second)) == nil {
			t.Fatal("tampered permit accepted")
		}
	}
	if VerifyPermit(permit, keys, permit.ExpiresAt) == nil || VerifyPermit(permit, keys, permit.IssuedAt.Add(-time.Second)) == nil {
		t.Fatal("permit lifetime not enforced")
	}
	rtt := 1.0
	attestation.ClientNetwork.RTTMS = &rtt
	if VerifyAttestation(attestation, permit, keys) == nil {
		t.Fatal("forged public RTT accepted")
	}
	revoked := keys
	revoked.RevokedKeyIDs = map[string]struct{}{"key-a": {}}
	if VerifyPermit(permit, revoked, permit.IssuedAt.Add(time.Second)) == nil {
		t.Fatal("revoked signer admitted")
	}
}

func TestProbeReportRequiresCompleteSameContentAndSameNetwork(t *testing.T) {
	report, _ := reportFixture(t)
	if cohort, err := ValidateReport(report); err != nil || cohort != "tcp_peer:203.0.113.0/24" {
		t.Fatal(cohort, err)
	}
	for _, edit := range []func(*model.EdgeClientProbeReport){
		func(value *model.EdgeClientProbeReport) {
			value.Plan.Permits = value.Plan.Permits[:1]
			value.Outcomes = value.Outcomes[:1]
		},
		func(value *model.EdgeClientProbeReport) { value.Outcomes[1].AttemptID = value.Outcomes[0].AttemptID },
		func(value *model.EdgeClientProbeReport) { value.Outcomes[1].BodySHA256 = "wrong" },
		func(value *model.EdgeClientProbeReport) { value.Outcomes[1].BytesReceived-- },
		func(value *model.EdgeClientProbeReport) { value.Outcomes[1].BodySeconds = 90 },
		func(value *model.EdgeClientProbeReport) {
			value.Outcomes[1].Attestation.ClientNetwork.Scope = "tcp_peer:198.51.100.0/24"
		},
		func(value *model.EdgeClientProbeReport) { value.Outcomes[1].Attestation.ClientNetwork.Backend = nil },
		func(value *model.EdgeClientProbeReport) { value.Outcomes[1].Attestation = nil },
		func(value *model.EdgeClientProbeReport) { value.Plan.Permits[1].BodySeed = strings.Repeat("cd", 32) },
	} {
		raw, _ := json.Marshal(report)
		var changed model.EdgeClientProbeReport
		_ = json.Unmarshal(raw, &changed)
		edit(&changed)
		if _, err := ValidateReport(changed); err == nil {
			t.Fatal("incomplete or incomparable client report accepted")
		}
	}
	for index := range report.Outcomes {
		report.Outcomes[index].Attestation = nil
		report.Outcomes[index].Failure = "connect"
		report.Outcomes[index].BytesReceived, report.Outcomes[index].BodySHA256, report.Outcomes[index].BodySeconds = 0, "", 0
	}
	if cohort, err := ValidateReport(report); err != nil || cohort != "" {
		t.Fatal("all-failed round fabricated a public peer", cohort, err)
	}
}

func TestPayloadIsFixedSizeReproducibleAndSeedBound(t *testing.T) {
	first := Payload(strings.Repeat("ab", 32))
	if len(first) != BodyBytes || Digest(first) != Digest(Payload(strings.Repeat("ab", 32))) || Digest(first) == Digest(Payload(strings.Repeat("cd", 32))) {
		t.Fatal("fixed-content probe is not reproducible")
	}
}

func TestClientWallClockSkewDoesNotAlterTransferDurationOrPermitLifetime(t *testing.T) {
	for _, offset := range []time.Duration{-1500 * time.Millisecond, 1500 * time.Millisecond, -4 * time.Second, 4 * time.Second} {
		report, keys := reportFixture(t)
		for index := range report.Outcomes {
			report.Outcomes[index].StartedAt = report.Outcomes[index].StartedAt.Add(offset)
			report.Outcomes[index].CompletedAt = report.Outcomes[index].CompletedAt.Add(offset)
			report.Outcomes[index].HTTPStatus = 200
		}
		_, err := ValidateReport(report)
		if (err != nil) != (offset < -ClientClockSkew || offset > ClientClockSkew) {
			t.Fatalf("clock offset %s validation: %v", offset, err)
		}
		permit := report.Plan.Permits[0]
		if VerifyPermit(permit, keys, permit.ExpiresAt) == nil {
			t.Fatal("client clock allowance extended a signed server permit")
		}
		report.Outcomes[0].BodySeconds = 4
		if _, err := ValidateReport(report); err == nil {
			t.Fatal("wall clock allowance admitted impossible transfer duration")
		}
	}
}
