package platformproducer

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestHoldWithoutRecoveryCannotBeReleasedByRuntimeProofOrDate(t *testing.T) {
	h := recoveryHold()
	h.Recovery = nil
	h.ExpectedRecoveryDate = "2020-01-01"
	at := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	o := OfflineRecoveryObservation{EdgeID: h.EdgeID, DeclarationDigest: OfflineRecoveryDigest(h), LastAttempt: at, LastSuccess: at, ProofValidUntil: at.Add(time.Hour), Successes: 3, Admitted: true}
	if o.Ready(h, at) || o.Record(h, at, true, at.Add(time.Hour)).Admitted {
		t.Fatal("unspecified recovery consumed runtime evidence")
	}
	nodes := []model.EdgeNode{{ID: h.EdgeID, EdgeGroupID: h.EdgeGroupID, PublicIPv4: h.PublicIPv4}}
	retained, err := EffectiveOfflineHolds(nodes, []OfflineEdge{h}, []OfflineRecoveryObservation{o}, at)
	if err != nil || len(retained) != 1 {
		t.Fatal("indefinite hold was removed", retained, err)
	}
	raw, _ := json.Marshal(map[string]any{"captured_at": at, "observations": []OfflineRecoveryObservation{o}})
	parent := model.PlatformArtifact{Metadata: map[string]string{OfflineRecoveryMetadata: string(raw)}, Provenance: model.PlatformArtifactProvenance{SignedAt: at.Add(time.Second)}}
	if _, err := PublicationOfflineHolds(parent, []OfflineEdge{h}); err == nil {
		t.Fatal("signed stale recovery evidence removed an indefinite hold")
	}
}

func recoveryHold() OfflineEdge {
	return OfflineEdge{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8", Reason: "quota_exhausted", Recovery: &OfflineEdgeRecoveryPolicy{Hostname: "api.example.test", Path: "/", ProbeIntervalSeconds: 300, ProbeTimeoutSeconds: 5, SuccessThreshold: 3, FailureThreshold: 1, FreshnessSeconds: 1800}}
}

func TestOfflineWorkerTargetIsBoundToCurrentProofAndSignedReplay(t *testing.T) {
	h := recoveryHold()
	old := OfflineRecoveryDigest(h)
	target := OfflineWorkerTarget{NodeUID: "11111111-1111-1111-1111-111111111111", ServiceUID: "22222222-2222-2222-2222-222222222222",
		ReleaseDigest: "sha256:" + strings.Repeat("c", 64), WorkerSourceSHA: strings.Repeat("d", 40), WorkerImage: "sha256:" + strings.Repeat("e", 64)}
	h.Recovery.WorkerTarget = &target
	if err := ValidateOfflineEdges([]OfflineEdge{h}); err != nil {
		t.Fatal(err)
	}
	digest := OfflineRecoveryDigest(h)
	if digest == old || digest != "sha256:db2945a0fd087a6d0c841ac29a710dac9d840f721ccc59fe0a96b82b4ccb3453" {
		t.Fatal("Worker target must participate in the cross-language evidence binding", digest)
	}
	at := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	code := capacityEvidence(at).Code
	code.NodeUID, code.ServiceUID = target.NodeUID, target.ServiceUID
	code.WorkerSourceSHA, code.WorkerImageDigest = target.WorkerSourceSHA, target.WorkerImage
	o := OfflineRecoveryObservation{EdgeID: h.EdgeID, DeclarationDigest: digest, LastAttempt: at, LastSuccess: at,
		ProofValidUntil: at.Add(time.Hour), Successes: h.Recovery.SuccessThreshold, Admitted: true, LastResult: "route_code_verified", RouteCode: &code}
	if o.Ready(h, at) {
		t.Fatal("a running image alone is not a receipt for the selected release")
	}
	code.WorkerReleaseDigest = target.ReleaseDigest
	if !o.Ready(h, at) {
		t.Fatal("fresh evidence for the exact Worker target did not admit it")
	}
	raw, err := json.Marshal(map[string]any{"captured_at": at, "observations": []OfflineRecoveryObservation{o}})
	if err != nil {
		t.Fatal(err)
	}
	parent := model.PlatformArtifact{Metadata: map[string]string{OfflineRecoveryMetadata: string(raw)}, Provenance: model.PlatformArtifactProvenance{SignedAt: at}}
	if holds, err := PublicationOfflineHolds(parent, []OfflineEdge{h}); err != nil || len(holds) != 0 {
		t.Fatalf("replay did not honor its own signed target: %v, %v", holds, err)
	}
	newTarget := target
	newTarget.ReleaseDigest = "sha256:" + strings.Repeat("f", 64)
	h.Recovery.WorkerTarget = &newTarget
	if o.Ready(h, at) {
		t.Fatal("new signed target reused an older checkpoint")
	}
	if _, err := PublicationOfflineHolds(parent, []OfflineEdge{h}); err == nil {
		t.Fatal("signed evidence was accepted with a different declaration")
	}

	h, o, at = singleRecoveryFixture(t)
	h.Recovery.WorkerTarget = &target
	o.DeclarationDigest = OfflineRecoveryDigest(h)
	o.Capacity.Code = code
	if !o.Ready(h, at) {
		t.Fatal("capacity observation did not recognize the exact Worker target")
	}
	o.Capacity.Code.ServiceUID = "wrong-service"
	if o.Ready(h, at) {
		t.Fatal("capacity observation bypassed the exact ingress binding")
	}
}

func TestOfflineRecoveryDeclarationWireDigestMatchesConfigurationPublisher(t *testing.T) {
	h := recoveryHold()
	h.ExpectedRecoveryDate = "2030-01-01"
	h.Recovery.Path = "/a&b"
	const wireDigest = "sha256:5f2a440655b056227813ab50cd3fe4ff772704af0fdc5b4f07c1c0e13ddb0dc0"
	recoveryKey := OfflineRecoveryKey(h)
	if got := OfflineRecoveryDigest(h); got != wireDigest {
		t.Fatalf("typed declaration digest drifted from configuration publisher contract: %s", got)
	}
	h.KeepHostedRouteTLS = true
	if got := OfflineRecoveryDigest(h); got != wireDigest || OfflineRecoveryKey(h) != recoveryKey {
		t.Fatalf("route/TLS permission changed DNS recovery identity: %s", got)
	}
	nodes := []model.EdgeNode{{ID: h.EdgeID, EdgeGroupID: h.EdgeGroupID, PublicIPv4: h.PublicIPv4}, {ID: "edge-b", EdgeGroupID: h.EdgeGroupID, PublicIPv4: "9.9.9.9"}}
	if selected, err := SelectDNSCandidateEndpoints(nodes, []OfflineEdge{h}); err != nil || len(selected) != 1 || selected[0].ID != "edge-b" {
		t.Fatalf("permission changed DNS candidates: %+v, %v", selected, err)
	}
	bad := h
	bad.Reason = "maintenance"
	if ValidateOfflineEdges([]OfflineEdge{bad}) == nil {
		t.Fatal("maintenance hold granted hosted execution")
	}
	bad = h
	bad.Recovery = nil
	if ValidateOfflineEdges([]OfflineEdge{bad}) == nil {
		t.Fatal("hold without recovery granted hosted execution")
	}
}

func TestIndependentWorkerGrantDoesNotChangeDNSRecoveryIdentity(t *testing.T) {
	h := recoveryHold()
	h.WorkerEnrollment = &OfflineWorkerEnrollment{ID: strings.Repeat("a", 32), NodeUID: "11111111-1111-1111-1111-111111111111",
		ServiceAccount: "worker-account", DaemonSetUIDs: []string{"22222222-2222-2222-2222-222222222222"}, PodCreatedAfter: "2030-01-01T00:00:00Z"}
	const oldDigest = "sha256:29db956b07289dc7761a3421444a3b9df04a2af3156a944f6b6590f541958f02"
	digest, key := OfflineRecoveryDigest(h), OfflineRecoveryKey(h)
	if digest != oldDigest {
		t.Fatalf("unexpected wire digest: %s", digest)
	}
	h.WorkerEnrollment.IndependentDeployment = &OfflineWorkerDeployment{Name: "local-worker", UID: "33333333-3333-3333-3333-333333333333",
		EdgeImage: "registry.example/system/edge@sha256:" + strings.Repeat("a", 64), CaddyImage: "registry.example/system/caddy@sha256:" + strings.Repeat("b", 64)}
	if err := ValidateOfflineEdges([]OfflineEdge{h}); err != nil {
		t.Fatal(err)
	}
	if OfflineRecoveryDigest(h) != digest || OfflineRecoveryKey(h) != key || h.WorkerEnrollment.IndependentDeployment == nil {
		t.Fatal("an additive deployment grant changed the DNS recovery identity or mutated the input")
	}
	h.WorkerEnrollment.IndependentDeployment.EdgeImage = "registry.example/system/edge:latest"
	if ValidateOfflineEdges([]OfflineEdge{h}) == nil {
		t.Fatal("a mutable Worker image was granted the enrollment")
	}
}

func TestOfflineRecoveryNeedsConsecutiveFreshProofsAndIdentity(t *testing.T) {
	h := recoveryHold()
	base := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	o := OfflineRecoveryObservation{}
	for i := 0; i < 2; i++ {
		o = o.Record(h, base.Add(time.Duration(i)*5*time.Minute), true, base.Add(20*time.Minute))
		if o.Admitted {
			t.Fatal("partial success admitted")
		}
		if o.Successes != i+1 {
			t.Fatalf("success streak %d", o.Successes)
		}
	}
	o = o.Record(h, base.Add(10*time.Minute), true, base.Add(30*time.Minute))
	if !o.Admitted || !o.Ready(h, base.Add(10*time.Minute)) {
		t.Fatal("threshold proof not admitted", o)
	}
	o = o.Record(h, base.Add(15*time.Minute), false, time.Time{})
	if o.Admitted || o.Successes != 0 {
		t.Fatal("failure retained admission", o)
	}
	changed := h
	changed.PublicIPv4 = "9.9.9.9"
	if o.Ready(changed, base.Add(15*time.Minute)) {
		t.Fatal("identity change reused proof")
	}
}

func TestOfflineRecoveryDateDoesNotGateProbeBasedReadmission(t *testing.T) {
	base := time.Date(2030, 1, 2, 0, 0, 0, 0, time.UTC)
	for _, zone := range []struct {
		name   string
		offset int
	}{{name: "UTC"}, {name: "UTC-12", offset: -12 * 60 * 60}, {name: "UTC+14", offset: 14 * 60 * 60}} {
		t.Run(zone.name, func(t *testing.T) {
			local := time.FixedZone(zone.name, zone.offset)
			for _, scenario := range []struct {
				name       string
				recoveryAt string
				attempts   int
				admitted   bool
			}{
				{name: "healthy before informational date", recoveryAt: "2030-01-10", attempts: 3, admitted: true},
				{name: "past date without enough successful probes", recoveryAt: "2030-01-01", attempts: 2, admitted: false},
			} {
				t.Run(scenario.name, func(t *testing.T) {
					hold := recoveryHold()
					hold.ExpectedRecoveryDate = scenario.recoveryAt
					observation := OfflineRecoveryObservation{}
					for index := 0; index < scenario.attempts; index++ {
						at := base.Add(time.Duration(index) * time.Duration(hold.Recovery.ProbeIntervalSeconds) * time.Second).In(local)
						observation = observation.Record(hold, at, true, at.Add(time.Hour))
					}
					at := base.Add(time.Duration(scenario.attempts-1) * time.Duration(hold.Recovery.ProbeIntervalSeconds) * time.Second).In(local)
					if got := observation.Ready(hold, at); got != scenario.admitted {
						t.Fatalf("recovery readiness=%t, want %t for informational date %s after %d successful probes", got, scenario.admitted, scenario.recoveryAt, scenario.attempts)
					}
				})
			}
		})
	}
}

func TestEffectiveOfflineHoldsUsesOnlyReadyObservations(t *testing.T) {
	h := recoveryHold()
	nodes := []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8"}, {ID: "edge-b", EdgeGroupID: "edge-group-a", PublicIPv4: "9.9.9.9"}}
	at := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	o := OfflineRecoveryObservation{EdgeID: h.EdgeID, DeclarationDigest: OfflineRecoveryDigest(h), LastAttempt: at, LastSuccess: at, ProofValidUntil: at.Add(time.Hour), Successes: 3, Admitted: true}
	holds, err := EffectiveOfflineHolds(nodes, []OfflineEdge{h}, []OfflineRecoveryObservation{o}, at)
	if err != nil || len(holds) != 0 {
		t.Fatal("ready recovery remained held", holds, err)
	}
	o.DeclarationDigest = "sha256:" + "0"
	holds, err = EffectiveOfflineHolds(nodes, []OfflineEdge{h}, []OfflineRecoveryObservation{o}, at)
	if err != nil || len(holds) != 1 {
		t.Fatal("unbound recovery bypassed hold", holds, err)
	}
}

func TestPublicationOfflineHoldsPinsRecoveryEvidence(t *testing.T) {
	h := recoveryHold()
	at := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	o := OfflineRecoveryObservation{EdgeID: h.EdgeID, DeclarationDigest: OfflineRecoveryDigest(h), LastAttempt: at, LastSuccess: at, ProofValidUntil: at.Add(time.Hour), Successes: 3, Admitted: true}
	raw := `{"captured_at":"2030-01-01T00:00:00Z","observations":[{"edge_id":"edge-a","declaration_digest":"` + o.DeclarationDigest + `","last_attempt":"2030-01-01T00:00:00Z","last_success":"2030-01-01T00:00:00Z","proof_valid_until":"2030-01-01T01:00:00Z","successes":3,"failures":0,"admitted":true}]}`
	parent := model.PlatformArtifact{Metadata: map[string]string{OfflineRecoveryMetadata: raw}, Provenance: model.PlatformArtifactProvenance{SignedAt: at.Add(time.Minute)}}
	out, err := PublicationOfflineHolds(parent, []OfflineEdge{h})
	if err != nil || len(out) != 0 {
		t.Fatal("pinned proof did not recover candidate", out, err)
	}
	parent.Provenance.SignedAt = at.Add(-time.Minute)
	if _, err = PublicationOfflineHolds(parent, []OfflineEdge{h}); err == nil {
		t.Fatal("future evidence accepted")
	}
}
