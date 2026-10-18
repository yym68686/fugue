package platformproducer

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func recoveryHold() OfflineEdge {
	return OfflineEdge{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8", Reason: "quota_exhausted", Recovery: &OfflineEdgeRecoveryPolicy{Hostname: "api.example.test", Path: "/", ProbeIntervalSeconds: 300, ProbeTimeoutSeconds: 5, SuccessThreshold: 3, FailureThreshold: 1, FreshnessSeconds: 1800}}
}

func TestOfflineRecoveryDeclarationWireDigestMatchesConfigurationPublisher(t *testing.T) {
	h := recoveryHold()
	h.ExpectedRecoveryDate = "2030-01-01"
	h.Recovery.Path = "/a&b"
	const wireDigest = "sha256:5f2a440655b056227813ab50cd3fe4ff772704af0fdc5b4f07c1c0e13ddb0dc0"
	if got := OfflineRecoveryDigest(h); got != wireDigest {
		t.Fatalf("typed declaration digest drifted from configuration publisher contract: %s", got)
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
