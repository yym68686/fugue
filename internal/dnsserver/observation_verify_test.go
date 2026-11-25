package dnsserver

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/dnsfacts"
)

func TestPairedDNSCheckpointCannotAuthorizeWholeHandoff(t *testing.T) {
	s, st, now := runtimeFactsFixture(t)
	st.record.Schema = dnsPairedCheckpoint
	st.record.HostedFloor = map[string]dnsHostedFloor{"reserved.example.test": {1, st.record.Parent.ContentHash}}
	if err := s.signDNSCheckpoint(&st.record); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(st.record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.decodeDNSCheckpoint(raw); err != nil {
		t.Fatal("valid paired checkpoint cannot be recovered", err)
	}
	if s.validPreviousDNSCheckpoint(raw) {
		t.Fatal("paired checkpoint would replace standalone LKG")
	}
	snapshot, err := dnsRuntimeFacts(st, now)
	if err != nil || snapshot.Ready {
		t.Fatal("global runtime exported a paired success", err)
	}
	snapshot.Ready = true
	if err := VerifyServingObservation(s.Config, raw, snapshot, now); err == nil {
		t.Fatal("scope-only checkpoint was accepted as whole listener evidence")
	}
	delete(st.record.HostedFloor, "reserved.example.test")
	if err := s.signDNSCheckpoint(&st.record); err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(st.record)
	if _, err := s.decodeDNSCheckpoint(raw); err == nil {
		t.Fatal("paired checkpoint reopened without the reservation manifest")
	}
}

func TestExpiredPredecessorCannotAuthorizeTraffic(t *testing.T) {
	s, st, now := runtimeFactsFixture(t)
	st.record.AppliedAt = now.Add(-time.Duration(st.payload.Policy.MaxStaleSeconds)*time.Second - time.Second)
	if err := s.signDNSCheckpoint(&st.record); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(st.record)
	if err != nil {
		t.Fatal(err)
	}
	base, err := dnsRuntimeFacts(st, now)
	if err != nil || base.Ready || len(base.Facts) == 0 {
		t.Fatal("invalid negative fixture", err)
	}
	for _, name := range []string{"exact", "ready", "positive member", "wrong member", "omitted", "substituted assignment", "pre-expiry capture", "stale capture", "forged expiry", "revoked"} {
		t.Run(name, func(t *testing.T) {
			snap, cfg := base, s.Config
			snap.Facts = append([]dnsfacts.Probe(nil), base.Facts...)
			switch name {
			case "ready":
				snap.Ready = true
			case "positive member":
				snap.Facts[0].Ready = true
			case "wrong member":
				snap.Facts[0].ProbeID = "foreign"
			case "omitted":
				snap.Facts = snap.Facts[1:]
			case "substituted assignment":
				snap.Assignment.FencingToken++
			case "pre-expiry capture":
				snap.EvaluatedAt = snap.CheckpointValidUntil.Add(-time.Nanosecond)
				snap.ObservedAt = snap.EvaluatedAt
			case "stale capture":
				snap.EvaluatedAt = now.Add(-time.Hour)
				snap.ObservedAt = snap.EvaluatedAt
			case "forged expiry":
				snap.CheckpointValidUntil = now
			case "revoked":
				cfg.BundleRevokedKeyIDs = []string{cfg.BundleSigningKeyID}
			}
			if err := VerifyExpiredPredecessorObservation(cfg, raw, snap, now); (err == nil) != (name == "exact") {
				t.Fatalf("unexpected expiry result: %v", err)
			}
			if VerifyServingObservation(cfg, raw, snap, now) == nil || VerifyUnreadyPredecessorObservation(cfg, raw, snap, now) == nil {
				t.Fatal("expired evidence accepted as live authority")
			}
		})
	}
}

func TestServingObservationAuthenticatesCheckpointAndEveryPositiveProof(t *testing.T) {
	service, state, now := runtimeFactsFixture(t)
	if err := service.signDNSCheckpoint(&state.record); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := json.Marshal(state.record)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := dnsRuntimeFacts(state, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyServingObservation(service.Config, checkpoint, snapshot, now); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"signature", "assignment", "quorum", "omitted", "duplicate", "expiry", "future", "binding", "route", "checkpoint lease", "proof lease", "revoked", "not ready", "expired observation", "future observation", "foreign unready observation"} {
		t.Run(scenario, func(t *testing.T) {
			raw, _ := json.Marshal(snapshot)
			var changed dnsfacts.Snapshot
			if json.Unmarshal(raw, &changed) != nil {
				t.Fatal("fixture encoding failed")
			}
			encoded := append([]byte(nil), checkpoint...)
			configuration := service.Config
			var expected string
			switch scenario {
			case "signature":
				encoded[len(encoded)-3] ^= 1
			case "assignment":
				changed.Assignment.FencingToken++
			case "quorum":
				for index := range changed.Facts {
					changed.Facts[index].Ready = false
				}
			case "omitted":
				changed.Facts = changed.Facts[1:]
			case "duplicate":
				changed.Facts = append(changed.Facts, changed.Facts[0])
			case "expiry":
				changed.Facts[0].Proof.ValidUntil = now.Add(-time.Second)
			case "future":
				changed.Facts[0].Proof.CheckedAt = now.Add(time.Second)
			case "binding":
				changed.Facts[0].Proof.TrafficRelease.FencingToken++
			case "route":
				changed.Facts[0].Proof.Digest = "foreign"
			case "checkpoint lease":
				changed.CheckpointValidUntil = changed.CheckpointValidUntil.Add(time.Hour)
			case "proof lease":
				changed.Facts[0].Proof.ValidUntil = now.Add(time.Hour)
			case "revoked":
				configuration.BundleRevokedKeyIDs = []string{configuration.BundleSigningKeyID}
			case "not ready":
				changed.Ready = false
				expected = "DNS observation lacks complete record readiness"
			case "expired observation":
				changed.ObservedAt = now.Add(-time.Hour)
				changed.EvaluatedAt = changed.ObservedAt
				expected = "DNS observation has expired"
			case "future observation":
				changed.EvaluatedAt = now.Add(time.Second)
				expected = "DNS observation temporal ordering mismatch"
			case "foreign unready observation":
				changed.PlanDigest = "foreign"
				changed.Ready = false
				expected = "DNS observation authority mismatch"
			}
			if err := VerifyServingObservation(configuration, encoded, changed, now); err == nil {
				t.Fatal("untrusted observation accepted")
			} else if expected != "" && err.Error() != expected {
				t.Fatalf("got %q, want %q", err, expected)
			}
		})
	}
}

func TestUnreadyPredecessorObservationIsNotServingAuthorization(t *testing.T) {
	service, state, now := runtimeFactsFixture(t)
	if err := service.signDNSCheckpoint(&state.record); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := json.Marshal(state.record)
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := dnsRuntimeFacts(state, now)
	if err != nil || !healthy.Ready {
		t.Fatal("expected ready observation", err)
	}
	if err := VerifyUnreadyPredecessorObservation(service.Config, checkpoint, healthy, now); err == nil {
		t.Fatal("ready observation was classified as unready")
	}
	snapshot := healthy
	snapshot.Ready = false
	if err := VerifyUnreadyPredecessorObservation(service.Config, checkpoint, snapshot, now); err == nil {
		t.Fatal("a negative flag alone proved an unready predecessor")
	}
	for _, scenario := range []string{"exact", "assignment", "plan", "foreign proof", "foreign member", "omitted", "expired"} {
		t.Run(scenario, func(t *testing.T) {
			raw, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			var unready dnsfacts.Snapshot
			if err := json.Unmarshal(raw, &unready); err != nil {
				t.Fatal(err)
			}
			for i := range unready.Facts {
				unready.Facts[i].Ready = false
			}
			switch scenario {
			case "assignment":
				unready.Assignment.FencingToken++
			case "plan":
				unready.PlanDigest = "sha256:foreign"
			case "foreign proof":
				unready.Facts[0].Ready = true
				unready.Facts[0].Proof.TrafficRelease.FencingToken++
			case "foreign member":
				unready.Facts[0].ProbeID = "sha256:foreign"
			case "omitted":
				unready.Facts = nil
			case "expired":
				unready.ObservedAt, unready.EvaluatedAt = now.Add(-time.Hour), now.Add(-time.Hour)
			}
			err = VerifyUnreadyPredecessorObservation(service.Config, checkpoint, unready, now)
			if (err == nil) != (scenario == "exact") {
				t.Fatalf("unexpected unready verdict: %v", err)
			}
			if VerifyServingObservation(service.Config, checkpoint, unready, now) == nil {
				t.Fatal("unready evidence authorized serving")
			}
		})
	}
}
