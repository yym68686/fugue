package dnsserver

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/dnsfacts"
)

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
