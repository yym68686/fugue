package dnsserver

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDNSLateSuccessorFactsRequireExactFreshObservation(t *testing.T) {
	for _, scenario := range []string{"valid", "old capture", "future capture", "expired", "parent", "parent digest", "release", "scope", "fence", "channel", "route", "edge", "group", "state", "failed probe", "unavailable probe", "cached proof", "unclassified negative", "duplicate observations", "duplicate targets", "unknown requirement"} {
		t.Run(scenario, func(t *testing.T) {
			old, bridge, nextFacts, now := initialCanaryDNSFixture(t)
			parent := model.PlatformArtifact{ID: "next-parent", ContentHash: "sha256:next-parent"}
			bridge.candidate.Assignment.ScopeKey = "global"
			bridge.candidate.Release.ReleaseChannel = "gray"
			bridge.candidate.Release.FencingToken = 2
			for index := range nextFacts {
				nextFacts[index].Ready = false
			}
			view := bridge.payload.Queries[0]
			next, err := buildDNSServingState(dnsServingCheckpoint{Parent: parent, Candidate: bridge.candidate}, bridge.payload,
				"next-route", view.NodeID, view.EdgeGroupID, nextFacts, now)
			if err != nil {
				t.Fatal(err)
			}
			old.facts = nil
			for index, requirement := range old.payload.Plan.Probes {
				observation := nextFacts[index]
				observation.ProbeID, observation.Reason, observation.ObservationID = requirement.ID, "route_digest_mismatch", "late-observation"
				observation.Proof.CheckedAt = now.Add(time.Millisecond)
				observation.Proof.TrafficRelease = &model.TrafficReleaseBinding{ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash,
					RouteArtifactID: "next-route", ReleaseID: bridge.candidate.Release.ID, ReleaseChannel: "gray", FencingToken: 2, ScopeKey: "global"}
				switch scenario {
				case "old capture":
					observation.Proof.CheckedAt = now
				case "future capture":
					observation.Proof.CheckedAt = now.Add(time.Hour)
				case "expired":
					observation.Proof.ValidUntil = now.Add(-time.Second)
				case "parent":
					observation.Proof.TrafficRelease.ReleaseSetID = "other"
				case "parent digest":
					observation.Proof.TrafficRelease.ReleaseSetDigest = "other"
				case "release":
					observation.Proof.TrafficRelease.ReleaseID = "other"
				case "scope":
					observation.Proof.TrafficRelease.ScopeKey = "other"
				case "fence":
					observation.Proof.TrafficRelease.FencingToken++
				case "channel":
					observation.Proof.TrafficRelease.ReleaseChannel = "full"
				case "route":
					observation.Proof.Digest = "other"
				case "edge":
					observation.Proof.EdgeID = "other"
				case "group":
					observation.Proof.GroupID = "other"
				case "state":
					observation.Proof.State = "unavailable"
				case "failed probe":
					observation.Reason = "probe_failed"
				case "unavailable probe":
					observation.Reason = "probe_unavailable"
				case "cached proof":
					observation.Ready, observation.Reason = true, "retained_valid_proof"
				case "unclassified negative":
					observation.Reason = ""
				case "unknown requirement":
					observation.ProbeID = "unknown"
				}
				old.facts = append(old.facts, observation)
			}
			if scenario == "duplicate observations" {
				old.facts = append(old.facts, old.facts...)
			}
			if scenario == "duplicate targets" {
				next.payload.Plan.Probes = append(next.payload.Plan.Probes, next.payload.Plan.Probes...)
			}
			before, after := mustDNSJSON(t, old.facts), mustDNSJSON(t, next.facts)
			result, changed := dnsSupplementTransitionFacts(next, old, now.Add(2*time.Millisecond), nil)
			if changed != (scenario == "valid") {
				t.Fatal("late observation changed authority", changed)
			}
			if changed {
				for index, fact := range result {
					if !fact.Ready || fact.ObservationID != "late-observation" || !fact.Proof.CheckedAt.Equal(old.facts[index].Proof.CheckedAt) ||
						fact.Proof.ValidUntil.After(old.facts[index].Proof.ValidUntil) {
						t.Fatal("late observation was invented or renewed", fact)
					}
				}
			}
			if string(before) != string(mustDNSJSON(t, old.facts)) || string(after) != string(mustDNSJSON(t, next.facts)) {
				t.Fatal("supplement changed either original observation set")
			}
		})
	}
}

func TestDNSLateTransitionMustNotEraseEstablishedSuccessorRecovery(t *testing.T) {
	old, bridge, nextFacts, now := initialCanaryDNSFixture(t)
	parent := model.PlatformArtifact{ID: "next-parent", ContentHash: "sha256:next-parent"}
	bridge.candidate.Assignment.ScopeKey = "global"
	bridge.candidate.Release.ReleaseChannel = "gray"
	bridge.candidate.Release.FencingToken = 2
	history := &dnsProbeHistoryStore{}
	scheduler := (&dnsReadinessScheduler{}).shareHistory(history)
	policy := &model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
	for i := range nextFacts {
		nextFacts[i].Proof.TrafficRelease = &model.TrafficReleaseBinding{ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, RouteArtifactID: "next-route", ReleaseID: bridge.candidate.Release.ID, ReleaseChannel: "gray", FencingToken: 2, ScopeKey: "global"}
		nextFacts[i].Ready = true
		nextFacts[i].Reason = ""
		for _, offset := range []time.Duration{-61 * time.Second, -31 * time.Second, -time.Second} {
			nextFacts[i].Proof.CheckedAt = now.Add(offset)
			scheduler.observeProbeHistory(&nextFacts[i], bridge.payload.Policy.DNSReadiness, now.Add(offset))
		}
	}
	view := bridge.payload.Queries[0]
	next, err := buildDNSServingState(dnsServingCheckpoint{Parent: parent, Candidate: bridge.candidate}, bridge.payload, "next-route", view.NodeID, view.EdgeGroupID, nextFacts, now)
	if err != nil {
		t.Fatal(err)
	}
	old.facts = nil
	for i, requirement := range old.payload.Plan.Probes {
		fact := nextFacts[i]
		fact.ProbeID = requirement.ID
		fact.Reason = "route_digest_mismatch"
		fact.Ready = false
		fact.ProbeHistory = nil
		fact.Proof.CheckedAt = now.Add(time.Millisecond)
		old.facts = append(old.facts, fact)
	}
	result, changed := dnsSupplementTransitionFacts(next, old, now.Add(2*time.Millisecond), scheduler)
	if !changed {
		t.Fatal("late successor proof not accepted")
	}
	for _, fact := range result {
		if !dnsAvailabilityRecovered(policy, fact, time.Duration(bridge.payload.Policy.DNSReadiness.FactFreshnessSeconds)*time.Second, now.Add(2*time.Millisecond)) {
			t.Fatal("late valid successor proof erased established recovery history", fact.ProbeHistory)
		}
	}
}

func TestDNSLateTransitionCannotBorrowOldHistoryOrCrossFailureBarrier(t *testing.T) {
	for _, scenario := range []string{"old-history-only", "failed-successor", "expired-successor-history"} {
		t.Run(scenario, func(t *testing.T) {
			old, bridge, nextFacts, now := initialCanaryDNSFixture(t)
			parent := model.PlatformArtifact{ID: "next-parent", ContentHash: "sha256:next-parent"}
			bridge.candidate.Assignment.ScopeKey = "global"
			bridge.candidate.Release.ReleaseChannel = "gray"
			bridge.candidate.Release.FencingToken = 2
			scheduler := (&dnsReadinessScheduler{}).shareHistory(&dnsProbeHistoryStore{})
			for i := range nextFacts {
				nextFacts[i].Ready = false
			}
			view := bridge.payload.Queries[0]
			next, err := buildDNSServingState(dnsServingCheckpoint{Parent: parent, Candidate: bridge.candidate}, bridge.payload, "next-route", view.NodeID, view.EdgeGroupID, nextFacts, now)
			if err != nil {
				t.Fatal(err)
			}
			old.facts = nil
			for i, requirement := range old.payload.Plan.Probes {
				fact := nextFacts[i]
				fact.Ready = true
				fact.Reason = ""
				fact.Proof.TrafficRelease = &model.TrafficReleaseBinding{ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, RouteArtifactID: "next-route", ReleaseID: bridge.candidate.Release.ID, ReleaseChannel: "gray", FencingToken: 2, ScopeKey: "global"}
				if scenario == "old-history-only" {
					fact.ProbeID = requirement.ID
				}
				shift := time.Duration(0)
				if scenario == "expired-successor-history" {
					shift = -time.Hour
				}
				for _, offset := range []time.Duration{-61 * time.Second, -31 * time.Second, -time.Second} {
					fact.Proof.CheckedAt = now.Add(shift + offset)
					scheduler.observeProbeHistory(&fact, bridge.payload.Policy.DNSReadiness, now.Add(shift+offset))
				}
				if scenario == "failed-successor" {
					fact.Ready = false
					scheduler.observeProbeHistory(&fact, bridge.payload.Policy.DNSReadiness, now)
					fact.Ready = true
				}
				fact.ProbeID = requirement.ID
				fact.Reason = "route_digest_mismatch"
				fact.Ready = false
				fact.Proof.CheckedAt = now.Add(time.Millisecond)
				old.facts = append(old.facts, fact)
			}
			result, changed := dnsSupplementTransitionFacts(next, old, now.Add(2*time.Millisecond), scheduler)
			if !changed {
				t.Fatal("exact late proof missing")
			}
			policy := &model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
			for _, fact := range result {
				if len(fact.ProbeHistory) != 1 || dnsAvailabilityRecovered(policy, fact, time.Duration(bridge.payload.Policy.DNSReadiness.FactFreshnessSeconds)*time.Second, now.Add(2*time.Millisecond)) {
					t.Fatal("late proof inherited unqualified recovery", fact.ProbeHistory)
				}
			}
		})
	}
}
