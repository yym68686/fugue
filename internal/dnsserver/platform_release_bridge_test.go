package dnsserver

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDNSReleaseBridgeSeparatesRankingFactsFromRecordAuthority(t *testing.T) {
	view, plan, readiness, _, now := queryExecutionFixture()
	view.Records[0].ScopedCandidates = []model.EdgeDNSScopedAnswerCandidates{{ScopeKey: "region:aa", Region: "aa", Candidates: append([]model.EdgeDNSAnswerCandidate(nil), view.Records[0].Candidates...)}}
	otherProbe := plan.Probes[0]
	otherProbe.ID, otherProbe.Hostname = "sha256:"+strings.Repeat("d", 64), "other.example.test"
	plan.Probes = append(plan.Probes, otherProbe)
	otherTarget := plan.Records[0].Targets[0]
	otherTarget.ProbeIDs = []string{otherProbe.ID}
	plan.Records = append(plan.Records, platformconfig.DNSReadinessRecord{Hostname: otherProbe.Hostname, MinimumHealthyEdges: 1, Targets: []platformconfig.DNSReadinessTarget{otherTarget}})
	base := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}, Policy: platformconfig.PolicySnapshot{MaxStaleSeconds: 3600, DNSReadiness: &readiness,
		DNSAnswerRules: []platformconfig.DNSAnswerRule{{NodeID: view.NodeID, Hostname: plan.Records[0].Hostname, Type: "A", SelectionMode: "global"}},
	}}
	raw, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"score", "ranking order", "scoped ranking order", "ranking order changed weight", "scoped ranking changed address", "unrelated rule", "unrelated proof", "proof digest", "record rule", "address", "edge", "tenant", "weight", "quorum", "release replay"} {
		t.Run(change, func(t *testing.T) {
			var previous, next dnsServingPayload
			if json.Unmarshal(raw, &previous) != nil || json.Unmarshal(raw, &next) != nil {
				t.Fatal("invalid fixture")
			}
			old := &dnsServingState{payload: previous, record: dnsServingCheckpoint{Candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "old", ReleasedAt: now.Add(-time.Minute)}}}}
			bridge := &dnsReleaseBridge{payload: next, candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "new", ReleasedAt: now}}}
			index := -1
			for i, record := range bridge.payload.Queries[0].Records {
				if record.Name == plan.Records[0].Hostname && len(record.Candidates) > 0 {
					index = i
					break
				}
			}
			if index < 0 {
				t.Fatal("missing dynamic fixture record")
			}
			record := &bridge.payload.Queries[0].Records[index]
			want := false
			switch change {
			case "score":
				record.Candidates[0].Score = 913
				record.Candidates[0].ScoreBreakdown = map[string]float64{"latency": 300}
				record.Candidates[0].Reason = "new measurement"
				want = true
			case "ranking order", "ranking order changed weight":
				slices.Reverse(record.Candidates)
				record.Candidates[0].Score = 1000
				record.Candidates[0].ScoreBreakdown = map[string]float64{"latency": 100}
				want = true
				if change == "ranking order changed weight" {
					record.Candidates[0].Weight++
					want = false
				}
			case "scoped ranking order", "scoped ranking changed address":
				slices.Reverse(record.ScopedCandidates[0].Candidates)
				record.ScopedCandidates[0].Candidates[0].Score = 1000
				want = true
				if change == "scoped ranking changed address" {
					record.ScopedCandidates[0].Candidates[0].IP = "1.1.1.1"
					want = false
				}
			case "unrelated rule":
				bridge.payload.Policy.DNSAnswerRules = append(bridge.payload.Policy.DNSAnswerRules, platformconfig.DNSAnswerRule{NodeID: view.NodeID, Hostname: "other.example.test", Type: "A", SelectionMode: "geo"})
				want = true
			case "unrelated proof":
				last := len(bridge.payload.Plan.Probes) - 1
				bridge.payload.Plan.Probes[last].RouteDigest = "sha256:" + strings.Repeat("f", 64)
				want = true
			case "proof digest":
				bridge.payload.Plan.Probes[0].RouteDigest = "sha256:" + strings.Repeat("f", 64)
			case "record rule":
				bridge.payload.Policy.DNSAnswerRules[0].SelectionMode = "geo"
			case "address":
				record.Candidates[0].IP = "9.9.9.9"
			case "edge":
				record.Candidates[0].EdgeID = "other"
			case "tenant":
				record.TenantID = "other"
			case "weight":
				record.Candidates[0].Weight++
			case "quorum":
				bridge.payload.Plan.Records[0].MinimumHealthyEdges++
			case "release replay":
				bridge.candidate.Release.ReleasedAt = old.record.Candidate.Release.ReleasedAt
			}
			allowed := compatibleDNSReleaseProbes(old, bridge)
			for _, target := range plan.Records[0].Targets {
				for _, id := range target.ProbeIDs {
					wantProbe := want
					if change == "proof digest" && id != plan.Probes[0].ID {
						wantProbe = true
					}
					if allowed[id] != wantProbe {
						t.Fatalf("probe %s bridge=%v want=%v", id, allowed[id], wantProbe)
					}
				}
			}
			if after, _ := json.Marshal(previous); string(after) != string(raw) {
				t.Fatal("bridge mutated retained payload")
			}
		})
	}
}
