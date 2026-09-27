package dnsserver

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDNSReleaseBridgeSeparatesRankingFactsFromRecordAuthority(t *testing.T) {
	view, plan, readiness, _, now := queryExecutionFixture()
	base := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}, Policy: platformconfig.PolicySnapshot{MaxStaleSeconds: 3600, DNSReadiness: &readiness,
		DNSAnswerRules: []platformconfig.DNSAnswerRule{{NodeID: view.NodeID, Hostname: plan.Records[0].Hostname, Type: "A", SelectionMode: "global"}},
	}}
	raw, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"score", "unrelated rule", "record rule", "address", "edge", "tenant", "weight", "quorum", "release replay"} {
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
			case "unrelated rule":
				bridge.payload.Policy.DNSAnswerRules = append(bridge.payload.Policy.DNSAnswerRules, platformconfig.DNSAnswerRule{NodeID: view.NodeID, Hostname: "other.example.test", Type: "A", SelectionMode: "geo"})
				want = true
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
					if allowed[id] != want {
						t.Fatalf("probe %s bridge=%v want=%v", id, allowed[id], want)
					}
				}
			}
			if after, _ := json.Marshal(previous); string(after) != string(raw) {
				t.Fatal("bridge mutated retained payload")
			}
		})
	}
}
