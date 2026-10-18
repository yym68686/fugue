package dnsserver

import (
	"reflect"
	"testing"

	"fugue/internal/model"
)

func TestDNSStableReplacementPreservesIndependentAuthority(t *testing.T) {
	for _, scenario := range []string{"replace", "tenant", "app", "policy", "fallback", "cookie", "header", "missing stable", "old missing owner", "old candidate", "new candidate", "old weights", "new weights", "old mode", "new mode", "endpoint", "quorum", "duplicate previous", "duplicate next", "older publication"} {
		t.Run(scenario, func(t *testing.T) {
			old, bridge, _, _ := initialCanaryDNSFixture(t)
			before, after := &old.payload.Policy.TrafficConstraints[0], &bridge.payload.Policy.TrafficConstraints[0]
			after.Mode, after.CandidateReleaseID = model.AppTrafficModeSingle, ""
			after.StableReleaseID, after.StableWeight, after.CandidateWeight = "replacement-stable", 100, 0
			switch scenario {
			case "tenant":
				after.TenantID = "other-tenant"
			case "app":
				after.AppID = "other-app"
			case "policy":
				after.ID = "other-policy"
			case "fallback":
				after.UnavailableCandidate = "fail"
			case "cookie":
				after.StickyCookie = "other-cookie"
			case "header":
				after.StickyHeader = "other-header"
			case "missing stable":
				after.StableReleaseID = ""
			case "old missing owner":
				before.TenantID, after.TenantID = "", ""
			case "old candidate":
				before.CandidateReleaseID = after.StableReleaseID
			case "new candidate":
				after.CandidateReleaseID = "inactive-candidate"
			case "old weights":
				before.StableWeight = 0
			case "new weights":
				after.StableWeight = 99
			case "old mode":
				before.Mode = model.AppTrafficModeCanary
				before.CandidateReleaseID, before.StableWeight, before.CandidateWeight = "previous-candidate", 75, 25
			case "new mode":
				after.Mode = model.AppTrafficModeCanary
				after.CandidateReleaseID, after.StableWeight, after.CandidateWeight = "new-candidate", 99, 1
			case "endpoint":
				bridge.payload.Queries[0].Records[0].Candidates[0].IP = "192.0.2.99"
			case "quorum":
				bridge.payload.Plan.Records[0].MinimumHealthyEdges--
			case "duplicate previous":
				old.payload.Policy.TrafficConstraints = append(old.payload.Policy.TrafficConstraints, *before)
			case "duplicate next":
				bridge.payload.Policy.TrafficConstraints = append(bridge.payload.Policy.TrafficConstraints, *after)
			case "older publication":
				bridge.candidate.Release.ReleasedAt = old.record.Candidate.Release.ReleasedAt
			}
			previousBytes, nextBytes := mustDNSJSON(t, old.payload), mustDNSJSON(t, bridge.payload)
			if got := dnsTransitionRecords(old, bridge)[old.payload.Plan.Records[0].Hostname]; got != (scenario == "replace") {
				t.Fatalf("replacement permission=%t for %s", got, scenario)
			}
			for _, allowed := range compatibleDNSReleaseProbes(old, bridge) {
				if allowed {
					t.Fatal("replacement proof was rebound into the old stable publication")
				}
			}
			if !reflect.DeepEqual(previousBytes, mustDNSJSON(t, old.payload)) || !reflect.DeepEqual(nextBytes, mustDNSJSON(t, bridge.payload)) {
				t.Fatal("comparison changed signed inputs")
			}
		})
	}
}
