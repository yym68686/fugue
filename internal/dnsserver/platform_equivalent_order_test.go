package dnsserver

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDNSReleaseBridgePreservesEquivalentPhysicalOrder(t *testing.T) {
	view, plan, readiness, _, now := queryExecutionFixture()
	record := &view.Records[0]
	order := &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}, PrimarySince: &now}
	record.AnswerPolicy = model.DNSAnswerPolicy{PolicyKind: model.DNSAnswerPolicyKindPhysicalOrder, PhysicalOrder: order, HealthRequired: true, RouteReadyRequired: true, TTLSeconds: 60, AllowedEdgeGroups: []string{view.EdgeGroupID}, Reason: "preserved_order"}
	base := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}, Policy: platformconfig.PolicySnapshot{MaxStaleSeconds: 3600, DNSReadiness: &readiness,
		DNSAnswerRules: []platformconfig.DNSAnswerRule{{NodeID: view.NodeID, Hostname: record.Name, Type: record.Type, SelectionMode: model.DNSAnswerPolicyKindPhysicalOrder, PhysicalOrder: order, TTLSeconds: 60}}}}
	raw := mustDNSJSON(t, base)
	for _, scenario := range []string{"equivalent", "reverse", "order", "epoch", "scope", "business", "address", "tenant", "quorum", "ecs", "ttl", "missing rule", "foreign node", "unknown version"} {
		t.Run(scenario, func(t *testing.T) {
			var previous, next dnsServingPayload
			json.Unmarshal(raw, &previous)
			json.Unmarshal(raw, &next)
			policy := &next.Queries[0].Records[0].AnswerPolicy
			selection := &model.DNSPhysicalSelection{Version: model.DNSPhysicalSelectionVersion, Scope: "global", PrimaryEdgeID: "edge-a", OrderedEdgeIDs: []string{"edge-a", "edge-b"}, PrimarySince: &now}
			policy.PhysicalOrder, policy.PhysicalSelection, policy.PolicyKind = nil, selection, model.DNSAnswerPolicyKindPhysicalQuality
			policy.RankingVersion, policy.RankingScope, policy.Reason = model.DNSPhysicalSelectionVersion, "global", "measured_order"
			next.Policy.DNSAnswerRules[0].SelectionMode, next.Policy.DNSAnswerRules[0].PhysicalOrder = model.DNSAnswerPolicyKindPhysicalQuality, nil
			switch scenario {
			case "reverse":
				previous, next = next, previous
			case "order":
				slices.Reverse(selection.OrderedEdgeIDs)
				selection.PrimaryEdgeID = "edge-b"
			case "epoch":
				changed := now.Add(time.Second)
				selection.PrimarySince = &changed
			case "scope":
				selection.Scope = "region:aa"
			case "business":
				selection.BusinessAllocation = &model.DNSBusinessAllocation{}
			case "address":
				next.Queries[0].Records[0].Candidates[0].IP = "192.0.2.99"
			case "tenant":
				next.Queries[0].Records[0].TenantID = "foreign"
			case "quorum":
				next.Plan.Records[0].MinimumHealthyEdges++
			case "ecs":
				next.Policy.DNSAnswerRules[0].ECSEnabled = true
			case "ttl":
				policy.TTLSeconds++
			case "missing rule":
				next.Policy.DNSAnswerRules = nil
			case "foreign node":
				next.Queries[0].NodeID = "foreign"
			case "unknown version":
				selection.Version = "future-selection"
			}
			oldRaw, nextRaw := mustDNSJSON(t, previous), mustDNSJSON(t, next)
			old := &dnsServingState{payload: previous, record: dnsServingCheckpoint{Positive: true, AppliedAt: now.Add(-time.Minute), Candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "old", ReleasedAt: now.Add(-time.Minute)}}}}
			bridge := &dnsReleaseBridge{payload: next, candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "next", ReleasedAt: now}}}
			want := scenario == "equivalent" || scenario == "reverse"
			allowed := compatibleDNSReleaseProbes(old, bridge)
			for _, target := range plan.Records[0].Targets {
				for _, id := range target.ProbeIDs {
					if allowed[id] != want {
						t.Fatal("equivalent order proof authorization", scenario, allowed)
					}
				}
			}
			if dnsTransitionRecords(old, bridge)[record.Name] != want {
				t.Fatal("equivalent order transition authorization", scenario)
			}
			if !reflect.DeepEqual(oldRaw, mustDNSJSON(t, previous)) || !reflect.DeepEqual(nextRaw, mustDNSJSON(t, next)) {
				t.Fatal("comparison mutated signed policy")
			}
		})
	}
}
