package dnsserver

import (
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDNSForwardQualityReadmitsOnlySignedCandidatesWithSuccessorProofs(t *testing.T) {
	old, bridge, facts, now := narrowedPhysicalTransitionFixture(t, false)
	prior := model.CloneDNSPhysicalSelection(bridge.payload.Queries[0].Records[0].AnswerPolicy.PhysicalSelection)
	prior.PrimaryEdgeID, prior.OrderedEdgeIDs = "edge-b", []string{"edge-b"}
	policy := &old.payload.Queries[0].Records[0].AnswerPolicy
	policy.PolicyKind, policy.PhysicalOrder, policy.PhysicalSelection = model.DNSAnswerPolicyKindPhysicalQuality, nil, prior
	policy.RankingVersion, policy.RankingScope = model.DNSPhysicalSelectionVersion, "global"
	old.payload.Policy.DNSAnswerRules[0].SelectionMode, old.payload.Policy.DNSAnswerRules[0].PhysicalOrder = model.DNSAnswerPolicyKindPhysicalQuality, nil
	selection := bridge.payload.Queries[0].Records[0].AnswerPolicy.PhysicalSelection
	selection.PrimaryEdgeID, selection.OrderedEdgeIDs, selection.PrimarySince = "edge-a", []string{"edge-a", "edge-b"}, &now
	before, after := mustDNSJSON(t, old.payload), mustDNSJSON(t, bridge.payload)
	assertDNSForwardOrderAnswer(t, old, bridge, facts, now, "8.8.8.8")
	selection.OrderedEdgeIDs = append(selection.OrderedEdgeIDs, "foreign")
	if dnsTransitionRecords(old, bridge)[old.payload.Plan.Records[0].Hostname] {
		t.Fatal("selection introduced an endpoint outside the signed inventory")
	}
	selection.OrderedEdgeIDs = selection.OrderedEdgeIDs[:2]
	if !reflect.DeepEqual(before, mustDNSJSON(t, old.payload)) || !reflect.DeepEqual(after, mustDNSJSON(t, bridge.payload)) {
		t.Fatal("readmission changed signed inputs")
	}
}

func TestDNSForwardPhysicalOrderChecksSharedWildcardRuleBeforeProjection(t *testing.T) {
	old, bridge, _, _ := narrowedPhysicalTransitionFixture(t, false)
	bridge.payload.Queries[0].Records[0].AnswerPolicy.PhysicalSelection.OrderedEdgeIDs = []string{"edge-a", "edge-b"}
	for _, payload := range []*dnsServingPayload{&old.payload, &bridge.payload} {
		payload.Policy.DNSAnswerRules[0].NodeID = ""
		other := payload.Queries[0]
		other.NodeID = "other-dns"
		payload.Queries = append(payload.Queries, other)
	}
	if !dnsTransitionRecords(old, bridge)[old.payload.Plan.Records[0].Hostname] {
		t.Fatal("projecting one node's shared rule vetoed the second node")
	}
	bridge.payload.Policy.DNSAnswerRules = append(bridge.payload.Policy.DNSAnswerRules, platformconfig.DNSAnswerRule{
		NodeID: "other-dns", Hostname: old.payload.Plan.Records[0].Hostname, Type: "A", SelectionMode: model.DNSAnswerPolicyKindPhysicalOrder,
		PhysicalOrder: &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"foreign"}},
	})
	if dnsTransitionRecords(old, bridge)[old.payload.Plan.Records[0].Hostname] {
		t.Fatal("node-specific rule disagreement was hidden by wildcard projection")
	}
}
