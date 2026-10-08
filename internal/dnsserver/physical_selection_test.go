package dnsserver

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"github.com/miekg/dns"
)

func physicalSelectionTestRecord(now time.Time) model.EdgeDNSRecord {
	digest := "sha256:" + strings.Repeat("a", 64)
	return model.EdgeDNSRecord{Name: "app.example.test", Type: "A", TTL: 60,
		AnswerPolicy: model.DNSAnswerPolicy{PolicyKind: model.DNSAnswerPolicyKindPhysicalQuality, HealthRequired: true, RouteReadyRequired: true,
			PhysicalSelection: &model.DNSPhysicalSelection{Version: model.DNSPhysicalSelectionVersion, PrimaryEdgeID: "edge-b", OrderedEdgeIDs: []string{"edge-b", "edge-a"},
				EvidenceDigest: digest, DNSReceiptID: "actual-answer-1", LoadedDigest: digest, PolicyDigest: digest, Scope: "global", CapturedAt: now}},
		Candidates: []model.EdgeDNSAnswerCandidate{
			{IP: "192.0.2.10", EdgeID: "edge-a", EdgeGroupID: "shared", Country: "aa", Score: 1, Weight: 9999, Healthy: true, RouteReady: true, TLSReady: true, DNSEligible: true},
			{IP: "192.0.2.11", EdgeID: "edge-b", EdgeGroupID: "shared", Country: "bb", Score: 999999, Priority: 9999, Healthy: true, RouteReady: true, TLSReady: true, DNSEligible: true},
			{IP: "192.0.2.12", EdgeID: "unranked", EdgeGroupID: "other", Healthy: true, RouteReady: true, TLSReady: true, DNSEligible: true},
		}}
}

func TestPhysicalDNSOrderIgnoresLegacyScoringAndGeography(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	record := physicalSelectionTestRecord(now)
	before, _ := json.Marshal(record)
	for offset := 0; offset < 100; offset++ {
		ordered, decision := edgeDNSOrderedCandidatesWithDecision(record, dnsGeoHint{Country: "aa", EdgeGroupID: "other"}, now.Add(time.Duration(offset)*time.Hour), false)
		if len(ordered) != 2 || ordered[0].EdgeID != "edge-b" || ordered[1].EdgeID != "edge-a" || decision.ExplorationKind != "" {
			t.Fatal(ordered, decision)
		}
		if edgeDNSSelectionResult(decision, ordered[:1], nil) != "physical_selected_primary" {
			t.Fatal("incorrect physical primary receipt")
		}
	}
	after, _ := json.Marshal(record)
	if string(before) != string(after) {
		t.Fatal("selection mutated signed input")
	}
}

func TestPhysicalDNSFastFailoverToSameGroupSibling(t *testing.T) {
	record := physicalSelectionTestRecord(time.Now())
	record.Candidates[1].RouteReady = false
	ordered, decision := edgeDNSOrderedCandidatesWithDecision(record, dnsGeoHint{}, time.Now(), false)
	if len(ordered) != 1 || ordered[0].EdgeID != "edge-a" || edgeDNSSelectionResult(decision, ordered, nil) != "physical_readiness_failover" {
		t.Fatal(ordered, decision)
	}
	record.Candidates[0].RouteReady = false
	ordered, decision = edgeDNSOrderedCandidatesWithDecision(record, dnsGeoHint{}, time.Now(), false)
	if len(ordered) != 0 || edgeDNSSelectionResult(decision, ordered, nil) != "physical_no_ready_endpoint" {
		t.Fatal("fell back to legacy or unranked endpoint", ordered)
	}
}

func TestPhysicalDNSActualReceiptReplays(t *testing.T) {
	state, now := decisionTestState(t)
	payload := state.payload
	for viewIndex := range payload.Queries {
		for recordIndex := range payload.Queries[viewIndex].Records {
			record := &payload.Queries[viewIndex].Records[recordIndex]
			if record.Name == "target.example.test" {
				record.AnswerPolicy = physicalSelectionTestRecord(now).AnswerPolicy
			}
		}
	}
	state, err := buildDNSServingState(state.record, payload, "route", "dns-a", "edge-group-a", state.facts, now)
	if err != nil {
		t.Fatal(err)
	}
	request := new(dns.Msg)
	request.SetQuestion("target.example.test.", dns.TypeA)
	receipt := captureDecision(t, state, request, "198.51.100.2:1234", now)
	replayed, err := ReplayDNSDecision(receipt)
	if err != nil || !replayed.Matched || len(receipt.RRSet) != 1 || !strings.Contains(receipt.RRSet[0], "9.9.9.9") || !reflect.DeepEqual(replayed.RRSet, receipt.RRSet) {
		t.Fatal(replayed, err)
	}
}

func TestPhysicalDNSMaterializationRetainsSignedPrimaryWhenRejected(t *testing.T) {
	view, plan, policy, facts, now := queryExecutionFixture()
	plan.Records[0].MinimumHealthyEdges = 1
	view.Records[0].AnswerPolicy = physicalSelectionTestRecord(now).AnswerPolicy
	for index := range facts {
		for _, probe := range plan.Probes {
			if facts[index].ProbeID == probe.ID && probe.EdgeID == "edge-b" {
				facts[index].Ready = false
			}
		}
	}
	records, err := materializeDNSQueries(view, &plan, &policy, facts, now)
	if err != nil {
		t.Fatal(err)
	}
	ordered, decision := edgeDNSOrderedCandidatesWithDecision(records[0], dnsGeoHint{}, now, false)
	if len(ordered) != 1 || ordered[0].EdgeID != "edge-a" || edgeDNSSelectionResult(decision, ordered, nil) != "physical_readiness_failover" {
		t.Fatal("materialization changed signed authority", ordered, decision)
	}
}
