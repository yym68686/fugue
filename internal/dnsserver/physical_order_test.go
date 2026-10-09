package dnsserver

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"github.com/miekg/dns"
)

func declaredOrderTestRecord() model.EdgeDNSRecord {
	record := physicalSelectionTestRecord(time.Now())
	record.AnswerPolicy = model.DNSAnswerPolicy{PolicyKind: model.DNSAnswerPolicyKindPhysicalOrder,
		HealthRequired: true, RouteReadyRequired: true,
		PhysicalOrder: &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-b", "edge-a"}}}
	return record
}

func TestDeclaredPhysicalOrderIgnoresScoresAndOnlyFailsOverOnReadiness(t *testing.T) {
	record := declaredOrderTestRecord()
	for index := 0; index < 100; index++ {
		ordered, decision := edgeDNSOrderedCandidatesWithDecision(record, dnsGeoHint{Country: "aa"}, time.Unix(int64(index)*600, 0), false)
		if len(ordered) != 2 || ordered[0].EdgeID != "edge-b" || decision.ExplorationKind != "" || edgeDNSSelectionResult(decision, ordered, nil) != "physical_order_primary" {
			t.Fatal("configured order overridden", ordered, decision)
		}
	}
	record.Candidates[1].RouteReady = false
	ordered, decision := edgeDNSOrderedCandidatesWithDecision(record, dnsGeoHint{}, time.Now(), false)
	if len(ordered) != 1 || ordered[0].EdgeID != "edge-a" || edgeDNSSelectionResult(decision, ordered, nil) != "physical_order_readiness_failover" {
		t.Fatal("readiness failover lost", ordered, decision)
	}
	record.Candidates[0].RouteReady = false
	ordered, decision = edgeDNSOrderedCandidatesWithDecision(record, dnsGeoHint{}, time.Now(), false)
	if len(ordered) != 0 || edgeDNSSelectionResult(decision, ordered, nil) != "physical_order_no_ready_endpoint" {
		t.Fatal("unconfigured endpoint gained authority", ordered)
	}
}

func TestDeclaredPhysicalOrderActualAnswerReplays(t *testing.T) {
	state, now := decisionTestState(t)
	payload := state.payload
	for viewIndex := range payload.Queries {
		for recordIndex := range payload.Queries[viewIndex].Records {
			record := &payload.Queries[viewIndex].Records[recordIndex]
			if record.Name == "target.example.test" {
				record.AnswerPolicy = declaredOrderTestRecord().AnswerPolicy
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
	if err != nil || !replayed.Matched || len(receipt.RRSet) != 1 || !strings.Contains(receipt.RRSet[0], "9.9.9.9") {
		t.Fatal("actual configured answer did not replay", replayed, err)
	}
}
