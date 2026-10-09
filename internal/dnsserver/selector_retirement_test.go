package dnsserver

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func physicalRecordForTest(record model.EdgeDNSRecord) model.EdgeDNSRecord {
	if len(record.Candidates) == 0 {
		return record
	}
	order := &model.DNSPhysicalOrder{Version: "physical-order-v1"}
	for index := range record.Candidates {
		if record.Candidates[index].EdgeID == "" {
			record.Candidates[index].EdgeID = fmt.Sprintf("edge-%d", index)
		}
		order.OrderedEdgeIDs = append(order.OrderedEdgeIDs, record.Candidates[index].EdgeID)
	}
	record.AnswerPolicy = model.DNSAnswerPolicy{PolicyKind: model.DNSAnswerPolicyKindPhysicalOrder, PhysicalOrder: order, HealthRequired: record.AnswerPolicy.HealthRequired, RouteReadyRequired: record.AnswerPolicy.RouteReadyRequired}
	record.ScopedCandidates = nil
	return record
}

func setPhysicalBundleForTest(t *testing.T, service *Service, bundle model.EdgeDNSBundle, etag string, stale bool, lastError string) {
	t.Helper()
	for index := range bundle.Records {
		bundle.Records[index] = physicalRecordForTest(bundle.Records[index])
	}
	service.setBundle(bundle, etag, stale, lastError)
}

func historicalDNSCandidatesForTest(record model.EdgeDNSRecord, hint dnsGeoHint, at time.Time) []model.EdgeDNSAnswerCandidate {
	candidates, _ := replayLegacyDNSCandidateOrder(record, hint, at, false)
	return candidates
}

func historicalDNSAnswerCandidatesForTest(record model.EdgeDNSRecord, hint dnsGeoHint, at time.Time, health edgeDNSLiveHealthFunc, peer edgeDNSPeerHealthFunc) ([]model.EdgeDNSAnswerCandidate, []edgeDNSFilteredCandidate, edgeDNSCandidateOrderDecision) {
	candidates, decision := replayLegacyDNSCandidateOrder(record, hint, at, health != nil)
	return filterOrderedDNSCandidates(record, candidates, decision, health, peer)
}

func TestRetiredSelectorsCannotEnterLiveServingPath(t *testing.T) {
	for _, mode := range []string{"geo", "latency_aware", "weighted", "global", "pinned", "disabled", ""} {
		record := declaredOrderTestRecord()
		record.AnswerPolicy.PolicyKind = mode
		record.AnswerPolicy.PhysicalOrder = nil
		record.AnswerPolicy.ExplorationPercent = 50
		views := []platformconfig.DNSQueryView{{Records: []model.EdgeDNSRecord{record}}}
		if err := validateServingDNSSelectors(views); err == nil {
			t.Fatal("retired candidate admitted", mode)
		}
		if state, err := buildDNSServingState(dnsServingCheckpoint{}, dnsServingPayload{Queries: views}, "", "", "", nil, time.Now()); state != nil || err == nil {
			t.Fatal("retired candidate built a serving snapshot", mode)
		}
		if answers, err := executeDNSQueryRecord(record, dnsGeoHint{}, time.Now()); len(answers) != 0 || err == nil {
			t.Fatal("retired selector executed", mode)
		}
		for _, hint := range []dnsGeoHint{{}, {Country: "aa", Source: "ecs"}, {decisionEntropy: &dnsDecisionEntropy{}}} {
			if ordered, _ := edgeDNSOrderedCandidatesWithDecision(record, hint, time.Now(), true); len(ordered) != 0 {
				t.Fatal("live fallback invoked historical algorithm", mode)
			}
		}
	}
}

func TestHistoricalDNSReceiptsReplayAfterSelectorRetirement(t *testing.T) {
	raw, err := os.ReadFile("testdata/retired-selector-receipts.json")
	if err != nil {
		t.Fatal(err)
	}
	var receipts []DNSDecisionReceipt
	if err := json.Unmarshal(raw, &receipts); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, receipt := range receipts {
		result, err := ReplayDNSDecision(receipt)
		if err != nil || !result.Matched {
			t.Fatal(receipt.DecisionID, err)
		}
		seen[receipt.Records[0].ExplorationKind] = true
		if result.Records[0].Policy.PolicyKind != "latency_aware" {
			t.Fatal("history reinterpreted with current selector")
		}
	}
	if !seen[""] || !seen["same_group"] || !seen["cross_group"] {
		t.Fatal("historical exploration coverage lost", seen)
	}
}
