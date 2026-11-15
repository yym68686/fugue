package dnsserver

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"github.com/miekg/dns"
)

func TestDecisionDistinguishesPhysicalMembershipFromAnswerLimit(t *testing.T) {
	for _, kind := range []string{model.DNSAnswerPolicyKindPhysicalQuality, model.DNSAnswerPolicyKindPhysicalOrder} {
		for _, primaryReady := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/empty", true: "/primary"}[primaryReady], func(t *testing.T) {
				now := time.Now().UTC()
				input := physicalSelectionTestRecord(now)
				want := "outside_physical_selection"
				input.AnswerPolicy.PhysicalSelection.OrderedEdgeIDs = []string{"edge-b"}
				if kind == model.DNSAnswerPolicyKindPhysicalOrder {
					input.AnswerPolicy = declaredOrderTestRecord().AnswerPolicy
					input.AnswerPolicy.PhysicalOrder.OrderedEdgeIDs = []string{"edge-b"}
					want = "outside_physical_order"
				}
				materialized := input
				if !primaryReady {
					materialized.Candidates = append(input.Candidates[:0:0], input.Candidates[0], input.Candidates[2])
				}
				_, audit, _ := rrForEdgeDNSRecordWithGeoAuditAt(materialized, input.Name, dns.TypeA, dnsGeoHint{}, nil, nil, now)
				before, _ := json.Marshal(input)
				capture := &dnsDecisionCapture{}
				capture.describeRecord(dnsServingRecord{record: input}, &materialized, dnsGeoHint{}, now, audit, "")
				outside := 0
				for _, filtered := range capture.records[0].Filtered {
					if filtered.EdgeID != "edge-b" {
						outside++
						if filtered.Reason != want {
							t.Errorf("outside member misclassified: %+v", filtered)
						}
					}
				}
				if outside != 2 || primaryReady && len(audit.Answered) != 1 || !primaryReady && len(audit.Answered) != 0 {
					t.Fatal("fixture did not retain both excluded members and the original answer", capture.records)
				}
				after, _ := json.Marshal(input)
				if string(before) != string(after) || !reflect.DeepEqual(capture.records[0].Answered, append([]model.EdgeDNSAnswerCandidate{}, audit.Answered...)) {
					t.Fatal("diagnostic changed policy or selection")
				}
			})
		}
	}
	// A ranked backup really omitted by the one-answer cap retains that reason.
	now := time.Now().UTC()
	record := physicalSelectionTestRecord(now)
	_, audit, _ := rrForEdgeDNSRecordWithGeoAuditAt(record, record.Name, dns.TypeA, dnsGeoHint{}, nil, nil, now)
	capture := &dnsDecisionCapture{}
	capture.describeRecord(dnsServingRecord{record: record}, &record, dnsGeoHint{}, now, audit, "")
	found := false
	for _, filtered := range capture.records[0].Filtered {
		if filtered.EdgeID == "edge-a" {
			found = true
			if filtered.Reason != "not_selected_by_answer_limit" {
				t.Fatal("real answer limit was relabeled", filtered)
			}
		}
	}
	if !found {
		t.Fatal("real answer-limit omission not recorded")
	}
}

func TestDecisionAvailabilityFallbackDoesNotMislabelAuthorizedBackup(t *testing.T) {
	for _, version := range []string{model.DNSAvailabilityVersion, model.DNSAvailabilityRedundantVersion} {
		t.Run(version, func(t *testing.T) { testDecisionAvailabilityBackup(t, version) })
	}
}

func testDecisionAvailabilityBackup(t *testing.T, version string) {
	now := time.Now().UTC()
	input := physicalSelectionTestRecord(now)
	input.AnswerPolicy.PhysicalSelection.OrderedEdgeIDs = []string{"edge-b"}
	input.AnswerPolicy.Availability = availabilityTestPolicy()
	input.AnswerPolicy.Availability.OrderedEdgeIDs = []string{"edge-a", "unranked"}
	materialized := input
	materialized.Candidates = append(input.Candidates[:0:0], input.Candidates[0], input.Candidates[2])
	wantAnswers := 1
	if version == model.DNSAvailabilityRedundantVersion {
		input.AnswerPolicy.Availability.Version = version
		input.AnswerPolicy.Availability.OrderedEdgeIDs = []string{"edge-b", "edge-a", "unranked"}
		input.AnswerPolicy.Availability.MaxAnswers = 2
		materialized = input
		wantAnswers = 2
	}
	_, audit, _ := rrForEdgeDNSRecordWithGeoAuditAt(materialized, input.Name, dns.TypeA, dnsGeoHint{}, nil, nil, now)
	if len(audit.Answered) != wantAnswers || version == model.DNSAvailabilityVersion && audit.SelectionResult != "availability_readiness_failover" {
		t.Fatal("fixture did not execute independent fallback", audit)
	}
	capture := &dnsDecisionCapture{}
	capture.describeRecord(dnsServingRecord{record: input}, &materialized, dnsGeoHint{}, now, audit, "")
	found := false
	for _, filtered := range capture.records[0].Filtered {
		if filtered.EdgeID == "unranked" {
			found = true
			if filtered.Reason != "not_selected_by_answer_limit" {
				t.Fatal("authorized fallback candidate incorrectly excluded by quality order", filtered)
			}
		}
	}
	if !found {
		t.Fatal("authorized backup omission not recorded")
	}
}

func TestDecisionPhysicalFilterReasonsReplayNewAndHistoricalReceipts(t *testing.T) {
	state, now := decisionTestState(t)
	entry := &state.zones["example.test"].records["target.example.test"][0]
	entry.record.AnswerPolicy = physicalSelectionTestRecord(now).AnswerPolicy
	entry.record.AnswerPolicy.PhysicalSelection.OrderedEdgeIDs = []string{"edge-b"}
	for i := range entry.plan.Records {
		entry.plan.Records[i].MinimumHealthyEdges = 1
	}
	// Existing fixture has edge-a and edge-b. Remove only edge-b's readiness.
	for i := range entry.facts {
		for _, probe := range entry.plan.Probes {
			if entry.facts[i].ProbeID == probe.ID && probe.EdgeID == "edge-b" {
				entry.facts[i].Ready = false
			}
		}
	}
	query := new(dns.Msg)
	query.SetQuestion("target.example.test.", dns.TypeA)
	receipt := captureDecision(t, state, query, "198.51.100.1:53", now)
	if receipt.RCode != dns.RcodeServerFailure {
		t.Fatal("expected empty physical selection", receipt)
	}
	var input dnsDecisionReplay
	if err := json.Unmarshal(receipt.ReplayInput, &input); err != nil || input.FilterReasonsVersion != dnsDecisionFilterReasonsVersion {
		t.Fatal("new receipt omitted diagnostic replay version", err)
	}
	found := false
	for _, filtered := range receipt.Records[0].Filtered {
		if filtered.EdgeID == "edge-a" && filtered.Reason == "outside_physical_selection" {
			found = true
		}
	}
	if !found {
		t.Fatal("new receipt still misreports physical rejection", receipt.Records)
	}
	if replay, err := ReplayDNSDecision(receipt); err != nil || !replay.Matched {
		t.Fatal("new diagnostic did not replay", err)
	}
	// Simulate the exact legacy wire semantics: no diagnostic subversion and
	// the original incorrect reason. Its evidence digest must still validate.
	input.FilterReasonsVersion = 0
	receipt.ReplayInput, _ = json.Marshal(input)
	for i := range receipt.Records[0].Filtered {
		if receipt.Records[0].Filtered[i].Reason == "outside_physical_selection" {
			receipt.Records[0].Filtered[i].Reason = "not_selected_by_answer_limit"
		}
	}
	receipt.EvidenceDigest = dnsDecisionDigest(receipt)
	if replay, err := ReplayDNSDecision(receipt); err != nil || !replay.Matched {
		t.Fatal("historical diagnostic no longer replays", err)
	}
	input.FilterReasonsVersion = dnsDecisionFilterReasonsVersion + 1
	receipt.ReplayInput, _ = json.Marshal(input)
	receipt.EvidenceDigest = dnsDecisionDigest(receipt)
	if _, err := ReplayDNSDecision(receipt); err == nil {
		t.Fatal("unknown diagnostic version accepted")
	}
}
