package platformconfig

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func physicalQueryFixture() CompileRequest {
	request := dnsQueryFixture()
	rule := &request.Policy.DNSAnswerRules[0]
	rule.SelectionMode = model.DNSAnswerPolicyKindPhysicalQuality
	rule.ScopedSelectionMode = ""
	rule.PreferredEdgeGroups = nil
	rule.FallbackEdgeGroups = nil
	rule.ExplorationPercent = 0
	fact := &request.RuntimeSnapshot.DNSSelections[0]
	fact.SelectedEdgeGroupID = ""
	fact.ScopedCandidates = nil
	fact.RankingVersion = model.DNSPhysicalSelectionVersion
	digest := "sha256:" + strings.Repeat("a", 64)
	fact.PhysicalSelection = &model.DNSPhysicalSelection{Version: model.DNSPhysicalSelectionVersion, PrimaryEdgeID: "edge-b", OrderedEdgeIDs: []string{"edge-b", "edge-a"},
		EvidenceDigest: digest, DNSReceiptID: "actual-answer-1", LoadedDigest: digest, PolicyDigest: digest, Scope: "global", CapturedAt: fact.ObservedAt}
	rebindPlacement(&request)
	return request
}

func TestPhysicalQueryCompilesImmutableOrderWithoutReadiness(t *testing.T) {
	request := physicalQueryFixture()
	before, _ := json.Marshal(request)
	compiled, err := Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	views := compiledQueryViews(t, compiled)
	record := views[0].Records[0]
	if record.AnswerPolicy.PhysicalSelection.PrimaryEdgeID != "edge-b" || len(record.ScopedCandidates) != 0 || record.AnswerPolicy.SelectedEdgeGroupID != "" {
		t.Fatal("compiled legacy selection authority", record)
	}
	for _, candidate := range record.Candidates {
		if candidate.Healthy || candidate.RouteReady || candidate.TLSReady || candidate.DNSEligible {
			t.Fatal("compiled fabricated readiness")
		}
	}
	after, _ := json.Marshal(request)
	if string(before) != string(after) {
		t.Fatal("mutated caller-owned observations")
	}
	normalized := normalizeDNSSelections(request.RuntimeSnapshot.DNSSelections)
	normalized[0].PhysicalSelection.OrderedEdgeIDs[0] = "other"
	if request.RuntimeSnapshot.DNSSelections[0].PhysicalSelection.OrderedEdgeIDs[0] != "edge-b" {
		t.Fatal("physical order aliases mutable caller state")
	}
}

func TestPhysicalQueryRejectsConflictingAuthority(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*CompileRequest)
	}{
		{"missing_evidence", func(request *CompileRequest) { request.RuntimeSnapshot.DNSSelections[0].PhysicalSelection = nil }},
		{"unauthorized_edge", func(request *CompileRequest) {
			request.RuntimeSnapshot.DNSSelections[0].PhysicalSelection.OrderedEdgeIDs = append(request.RuntimeSnapshot.DNSSelections[0].PhysicalSelection.OrderedEdgeIDs, "unknown")
		}},
		{"duplicate_edge", func(request *CompileRequest) {
			request.RuntimeSnapshot.DNSSelections[0].PhysicalSelection.OrderedEdgeIDs = []string{"edge-b", "edge-b"}
		}},
		{"group_override", func(request *CompileRequest) {
			request.RuntimeSnapshot.DNSSelections[0].SelectedEdgeGroupID = "edge-group-a"
		}},
		{"exploration", func(request *CompileRequest) { request.Policy.DNSAnswerRules[0].ExplorationPercent = 1 }},
		{"unbound_scope", func(request *CompileRequest) {
			request.RuntimeSnapshot.DNSSelections[0].PhysicalSelection.Scope = "asn:example"
		}},
		{"stale_capture_identity", func(request *CompileRequest) {
			request.RuntimeSnapshot.DNSSelections[0].PhysicalSelection.CapturedAt = request.RuntimeSnapshot.DNSSelections[0].ObservedAt.Add(-time.Second)
		}},
		{"legacy_mode", func(request *CompileRequest) { request.Policy.DNSAnswerRules[0].SelectionMode = "geo" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := physicalQueryFixture()
			test.edit(&request)
			rebindPlacement(&request)
			if _, err := Compile(request); err == nil {
				t.Fatal("compiled conflicting physical selection")
			}
		})
	}
}
