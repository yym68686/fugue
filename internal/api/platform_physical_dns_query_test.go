package api

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func physicalQueryProjectionFixture(t *testing.T) (platformIntentProjectionResponse, platformconfig.DNSQueryPolicy, map[string]compiledPhysicalDNSSelection, time.Time) {
	t.Helper()
	projection, nodes, policy, now := directQueryFixture()
	policy.PhysicalRoutes = []platformconfig.PhysicalQualityRoute{{Hostname: "app.example.test", TrafficClass: "streaming", Policy: edgequality.DefaultNetworkPolicy()}}
	if err := projectDirectDNSQueries(&projection, policy, nodes, edgeDNSLatencyProfileCatalog{}, now); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	selection := &model.DNSPhysicalSelection{Version: model.DNSPhysicalSelectionVersion, PrimaryEdgeID: "edge-b", OrderedEdgeIDs: []string{"edge-b", "edge-a"},
		EvidenceDigest: digest, DNSReceiptID: "actual-answer-a", LoadedDigest: digest, PolicyDigest: digest, Scope: "global", CapturedAt: now, PrimarySince: &now}
	return projection, policy, map[string]compiledPhysicalDNSSelection{"dns-a\x00app.example.test": {Selection: selection, Evidence: json.RawMessage(`{"schema":"synthetic-preverified-evidence"}`)}}, now
}

func TestPhysicalDNSPublicationReplacesOnlyExplicitDynamicQueries(t *testing.T) {
	projection, policy, selections, now := physicalQueryProjectionFixture(t)
	otherFact := projection.RuntimeSnapshot.DNSSelections[0]
	otherFact.Hostname = "other.example.test"
	otherRule := projection.Policy.DNSAnswerRules[0]
	otherRule.Hostname = otherFact.Hostname
	projection.RuntimeSnapshot.DNSSelections = append(projection.RuntimeSnapshot.DNSSelections, otherFact)
	projection.Policy.DNSAnswerRules = append(projection.Policy.DNSAnswerRules, otherRule)
	static := platformconfig.DNSIntent{Hostname: "static.example.test", Type: "A", Values: []string{"8.8.4.4"}, TTL: 300}
	projection.Intent.DNS = append(projection.Intent.DNS, static)
	if err := applyPhysicalDNSSelections(&projection, policy, selections, now); err != nil {
		t.Fatal(err)
	}
	rule, fact := projection.Policy.DNSAnswerRules[0], projection.RuntimeSnapshot.DNSSelections[0]
	if rule.SelectionMode != model.DNSAnswerPolicyKindPhysicalQuality || rule.ECSEnabled || rule.ExplorationPercent != 0 || rule.ScopedSelectionMode != "" || len(rule.PreferredEdgeGroups) != 0 ||
		fact.PhysicalSelection == nil || fact.PhysicalSelection.PrimaryEdgeID != "edge-b" || fact.SelectedEdgeGroupID != "" || len(fact.ScopedCandidates) != 0 || len(fact.PhysicalEvidence) == 0 {
		t.Fatal("legacy authority leaked into signed physical order", rule, fact)
	}
	for _, candidate := range fact.Candidates {
		if candidate.Score != 0 || candidate.Weight != 0 || candidate.Priority != 0 || candidate.Country != "" {
			t.Fatal("legacy score or geography retained as authority", candidate)
		}
	}
	if !reflect.DeepEqual(otherFact, projection.RuntimeSnapshot.DNSSelections[1]) || !reflect.DeepEqual(otherRule, projection.Policy.DNSAnswerRules[1]) || !reflect.DeepEqual(static, projection.Intent.DNS[1]) {
		t.Fatal("physical opt-in modified unrelated or static DNS")
	}
	if projection.RuntimeSnapshot.PolicyGeneration != projection.Policy.Generation || projection.Policy.Generation == "policy" {
		t.Fatal("new policy not content addressed")
	}
}

func TestPhysicalDNSPublicationRejectsUnsafeProjectionAtomically(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*platformIntentProjectionResponse, *platformconfig.DNSQueryPolicy, map[string]compiledPhysicalDNSSelection)
	}{
		{"missing_selection", func(_ *platformIntentProjectionResponse, _ *platformconfig.DNSQueryPolicy, values map[string]compiledPhysicalDNSSelection) {
			delete(values, "dns-a\x00app.example.test")
		}},
		{"undeclared_edge", func(_ *platformIntentProjectionResponse, _ *platformconfig.DNSQueryPolicy, values map[string]compiledPhysicalDNSSelection) {
			values["dns-a\x00app.example.test"].Selection.OrderedEdgeIDs = append(values["dns-a\x00app.example.test"].Selection.OrderedEdgeIDs, "edge-other")
		}},
		{"stale", func(_ *platformIntentProjectionResponse, _ *platformconfig.DNSQueryPolicy, values map[string]compiledPhysicalDNSSelection) {
			values["dns-a\x00app.example.test"].Selection.CapturedAt = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		}},
		{"static", func(value *platformIntentProjectionResponse, _ *platformconfig.DNSQueryPolicy, _ map[string]compiledPhysicalDNSSelection) {
			value.Intent.DNS[0].Type = "A"
			value.Intent.DNS[0].Route = nil
		}},
		{"pinned", func(value *platformIntentProjectionResponse, _ *platformconfig.DNSQueryPolicy, _ map[string]compiledPhysicalDNSSelection) {
			value.Intent.Routes[0].EdgeGroupMode = model.PlatformRouteEdgeGroupModePinned
			value.Intent.Routes[0].EdgeGroupID = "edge-group-a"
		}},
		{"missing_rule", func(value *platformIntentProjectionResponse, _ *platformconfig.DNSQueryPolicy, _ map[string]compiledPhysicalDNSSelection) {
			value.Policy.DNSAnswerRules = nil
		}},
		{"unknown_host", func(_ *platformIntentProjectionResponse, policy *platformconfig.DNSQueryPolicy, _ map[string]compiledPhysicalDNSSelection) {
			policy.PhysicalRoutes[0].Hostname = "absent.example.test"
		}},
		{"unmeasured_ipv6", func(value *platformIntentProjectionResponse, _ *platformconfig.DNSQueryPolicy, _ map[string]compiledPhysicalDNSSelection) {
			value.RuntimeSnapshot.DNSSelections[0].Type = "AAAA"
		}},
		{"missing_evidence", func(_ *platformIntentProjectionResponse, _ *platformconfig.DNSQueryPolicy, values map[string]compiledPhysicalDNSSelection) {
			compiled := values["dns-a\x00app.example.test"]
			compiled.Evidence = nil
			values["dns-a\x00app.example.test"] = compiled
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			projection, policy, selections, now := physicalQueryProjectionFixture(t)
			test.edit(&projection, &policy, selections)
			before, _ := json.Marshal(projection)
			if applyPhysicalDNSSelections(&projection, policy, selections, now) == nil {
				t.Fatal("unsafe projection accepted")
			}
			after, _ := json.Marshal(projection)
			if string(before) != string(after) {
				t.Fatal("rejected physical query partially mutated compilation")
			}
		})
	}
}

func TestPhysicalDNSPublicationCompilesSignedOrderWithoutInventingReadiness(t *testing.T) {
	projection, policy, selections, now := physicalQueryProjectionFixture(t)
	projection.Policy.DNSReadiness = &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxConcurrency: 8, MaxProbes: 4096}
	projection.RuntimeSnapshot.DNSConsumers = []platformconfig.DNSConsumerObservation{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", ObservedAt: now, A: []string{"1.1.1.1"}}}
	if err := applyPhysicalDNSSelections(&projection, policy, selections, now); err != nil {
		t.Fatal(err)
	}
	request := platformconfig.CompileRequest{Intent: projection.Intent, Policy: projection.Policy, RuntimeSnapshot: projection.RuntimeSnapshot}
	if _, err := platformconfig.Compile(request); err == nil {
		t.Fatal("physical order substituted for independent readiness topology")
	}
	owners, _, err := placementRecordRoutes(projection, projection.Intent.DNS[0])
	if err != nil {
		t.Fatal(err)
	}
	digest, err := platformconfig.DNSPlacementInputDigest(projection.Intent.DNS[0], owners, projection.Policy)
	if err != nil {
		t.Fatal(err)
	}
	planned := platformconfig.DNSPlacementObservation{InputDigest: digest, CheckedAt: now, Status: "resolved", TargetTTL: 60}
	for _, endpoint := range projection.RuntimeSnapshot.DNSEdgeEndpoints {
		planned.Candidates = append(planned.Candidates, platformconfig.DNSPlacementCandidate{EdgeID: endpoint.EdgeID, EdgeGroupID: endpoint.EdgeGroupID, ServingGeneration: "verified-route", ObservedAt: now, ValidUntil: now.Add(time.Minute), Healthy: true, RouteReady: true, TLSReady: true, A: endpoint.A})
	}
	request.RuntimeSnapshot.DNSPlacements = []platformconfig.DNSPlacementObservation{planned}
	compiled, err := platformconfig.Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	var views []platformconfig.DNSQueryView
	raw, _ := json.Marshal(compiled.DNSArtifact.Content["query_views"])
	if err := json.Unmarshal(raw, &views); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, view := range views {
		for _, record := range view.Records {
			if record.Name != "app.example.test" {
				continue
			}
			found = true
			if record.AnswerPolicy.PolicyKind != model.DNSAnswerPolicyKindPhysicalQuality || record.AnswerPolicy.PhysicalSelection.PrimaryEdgeID != "edge-b" || record.AnswerPolicy.ExplorationPercent != 0 {
				t.Fatal("compiled query lost physical order", record)
			}
			for _, candidate := range record.Candidates {
				if candidate.Healthy || candidate.RouteReady || candidate.TLSReady {
					t.Fatal("quality compilation fabricated current route readiness")
				}
			}
		}
	}
	if !found {
		t.Fatal("compiled physical query missing")
	}
	request.CreatedAt = now.Add(time.Hour)
	replayed, err := platformconfig.Compile(request)
	if err != nil || !reflect.DeepEqual(compiled.DNSArtifact.Content, replayed.DNSArtifact.Content) {
		t.Fatal("immutable network input compilation was not replayable", err)
	}
}
