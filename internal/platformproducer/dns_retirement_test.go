package platformproducer

import (
	"encoding/json"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func retirementTestArtifact(t *testing.T, id, kind, generation string, sequence int64, value any) model.PlatformArtifact {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	artifact := model.PlatformArtifact{ID: id, ArtifactKind: kind, ScopeKey: "global", Generation: generation, GenerationSequence: sequence, ContentHash: "sha256:" + id}
	if json.Unmarshal(raw, &artifact.Content) != nil {
		t.Fatal("fixture cannot decode")
	}
	return artifact
}

func TestDNSSelectorRetirementPreservesConstraintsAndExactNormalOrder(t *testing.T) {
	for _, scenario := range []string{"valid", "candidate_removed", "candidate_added", "reordered", "missing_override", "static_override", "default_missing", "default_unknown", "client_scope", "ttl", "physical_policy", "schedule", "static", "scoped_record", "foreign_source", "same_generation"} {
		t.Run(scenario, func(t *testing.T) {
			consumers := []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}}
			intent := platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Generation: "static", Scope: "global", DNSConsumers: consumers}
			static := retirementTestArtifact(t, "static", model.PlatformArtifactKindPlatformIntent, "static", 1, intent)
			probe := &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 90, MaxConcurrency: 4, MaxProbes: 1024}
			before := ProjectionPolicyInput{SchemaVersion: platformconfig.SchemaVersion, Scope: "global", Generation: "before", DNSPlacementMode: platformconfig.DNSPlacementConsumerReadiness,
				DNSQueryPolicy: &platformconfig.DNSQueryPolicy{RankingMode: "active", PreferenceMode: "runtime_locality", ECSEnabled: true, ExplorationPercent: 5, MinimumTTLSeconds: 60, MaximumTTLSeconds: 120},
				Authorities:    []platformconfig.DNSAuthorityPolicy{{NodeID: "dns-a", Zone: "example.test", Nameservers: []string{"ns.example.test"}, TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600}},
				Clients:        []platformconfig.DNSClientPolicy{{NodeID: "dns-a"}}, DNSReadiness: probe, TLSReadiness: probe, Cohorts: []platformconfig.TrafficRolloutCohort{{ID: "test", EdgeGroupIDs: []string{"edge-group-a"}}}}
			if scenario == "client_scope" {
				before.Clients[0].Rules = []platformconfig.DNSClientRule{{CIDR: "10.0.0.0/8", Country: "aa"}}
			}
			oldDNS := retirementTestArtifact(t, "old", model.PlatformArtifactKindPolicySnapshot, "before", 2, before)
			after := before
			query := *before.DNSQueryPolicy
			after.DNSQueryPolicy = &query
			after.Generation, query.ECSEnabled, query.ExplorationPercent = "after", false, 0
			order := model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}
			query.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: order, Overrides: []platformconfig.DNSOrderOverride{{NodeID: "dns-a", Hostname: "app.example.test", Type: "A", Order: order}}}
			switch scenario {
			case "candidate_removed":
				query.OrderedProjection.Overrides[0].Order.OrderedEdgeIDs = []string{"edge-a"}
			case "candidate_added":
				query.OrderedProjection.Overrides[0].Order.OrderedEdgeIDs = []string{"edge-a", "edge-b", "edge-c"}
			case "reordered":
				query.OrderedProjection.Overrides[0].Order.OrderedEdgeIDs = []string{"edge-b", "edge-a"}
			case "missing_override":
				query.OrderedProjection.Overrides = []platformconfig.DNSOrderOverride{}
			case "static_override":
				query.OrderedProjection.Overrides[0].Hostname = "static.example.test"
			case "default_missing":
				query.OrderedProjection.DefaultOrder.OrderedEdgeIDs = []string{"edge-a"}
			case "default_unknown":
				query.OrderedProjection.DefaultOrder.OrderedEdgeIDs = []string{"edge-a", "edge-b", "foreign"}
			case "ttl":
				query.MaximumTTLSeconds++
			case "physical_policy":
				query.SwitchCooldownSeconds++
			case "same_generation":
				after.Generation = before.Generation
			}
			newDNS := retirementTestArtifact(t, "new", model.PlatformArtifactKindPolicySnapshot, after.Generation, 3, after)
			previous := Policy{Generation: "producer-before", TargetScope: "global", Mode: "serving", RequireDNSQueryPolicy: true, StaticIntentArtifactID: static.ID, StaticIntentDigest: static.ContentHash, DNSPolicyArtifactID: oldDNS.ID, DNSPolicyDigest: oldDNS.ContentHash, Serving: &ServingPolicy{}}
			next := previous
			next.Generation, next.DNSPolicyArtifactID, next.DNSPolicyDigest = "producer-after", newDNS.ID, newDNS.ContentHash
			switch scenario {
			case "schedule":
				next.RefreshSeconds++
			case "static":
				next.StaticIntentArtifactID = "foreign"
			case "foreign_source":
				next.DNSPolicyDigest = "sha256:foreign"
			}
			record := model.EdgeDNSRecord{Name: "app.example.test", Type: "A", AnswerPolicy: model.DNSAnswerPolicy{PolicyKind: "latency_aware", SelectedEdgeGroupID: "edge-group-a"}, Candidates: []model.EdgeDNSAnswerCandidate{{IP: "8.8.8.8", EdgeID: "edge-a", EdgeGroupID: "edge-group-a", Score: 20}, {IP: "9.9.9.9", EdgeID: "edge-b", EdgeGroupID: "edge-group-b", Score: 40}}}
			if scenario == "scoped_record" {
				record.ScopedCandidates = []model.EdgeDNSScopedAnswerCandidates{{ScopeKey: "country:aa"}}
			}
			baseline := retirementTestArtifact(t, "baseline", model.PlatformArtifactKindDNSAnswerBundle, "baseline", 4, map[string]any{"query_views": []platformconfig.DNSQueryView{{NodeID: "dns-a", Zone: "example.test", Records: []model.EdgeDNSRecord{record, {Name: "static.example.test", Type: "A", Values: []string{"1.1.1.1"}}}}}})
			err := ValidateDNSSelectorRetirement(previous, next, static, oldDNS, newDNS, baseline)
			if (scenario == "valid") != (err == nil) {
				t.Fatalf("retirement boundary %s: %v", scenario, err)
			}
		})
	}
}
