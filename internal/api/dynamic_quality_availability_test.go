package api

import (
	"encoding/json"
	"reflect"
	"testing"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDynamicQualityUnavailablePublishesOnlyExplicitAvailability(t *testing.T) {
	for _, scenario := range []string{"missing", "partial", "invalid receipt", "no opt in", "unavailable pool"} {
		t.Run(scenario, func(t *testing.T) {
			projection, nodes, policy, now := directQueryFixture()
			policy.ECSEnabled, policy.ExplorationPercent = false, 0
			policy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}}
			policy.DynamicQuality = &platformconfig.DynamicQualityPolicy{Mode: "all_dynamic", Policy: edgequality.DefaultDeliveryNetworkPolicy(), RefreshQueriesPerCycle: 4, RefreshConcurrency: 2}
			availability := model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-b", "edge-a", "foreign"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
			if scenario == "unavailable pool" {
				availability.OrderedEdgeIDs = []string{"foreign"}
			}
			if scenario != "no opt in" {
				policy.AvailabilityRoutes = []platformconfig.DNSAvailabilityRoute{{Hostname: "app.example.test", Policy: availability}}
			}
			other := projection.Intent.DNS[0]
			other.Hostname = "alias.example.test"
			projection.Intent.DNS = append(projection.Intent.DNS, other, platformconfig.DNSIntent{Hostname: "static.example.test", Type: "TXT", Values: []string{"unchanged"}, TTL: 300})
			consumer := projection.Intent.DNSConsumers[0]
			consumer.NodeID = "dns-b"
			projection.Intent.DNSConsumers = append(projection.Intent.DNSConsumers, consumer)
			if err := projectDirectDNSQueries(&projection, policy, nodes, now); err != nil {
				t.Fatal(err)
			}
			allJobs, _, err := dynamicQualityRoutes(projection, policy)
			if err != nil {
				t.Fatal(err)
			}
			var jobs []dynamicQualityJob
			for _, job := range allJobs {
				if job.Route.Hostname == "app.example.test" {
					jobs = append(jobs, job)
				}
			}
			if len(jobs) != 2 {
				t.Fatal("fixture must require both DNS nodes", jobs)
			}
			selections := map[string]compiledPhysicalDNSSelection{}
			if scenario == "partial" || scenario == "invalid receipt" {
				// An incomplete or invalid cache must not become quality evidence.
				selections[jobs[0].Key] = compiledPhysicalDNSSelection{}
				if scenario == "invalid receipt" {
					selections[jobs[1].Key] = compiledPhysicalDNSSelection{}
				}
			}
			before, _ := json.Marshal(projection)
			intentBefore, _ := json.Marshal(projection.Intent)
			otherRules, otherFacts := map[string]platformconfig.DNSAnswerRule{}, map[string]platformconfig.DNSSelectionObservation{}
			for _, rule := range projection.Policy.DNSAnswerRules {
				if rule.Hostname != "app.example.test" {
					otherRules[rule.NodeID] = rule
				}
			}
			for _, fact := range projection.RuntimeSnapshot.DNSSelections {
				if fact.Hostname != "app.example.test" {
					otherFacts[fact.NodeID] = fact
				}
			}
			state, err := applyDynamicQualityHost(&projection, policy, jobs, selections, now)
			if scenario == "no opt in" || scenario == "unavailable pool" {
				if (err != nil) != (scenario == "unavailable pool") {
					t.Fatal("missing authority must not silently succeed", err)
				}
				after, _ := json.Marshal(projection)
				if string(before) != string(after) || state == "availability_only_quality_unavailable" {
					t.Fatal("unavailable authority changed verified learning baseline")
				}
				return
			}
			if err != nil || state != "availability_only_quality_unavailable" {
				t.Fatal("dynamic quality did not use independently declared availability", state, err)
			}
			for _, rule := range projection.Policy.DNSAnswerRules {
				if rule.Hostname != "app.example.test" {
					if !reflect.DeepEqual(rule, otherRules[rule.NodeID]) {
						t.Fatal("fallback changed another hostname", rule)
					}
					continue
				}
				if rule.SelectionMode != model.DNSAnswerPolicyKindPhysicalOrder || rule.PhysicalOrder == nil || !reflect.DeepEqual(rule.PhysicalOrder.OrderedEdgeIDs, []string{"edge-b", "edge-a"}) {
					t.Fatal("fallback retained stale order or authorized a foreign edge", rule)
				}
			}
			for _, fact := range projection.RuntimeSnapshot.DNSSelections {
				if fact.Hostname != "app.example.test" {
					if !reflect.DeepEqual(fact, otherFacts[fact.NodeID]) {
						t.Fatal("fallback changed another hostname observation")
					}
					continue
				}
				if fact.PhysicalSelection != nil || len(fact.PhysicalEvidence) != 0 || fact.Reason != state {
					t.Fatal("fallback manufactured a quality receipt", fact)
				}
			}
			intentAfter, _ := json.Marshal(projection.Intent)
			if string(intentBefore) != string(intentAfter) || projection.RuntimeSnapshot.PolicyGeneration != projection.Policy.Generation {
				t.Fatal("fallback changed ownership/static intent or broke signed lineage")
			}
		})
	}
}

func TestDynamicQualityAvailabilityStillPrefersRevalidatedQuality(t *testing.T) {
	receipt, route := businessPublicationFixture(t)
	route.BusinessExploration, route.PathPrefix = nil, ""
	now := receipt.Snapshot.CapturedAt
	selection, err := compileBoundPhysicalQualitySelection(receipt, now)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	projection, policy, _, _ := physicalQueryProjectionFixture(t)
	policy.DynamicQuality = &platformconfig.DynamicQualityPolicy{Mode: "all_dynamic", Policy: route.Policy, RefreshQueriesPerCycle: 4, RefreshConcurrency: 2}
	policy.AvailabilityRoutes = []platformconfig.DNSAvailabilityRoute{{Hostname: route.Hostname, Policy: model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}}}
	projection.Policy.DNSQueryPolicy = &policy
	job := dynamicQualityJob{NodeID: "dns-a", Key: "dns-a\x00" + route.Hostname, Route: route}
	state, err := applyDynamicQualityHost(&projection, policy, []dynamicQualityJob{job}, map[string]compiledPhysicalDNSSelection{job.Key: {Selection: selection, Evidence: raw}}, now)
	if err != nil || state != "measured" && state != "learning_current_ready_primary" {
		t.Fatal("valid quality evidence was replaced by availability", state, err)
	}
	if !reflect.DeepEqual(projection.RuntimeSnapshot.DNSSelections[0].PhysicalSelection, selection) || projection.Policy.DNSAnswerRules[0].SelectionMode != model.DNSAnswerPolicyKindPhysicalQuality {
		t.Fatal("independent pool overwrote quality preference")
	}
}
