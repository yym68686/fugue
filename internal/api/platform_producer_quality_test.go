package api

import (
	"reflect"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

func TestProducerQualityPlanKeepsDetachedContextAndRejectsChangedPolicy(t *testing.T) {
	projection, nodes, query, _ := directQueryFixture()
	now := time.Now().UTC()
	projection.CapturedAt = now
	projection.RuntimeSnapshot.CapturedAt = &now
	for i := range projection.RuntimeSnapshot.Origins {
		projection.RuntimeSnapshot.Origins[i].ObservedAt = now
	}
	for i := range projection.RuntimeSnapshot.DNSEdgeEndpoints {
		projection.RuntimeSnapshot.DNSEdgeEndpoints[i].ObservedAt = now
	}
	query.ECSEnabled, query.ExplorationPercent = false, 0
	query.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}}
	query.DynamicQuality = &platformconfig.DynamicQualityPolicy{Mode: "all_dynamic", Policy: edgequality.DefaultDeliveryNetworkPolicy(), RefreshQueriesPerCycle: 4, RefreshConcurrency: 2}
	if err := projectDirectDNSQueries(&projection, query, nodes, now); err != nil {
		t.Fatal(err)
	}
	projection.RuntimeSnapshot.DNSConsumers = []platformconfig.DNSConsumerObservation{{NodeID: "dns-a", EdgeGroupID: "group-a", A: []string{"8.8.4.4"}, ObservedAt: now}}
	policy := platformproducer.Policy{Generation: "producer-one", TargetScope: projection.Intent.Scope, DNSPolicyDigest: "signed-policy-one"}
	server := &Server{}
	server.rememberProducerQualityPlan(policy, projection)
	plan := server.pendingProducerQualityPlan(policy, now)
	if plan == nil || len(plan.jobs) != 1 || plan.contexts[plan.jobs[0].Key] == "" {
		t.Fatal("producer capture did not retain its observation work list")
	}
	projection.RuntimeSnapshot.DNSConsumers[0].A[0] = "8.8.8.8"
	if plan.consumers[0].A[0] != "8.8.4.4" {
		t.Fatal("caller mutation changed retained observation endpoint")
	}
	changed := policy
	changed.DNSPolicyDigest = "signed-policy-two"
	if server.pendingProducerQualityPlan(changed, now) != nil || server.pendingProducerQualityPlan(policy, plan.expiresAt) != nil || server.pendingProducerQualityPlan(policy, now.Add(-time.Second)) != nil {
		t.Fatal("changed, expired or future observation context was reused")
	}
	// Later publication must compare its fresh context. The retained plan does
	// not make a receipt valid after an owning route changes.
	projection.Intent.Routes[0].PathPrefix = "/changed"
	current, err := newDynamicQualityContextReader(projection, plan.jobs)(plan.jobs[0])
	if err != nil || current == plan.contexts[plan.jobs[0].Key] {
		t.Fatal("route change did not change publication context", err)
	}
	entry := dynamicQualityEntry{ContextDigest: plan.contexts[plan.jobs[0].Key], Compiled: compiledPhysicalDNSSelection{Selection: &model.DNSPhysicalSelection{CapturedAt: now}}}
	if reason := dynamicQualityEntryUnavailable(entry, current, nil, query.DynamicQuality.Policy, now); reason != "configuration_context_changed" {
		t.Fatal("old observation context authorized a changed route", reason)
	}
}

func TestPendingQualityRequiresUnchangedFreshPhysicalEndpoints(t *testing.T) {
	now := time.Now().UTC()
	previous := []platformconfig.DNSConsumerObservation{{NodeID: "dns-a", EdgeGroupID: "group-a", A: []string{"192.0.2.1"}, ObservedAt: now.Add(-time.Minute)}}
	match := model.DNSNode{ID: "instance-a", PhysicalNodeID: "dns-a", EdgeGroupID: "group-a", PublicIPv4: "192.0.2.1"}
	for _, scenario := range []string{"unchanged", "duplicate instance", "missing", "changed address", "changed group", "changed owner", "ambiguous physical owner"} {
		t.Run(scenario, func(t *testing.T) {
			nodes := []model.DNSNode{match}
			switch scenario {
			case "duplicate instance":
				nodes = append(nodes, match)
				nodes[1].ID = "instance-b"
			case "missing":
				nodes = nil
			case "changed address":
				nodes[0].PublicIPv4 = "192.0.2.2"
			case "changed group":
				nodes[0].EdgeGroupID = "group-b"
			case "changed owner":
				nodes[0].PhysicalNodeID = "dns-b"
			case "ambiguous physical owner":
				nodes = append(nodes, match)
				nodes[1].PublicIPv4 = "192.0.2.2"
			}
			current := freshProducerQualityConsumers(previous, nodes, now)
			if scenario == "unchanged" || scenario == "duplicate instance" {
				want := previous[0]
				want.ObservedAt = now
				if !reflect.DeepEqual(current, []platformconfig.DNSConsumerObservation{want}) {
					t.Fatal("fresh identical inventory was not used", current)
				}
				current[0].A[0] = "192.0.2.3"
			} else if len(current) != 0 {
				t.Fatal("missing or changed endpoint was admitted for frozen observation", current)
			}
			if previous[0].ObservedAt.Equal(now) || previous[0].A[0] != "192.0.2.1" {
				t.Fatal("old endpoint observation was mutated")
			}
		})
	}
}
