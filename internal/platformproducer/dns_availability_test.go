package platformproducer

import (
	"encoding/json"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestAvailabilityConfigurationChangesOnlyOneExplicitHost(t *testing.T) {
	before := ProjectionPolicyInput{Generation: "before", Scope: "global", DNSQueryPolicy: &platformconfig.DNSQueryPolicy{RankingMode: "active"}}
	next := cloneBusinessProjectionPolicy(before)
	next.Generation = "next"
	next.DNSQueryPolicy.AvailabilityRoutes = []platformconfig.DNSAvailabilityRoute{{Hostname: "app.example.test", Policy: model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}}}
	original, _ := json.Marshal(next)
	if err := validateDNSAvailabilityDelta(before, next); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(next)
	if string(original) != string(after) {
		t.Fatal("validation changed signed policy")
	}
	for _, scenario := range []string{"unchanged", "two_hosts", "constraint", "generation", "nil"} {
		t.Run(scenario, func(t *testing.T) {
			candidate := cloneBusinessProjectionPolicy(next)
			switch scenario {
			case "unchanged":
				candidate = cloneBusinessProjectionPolicy(before)
				candidate.Generation = "new"
			case "two_hosts":
				r := candidate.DNSQueryPolicy.AvailabilityRoutes[0]
				r.Hostname = "other.example.test"
				candidate.DNSQueryPolicy.AvailabilityRoutes = append(candidate.DNSQueryPolicy.AvailabilityRoutes, r)
			case "constraint":
				candidate.DNSQueryPolicy.ECSEnabled = true
			case "generation":
				candidate.Generation = before.Generation
			case "nil":
				candidate.DNSQueryPolicy = nil
			}
			if validateDNSAvailabilityDelta(before, candidate) == nil {
				t.Fatal("broad or unfenced configuration accepted")
			}
		})
	}
	for _, mode := range []string{"change", "remove"} {
		candidate := cloneBusinessProjectionPolicy(next)
		candidate.Generation = "third"
		if mode == "remove" {
			candidate.DNSQueryPolicy.AvailabilityRoutes = nil
		} else {
			candidate.DNSQueryPolicy.AvailabilityRoutes[0].Policy.RecoverySuccesses = 4
		}
		if err := validateDNSAvailabilityDelta(next, candidate); err != nil {
			t.Fatal("independent configuration recovery rejected", err)
		}
	}
}

func TestAvailabilityRecoveryPreservesConstraintsAndRequiresAutomaticReadmission(t *testing.T) {
	for _, scenario := range []string{"success", "without_hold", "two_holds", "outside_pool", "no_recovery", "administrative_hold", "remove_hold", "change_hold", "two_hosts", "other_constraint"} {
		t.Run(scenario, func(t *testing.T) {
			hold := OfflineEdge{EdgeID: "edge-c", EdgeGroupID: "edge-group-test", PublicIPv4: "8.8.4.4", Reason: "maintenance"}
			before := ProjectionPolicyInput{Generation: "before", Scope: "global", OfflineEdges: []OfflineEdge{hold}, DNSQueryPolicy: &platformconfig.DNSQueryPolicy{RankingMode: "active"}}
			next := cloneBusinessProjectionPolicy(before)
			next.Generation = "after"
			next.DNSQueryPolicy.AvailabilityRoutes = []platformconfig.DNSAvailabilityRoute{{Hostname: "app.example.test", Policy: model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}}}
			added := OfflineEdge{EdgeID: "edge-a", EdgeGroupID: "edge-group-test", PublicIPv4: "8.8.8.8", Reason: "network_unavailable", Recovery: &OfflineEdgeRecoveryPolicy{Hostname: "app.example.test", Path: "/", ProbeIntervalSeconds: 300, ProbeTimeoutSeconds: 5, SuccessThreshold: 3, FailureThreshold: 1, FreshnessSeconds: 1800}}
			next.OfflineEdges = []OfflineEdge{added, hold}
			switch scenario {
			case "without_hold":
				next.OfflineEdges = []OfflineEdge{hold}
			case "two_holds":
				second := added
				second.EdgeID = "edge-b"
				next.OfflineEdges = []OfflineEdge{added, second, hold}
			case "outside_pool":
				next.DNSQueryPolicy.AvailabilityRoutes[0].Policy.OrderedEdgeIDs = []string{"edge-b"}
			case "no_recovery":
				next.OfflineEdges[0].Recovery = nil
			case "administrative_hold":
				next.OfflineEdges[0].Reason = "maintenance"
			case "remove_hold":
				next.OfflineEdges = []OfflineEdge{added}
			case "change_hold":
				next.OfflineEdges[1].Reason = "quota_exhausted"
			case "two_hosts":
				route := next.DNSQueryPolicy.AvailabilityRoutes[0]
				route.Hostname = "other.example.test"
				next.DNSQueryPolicy.AvailabilityRoutes = append(next.DNSQueryPolicy.AvailabilityRoutes, route)
			case "other_constraint":
				next.DNSQueryPolicy.ECSEnabled = true
			}
			original, _ := json.Marshal(next)
			err := validateDNSAvailabilityRecoveryDelta(before, next)
			if (scenario == "success" || scenario == "without_hold") != (err == nil) {
				t.Fatalf("unexpected result: %v", err)
			}
			after, _ := json.Marshal(next)
			if string(original) != string(after) {
				t.Fatal("validation mutated immutable input")
			}
		})
	}
}
