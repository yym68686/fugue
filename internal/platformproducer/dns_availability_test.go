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
