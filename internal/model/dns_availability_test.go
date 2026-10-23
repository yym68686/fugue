package model

import (
	"encoding/json"
	"testing"
)

func TestDNSAvailabilityPolicyRequiresExplicitBoundedAuthority(t *testing.T) {
	raw := []byte(`{"version":"dns-availability-v1","ordered_edge_ids":["edge-a","edge-b"],"recovery_successes":3,"recovery_min_seconds":60}`)
	var policy DNSAvailabilityPolicy
	if err := json.Unmarshal(raw, &policy); err != nil {
		t.Fatal(err)
	}
	clone := CloneDNSAvailabilityPolicy(&policy)
	clone.OrderedEdgeIDs[0] = "other"
	if policy.OrderedEdgeIDs[0] != "edge-a" {
		t.Fatal("policy copy aliases signed membership")
	}
	for _, name := range []string{"omitted", "null", "version", "duplicate", "empty", "unknown", "single_success", "negative_interval", "large_interval"} {
		t.Run(name, func(t *testing.T) {
			var fields map[string]any
			_ = json.Unmarshal(raw, &fields)
			switch name {
			case "omitted":
				delete(fields, "recovery_successes")
			case "null":
				fields["ordered_edge_ids"] = nil
			case "version":
				fields["version"] = "unknown"
			case "duplicate":
				fields["ordered_edge_ids"] = []string{"edge-a", "edge-a"}
			case "empty":
				fields["ordered_edge_ids"] = []string{}
			case "unknown":
				fields["skip_tls"] = true
			case "single_success":
				fields["recovery_successes"] = 1
			case "negative_interval":
				fields["recovery_min_seconds"] = -1
			case "large_interval":
				fields["recovery_min_seconds"] = 301
			}
			changed, _ := json.Marshal(fields)
			if err := json.Unmarshal(changed, new(DNSAvailabilityPolicy)); err == nil {
				t.Fatal("accepted incomplete or unsafe authority")
			}
		})
	}
}
