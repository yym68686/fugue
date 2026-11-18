package api

import (
	"encoding/json"
	"net/netip"
	"testing"
	"time"

	"fugue/internal/edgequality"
)

func TestPassiveFleetKeepsCompleteHistoricalTierWhenCurrentEvidenceConflicts(t *testing.T) {
	now := time.Date(2026, 11, 18, 20, 0, 0, 0, time.UTC)
	a := edgequality.PassiveEndpoint{EdgeID: "a", GroupID: "us", Address: "8.8.8.8"}
	b := edgequality.PassiveEndpoint{EdgeID: "b", GroupID: "de", Address: "1.1.1.1"}
	c := edgequality.PassiveEndpoint{EdgeID: "c", GroupID: "us", Address: "9.9.9.9"}
	endpoints := []edgequality.PassiveEndpoint{b, c, a}
	prefix := netip.MustParsePrefix("8.8.8.0/24")
	pair := func(left, right edgequality.PassiveEndpoint, preferred string) edgequality.PassiveASNComparison {
		return edgequality.PassiveASNComparison{ASN: 15169, Family: 4, Left: left, Right: right, Preferred: preferred, ObservedAt: now.Add(-time.Minute), Prefixes: []netip.Prefix{prefix}}
	}
	resolve := func(netip.Prefix) uint32 { return 15169 }
	qualified := []edgequality.PassiveASNComparison{pair(a, b, "a"), pair(b, c, "b")}
	live, effective, snapshot, fallback := selectPassiveFleet(now, 4, endpoints, nil, qualified, time.Time{}, nil, nil, passiveFleetSnapshot{}, resolve)
	if !fallback.IsZero() || snapshot.At != now || len(live) != 3 || effective[2].Tier != 3 {
		t.Fatalf("complete live rank did not enter history: %+v %+v %v", effective, snapshot, fallback)
	}
	data, err := json.Marshal(passiveASNCheckpoint{FleetSnapshots: map[int]passiveFleetSnapshot{4: snapshot}})
	var restored passiveASNCheckpoint
	if err != nil || json.Unmarshal(data, &restored) != nil {
		t.Fatalf("snapshot is not durable: %v", err)
	}
	conflict := []edgequality.PassiveASNComparison{pair(a, b, ""), pair(b, c, "c")}
	later := now.Add(72 * time.Hour)
	live, effective, snapshot, fallback = selectPassiveFleet(later, 4, endpoints, nil, conflict, time.Time{}, nil, nil, restored.FleetSnapshots[4], resolve)
	if live[0].Tier != 0 || live[0].Basis != "insufficient_aggregate_evidence" || effective[0].Tier != 1 || effective[2].Tier != 3 ||
		effective[0].Basis != "historical_provisional_matched_asn" || effective[0].EvidenceAt != qualified[0].ObservedAt || fallback != now || snapshot.At != now {
		t.Fatalf("did not keep the actual prior tiers with separate conflict status: live=%+v effective=%+v fallback=%v", live, effective, fallback)
	}
	if _, effective, _, fallback = selectPassiveFleet(later, 4, endpoints, nil, conflict, time.Time{}, nil, nil, snapshot, func(netip.Prefix) uint32 { return 15170 }); !fallback.IsZero() || effective[0].Tier != 0 {
		t.Fatal("reused the rank after its ASN attribution changed")
	}
	replaced := append([]edgequality.PassiveEndpoint(nil), endpoints...)
	replaced[2].Address = "8.8.4.4"
	if _, effective, _, fallback = selectPassiveFleet(later, 4, replaced, nil, conflict, time.Time{}, nil, nil, snapshot, resolve); !fallback.IsZero() || effective[0].Tier != 0 {
		t.Fatal("reused another physical endpoint's rank")
	}
	if _, effective, _, fallback = selectPassiveFleet(later, 4, endpoints, nil, conflict, now, nil, qualified, passiveFleetSnapshot{}, resolve); fallback != now || effective[0].Tier != 1 {
		t.Fatal("did not recover the complete independently replayed prior window")
	}
	fresh := []edgequality.PassiveASNComparison{pair(a, b, "b"), pair(a, c, "c")}
	if _, effective, _, fallback = selectPassiveFleet(later, 4, endpoints, nil, fresh, time.Time{}, nil, nil, snapshot, resolve); !fallback.IsZero() || effective[0].Tier != 2 {
		t.Fatal("did not switch back to the complete current ranking")
	}
}
