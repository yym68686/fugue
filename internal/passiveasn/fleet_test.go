package passiveasn

import (
	"testing"
	"time"

	"fugue/internal/edgequality"
)

func TestPassiveFleetRanksOnlySupportedRelationships(t *testing.T) {
	a := edgequality.PassiveEndpoint{EdgeID: "a", GroupID: "g", Address: "8.8.8.8"}
	b := edgequality.PassiveEndpoint{EdgeID: "b", GroupID: "g", Address: "9.9.9.9"}
	c := edgequality.PassiveEndpoint{EdgeID: "c", GroupID: "g", Address: "1.1.1.1"}
	d := edgequality.PassiveEndpoint{EdgeID: "d", GroupID: "g", Address: "4.2.2.2"}
	now := time.Now().UTC()
	var pairs []edgequality.PassiveASNComparison
	for _, asn := range []uint32{64501, 64502, 64503} {
		pairs = append(pairs, edgequality.PassiveASNComparison{Family: 4, ASN: asn, Left: a, Right: b, Preferred: b.EdgeID, ObservedAt: now})
	}
	pairs = append(pairs, edgequality.PassiveASNComparison{Family: 4, ASN: 64501, Left: b, Right: c, Preferred: c.EdgeID, ObservedAt: now})
	rows := RankFleet(4, []edgequality.PassiveEndpoint{c, d, b, a}, pairs)
	if len(rows) != 4 || rows[0].Edge != a || rows[0].Tier != 2 || rows[1].Edge != b || rows[1].Tier != 1 ||
		rows[2].Tier != 0 || rows[2].Basis != "insufficient_aggregate_evidence" || rows[3].Tier != 0 || rows[3].Basis != "no_matched_evidence" {
		t.Fatalf("fabricated an unsupported ranking: %+v", rows)
	}
	for _, asn := range []uint32{64501, 64502, 64503} {
		pairs = append(pairs, edgequality.PassiveASNComparison{Family: 4, ASN: asn, Left: a, Right: c, Preferred: a.EdgeID, ObservedAt: now})
		if asn != 64501 {
			pairs = append(pairs, edgequality.PassiveASNComparison{Family: 4, ASN: asn, Left: b, Right: c, Preferred: c.EdgeID, ObservedAt: now})
		}
	}
	rows = RankFleet(4, []edgequality.PassiveEndpoint{a, b, c}, pairs)
	for _, row := range rows {
		if row.Tier != 0 || row.Basis != "aggregate_conflict" {
			t.Fatalf("cyclic fleet evidence was forced into a ranking: %+v", rows)
		}
	}
}

func TestComparableFleetRequiresEveryPairAndCountsEachASNOnce(t *testing.T) {
	a := edgequality.PassiveEndpoint{EdgeID: "a", GroupID: "g", Address: "8.8.8.8"}
	b := edgequality.PassiveEndpoint{EdgeID: "b", GroupID: "g", Address: "9.9.9.9"}
	c := edgequality.PassiveEndpoint{EdgeID: "c", GroupID: "g", Address: "1.1.1.1"}
	now := time.Now()
	makePair := func(asn uint32, left, right edgequality.PassiveEndpoint, winner string) edgequality.PassiveASNComparison {
		return edgequality.PassiveASNComparison{ASN: asn, Family: 4, Left: left, Right: right, Preferred: winner, ObservedAt: now}
	}
	confident := []edgequality.PassiveASNComparison{makePair(1, a, b, "a")}
	estimates := []edgequality.PassiveASNComparison{
		makePair(1, a, b, "b"), // Same ASN and pair cannot overrule confident evidence.
		makePair(1, a, c, ""), makePair(2, a, c, "a"), makePair(1, b, c, "b"),
	}
	rows := RankFleet(4, []edgequality.PassiveEndpoint{b, a, c}, confident, estimates)
	if scores := RankComparableFleet(rows, confident, estimates[:3]); scores != nil {
		t.Fatal("missing pair was counted")
	}
	scores := RankComparableFleet(rows, confident, estimates)
	if len(scores) != 3 || scores[0].Tier != 1 || scores[1].Tier != 2 || scores[2].Tier != 3 || scores[0].Basis != "provisional_asn_votes" {
		t.Fatalf("unexpected scores: %+v", scores)
	}
}
