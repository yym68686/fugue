package passiveasn

import (
	"slices"
	"strings"
	"time"

	"fugue/internal/edgequality"
)

// FleetRank is diagnostic; it is never DNS candidate authority. Tier zero
// means there is no justified position, not that the endpoint came last.
type FleetRank struct {
	Edge                            edgequality.PassiveEndpoint
	Family, Tier                    int
	Basis                           string
	ConfidentPeers, ComparablePeers int
	EstimatedPeers                  int
	EvidenceAt                      time.Time
}

// RankFleet shows all observed inventory members and ranks the supported
// partial order. Missing matches and contradictory cycles aren't victories.
func RankFleet(family int, endpoints []edgequality.PassiveEndpoint, pairs []edgequality.PassiveASNComparison, estimates ...[]edgequality.PassiveASNComparison) []FleetRank {
	endpoints = slices.Clone(endpoints)
	slices.SortFunc(endpoints, func(a, b edgequality.PassiveEndpoint) int { return strings.Compare(a.EdgeID, b.EdgeID) })
	idx := make(map[edgequality.PassiveEndpoint]int, len(endpoints))
	result := make([]FleetRank, len(endpoints))
	for i, edge := range endpoints {
		idx[edge] = i
		result[i] = FleetRank{Edge: edge, Family: family, Basis: "no_matched_evidence"}
	}
	v := View{aggregate: make(map[comparisonKey]aggregatePair)}
	provisional := View{aggregate: make(map[comparisonKey]aggregatePair)}
	seen := make(map[comparisonKey]bool)
	for _, pair := range pairs {
		_, left := idx[pair.Left]
		_, right := idx[pair.Right]
		key := comparisonKey{pair.ASN, family, pair.Left, pair.Right}
		if pair.Family != family || !left || !right || pair.Left.EdgeID >= pair.Right.EdgeID || seen[key] {
			continue
		}
		seen[key] = true
		v.recordAggregate(pair)
		provisional.recordAggregate(pair)
	}
	for _, batch := range estimates {
		for _, pair := range batch {
			_, left := idx[pair.Left]
			_, right := idx[pair.Right]
			key := comparisonKey{pair.ASN, family, pair.Left, pair.Right}
			if pair.Family == family && left && right && pair.Left.EdgeID < pair.Right.EdgeID && !seen[key] {
				seen[key] = true
				provisional.recordAggregate(pair)
			}
		}
	}
	outgoing := make([][]int, len(endpoints))
	incoming := make([]int, len(endpoints))
	for i, a := range endpoints {
		for j, b := range endpoints[i+1:] {
			j += i + 1
			key := comparisonKey{0, family, a, b}
			if provisional.aggregate[key].total == 0 {
				continue
			}
			result[i].ComparablePeers++
			result[j].ComparablePeers++
			choice := v.aggregateChoice(a, b, family)
			estimated := false
			if choice.Edge.EdgeID == "" {
				p := provisional.aggregate[key]
				// Strict agreement among available matched strata. An
				// estimate never turns a contradictory set into an order.
				switch {
				case len(estimates) == 0:
					continue
				case p.leftWins == p.total:
					choice = Choice{Edge: a, EvidenceAt: p.leftAt}
				case p.rightWins == p.total:
					choice = Choice{Edge: b, EvidenceAt: p.rightAt}
				default:
					continue
				}
				estimated = true
			}
			from, to := i, j
			if choice.Edge == b {
				from, to = j, i
			}
			outgoing[from] = append(outgoing[from], to)
			incoming[to]++
			for _, k := range []int{i, j} {
				if estimated {
					result[k].EstimatedPeers++
				} else {
					result[k].ConfidentPeers++
				}
				result[k].EvidenceAt = earlierEvidence(result[k].EvidenceAt, choice.EvidenceAt)
			}
		}
	}
	for i := range result {
		if result[i].ConfidentPeers+result[i].EstimatedPeers > 0 {
			result[i].Basis = "aggregate_conflict"
		} else if result[i].ComparablePeers > 0 {
			result[i].Basis = "insufficient_aggregate_evidence"
		}
	}
	for tier := 1; ; tier++ {
		var ready []int
		for i := range result {
			if result[i].ConfidentPeers+result[i].EstimatedPeers > 0 && result[i].Tier == 0 && incoming[i] == 0 {
				ready = append(ready, i)
			}
		}
		if len(ready) == 0 {
			break
		}
		for _, i := range ready {
			result[i].Tier, result[i].Basis = tier, "aggregate_partial_order"
			if result[i].ConfidentPeers == len(endpoints)-1 {
				result[i].Basis = "aggregate_compared_to_all"
			} else if result[i].EstimatedPeers > 0 {
				result[i].Basis = "provisional_matched_asn"
			}
			for _, peer := range outgoing[i] {
				incoming[peer]--
			}
		}
	}
	return result
}
