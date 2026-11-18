package edgequality

import (
	"cmp"
	"net/netip"
	"slices"
	"time"

	"fugue/internal/model"
)

// estimatePassiveFleetRTT compares same-ASN, same-family observations in
// closed windows. Its populations can span different prefixes, so these
// provisional estimates must never enter the signed DNS pair publication.
func estimatePassiveFleetRTT(windows map[passiveWindow]map[PassiveEndpoint][]*model.EdgeClientNetworkSample, asns map[netip.Prefix]uint32) []PassiveASNComparison {
	type cohort struct {
		asn    uint32
		family int
		at     time.Time
	}
	type population struct {
		prefixes []netip.Prefix
		medians  []float64
		n        int
		at       time.Time
	}
	cohorts := make(map[cohort]map[PassiveEndpoint]*population)
	for w, edges := range windows {
		family := 6
		if w.scope.Addr().Is4() {
			family = 4
		}
		key := cohort{asns[w.scope], family, w.at}
		if cohorts[key] == nil {
			cohorts[key] = make(map[PassiveEndpoint]*population)
		}
		for endpoint, observations := range edges {
			if len(observations) == 0 {
				continue
			}
			group := cohorts[key][endpoint]
			if group == nil {
				group = &population{}
				cohorts[key][endpoint] = group
			}
			rtts := make([]float64, 0, len(observations))
			for _, obs := range observations {
				rtts = append(rtts, *obs.RTTMS)
				if obs.ObservedAt.After(group.at) {
					group.at = obs.ObservedAt
				}
			}
			slices.Sort(rtts)
			group.n += len(rtts)
			group.medians = append(group.medians, passiveSortedMedian(rtts))
			group.prefixes = append(group.prefixes, w.scope)
		}
	}
	type evidence struct {
		row      PassiveASNComparison
		prefixes map[netip.Prefix]bool
		dense    bool
		wide     bool
	}
	pairs := make(map[string]*evidence)
	for scope, edges := range cohorts {
		if len(edges) < 2 {
			continue
		}
		endpoints := make([]PassiveEndpoint, 0, len(edges))
		for edge, group := range edges {
			endpoints = append(endpoints, edge)
			slices.Sort(group.medians)
		}
		slices.SortFunc(endpoints, func(a, b PassiveEndpoint) int { return cmp.Compare(a.EdgeID, b.EdgeID) })
		for i, a := range endpoints {
			for _, b := range endpoints[i+1:] {
				left, right := edges[a], edges[b]
				row := PassiveASNComparison{ASN: scope.asn, Family: scope.family, Left: a, Right: b}
				key := row.Key()
				ev := pairs[key]
				if ev == nil {
					ev = &evidence{row: row, prefixes: make(map[netip.Prefix]bool)}
					pairs[key] = ev
				}
				ev.dense = ev.dense || left.n >= 3 && right.n >= 3
				ev.row.SharedWindows++
				l, r := passiveSortedMedian(left.medians), passiveSortedMedian(right.medians)
				ev.wide = ev.wide || min(l, r) <= 0.75*max(l, r)
				if l <= 0.90*r {
					ev.row.LeftWinWindows++
				} else if r <= 0.90*l {
					ev.row.RightWinWindows++
				}
				if at := minTime(left.at, right.at); at.After(ev.row.ObservedAt) {
					ev.row.ObservedAt = at
				}
				for _, pop := range []*population{left, right} {
					for _, prefix := range pop.prefixes {
						if len(ev.prefixes) <= 32 {
							ev.prefixes[prefix] = true
						}
					}
				}
			}
		}
	}
	var estimates []PassiveASNComparison
	for _, ev := range pairs {
		// One sparse window is a weak operator hint only, and needs a
		// larger margin than an estimate supported by more observations.
		if (!ev.dense && ev.row.SharedWindows < 2 && !ev.wide) || len(ev.prefixes) > 32 {
			continue
		}
		row := ev.row
		if row.LeftWinWindows == row.SharedWindows {
			row.Preferred = row.Left.EdgeID
		} else if row.RightWinWindows == row.SharedWindows {
			row.Preferred = row.Right.EdgeID
		}
		for prefix := range ev.prefixes {
			row.Prefixes = append(row.Prefixes, prefix)
		}
		slices.SortFunc(row.Prefixes, func(a, b netip.Prefix) int { return a.Addr().Compare(b.Addr()) })
		estimates = append(estimates, row)
	}
	slices.SortFunc(estimates, func(a, b PassiveASNComparison) int { return cmp.Compare(a.Key(), b.Key()) })
	return estimates
}

func passiveSortedMedian(x []float64) float64 {
	return (x[(len(x)-1)/2] + x[len(x)/2]) / 2
}
