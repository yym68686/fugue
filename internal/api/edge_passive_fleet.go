package api

import (
	"net/netip"
	"slices"
	"strings"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/passiveasn"
)

const passiveFleetSnapshotMaxBindings = 4096

// Fleet snapshots are diagnostic only. They cannot sign a comparison, affect
// a live answer, or renew its evidence time. Retain one per address family.
type passiveFleetSnapshot struct {
	At       time.Time              `json:"at"`
	Ranks    []passiveasn.FleetRank `json:"ranks"`
	Bindings []passiveasn.Binding   `json:"bindings"`
}

func capturePassiveFleet(now time.Time, ranks []passiveasn.FleetRank, batches ...[]edgequality.PassiveASNComparison) passiveFleetSnapshot {
	if len(ranks) < 2 || len(ranks) > 64 {
		return passiveFleetSnapshot{}
	}
	members := make(map[edgequality.PassiveEndpoint]bool, len(ranks))
	for _, rank := range ranks {
		if rank.Tier == 0 || rank.EvidenceAt.IsZero() || rank.EvidenceAt.After(now) {
			return passiveFleetSnapshot{}
		}
		members[rank.Edge] = true
	}
	bindings := make(map[netip.Prefix]uint32)
	for _, batch := range batches {
		for _, pair := range batch {
			if pair.Family != ranks[0].Family || !members[pair.Left] || !members[pair.Right] {
				continue
			}
			for _, prefix := range pair.Prefixes {
				if asn := bindings[prefix]; asn != 0 && asn != pair.ASN {
					return passiveFleetSnapshot{}
				}
				bindings[prefix] = pair.ASN
				if len(bindings) > passiveFleetSnapshotMaxBindings {
					return passiveFleetSnapshot{}
				}
			}
		}
	}
	if len(bindings) == 0 {
		return passiveFleetSnapshot{}
	}
	out := passiveFleetSnapshot{At: now, Ranks: slices.Clone(ranks), Bindings: make([]passiveasn.Binding, 0, len(bindings))}
	for prefix, asn := range bindings {
		out.Bindings = append(out.Bindings, passiveasn.Binding{Prefix: prefix, ASN: asn})
	}
	slices.SortFunc(out.Bindings, func(a, b passiveasn.Binding) int { return strings.Compare(a.Prefix.String(), b.Prefix.String()) })
	return out
}

func (p passiveFleetSnapshot) matches(now time.Time, live []passiveasn.FleetRank, resolve func(netip.Prefix) uint32) bool {
	if p.At.IsZero() || p.At.After(now) || len(p.Bindings) == 0 || len(p.Bindings) > passiveFleetSnapshotMaxBindings ||
		len(live) < 2 || len(live) > 64 || len(live) != len(p.Ranks) {
		return false
	}
	// RankFleet returns canonical Edge ID order. Do not reuse numeric tiers
	// across an inventory change or combine ranks from unrelated snapshots.
	for i, r := range p.Ranks {
		if r.Edge != live[i].Edge || r.Family != live[i].Family || r.Tier <= 0 || r.EvidenceAt.IsZero() || r.EvidenceAt.After(p.At) {
			return false
		}
	}
	for _, b := range p.Bindings {
		if b.ASN == 0 || resolve(b.Prefix) != b.ASN {
			return false
		}
	}
	return true
}

func selectPassiveFleet(now time.Time, family int, endpoints []edgequality.PassiveEndpoint, comparisons, estimates []edgequality.PassiveASNComparison,
	replayAt time.Time, replayComparisons, replayEstimates []edgequality.PassiveASNComparison, previous passiveFleetSnapshot, resolve func(netip.Prefix) uint32,
) (live, effective []passiveasn.FleetRank, retained passiveFleetSnapshot, fallbackAt time.Time) {
	live = passiveasn.RankFleet(family, endpoints, comparisons, estimates)
	if !previous.matches(now, live, resolve) {
		previous = passiveFleetSnapshot{}
	}
	if !slices.ContainsFunc(live, func(r passiveasn.FleetRank) bool { return r.Tier == 0 }) {
		retained = capturePassiveFleet(now, live, comparisons, estimates)
		if !retained.matches(now, live, resolve) {
			retained = previous
		}
		return live, live, retained, time.Time{}
	}
	// Bootstrap from an actual independently replayed historical window,
	// not by mixing individual ranks or counting a tie as a win.
	if replayAt.After(previous.At) && (len(replayComparisons) != 0 || len(replayEstimates) != 0) {
		prior := passiveasn.RankFleet(family, endpoints, replayComparisons, replayEstimates)
		if snap := capturePassiveFleet(replayAt, prior, replayComparisons, replayEstimates); snap.matches(now, live, resolve) {
			previous = snap
		}
	}
	if previous.At.IsZero() {
		if scored := passiveasn.RankComparableFleet(live, comparisons, estimates); len(scored) != 0 {
			snapshot := capturePassiveFleet(now, scored, comparisons, estimates)
			if snapshot.matches(now, live, resolve) {
				return live, scored, snapshot, time.Time{}
			}
		}
		return live, live, previous, time.Time{}
	}
	effective = slices.Clone(previous.Ranks)
	for i := range effective {
		effective[i].Basis = "historical_" + effective[i].Basis
	}
	return live, effective, previous, previous.At
}
