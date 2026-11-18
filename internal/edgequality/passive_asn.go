package edgequality

import (
	"cmp"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	PassiveASNInterval = 5 * time.Minute
	PassiveASNWindow   = 30 * time.Minute
	// Exhaustion is reported, not handled by age-based eviction.
	MaxPassiveASNComparisons = 1024
)

type PassiveEndpoint struct {
	EdgeID  string `json:"edge_id"`
	GroupID string `json:"group_id"`
	Address string `json:"address"`
}

type PassiveEdgeIdentity struct {
	GroupID, IPv4, IPv6, Bundle string
	Front                       *PassiveFrontIdentity
}

type PassiveFrontIdentity struct {
	Backend          model.EdgeClientNetworkBackend
	ProcessStartedAt time.Time
	IPv4, IPv6       string
}

// PassiveSamplePrefix admits older route-scoped observations only when their
// original transport provenance still matches the selected public listener.
// A local socket witness has no such provenance and remains bundle-scoped.
func PassiveSamplePrefix(sample model.EdgeNetworkSample, id PassiveEdgeIdentity) (netip.Prefix, bool) {
	if sample.EdgeGroupID != id.GroupID ||
		(sample.Source != "public_front_tcp_info_v1" && sample.Source != model.EdgeNetworkPassiveFrontSource) ||
		model.ValidateEdgeNetworkSample(sample) != nil {
		return netip.Prefix{}, false
	}
	c := sample.ClientNetwork
	prefix, _ := netip.ParsePrefix(strings.TrimPrefix(c.Scope, "tcp_peer:"))
	if sample.Source == "public_front_tcp_info_v1" && c.Backend == nil {
		return prefix, id.Bundle != "" && id.Bundle == sample.BundleVersion
	}
	if id.Front == nil {
		return prefix, sample.Source == "public_front_tcp_info_v1" && id.Bundle != "" && id.Bundle == sample.BundleVersion
	}
	f := id.Front
	a, b := c.Backend, f.Backend
	if a == nil || a.Namespace != b.Namespace || a.PodName != b.PodName || a.PodUID != b.PodUID ||
		a.ServiceName != b.ServiceName || a.ServiceUID != b.ServiceUID || a.ServiceVersion != b.ServiceVersion ||
		f.ProcessStartedAt.IsZero() || c.StartedAt.Before(f.ProcessStartedAt) {
		return netip.Prefix{}, false
	}
	if prefix.Addr().Is4() {
		return prefix, f.IPv4 != "" && f.IPv4 == id.IPv4
	}
	return prefix, f.IPv6 != "" && f.IPv6 == id.IPv6
}

type PassiveASNComparison struct {
	ASN             uint32          `json:"asn"`
	Family          int             `json:"family"`
	Left            PassiveEndpoint `json:"left"`
	Right           PassiveEndpoint `json:"right"`
	Preferred       string          `json:"preferred,omitempty"`
	ObservedAt      time.Time       `json:"observed_at"`
	SharedWindows   int             `json:"shared_windows"`
	LeftWinWindows  int             `json:"left_win_windows"`
	RightWinWindows int             `json:"right_win_windows"`
	// Only coarse prefixes that contributed comparable windows. Source
	// refreshes must revalidate these before historical results are reused.
	Prefixes []netip.Prefix `json:"prefixes,omitempty"`
}

func (c PassiveASNComparison) Key() string {
	return fmt.Sprintf("%d/%d/%s/%s", c.ASN, c.Family, c.Left.EdgeID, c.Right.EdgeID)
}

type PassiveASNCoverage struct {
	Connections, Classified, TCPRTT, ClassifiedRTT int
	ReusedBundleSamples                            int
	FleetEstimates                                 []PassiveASNComparison
}

type passiveWindow struct {
	scope netip.Prefix
	at    time.Time
}

type passiveObservation struct {
	sample *model.EdgeClientNetworkSample
	edge   PassiveEndpoint
	prefix netip.Prefix
	asn    uint32
}

// ComparePassiveASN learns only the public Front TCP segment. Hostname,
// request, and upstream attributes never enter a comparison. A match within
// the same coarse prefix and time window limits population differences; an
// ASN is a sharing scope, not a claim that all of its paths are equivalent.
func ComparePassiveASN(samples []model.EdgeNetworkSample, identities map[string]PassiveEdgeIdentity, resolve func(netip.Prefix) uint32, now time.Time) ([]PassiveASNComparison, PassiveASNCoverage) {
	complete := now.UTC().Truncate(PassiveASNInterval)
	type connection struct {
		edge, slot, id string
		start          time.Time
	}
	latest := make(map[connection]passiveObservation)
	asns := make(map[netip.Prefix]uint32)
	var coverage PassiveASNCoverage
	for _, sample := range samples {
		id, ok := identities[sample.EdgeID]
		if !ok || sample.ObservedAt.Before(complete.Add(-PassiveASNWindow)) || sample.ObservedAt.After(now) {
			continue
		}
		prefix, admitted := PassiveSamplePrefix(sample, id)
		if !admitted {
			continue
		}
		if sample.Source == "public_front_tcp_info_v1" && id.Bundle != sample.BundleVersion {
			coverage.ReusedBundleSamples++
		}
		client := sample.ClientNetwork
		address := id.IPv6
		if prefix.Addr().Is4() {
			address = id.IPv4
		}
		endpoint, err := netip.ParseAddr(address)
		if err != nil || !endpoint.IsGlobalUnicast() || endpoint.IsPrivate() || endpoint.Is4In6() ||
			endpoint.Is4() != prefix.Addr().Is4() {
			continue
		}
		key := connection{sample.EdgeID, client.Slot, client.ConnectionID, client.StartedAt.UTC()}
		if previous := latest[key]; previous.sample != nil && !betterPassiveSnapshot(client, previous.sample) {
			continue
		}
		asn, known := asns[prefix]
		if !known {
			asn = resolve(prefix)
			asns[prefix] = asn
		}
		latest[key] = passiveObservation{client, PassiveEndpoint{sample.EdgeID, id.GroupID, endpoint.String()}, prefix, asn}
	}
	windows := make(map[passiveWindow]map[PassiveEndpoint][]*model.EdgeClientNetworkSample)
	for _, obs := range latest {
		coverage.Connections++
		if obs.asn != 0 {
			coverage.Classified++
		}
		if obs.sample.RTTMS == nil || *obs.sample.RTTMS <= 0 {
			continue
		}
		coverage.TCPRTT++
		if obs.asn == 0 {
			continue
		}
		coverage.ClassifiedRTT++
		if !obs.sample.ObservedAt.Before(complete) {
			continue // An open window never authorizes a new result.
		}
		key := passiveWindow{obs.prefix, obs.sample.ObservedAt.UTC().Truncate(PassiveASNInterval)}
		if windows[key] == nil {
			windows[key] = make(map[PassiveEndpoint][]*model.EdgeClientNetworkSample)
		}
		windows[key][obs.edge] = append(windows[key][obs.edge], obs.sample)
	}
	type tally struct {
		comparison PassiveASNComparison
		windows    map[time.Time]uint8
		prefixes   map[netip.Prefix]struct{}
	}
	tallies := map[string]*tally{}
	for window, edges := range windows {
		ranks := make([]passiveWindowEdge, 0, len(edges))
		for edge, connections := range edges {
			if len(connections) >= 3 {
				ranks = append(ranks, summarizePassiveWindow(edge, connections))
			}
		}
		slices.SortFunc(ranks, func(a, b passiveWindowEdge) int { return cmp.Compare(a.endpoint.EdgeID, b.endpoint.EdgeID) })
		for i, left := range ranks {
			for _, right := range ranks[i+1:] {
				family := 6
				if window.scope.Addr().Is4() {
					family = 4
				}
				row := PassiveASNComparison{ASN: asns[window.scope], Family: family, Left: left.endpoint, Right: right.endpoint}
				key := row.Key()
				state := tallies[key]
				if state == nil {
					state = &tally{comparison: row, windows: make(map[time.Time]uint8), prefixes: make(map[netip.Prefix]struct{})}
					tallies[key] = state
				}
				// Multiple prefixes must agree within one window. Neither
				// repeated observations nor more prefixes multiply its weight.
				flag := uint8(4)
				leftBetter, rightBetter := passiveWindowBetter(left, right), passiveWindowBetter(right, left)
				if leftBetter && !rightBetter {
					flag = 1
				} else if rightBetter && !leftBetter {
					flag = 2
				}
				state.windows[window.at] |= flag
				if len(state.prefixes) <= 32 {
					state.prefixes[window.scope] = struct{}{}
				}
				at := minTime(left.at, right.at)
				if at.After(state.comparison.ObservedAt) {
					state.comparison.ObservedAt = at
				}
			}
		}
	}
	result := []PassiveASNComparison{}
	for _, state := range tallies {
		row := state.comparison
		row.SharedWindows = len(state.windows)
		if row.SharedWindows < 3 {
			continue
		}
		if len(state.prefixes) <= 32 {
			row.Prefixes = slices.SortedFunc(maps.Keys(state.prefixes), func(a, b netip.Prefix) int {
				if c := a.Addr().Compare(b.Addr()); c != 0 {
					return c
				}
				return cmp.Compare(a.Bits(), b.Bits())
			})
		}
		for _, flag := range state.windows {
			if flag == 1 {
				row.LeftWinWindows++
			} else if flag == 2 {
				row.RightWinWindows++
			}
		}
		if row.LeftWinWindows == row.SharedWindows {
			row.Preferred = row.Left.EdgeID
		}
		if row.RightWinWindows == row.SharedWindows {
			row.Preferred = row.Right.EdgeID
		}
		result = append(result, row)
	}
	slices.SortFunc(result, func(a, b PassiveASNComparison) int { return cmp.Compare(a.Key(), b.Key()) })
	coverage.FleetEstimates = estimatePassiveFleetRTT(windows, asns)
	return result, coverage
}

func betterPassiveSnapshot(current, previous *model.EdgeClientNetworkSample) bool {
	currentRTT, previousRTT := current.RTTMS != nil && *current.RTTMS > 0, previous.RTTMS != nil && *previous.RTTMS > 0
	if currentRTT != previousRTT {
		return currentRTT
	}
	currentDelivery, _ := model.EdgeClientDeliveryMetrics(current)
	previousDelivery, _ := model.EdgeClientDeliveryMetrics(previous)
	if (currentDelivery != nil) != (previousDelivery != nil) {
		return currentDelivery != nil
	}
	return current.ObservedAt.After(previous.ObservedAt)
}

type passiveWindowEdge struct {
	endpoint         PassiveEndpoint
	at               time.Time
	rtt, dl, retrans []float64
}

func summarizePassiveWindow(edge PassiveEndpoint, observations []*model.EdgeClientNetworkSample) passiveWindowEdge {
	result := passiveWindowEdge{endpoint: edge}
	for _, obs := range observations {
		result.rtt = append(result.rtt, *obs.RTTMS)
		download, retrans := model.EdgeClientDeliveryMetrics(obs)
		if download != nil {
			result.dl = append(result.dl, *download)
		}
		if retrans != nil {
			result.retrans = append(result.retrans, *retrans)
		}
		if obs.ObservedAt.After(result.at) {
			result.at = obs.ObservedAt
		}
	}
	slices.Sort(result.rtt)
	slices.Sort(result.dl)
	slices.Sort(result.retrans)
	return result
}

func passiveWindowBetter(a, b passiveWindowEdge) bool {
	lower := func(values []float64) float64 { return values[(len(values)-1)/10] }
	middle := func(values []float64) float64 { return values[len(values)/2] }
	upper := func(values []float64) float64 { return values[len(values)-1-(len(values)-1)/10] }
	if upper(a.rtt) > 1.15*upper(b.rtt) ||
		len(a.dl) >= 3 && len(b.dl) >= 3 && lower(a.dl) < 0.85*lower(b.dl) ||
		len(a.retrans) >= 3 && len(b.retrans) >= 3 && upper(a.retrans) > upper(b.retrans)+0.02 {
		return false
	}
	return middle(b.rtt)-middle(a.rtt) >= 10 && middle(a.rtt) <= 0.85*middle(b.rtt) ||
		len(a.dl) >= 3 && len(b.dl) >= 3 && middle(a.dl) >= 1.2*middle(b.dl)
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
