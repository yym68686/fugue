package platformconfig

import (
	"errors"
	"maps"
	"net/netip"
	"reflect"
	"slices"
)

// HostedAddressHold identifies a suspended physical address, not a grant to
// offer it in a DNS answer. An IPv4 hold does not imply an IPv6 sibling hold.
type HostedAddressHold struct {
	EdgeID, EdgeGroupID, Address string
}

// HostedAddressReservation is a physical roster, not a set of positive
// readiness proofs. Persist it in the authenticated compiler input, never as
// a source of addresses for a query. Source names the full publication from
// which this capture inherited the roster, not the candidate itself.
type HostedAddressReservation struct {
	Source   PreviousTrafficPublicationReference `json:"source"`
	Record   DNSIntent                           `json:"record"`
	Physical DNSReadinessRecord                  `json:"physical"`
}

func hostedPhysicalRoster(in DNSReadinessRecord, family string) DNSReadinessRecord {
	out := in
	out.MinDistinctDomains = maps.Clone(in.MinDistinctDomains)
	out.Targets = nil
	for _, t := range in.Targets {
		if t.Family != family {
			continue
		}
		t.ProbeIDs = []string{}
		t.FailureDomains = maps.Clone(t.FailureDomains)
		out.Targets = append(out.Targets, t)
	}
	return out
}

// CaptureHostedAddressReservation must receive a signature-verified full and
// its own verified runtime input. An inherited reservation is read from THAT
// input, not picked by host from an unrelated older successful publication.
// Both the old and new effective rosters are checked against the physical
// roster; a matching IP on a different owner or a disappeared hold is not a
// reason to keep a reservation. No readiness proof is copied across captures.
func CaptureHostedAddressReservation(source PreviousTrafficPublicationReference, prior DNSIntent, priorEffective DNSReadinessRecord, priorHolds []HostedAddressHold, inherited *HostedAddressReservation, next DNSIntent, nextEffective DNSReadinessRecord, nextHolds []HostedAddressHold) (HostedAddressReservation, error) {
	bad := errors.New("cannot establish hosted address reservation lineage")
	if source.Validate() != nil {
		return HostedAddressReservation{}, bad
	}
	physical := hostedPhysicalRoster(priorEffective, prior.Type)
	if inherited != nil {
		if inherited.Source.Validate() != nil || inherited.Source.FencingToken >= source.FencingToken ||
			inherited.Source.ReleaseID == source.ReleaseID || inherited.Source.ReleaseSetID == source.ReleaseSetID ||
			!reflect.DeepEqual(inherited.Record, prior) ||
			!reflect.DeepEqual(inherited.Physical, hostedPhysicalRoster(inherited.Physical, prior.Type)) {
			return HostedAddressReservation{}, bad
		}
		physical = hostedPhysicalRoster(inherited.Physical, prior.Type)
		oldSelected := prior
		oldSelected.Values = nil
		for _, t := range priorEffective.Targets {
			if t.Family == prior.Type {
				oldSelected.Values = append(oldSelected.Values, t.Address)
			}
		}
		if _, err := RetainHostedAddressRecord(prior, oldSelected, physical, priorEffective, priorHolds); err != nil {
			return HostedAddressReservation{}, bad
		}
	}
	record, err := RetainHostedAddressRecord(prior, next, physical, nextEffective, nextHolds)
	if err != nil {
		return HostedAddressReservation{}, err
	}
	return HostedAddressReservation{Source: source, Record: record, Physical: physical}, nil
}

// RetainHostedAddressRecord computes only an address declaration, not
// readiness, a query view or permission to publish. The producer must supply
// the verified prior record and independently authenticated effective holds.
// Any difference other than removing precisely held targets from selection is
// a separate topology operation. Never synthesize a DNS value from a hold.
func RetainHostedAddressRecord(prior, next DNSIntent, before, after DNSReadinessRecord, holds []HostedAddressHold) (DNSIntent, error) {
	bad := errors.New("cannot separate hosted address declaration from effective membership")
	if prior.RecordKind != "hosted" || prior.AppID == "" || prior.TenantID == "" || prior.Hostname == "" ||
		prior.Type != "A" && prior.Type != "AAAA" || prior.TTL < 1 || len(prior.ValueExpirations) != 0 ||
		prior.Route != nil || prior.Application != nil || prior.Flatten != nil ||
		before.Hostname != prior.Hostname || after.Hostname != before.Hostname || len(holds) == 0 || len(holds) > 4096 {
		return DNSIntent{}, bad
	}
	p, n := prior, next
	p.Values, n.Values = nil, nil
	a, b := before, after
	a.Targets, b.Targets = nil, nil
	if !reflect.DeepEqual(p, n) || !reflect.DeepEqual(a, b) {
		return DNSIntent{}, bad
	}
	held := map[HostedAddressHold]bool{}
	for _, h := range holds {
		ip, e := netip.ParseAddr(h.Address)
		if e != nil || ip.String() != h.Address || !PublicDNSFlattenIP(ip) || !validDNSConsumerIdentity(h.EdgeID) || !platformRouteArtifactGroupID.MatchString(h.EdgeGroupID) || held[h] {
			return DNSIntent{}, bad
		}
		held[h] = true
	}
	targets := func(record DNSReadinessRecord, rr DNSIntent) (map[string]DNSReadinessTarget, bool) {
		out := map[string]DNSReadinessTarget{}
		if len(record.Targets) > 4096 || len(rr.Values) == 0 || len(rr.Values) > 4096 {
			return nil, false
		}
		for _, t := range record.Targets {
			if t.Family != rr.Type {
				continue
			}
			ip, e := netip.ParseAddr(t.Address)
			_, duplicate := out[t.Address]
			if e != nil || ip.String() != t.Address || !PublicDNSFlattenIP(ip) || (rr.Type == "A") != ip.Is4() ||
				!validDNSConsumerIdentity(t.EdgeID) || !platformRouteArtifactGroupID.MatchString(t.EdgeGroupID) || duplicate {
				return nil, false
			}
			t.ProbeIDs = nil // path proofs must come from the successor, never from this record
			out[t.Address] = t
		}
		values := slices.Clone(rr.Values)
		slices.Sort(values)
		return out, slices.Equal(values, slices.Sorted(maps.Keys(out)))
	}
	old, ok := targets(before, prior)
	active, okNext := targets(after, next)
	if !ok || !okNext || len(active) >= len(old) {
		return DNSIntent{}, bad
	}
	for addr, t := range old {
		live, present := active[addr]
		suspended := held[HostedAddressHold{t.EdgeID, t.EdgeGroupID, addr}]
		if present == suspended || present && !reflect.DeepEqual(t, live) {
			return DNSIntent{}, bad
		}
	}
	for addr := range active {
		if _, exists := old[addr]; !exists {
			return DNSIntent{}, bad
		}
	}
	result := prior
	result.Values = slices.Clone(prior.Values)
	return result, nil
}
