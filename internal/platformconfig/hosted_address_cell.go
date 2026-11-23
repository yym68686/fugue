package platformconfig

import (
	"errors"
	"maps"
	"reflect"
	"slices"
)

// Metadata is signed separately from content. Old consumers must still be
// able to strictly decode the DNS payload. The marker is for discovery only:
// inheritance requires the exact signed global predecessor's record, plan
// and alias-mapped eligible endpoints. Final full-behavior handoff is separate.
const HostedAddressReservationsDigestMetadata = "hosted_address_reservations_digest"

func inheritHostedCellRecords(intent PlatformIntent, previous *PreviousTrafficPublicationInput, current []DNSIntent, plan *DNSReadinessPlan, views []DNSQueryView, policy PolicySnapshot) ([]DNSIntent, error) {
	if previous == nil {
		return current, nil
	}
	ref, digest, err := HostedAddressAuthority(previous.DNS)
	parentRef, parentDigest, parentErr := HostedAddressAuthority(previous.Parent)
	bad := errors.New("cell cannot demonstrate the global reserved address and exact selected membership")
	if err != nil || parentErr != nil || digest != parentDigest {
		return nil, bad
	}
	if ref == nil {
		return current, nil
	}
	if parentRef == nil || *parentRef != *ref || previous.Reference.FencingToken <= ref.FencingToken || !DNSUsesRetainedRoutes(intent) || previous.Reference != intent.RouteAuthorityTransition.PreviousPublication ||
		previous.Reference.Validate() != nil || previous.Reference.DNSArtifactID != previous.DNS.ID || previous.Reference.DNSArtifactDigest != previous.DNS.ContentHash || plan == nil {
		return nil, bad
	}
	aliases, err := RouteAuthorityAliases(intent)
	if err != nil {
		return nil, err
	}
	alias := func(g string) string {
		if a, ok := aliases[g]; ok {
			return a
		}
		return g
	}
	type key = hostedRecordKey
	payload, err := DecodeDNSArtifact(previous.DNS)
	if err != nil || payload.ReadinessPlan == nil {
		return nil, bad
	}
	src := map[key]DNSIntent{}
	for _, r := range payload.Records {
		if r.Type != "A" && r.Type != "AAAA" {
			continue
		}
		k := key{r.Hostname, r.Type}
		if _, exists := src[k]; exists {
			return nil, bad
		}
		r.EdgeGroupID = alias(r.EdgeGroupID)
		r.FallbackEdgeGroupID = alias(r.FallbackEdgeGroupID)
		src[k] = r
	}
	// Compile readiness independently on the cell. Neither a declared extra
	// IP nor the original global ordering may introduce a new physical target.
	roster := func(p *DNSReadinessPlan, remap bool) (map[key]map[string]HostedAddressHold, error) {
		result := map[key]map[string]HostedAddressHold{}
		for _, r := range p.Records {
			for _, t := range r.Targets {
				k := key{r.Hostname, t.Family}
				if result[k] == nil {
					result[k] = map[string]HostedAddressHold{}
				}
				if _, exists := result[k][t.Address]; exists {
					return nil, bad
				}
				group := t.EdgeGroupID
				if remap {
					group = alias(group)
				}
				result[k][t.Address] = HostedAddressHold{t.EdgeID, group, t.Address}
			}
		}
		return result, nil
	}
	a, err := roster(payload.ReadinessPlan, true)
	if err != nil {
		return nil, err
	}
	b, err := roster(plan, false)
	if err != nil {
		return nil, err
	}
	out := slices.Clone(current)
	wanted := map[key]bool{}
	for k, p := range src {
		if p.RecordKind == "hosted" && len(a[k]) > 0 && len(p.Values) > len(a[k]) {
			wanted[k] = true
		}
	}
	if len(wanted) == 0 {
		return nil, bad
	}
	oldParents := hostedAliasParents(payload.Records, payload.ReadinessPlan)
	newParents := hostedAliasParents(current, plan)
	for k, parent := range oldParents {
		if wanted[parent] && len(src[k].Values) > len(a[k]) {
			if newParents[k] != parent {
				return nil, bad
			}
			wanted[k] = true
		}
	}
	coverage := map[hostedRecordKey]bool{}
	for k := range wanted {
		coverage[hostedRecordKey{k.host, k.family}] = true
	}
	if err := checkHostedDynamicSelectors(coverage, views, plan, policy); err != nil {
		return nil, err
	}
	for i, r := range current {
		k := key{r.Hostname, r.Type}
		if !wanted[k] {
			continue
		}
		p := src[k]
		if r.RecordKind != p.RecordKind || len(p.Values) <= len(r.Values) {
			return nil, bad
		}
		x, y := p, r
		x.Values, y.Values = nil, nil
		if len(a[k]) == 0 || !maps.Equal(a[k], b[k]) || !reflect.DeepEqual(x, y) {
			return nil, bad
		}
		active := slices.Clone(r.Values)
		slices.Sort(active)
		if !slices.Equal(active, slices.Sorted(maps.Keys(a[k]))) || len(p.ValueExpirations) != 0 {
			return nil, bad
		}
		reserved := map[string]bool{}
		for _, addr := range p.Values {
			if reserved[addr] {
				return nil, bad
			}
			reserved[addr] = true
		}
		for addr := range b[k] {
			if !reserved[addr] {
				return nil, bad
			}
		}
		p.Values = slices.Clone(p.Values)
		out[i] = p
		delete(wanted, k)
	}
	if len(wanted) != 0 {
		return nil, bad
	}
	return out, nil
}
