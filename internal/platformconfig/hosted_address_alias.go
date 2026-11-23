package platformconfig

import (
	"maps"
	"reflect"
	"slices"
)

// A generated address alias is not itself a managed host. Its reservation is
// derived per address family and per capture, from a real same-owner parent.
// No retention for a CNAME, mixed-host alias, or a record that merely happens
// to use the same IP. Compare each physical target's full probe ID set.
func hostedAliasParents(records []DNSIntent, plan *DNSReadinessPlan) map[hostedRecordKey]hostedRecordKey {
	out := map[hostedRecordKey]hostedRecordKey{}
	if plan == nil {
		return out
	}
	rr := map[hostedRecordKey]DNSIntent{}
	for _, r := range records {
		k := hostedRecordKey{r.Hostname, r.Type}
		if _, dup := rr[k]; dup {
			return nil
		}
		rr[k] = r
	}
	probe := map[string]DNSReadinessProbe{}
	for _, p := range plan.Probes {
		if p.ID == "" || probe[p.ID].ID != "" {
			return nil
		}
		probe[p.ID] = p
	}
	type target struct{ edge, group, addr string }
	byKey := map[hostedRecordKey]map[target][]string{}
	for _, r := range plan.Records {
		for _, t := range r.Targets {
			k := hostedRecordKey{r.Hostname, t.Family}
			if byKey[k] == nil {
				byKey[k] = map[target][]string{}
			}
			pk := target{t.EdgeID, t.EdgeGroupID, t.Address}
			if _, dup := byKey[k][pk]; dup {
				return nil
			}
			ids := slices.Clone(t.ProbeIDs)
			slices.Sort(ids)
			byKey[k][pk] = ids
		}
	}
	for k, r := range rr {
		if r.RecordKind != "custom-domain-target" || (k.family != "A" && k.family != "AAAA") || r.AppID == "" || r.TenantID == "" || len(byKey[k]) == 0 {
			continue
		}
		host := ""
		ok := true
		for t, ids := range byKey[k] {
			if len(ids) == 0 {
				ok = false
			}
			for i, id := range ids {
				p := probe[id]
				if p.Hostname == "" || p.Hostname == k.host || (host != "" && p.Hostname != host) || p.EdgeID != t.edge || p.EdgeGroupID != t.group || p.Address != t.addr || i > 0 && id == ids[i-1] {
					ok = false
					break
				}
				host = p.Hostname
			}
			if !ok {
				break
			}
		}
		parent := hostedRecordKey{host, k.family}
		root := rr[parent]
		if ok && root.RecordKind == "hosted" && root.AppID == r.AppID && root.TenantID == r.TenantID && maps.EqualFunc(byKey[k], byKey[parent], slices.Equal[[]string]) && sameHostedAddressSet(r.Values, root.Values) {
			out[k] = parent
		}
	}
	return out
}

func sameHostedAddressSet(a, b []string) bool {
	if len(a) == 0 || len(a) != len(b) {
		return false
	}
	x, y := slices.Clone(a), slices.Clone(b)
	slices.Sort(x)
	slices.Sort(y)
	return slices.Equal(x, y)
}

// Return the records whose physical dependencies and business owner identify
// this host in the verified DNS child. A client-provided alias list is never
// used to determine the local predecessor or the initial reservation.
func HostedDNSOwnedRecords(host string, records []DNSIntent, plan *DNSReadinessPlan) []DNSIntent {
	aliases := hostedAliasParents(records, plan)
	var owned []DNSIntent
	for _, rr := range records {
		if rr.Type != "A" && rr.Type != "AAAA" {
			continue
		}
		if (rr.Hostname == host && rr.RecordKind == "hosted") || aliases[hostedRecordKey{rr.Hostname, rr.Type}].host == host {
			owned = append(owned, rr)
		}
	}
	return owned
}

// Keep explicit symbolic changes outside this operation. Only the per-path
// bindings of the one retained host can legitimately differ here. Their
// actual membership is checked by the regular route compiler and by the
// independently compiled probe graph above.
func stableHostedAliasDeclarations(before, after PlatformIntent) map[string]string {
	old := map[string]DNSIntent{}
	for _, r := range before.DNS {
		if r.Type == "FUGUE_ROUTE" {
			old[r.Hostname] = r
		}
	}
	out := map[string]string{}
	for _, r := range after.DNS {
		b := old[r.Hostname]
		if r.Type != "FUGUE_ROUTE" || r.RecordKind != "custom-domain-target" || r.Route == nil || b.Route == nil || r.Route.ManagedHost || len(r.Route.Hostnames) != 1 {
			continue
		}
		x, y := *r.Route, *b.Route
		x.Bindings, y.Bindings = nil, nil
		r.Route, b.Route = &x, &y
		if reflect.DeepEqual(b, r) {
			out[r.Hostname] = x.Hostnames[0]
		}
	}
	return out
}
