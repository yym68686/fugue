package platformconfig

import (
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"slices"

	"fugue/internal/model"
)

// SelectHostedHoldRequirements returns only this host's prerequisite
// changes, not a full DNS bundle. Foreign records retain their existing
// authority, expiry and consumer proofs; none can be published from this
// result. Inputs and exact holds must come from the fenced publisher, never
// from user-supplied target addresses or from computing a set difference.
type HostedHoldRequirements struct {
	Record string             `json:"record"`
	Family string             `json:"family"`
	Before DNSReadinessRecord `json:"before"`
	After  DNSReadinessRecord `json:"after"`
}

func SelectHostedHoldRequirements(host string, prior, staged DNSArtifactPayload, holds []HostedAddressHold) ([]HostedHoldRequirements, error) {
	bad := errors.New("hold stage is not an isolated same-owner prerequisite")
	if !validDNSConsumerZone(host) || len(holds) == 0 || prior.ReadinessPlan == nil || staged.ReadinessPlan == nil ||
		!reflect.DeepEqual(prior.Policy.DNSReadiness, staged.Policy.DNSReadiness) || ValidateDNSReadinessPlan(prior.ReadinessPlan, prior.Policy.DNSReadiness) != nil || ValidateDNSReadinessPlan(staged.ReadinessPlan, staged.Policy.DNSReadiness) != nil {
		return nil, errors.Join(bad, errors.New("missing hold or different/invalid readiness policy"))
	}
	type side struct {
		rows   map[hostedRecordKey]DNSIntent
		plans  map[string]DNSReadinessRecord
		probes map[string]DNSReadinessProbe
		keys   map[hostedRecordKey]bool
	}
	read := func(v DNSArtifactPayload) (side, error) {
		s := side{map[hostedRecordKey]DNSIntent{}, map[string]DNSReadinessRecord{}, map[string]DNSReadinessProbe{}, map[hostedRecordKey]bool{}}
		parents := hostedAliasParents(v.Records, v.ReadinessPlan)
		for _, p := range v.ReadinessPlan.Probes {
			s.probes[p.ID] = p
		}
		for _, row := range v.Records {
			k := hostedRecordKey{row.Hostname, row.Type}
			if _, ok := s.rows[k]; ok {
				return side{}, bad
			}
			s.rows[k] = row
			if row.Hostname == host && row.RecordKind == "hosted" && (k.family == "A" || k.family == "AAAA") || parents[k].host == host {
				s.keys[k] = true
			}
		}
		for _, row := range v.ReadinessPlan.Records {
			s.plans[row.Hostname] = row
			for _, t := range row.Targets {
				bound := s.keys[hostedRecordKey{row.Hostname, t.Family}]
				for _, id := range t.ProbeIDs {
					p := s.probes[id]
					if (p.Hostname == host) != bound {
						return side{}, errors.Join(bad, errors.New("reverse dependency is not the same owner's alias"))
					}
					if bound && (p.Path != "/" || p.State != "" || p.StaticEdge != nil || p.PreviousAuthority != nil || p.CellPublicationDigest != "") {
						return side{}, errors.Join(bad, errors.New("baseline probe is not an ordinary root requirement"))
					}
				}
			}
		}
		return s, nil
	}
	a, e := read(prior)
	if e != nil {
		return nil, e
	}
	b, e := read(staged)
	if e != nil {
		return nil, e
	}
	if len(a.keys) == 0 || !maps.Equal(a.keys, b.keys) {
		return nil, errors.Join(bad, errors.New("host or dependent keys changed"))
	}
	active := func(s side, k hostedRecordKey) (DNSReadinessRecord, []string) {
		r := s.plans[k.host]
		r.Targets = slices.DeleteFunc(slices.Clone(r.Targets), func(t DNSReadinessTarget) bool { return t.Family != k.family })
		var values []string
		for _, t := range r.Targets {
			values = append(values, t.Address)
		}
		return r, values
	}
	var out []HostedHoldRequirements
	for _, r := range staged.Records {
		k := hostedRecordKey{r.Hostname, r.Type}
		if !a.keys[k] {
			continue
		}
		old := a.rows[k]
		x, y := old, r
		x.Values, y.Values = nil, nil
		if !reflect.DeepEqual(x, y) || !sameHostedAddressSet(old.Values, r.Values) {
			return nil, errors.Join(bad, errors.New("wire record changed"))
		}
		from, previousValues := active(a, k)
		to, remaining := active(b, k)
		if len(previousValues) == len(remaining) {
			if !reflect.DeepEqual(from, to) {
				return nil, bad
			}
			continue
		}
		effective := r
		effective.Values = remaining
		if _, e := retainBoundHostedRecord(old, effective, from, to, holds, k.host != host); e != nil {
			return nil, bad
		}
		// Require exactly the same root probe *shape* at each surviving
		// target. A current route digest can differ; the caller separately
		// binds it to the signed old-shape route and TLS. No fact is copied.
		shape := func(s side, t DNSReadinessTarget) (DNSReadinessProbe, bool) {
			if len(t.ProbeIDs) != 1 {
				return DNSReadinessProbe{}, false
			}
			p := s.probes[t.ProbeIDs[0]]
			p.ID, p.RouteDigest = "", ""
			return p, true
		}
		oldByIP := map[string]DNSReadinessProbe{}
		for _, t := range from.Targets {
			p, ok := shape(a, t)
			if !ok {
				return nil, bad
			}
			oldByIP[t.Address] = p
		}
		for _, t := range to.Targets {
			p, ok := shape(b, t)
			if !ok || !reflect.DeepEqual(oldByIP[t.Address], p) {
				return nil, errors.Join(bad, errors.New("retained root requirement changed shape"))
			}
		}
		out = append(out, HostedHoldRequirements{r.Hostname, r.Type, from, to})
	}
	if len(out) == 0 || checkHostedDynamicSelectors(b.keys, staged.QueryViews, staged.ReadinessPlan, staged.Policy) != nil {
		return nil, errors.Join(bad, errors.New("no scoped changes or missing dynamic selector"))
	}
	// DNS query views are per (consumer, group, zone), not per machine.
	// A hosted root and its generated alias can legitimately be in two
	// different zones. Preserve the exact previously selected views of EACH
	// key; do not require unrelated zones to start publishing it.
	type viewKey struct{ node, group, zone string }
	coverage := map[viewKey]map[hostedRecordKey]bool{}
	for _, v := range prior.QueryViews {
		key := viewKey{v.NodeID, v.EdgeGroupID, v.Zone}
		wanted := map[hostedRecordKey]bool{}
		for _, q := range v.Records {
			k := hostedRecordKey{q.Name, q.Type}
			if a.keys[k] {
				if wanted[k] {
					return nil, bad
				}
				wanted[k] = true
			}
		}
		if len(wanted) > 0 {
			if coverage[key] != nil {
				return nil, bad
			}
			coverage[key] = wanted
		}
	}
	for _, view := range staged.QueryViews {
		seen := map[hostedRecordKey]bool{}
		for _, q := range view.Records {
			k := hostedRecordKey{q.Name, q.Type}
			if !b.keys[k] {
				continue
			}
			if seen[k] {
				return nil, errors.Join(bad, errors.New("duplicate scoped selector"))
			}
			seen[k] = true
			row, _ := active(b, k)
			byIP := map[string]DNSReadinessTarget{}
			for _, t := range row.Targets {
				byIP[t.Address] = t
			}
			check := func(cs []model.EdgeDNSAnswerCandidate) bool {
				for _, c := range cs {
					t, ok := byIP[c.IP]
					if !ok || c.EdgeID != t.EdgeID || c.EdgeGroupID != t.EdgeGroupID {
						return false
					}
				}
				return true
			}
			if !check(q.Candidates) {
				return nil, errors.Join(bad, errors.New("default selection is outside effective scope"))
			}
			for _, s := range q.ScopedCandidates {
				if !check(s.Candidates) {
					return nil, errors.Join(bad, errors.New("scoped selection is outside effective scope"))
				}
			}
		}
		key := viewKey{view.NodeID, view.EdgeGroupID, view.Zone}
		if !maps.Equal(seen, coverage[key]) {
			return nil, errors.Join(bad, errors.New("target consumer/zone selector coverage changed"))
		}
		delete(coverage, key)
	}
	if len(coverage) != 0 {
		return nil, errors.Join(bad, errors.New("a previously covered consumer/zone is missing"))
	}
	raw, e := json.Marshal(out)
	var owned []HostedHoldRequirements
	if e != nil || json.Unmarshal(raw, &owned) != nil {
		return nil, bad
	}
	return owned, nil
}
