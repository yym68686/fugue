package platformconfig

import (
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"slices"
	"strings"

	"fugue/internal/model"
)

// HostedDNSContent is executable configuration, not a publication. There is
// intentionally no lineage, lease, observation, generation or wire RR here.
// A publisher must authenticate both inputs and bind the result to a new
// selection; these fields may not be served under the old signature.
type HostedDNSContent struct {
	Plan  DNSReadinessPlan
	Views []DNSQueryView
	Rules []DNSAnswerRule
}

// ComposeHostedDNSPhase takes an already authenticated selected base and a
// compiler-owned proposal. It reads no global shadow. A hold subtracts only
// the signed addresses (including from the default and every client view).
// Path withdrawal returns to prepared, not before: it cannot admit an edge.
func ComposeHostedDNSPhase(base DNSArtifactPayload, p HostedScopeProposal, phase string) (HostedDNSContent, error) {
	if p.ExternalPath != nil {
		return replayExternalDNSPhase(base, p, phase)
	}
	var out, empty HostedDNSContent
	bad := errors.New("hosted DNS phase does not match its selected predecessor")
	if p.Schema != HostedProposalSchema || p.Base.Validate() != nil || p.Host != p.Before.Owner.Hostname || p.Before.Owner != p.After.Owner || p.Before.Owner != p.Prepared.Owner ||
		base.Policy.Scope != GlobalScopeKey || base.Policy.DNSPlacementMode != DNSPlacementConsumerReadiness || base.ReadinessPlan == nil || len(p.Hold) == 0 || len(p.Changes) == 0 {
		return empty, bad
	}
	source, dest := p.HoldProbes, p.PathProbes
	switch phase {
	case "prepare":
		source, dest = p.BaseProbes, p.HoldProbes
	case "forward":
	case "withdraw":
		source, dest = p.PathProbes, p.HoldProbes
	default:
		return empty, bad
	}
	probes := func(rows []DNSReadinessProbe) (map[string]DNSReadinessProbe, error) {
		m := map[string]DNSReadinessProbe{}
		for _, v := range rows {
			if _, dup := m[v.ID]; dup || v.Hostname != p.Host || v.ID == "" {
				return nil, bad
			}
			m[v.ID] = v
		}
		if len(m) == 0 {
			return nil, bad
		}
		return m, nil
	}
	prior, e := probes(source)
	if e != nil {
		return empty, e
	}
	next, e := probes(dest)
	if e != nil {
		return empty, e
	}
	// Copy nested values as well. Compiling a failed phase must never
	// mutate a cached immutable artifact or a concurrent reader's plan.
	raw, e := json.Marshal(HostedDNSContent{*base.ReadinessPlan, base.QueryViews, base.Policy.DNSAnswerRules})
	if e != nil || json.Unmarshal(raw, &out) != nil {
		return empty, bad
	}
	got := map[string]DNSReadinessProbe{}
	ps := make([]DNSReadinessProbe, 0, len(out.Plan.Probes)+len(next))
	for _, v := range out.Plan.Probes {
		if v.Hostname == p.Host {
			if _, dup := got[v.ID]; dup {
				return empty, bad
			}
			got[v.ID] = v
		} else {
			if _, collides := next[v.ID]; collides {
				return empty, bad
			}
			ps = append(ps, v)
		}
	}
	if !reflect.DeepEqual(got, prior) {
		return empty, bad
	}
	ps = append(ps, dest...)
	slices.SortFunc(ps, func(a, b DNSReadinessProbe) int { return strings.Compare(a.ID, b.ID) })
	out.Plan.Probes = ps
	owners := map[hostedRecordKey]bool{}
	parents := hostedAliasParents(base.Records, base.ReadinessPlan)
	declared := map[hostedRecordKey]DNSIntent{}
	for _, v := range p.Records {
		k := hostedRecordKey{v.Hostname, v.Type}
		if _, dup := declared[k]; dup {
			return empty, bad
		}
		declared[k] = v
	}
	for _, v := range base.Records {
		k := hostedRecordKey{v.Hostname, v.Type}
		if v.Type != "A" && v.Type != "AAAA" || !(v.Hostname == p.Host && v.RecordKind == "hosted" || parents[k].host == p.Host) {
			continue
		}
		if !reflect.DeepEqual(v, declared[k]) || v.AppID != p.Before.Owner.AppID || v.TenantID != p.Before.Owner.TenantID {
			return empty, bad
		}
		owners[k] = true
	}
	if len(owners) == 0 || len(owners) != len(declared) {
		return empty, bad
	}
	type targetKey struct{ record, edge, address string }
	edits := map[targetKey]HostedReadinessTargetEdit{}
	holds := map[hostedRecordKey]HostedHoldRequirements{}
	for _, v := range p.Hold {
		k := hostedRecordKey{v.Record, v.Family}
		if _, dup := holds[k]; dup || !owners[k] || v.Before.Hostname != v.Record || v.After.Hostname != v.Record {
			return empty, bad
		}
		holds[k] = v
	}
	if phase != "prepare" {
		for _, v := range p.Changes {
			k := targetKey{v.Record, v.Edge, v.Address}
			if _, dup := edits[k]; dup {
				return empty, bad
			}
			edits[k] = v
		}
	}
	unusedHolds := maps.Clone(holds)
	for i, r := range out.Plan.Records {
		// Same addresses and probe IDs do not imply the same minimum
		// quorum, cell spread, failure domains or publication requirement.
		// Check the whole prepared family, including on withdrawal.
		if phase != "prepare" {
			for _, family := range []string{"A", "AAAA"} {
				k := hostedRecordKey{r.Hostname, family}
				h, ok := holds[k]
				if !ok {
					continue
				}
				familyShape := func(v DNSReadinessRecord) DNSReadinessRecord {
					ts := make([]DNSReadinessTarget, 0, len(v.Targets))
					for _, t := range v.Targets {
						if t.Family == family {
							t.ProbeIDs = nil
							ts = append(ts, t)
						}
					}
					v.Targets = ts
					return v
				}
				if !reflect.DeepEqual(familyShape(r), familyShape(h.After)) {
					return empty, bad
				}
				delete(unusedHolds, k)
			}
		}
		var ts []DNSReadinessTarget
		for _, t := range r.Targets {
			k := hostedRecordKey{r.Hostname, t.Family}
			owned := owners[k]
			for _, id := range t.ProbeIDs {
				if _, ok := prior[id]; ok != owned {
					return empty, bad
				}
			}
			if phase == "prepare" {
				if h, ok := holds[k]; ok {
					if _, first := unusedHolds[k]; first {
						from := r
						from.Targets = slices.DeleteFunc(slices.Clone(r.Targets), func(t DNSReadinessTarget) bool { return t.Family != k.family })
						if !reflect.DeepEqual(from, h.Before) {
							return empty, bad
						}
						ts = append(ts, h.After.Targets...)
						delete(unusedHolds, k)
					}
					continue
				}
			} else if owned {
				at := targetKey{r.Hostname, t.EdgeID, t.Address}
				v, ok := edits[at]
				before, after := v.Before, v.After
				if phase == "withdraw" {
					before, after = after, before
				}
				if !ok || !slices.Equal(slices.Sorted(slices.Values(t.ProbeIDs)), before) {
					return empty, bad
				}
				t.ProbeIDs = slices.Clone(after)
				delete(edits, at)
			}
			ts = append(ts, t)
		}
		out.Plan.Records[i].Targets = ts
	}
	if len(edits) != 0 || len(unusedHolds) != 0 {
		return empty, bad
	}
	fallbacks := map[hostedQueryKey]HostedQueryFallback{}
	for _, f := range p.Fallbacks {
		if _, dup := fallbacks[f.key()]; dup {
			return empty, bad
		}
		fallbacks[f.key()] = f
	}
	if len(fallbacks) > 0 {
		rules := map[string]int{}
		for i, r := range out.Rules {
			key := dnsQueryKey(r.NodeID, r.Hostname, r.Type)
			if _, dup := rules[key]; dup {
				return empty, bad
			}
			rules[key] = i
		}
		for i := range out.Views {
			v := &out.Views[i]
			for j := range v.Records {
				q := &v.Records[j]
				k := hostedQueryKey{v.NodeID, v.EdgeGroupID, v.Zone, q.Name, q.Type}
				f, ok := fallbacks[k]
				if !ok {
					continue
				}
				z, ok := rules[dnsQueryKey(v.NodeID, q.Name, q.Type)]
				if !ok {
					return empty, bad
				}
				h, ok := holds[hostedRecordKey{q.Name, q.Type}]
				if !ok {
					return empty, bad
				}
				if phase == "prepare" {
					if !validHostedFallback(*q, out.Rules[z], base.Policy, h, f) {
						return empty, errHostedFallback
					}
					// Filtering the output below must never overwrite a signed
					// proposal's backing arrays, including on validation failure.
					cloned, err := json.Marshal(f)
					var detached HostedQueryFallback
					if err != nil || json.Unmarshal(cloned, &detached) != nil {
						return empty, bad
					}
					*q, out.Rules[z] = detached.Query, detached.Rule
				} else if !reflect.DeepEqual(*q, f.Query) || !reflect.DeepEqual(out.Rules[z], f.Rule) {
					return empty, errHostedFallback
				}
				delete(fallbacks, k)
			}
		}
		if len(fallbacks) > 0 {
			return empty, bad
		}
	}
	if phase == "prepare" {
		for i := range out.Views {
			for j := range out.Views[i].Records {
				r := &out.Views[i].Records[j]
				h, ok := holds[hostedRecordKey{r.Name, r.Type}]
				if !ok {
					continue
				}
				permit := map[string]bool{}
				for _, t := range h.After.Targets {
					permit[t.Address] = true
				}
				r.Values = slices.DeleteFunc(r.Values, func(address string) bool { return !permit[address] })
				drop := func(c model.EdgeDNSAnswerCandidate) bool { return !permit[c.IP] }
				r.Candidates = slices.DeleteFunc(r.Candidates, drop)
				for q := range r.ScopedCandidates {
					v := &r.ScopedCandidates[q]
					v.Candidates = slices.DeleteFunc(v.Candidates, drop)
				}
			}
		}
	}
	if e := ValidateDNSReadinessPlan(&out.Plan, base.Policy.DNSReadiness); e != nil {
		return empty, errors.Join(bad, e)
	}
	projectedPolicy := base.Policy
	projectedPolicy.DNSAnswerRules = out.Rules
	if e := ValidateDNSQueryViews(out.Views, base.Records, base.ConsumerViews, &out.Plan, projectedPolicy); e != nil {
		return empty, errors.Join(bad, e)
	}
	if e := checkHostedDynamicSelectors(owners, out.Views, &out.Plan, base.Policy); e != nil {
		return empty, errors.Join(bad, e)
	}
	if phase == "prepare" {
		eligible := map[HostedAddressHold]bool{}
		for _, n := range p.Eligible {
			eligible[n] = true
		}
		var excluded []HostedAddressHold
		for _, n := range p.Physical {
			if !eligible[n] {
				excluded = append(excluded, n)
			}
		}
		staged := base
		staged.Policy = projectedPolicy
		staged.QueryViews, staged.ReadinessPlan = out.Views, &out.Plan
		checked, e := SelectHostedHoldRequirements(p.Host, base, staged, excluded)
		if e != nil || !reflect.DeepEqual(checked, p.Hold) {
			return empty, errors.Join(bad, e)
		}
	} else {
		paths := func(v HostedDispatchRows) []string {
			ps := make([]string, 0, len(v.Routes))
			for _, r := range v.Routes {
				ps = append(ps, r.PathPrefix)
			}
			return ps
		}
		a, b := base.ReadinessPlan, &out.Plan
		if phase == "withdraw" {
			a, b = b, a
		}
		d, e := PlanHostedReadinessDelta(p.Host, a, b, base.Policy.DNSReadiness, paths(p.Prepared), paths(p.After))
		if e != nil || !reflect.DeepEqual(d.Edits, p.Changes) {
			return empty, errors.Join(bad, e)
		}
	}
	// No alias into the caller's proposal either.
	raw, e = json.Marshal(out)
	if e != nil {
		return empty, e
	}
	var detached HostedDNSContent
	e = json.Unmarshal(raw, &detached)
	return detached, e
}
