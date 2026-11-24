package platformconfig

import (
	"errors"
	"maps"
	"reflect"
	"slices"
)

// A physical RR is a declaration. A hold removes a target from readiness,
// not an address from that declaration. Only the selected shared target is
// projected here; do not remove a peer's probe if another record uses it.
// This function returns requirements, never observations or eligibility grants.
func externalHeldView(base DNSArtifactPayload, target ExternalPathTarget, held []HostedAddressHold, already bool) (DNSArtifactPayload, error) {
	bad := errors.New("external DNS hold does not describe the selected physical target")
	var empty DNSArtifactPayload
	if base.ReadinessPlan == nil || len(held) == 0 || len(held) > 4096 || ValidateDNSReadinessPlan(base.ReadinessPlan, base.Policy.DNSReadiness) != nil {
		return empty, bad
	}
	p := *base.ReadinessPlan
	p.Records = slices.Clone(p.Records)
	pending := map[HostedAddressHold]bool{}
	byAddress := map[string]HostedAddressHold{}
	for _, h := range held {
		if _, dup := byAddress[h.Address]; dup || h.EdgeID == "" || h.EdgeGroupID == "" || h.Address == "" {
			return empty, bad
		}
		pending[h] = true
		byAddress[h.Address] = h
	}
	found := false
	dropped := map[string]bool{}
	for i, r := range p.Records {
		if r.Hostname != target.Target {
			continue
		}
		if found {
			return empty, bad
		}
		found = true
		next := r
		next.Targets = nil
		for _, n := range r.Targets {
			h := HostedAddressHold{n.EdgeID, n.EdgeGroupID, n.Address}
			if _, claimed := byAddress[n.Address]; claimed {
				if already || !pending[h] {
					return empty, bad
				}
				delete(pending, h)
				for _, id := range n.ProbeIDs {
					dropped[id] = true
				}
				continue
			}
			next.Targets = append(next.Targets, n)
		}
		if len(next.Targets) == 0 || !DNSReadinessQuorum(next, func(DNSReadinessTarget) bool { return true }) {
			return empty, bad
		}
		p.Records[i] = next
	}
	if !found || !already && len(pending) != 0 {
		return empty, bad
	}
	for _, r := range p.Records {
		for _, t := range r.Targets {
			for _, id := range t.ProbeIDs {
				delete(dropped, id)
			}
		}
	}
	p.Probes = slices.DeleteFunc(slices.Clone(p.Probes), func(pr DNSReadinessProbe) bool { return dropped[pr.ID] })
	physical := map[string]bool{}
	for _, r := range base.Records {
		if r.Hostname == target.Target && (r.Type == "A" || r.Type == "AAAA") {
			if r.AppID != target.AppID || r.TenantID != target.TenantID {
				return empty, bad
			}
			for _, ip := range r.Values {
				if physical[ip] {
					return empty, bad
				}
				physical[ip] = true
			}
		}
	}
	accounted := map[string]bool{}
	for _, r := range p.Records {
		if r.Hostname == target.Target {
			for _, n := range r.Targets {
				accounted[n.Address] = true
			}
		}
	}
	for ip := range byAddress {
		accounted[ip] = true
	}
	if !maps.Equal(physical, accounted) || ValidateDNSReadinessPlan(&p, base.Policy.DNSReadiness) != nil {
		return empty, bad
	}
	base.ReadinessPlan = &p
	members, name, app, tenant, ok := HostedExternalDNSMembers(target.Hostname, base.Records, &p)
	if !ok || name != target.Target || app != target.AppID || tenant != target.TenantID || !slices.Equal(members, target.Members) {
		return empty, bad
	}
	return base, nil
}

// A disposable comparison view of the *effective* records. Never use these
// rows for a published wire declaration. Refuse loss of a whole family.
func externalEffectiveRows(target string, full []DNSIntent, held []HostedAddressHold) ([]DNSIntent, error) {
	deny := map[string]bool{}
	for _, h := range held {
		deny[h.Address] = true
	}
	out := slices.Clone(full)
	for i, r := range out {
		if r.Hostname == target && (r.Type == "A" || r.Type == "AAAA") {
			out[i].Values = slices.DeleteFunc(slices.Clone(r.Values), func(ip string) bool { return deny[ip] })
			if len(out[i].Values) == 0 {
				return nil, errors.New("external hold would remove a complete address family")
			}
		}
	}
	return out, nil
}

func externalAddressKeys(target string, rows []DNSIntent) map[hostedRecordKey]bool {
	keys := make(map[hostedRecordKey]bool, 2)
	for _, r := range viewScopedAddressRecords(rows, target) {
		keys[hostedRecordKey{r.Hostname, r.Type}] = true
	}
	return keys
}

func externalProposalHolds(members, effective []HostedAddressHold) []HostedAddressHold {
	veto := map[HostedAddressHold]bool{}
	for _, h := range effective {
		veto[h] = true
	}
	var held []HostedAddressHold
	for _, m := range members {
		if veto[m] {
			held = append(held, m)
		}
	}
	return held
}

// A missing effective member is physical only when its IP is already in the
// signed, same-owner RR and its complete identity is bound to both policies.
// Never restore a declaration here or derive an identity from an IP alone.
func externalPredecessorHolds(target ExternalPathTarget, base DNSArtifactPayload, members, prior, effective []HostedAddressHold) ([]HostedAddressHold, error) {
	bad := errors.New("external path retained address lacks an unchanged physical hold")
	seen := map[string]bool{}
	for _, n := range members {
		seen[n.Address] = true
	}
	byAddress := map[string]HostedAddressHold{}
	for _, h := range externalProposalHolds(prior, effective) {
		if _, duplicate := byAddress[h.Address]; duplicate {
			return nil, bad
		}
		byAddress[h.Address] = h
	}
	var held []HostedAddressHold
	for _, r := range base.Records {
		if r.Hostname != target.Target || r.Type != "A" && r.Type != "AAAA" {
			continue
		}
		for _, ip := range r.Values {
			if seen[ip] {
				continue
			}
			h, ok := byAddress[ip]
			if !ok {
				return nil, bad
			}
			held = append(held, h)
			seen[ip] = true
		}
	}
	return held, nil
}

func replayExternalHeldPhase(base DNSArtifactPayload, p HostedScopeProposal, phase string) (HostedDNSContent, error) {
	empty := HostedDNSContent{}
	a := p.ExternalPath
	bad := errors.New("external path hold differs from selected declaration or eligible roster")
	if !reflect.DeepEqual(viewScopedAddressRecords(base.Records, a.Target.Target), p.Records) || len(p.Physical) != len(p.Eligible)+len(a.Held) || a.Shape == "" || a.EffectiveShape == "" {
		return empty, bad
	}
	before, members, e := externalPathShape(a.Target, base)
	if e != nil {
		return empty, e
	}
	deny := map[HostedAddressHold]bool{}
	for _, h := range a.Held {
		deny[h] = true
	}
	live := slices.DeleteFunc(slices.Clone(p.Physical), func(h HostedAddressHold) bool { return deny[h] })
	block := slices.DeleteFunc(slices.Clone(p.Physical), func(h HostedAddressHold) bool { return !deny[h] })
	if !slices.Equal(live, p.Eligible) || !slices.Equal(block, a.Held) {
		return empty, bad
	}
	already := a.PredecessorWithheld()
	if already {
		// A pre-existing veto must also hold in the independently loaded
		// dynamic selectors, even before its scoped packet reaches DNS.
		if err := checkHostedDynamicSelectors(externalAddressKeys(a.Target.Target, p.Records), base.QueryViews, base.ReadinessPlan, base.Policy); err != nil {
			return empty, errors.Join(bad, err)
		}
	}
	if phase == "prepare" {
		expected := p.Physical
		if already {
			expected = p.Eligible
		}
		if before != a.Shape || !slices.Equal(members, expected) {
			return empty, bad
		}
		var own []DNSReadinessProbe
		for _, pr := range base.ReadinessPlan.Probes {
			if pr.Hostname == p.Host {
				own = append(own, pr)
			}
		}
		if !reflect.DeepEqual(own, p.BaseProbes) {
			return empty, bad
		}
	} else if before != a.EffectiveShape || !slices.Equal(members, p.Eligible) {
		return empty, bad
	}
	projected, e := externalHeldView(base, a.Target, a.Held, already || phase != "prepare")
	if e != nil {
		return empty, e
	}
	shape, members, e := externalPathShape(a.Target, projected)
	if e != nil || shape != a.EffectiveShape || !slices.Equal(members, p.Eligible) {
		return empty, bad
	}
	projected.Records, e = externalEffectiveRows(a.Target.Target, base.Records, a.Held)
	if e != nil {
		return empty, e
	}
	p.ExternalPath = &ExternalPathAnchor{Target: a.Target, Shape: shape}
	p.Records = viewScopedAddressRecords(projected.Records, a.Target.Target)
	p.BaseProbes = p.HoldProbes
	p.Physical = p.Eligible
	out, e := replayExternalDNSPhase(projected, p, phase)
	if e != nil {
		return empty, e
	}
	out.Withheld = slices.Clone(a.Held)
	return out, nil
}
