package platformconfig

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
)

// The external form pins the selected physical declaration separately from
// any policy-withheld readiness targets. Withdrawal does not lift a hold.
const ExternalPathProposalSchema = HostedProposalSchema

type ExternalPathAnchor struct {
	Target         ExternalPathTarget  `json:"target"`
	Shape          string              `json:"shape_digest"`
	Held           []HostedAddressHold `json:"held,omitempty"`
	EffectiveShape string              `json:"effective_shape_digest,omitempty"`
}

// The authenticated parent and the proposal both bind the exact wire RR in
// ComposeExternalPathRequirements. Independently pin quorum and targeting:
// concurrent path decisions may change probe IDs but not this topology.
func externalPathShape(target ExternalPathTarget, base DNSArtifactPayload) (string, []HostedAddressHold, error) {
	bad := errors.New("external path target does not have one selected readiness topology")
	aliases, name, app, tenant, memberOK := HostedExternalDNSMembers(target.Hostname, base.Records, base.ReadinessPlan)
	if base.ReadinessPlan == nil || base.Policy.DNSReadiness == nil || !memberOK || name != target.Target || app != target.AppID || tenant != target.TenantID || !slices.Equal(aliases, target.Members) {
		return "", nil, bad
	}
	var shape DNSReadinessRecord
	var members []HostedAddressHold
	seen := map[HostedAddressHold]bool{}
	for _, r := range base.ReadinessPlan.Records {
		if r.Hostname != target.Target {
			continue
		}
		if shape.Hostname != "" || len(r.Targets) == 0 {
			return "", nil, bad
		}
		shape = r
		shape.Targets = slices.Clone(r.Targets)
		for i, t := range shape.Targets {
			n := HostedAddressHold{EdgeID: t.EdgeID, EdgeGroupID: t.EdgeGroupID, Address: t.Address}
			if t.EdgeID == "" || t.EdgeGroupID == "" || t.Address == "" || seen[n] {
				return "", nil, bad
			}
			seen[n] = true
			members = append(members, n)
			shape.Targets[i].ProbeIDs = nil
		}
	}
	if len(members) == 0 {
		return "", nil, bad
	}
	d, e := Digest([]any{ExternalPathProposalSchema, target, base.Policy.DNSReadiness, shape})
	return d, members, e
}

func replayExternalDNSPhase(base DNSArtifactPayload, p HostedScopeProposal, phase string) (HostedDNSContent, error) {
	if p.ExternalPath != nil && len(p.ExternalPath.Held) > 0 {
		return replayExternalHeldPhase(base, p, phase)
	}
	if p.ExternalPath != nil && p.ExternalPath.EffectiveShape != "" {
		return HostedDNSContent{}, errors.New("unbound external effective shape")
	}
	var empty HostedDNSContent
	bad := errors.New("external path phase does not match its selected predecessor")
	a, owner := p.ExternalPath, p.Before.Owner
	if p.Schema != ExternalPathProposalSchema || a == nil || p.Base.Validate() != nil ||
		base.Policy.Scope != GlobalScopeKey || base.Policy.DNSPlacementMode != DNSPlacementConsumerReadiness ||
		p.Host == "" || p.Host != a.Target.Hostname || p.Host != owner.Hostname || owner.Status != "verified" ||
		owner.TenantID != a.Target.TenantID || owner.AppID != a.Target.AppID || owner != p.After.Owner ||
		!reflect.DeepEqual(p.Before, p.Prepared) || !reflect.DeepEqual(p.BaseProbes, p.HoldProbes) ||
		len(p.Hold) != 0 || len(p.Fallbacks) != 0 || len(p.Changes) == 0 || !slices.Equal(p.Physical, p.Eligible) {
		return empty, bad
	}
	shape, members, e := externalPathShape(a.Target, base)
	if e != nil || a.Shape != shape || len(p.Physical) != len(members) {
		return empty, errors.Join(bad, e)
	}
	roster := map[HostedAddressHold]bool{}
	for _, m := range members {
		roster[m] = true
	}
	for _, m := range p.Physical {
		if !roster[m] {
			return empty, bad
		}
		delete(roster, m)
	}
	old, next, from, to := p.BaseProbes, p.PathProbes, p.Before, p.After
	edits := slices.Clone(p.Changes)
	switch phase {
	case "prepare", "forward":
	case "withdraw":
		old, next, from, to = next, old, to, from
		for i, edit := range edits {
			edits[i].Before, edits[i].After = edit.After, edit.Before
		}
	default:
		return empty, bad
	}
	probesMatch := func(plan *DNSReadinessPlan, rows []DNSReadinessProbe) bool {
		expected := map[string]DNSReadinessProbe{}
		for _, pr := range rows {
			if _, dup := expected[pr.ID]; dup || pr.ID == "" || pr.Hostname != p.Host {
				return false
			}
			expected[pr.ID] = pr
		}
		if len(expected) == 0 {
			return false
		}
		for _, pr := range plan.Probes {
			if pr.Hostname == p.Host {
				v, ok := expected[pr.ID]
				if !ok || !reflect.DeepEqual(v, pr) {
					return false
				}
				delete(expected, pr.ID)
			}
		}
		return len(expected) == 0
	}
	if !probesMatch(base.ReadinessPlan, old) {
		return empty, bad
	}
	view, _, e := applyHostedReadinessDelta(p.Host, base.ReadinessPlan, next, base.Policy.DNSReadiness, HostedReadinessDelta{Host: p.Host, Edits: edits})
	if e != nil || !probesMatch(view, next) {
		return empty, errors.Join(bad, e)
	}
	paths := func(rows HostedDispatchRows) []string {
		out := make([]string, 0, len(rows.Routes))
		for _, r := range rows.Routes {
			if r.Hostname != p.Host || r.TenantID != owner.TenantID || r.AppID == "" {
				return nil
			}
			out = append(out, r.PathPrefix)
		}
		return out
	}
	_, checked, e := ComposeExternalPathRequirements(a.Target, base.ReadinessPlan, view, base.Policy.DNSReadiness, paths(from), paths(to), base.Records, p.Records)
	if e != nil || !reflect.DeepEqual(checked.Edits, edits) {
		return empty, errors.Join(bad, e)
	}
	if phase == "prepare" {
		view = base.ReadinessPlan
	}
	// The wire view is always the selected one, including each client's
	// address order, TTL and zone. Detach it before returning to the caller.
	raw, e := json.Marshal(HostedDNSContent{Plan: *view, Views: base.QueryViews, Rules: base.Policy.DNSAnswerRules})
	if e != nil {
		return empty, e
	}
	var out HostedDNSContent
	e = json.Unmarshal(raw, &out)
	return out, e
}
