package platformconfig

import (
	"errors"
	"reflect"
	"slices"
)

// HostedDNSProgramSource narrows an already authenticated selected DNS child
// to the inputs needed to execute a single host's proposal. It never selects
// a release or authorizes serving. Check reverse dependencies on the complete
// graph BEFORE filtering: otherwise an unowned record using A's probe could
// silently lose its prerequisite. Listener probes remain inputs to the normal
// strict view validator, not records owned by this host.
func HostedDNSProgramSource(base DNSArtifactPayload, p HostedScopeProposal) (DNSArtifactPayload, error) {
	bad := errors.New("host DNS program source is not a closed dependency set")
	if !p.SupportedSchema() || p.Host == "" || p.Before.Owner.Hostname != p.Host || p.Base.Validate() != nil ||
		base.Policy.Scope != GlobalScopeKey || base.Policy.PublicationRole != "" || base.Policy.DNSPlacementMode != DNSPlacementConsumerReadiness ||
		base.ReadinessPlan == nil || len(p.Records) == 0 || len(p.Records) > 4096 {
		return DNSArtifactPayload{}, bad
	}
	want := map[hostedRecordKey]DNSIntent{}
	for _, r := range p.Records {
		k := hostedRecordKey{r.Hostname, r.Type}
		if _, dup := want[k]; dup || r.Type != "A" && r.Type != "AAAA" || r.AppID != p.Before.Owner.AppID || r.TenantID != p.Before.Owner.TenantID {
			return DNSArtifactPayload{}, bad
		}
		want[k] = r
	}
	out := DNSArtifactPayload{Policy: base.Policy, ConsumerViews: base.ConsumerViews, ReadinessPlan: &DNSReadinessPlan{}}
	out.Policy.DNSAnswerRules = nil
	// The authorization comes from the selected full, never from p.Records.
	// An external path proposal may read peer prerequisites on this target;
	// only the probes of p.Host may be replaced.
	peerIDs := map[string]bool{}
	if a := p.ExternalPath; a != nil {
		aliases, name, app, tenant, ok := HostedExternalDNSMembers(p.Host, base.Records, base.ReadinessPlan)
		if !ok || a.Target.Hostname != p.Host || name != a.Target.Target || app != a.Target.AppID || tenant != a.Target.TenantID || !slices.Equal(aliases, a.Target.Members) {
			return DNSArtifactPayload{}, bad
		}
		for _, r := range base.ReadinessPlan.Records {
			if r.Hostname == name {
				for _, t := range r.Targets {
					for _, id := range t.ProbeIDs {
						peerIDs[id] = true
					}
				}
			}
		}
	}
	owned := map[hostedRecordKey]bool{}
	for _, r := range HostedDNSOwnedRecords(p.Host, base.Records, base.ReadinessPlan) {
		k := hostedRecordKey{r.Hostname, r.Type}
		declared, ok := want[k]
		if !ok || owned[k] || !reflect.DeepEqual(declared, r) {
			return DNSArtifactPayload{}, bad
		}
		owned[k] = true
		out.Records = append(out.Records, r)
	}
	if len(owned) != len(want) {
		return DNSArtifactPayload{}, bad
	}
	probes := map[string]bool{}
	reserved := map[string]bool{}
	for _, phase := range [][]DNSReadinessProbe{p.BaseProbes, p.HoldProbes, p.PathProbes} {
		for _, probe := range phase {
			if probe.Hostname != p.Host || probe.ID == "" {
				return DNSArtifactPayload{}, bad
			}
			reserved[probe.ID] = true
		}
	}
	for _, probe := range base.ReadinessPlan.Probes {
		if probe.Hostname != p.Host && reserved[probe.ID] {
			return DNSArtifactPayload{}, bad
		}
		if probe.Hostname == p.Host || peerIDs[probe.ID] {
			if probe.ID == "" || probes[probe.ID] {
				return DNSArtifactPayload{}, bad
			}
			probes[probe.ID] = true
			out.ReadinessPlan.Probes = append(out.ReadinessPlan.Probes, probe)
		}
	}
	ownIDs := map[string]bool{}
	for _, probe := range out.ReadinessPlan.Probes {
		ownIDs[probe.ID] = probe.Hostname == p.Host
	}
	for _, record := range base.ReadinessPlan.Records {
		r := record
		r.Targets = nil
		for _, target := range record.Targets {
			keep := owned[hostedRecordKey{record.Hostname, target.Family}]
			for _, id := range target.ProbeIDs {
				if keep && !probes[id] || !keep && ownIDs[id] {
					return DNSArtifactPayload{}, bad
				}
			}
			if keep {
				r.Targets = append(r.Targets, target)
			}
		}
		if len(r.Targets) != 0 {
			out.ReadinessPlan.Records = append(out.ReadinessPlan.Records, r)
		}
	}
	type listenerKey struct{ node, group, zone string }
	listeners := map[listenerKey]map[hostedRecordKey]bool{}
	for _, view := range base.ConsumerViews {
		k := listenerKey{view.NodeID, view.EdgeGroupID, view.Zone}
		if listeners[k] != nil {
			return DNSArtifactPayload{}, bad
		}
		listeners[k] = map[hostedRecordKey]bool{}
		for _, r := range view.Records {
			listeners[k][hostedRecordKey{r.Hostname, r.Type}] = true
		}
	}
	for _, view := range base.QueryViews {
		local, ok := listeners[listenerKey{view.NodeID, view.EdgeGroupID, view.Zone}]
		if !ok {
			return DNSArtifactPayload{}, bad
		}
		v := view
		v.Records = nil
		for _, r := range view.Records {
			k := hostedRecordKey{r.Name, r.Type}
			if owned[k] || local[k] {
				v.Records = append(v.Records, r)
			}
		}
		out.QueryViews = append(out.QueryViews, v)
	}
	for _, r := range base.Policy.DNSAnswerRules {
		if owned[hostedRecordKey{r.Hostname, r.Type}] {
			out.Policy.DNSAnswerRules = append(out.Policy.DNSAnswerRules, r)
		}
	}
	if err := ValidateDNSReadinessPlan(out.ReadinessPlan, out.Policy.DNSReadiness); err != nil {
		return DNSArtifactPayload{}, errors.Join(bad, err)
	}
	if err := ValidateDNSQueryViews(out.QueryViews, out.Records, out.ConsumerViews, out.ReadinessPlan, out.Policy); err != nil {
		return DNSArtifactPayload{}, errors.Join(bad, err)
	}
	return out, nil
}
