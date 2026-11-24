package platformconfig

import (
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"slices"
)

// Discover only existing hosted owners with a root-to-managed path edit and
// a previously selected target under the exact physical hold. Other tenants'
// ordinary address changes are not converted to reservations.
func NewHostedPathReservationHosts(prior DNSArtifactPayload, desired PlatformIntent, holds []HostedAddressHold) map[string]bool {
	out := map[string]bool{}
	if prior.ReadinessPlan == nil {
		return out
	}
	suspended := map[HostedAddressHold]bool{}
	for _, h := range holds {
		suspended[h] = true
	}
	affected := map[string]bool{}
	for _, r := range prior.ReadinessPlan.Records {
		for _, t := range r.Targets {
			if suspended[HostedAddressHold{t.EdgeID, t.EdgeGroupID, t.Address}] {
				affected[r.Hostname] = true
			}
		}
	}
	paths := map[string]map[string]bool{}
	for _, p := range prior.ReadinessPlan.Probes {
		if affected[p.Hostname] {
			if paths[p.Hostname] == nil {
				paths[p.Hostname] = map[string]bool{}
			}
			paths[p.Hostname][p.Path] = true
		}
	}
	type owner struct{ host, tenant, app string }
	managed := map[owner]bool{}
	for _, d := range desired.DNS {
		if d.RecordKind == "hosted" && d.Type == "FUGUE_ROUTE" && d.Route != nil && d.Route.ManagedHost && len(d.Route.Bindings) > 1 &&
			len(paths[d.Hostname]) == 1 && paths[d.Hostname]["/"] {
			managed[owner{d.Hostname, d.TenantID, d.AppID}] = true
		}
	}
	for _, r := range prior.Records {
		if r.RecordKind == "hosted" && (r.Type == "A" || r.Type == "AAAA") && managed[owner{r.Hostname, r.TenantID, r.AppID}] {
			out[r.Hostname] = true
		}
	}
	return out
}

// CompileHostedPathTransition separates two changes: (1) publish the
// already-declared hold, retaining the old route shape and address identities;
// (2) on a later verified full with the same eligible roster, publish paths.
// There is no direct mixed-topology promotion. If the intermediate full did
// not verify, recapturing the desired state yields the first step again.
func CompileHostedPathTransition(req CompileRequest, source HostedAddressSource) (CompileResult, bool, error) {
	return compileHostedPathTransition(req, source, nil)
}

// A proposal is extracted from the same original global compile that the
// ordinary producer already performs. It is not obtained by borrowing a
// routing Cell's authority for machines outside that Cell's enrollment.
type HostedTransitionCapture struct {
	Hosts         []string
	StableHosts   []string // independent path programs; not global hold successors
	ExternalHosts []string
	Desired       CompileResult
	baseline      *CompileRequest
}

func CompileHostedPathWithCapture(req CompileRequest, source HostedAddressSource) (CompileResult, *HostedTransitionCapture, error) {
	var capture HostedTransitionCapture
	baseline, staged, err := compileHostedPathTransition(req, source, &capture)
	if err != nil {
		return baseline, nil, err
	}
	capture.ExternalHosts = ExternalChangedHosts(source.PriorIntent, req.Intent)
	held := NewHostedPathReservationHosts(source.Prior, req.Intent, source.EffectiveHolds)
	for _, v := range source.Inherited {
		if v.Record.RecordKind == "hosted" {
			held[v.Record.Hostname] = true
		}
	}
	for _, h := range ManagedChangedHosts(source.PriorIntent, req.Intent) {
		if !held[h] {
			capture.StableHosts = append(capture.StableHosts, h)
		}
	}
	if !staged {
		if len(capture.ExternalHosts)+len(capture.StableHosts) == 0 {
			return baseline, nil, nil
		}
		capture.Desired = baseline
	}
	return baseline, &capture, nil
}

func compileHostedPathTransition(req CompileRequest, source HostedAddressSource, capture *HostedTransitionCapture) (CompileResult, bool, error) {
	fail := errors.New("hosted path transition cannot prove intermediate topology")
	source.managedHosts = NewHostedPathReservationHosts(source.Prior, req.Intent, source.EffectiveHolds)
	for _, retained := range source.Inherited {
		if retained.Record.RecordKind == "hosted" {
			source.managedHosts[retained.Record.Hostname] = true
		}
	}
	source.stableAliases = stableHostedAliasDeclarations(source.PriorIntent, req.Intent)
	with, err := CompileWithHostedAddressSource(req, source)
	if err != nil {
		return CompileResult{}, false, err
	}
	if len(with.InputSnapshot.HostedAddressReservations) == 0 {
		return with, false, nil
	}
	// Read the compiler's own immutable typed result; no diagnostic CLI data.
	readPlan := func(c CompileResult) (*DNSReadinessPlan, bool) {
		var p *DNSReadinessPlan
		raw, e := json.Marshal(c.DNSArtifact.Content["readiness_plan"])
		if e != nil || json.Unmarshal(raw, &p) != nil || p == nil {
			return nil, false
		}
		return p, true
	}
	plan, ok := readPlan(with)
	if !ok || plan == nil || source.Prior.ReadinessPlan == nil {
		return CompileResult{}, false, fail
	}
	type state struct {
		paths  map[string]bool
		record DNSReadinessRecord
	}
	view := func(p *DNSReadinessPlan) map[string]state {
		m := map[string]state{}
		for _, r := range p.Records {
			m[r.Hostname] = state{map[string]bool{}, r}
		}
		for _, probe := range p.Probes {
			if r, ok := m[probe.Hostname]; ok {
				r.paths[probe.Path] = true
			}
		}
		return m
	}
	old, next := view(source.Prior.ReadinessPlan), view(plan)
	blocked := map[string]bool{}
	for _, v := range with.InputSnapshot.HostedAddressReservations {
		h := v.Record.Hostname
		a, b := old[h], next[h]
		if !maps.Equal(a.paths, b.paths) && !reflect.DeepEqual(hostedPhysicalRoster(a.record, v.Record.Type), hostedPhysicalRoster(b.record, v.Record.Type)) {
			if !a.paths["/"] || !b.paths["/"] {
				return CompileResult{}, false, fail
			}
			blocked[h] = true
		}
	}
	if len(blocked) == 0 {
		return with, false, nil
	}
	hosts := slices.Sorted(maps.Keys(blocked))
	stage, err := StageHostedPathBaseline(source.PriorIntent, req.Intent, hosts)
	if err != nil {
		return CompileResult{}, false, err
	}
	facts, err := StageHostedOriginFacts(req.Intent, stage, req.RuntimeSnapshot, hosts)
	if err != nil {
		return CompileResult{}, false, err
	}
	req.Intent, req.RuntimeSnapshot = stage, facts
	staged, err := CompileWithHostedAddressSource(req, source)
	if err != nil {
		return CompileResult{}, false, err
	}
	p, ok := readPlan(staged)
	if !ok || p == nil {
		return CompileResult{}, false, fail
	}
	actual := view(p)
	for _, h := range hosts {
		if !maps.Equal(actual[h].paths, old[h].paths) {
			return CompileResult{}, false, fail
		}
		for _, family := range []string{"A", "AAAA"} {
			if !reflect.DeepEqual(hostedPhysicalRoster(actual[h].record, family), hostedPhysicalRoster(next[h].record, family)) {
				return CompileResult{}, false, fail
			}
		}
	}
	if capture != nil {
		capture.Hosts = slices.Clone(hosts)
		capture.Desired = with
		capture.baseline = &req
	}
	return staged, true, nil
}
