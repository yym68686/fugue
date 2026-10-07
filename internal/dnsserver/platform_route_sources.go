package dnsserver

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/routeprobe"
)

func (s *Service) replayDNSRouteContext(p dnsServingPayload, c dnsPlatformCandidate, source *dnsroutesource.Context, now time.Time) (dnsServingPayload, error) {
	if source == nil {
		return p, nil
	}
	// The checkpoint authenticates the original observation. Retained-only LKG
	// proof deadlines are enforced independently; an unused expired LKG entry
	// cannot invalidate an active source during bounded offline recovery.
	plans, err := dnsroutesource.Build(c.Artifact, source.Snapshot, s.platformDNSKeys(), source.Snapshot.ObservedAt)
	if err != nil {
		return p, err
	}
	p.Plan, err = plans.Replay(source)
	if err != nil {
		return p, err
	}
	if err = platformconfig.ValidateDNSReadinessPlan(p.Plan, p.Policy.DNSReadiness); err != nil {
		return p, err
	}
	p.routeSources, p.sourceBindings = source, plans.Bindings
	p.indexSourceRequirements()
	return p, nil
}

func (s *Service) observeDNSRouteSources(ctx context.Context, client platformconsumer.Client, id platformconsumer.Identity, c dnsPlatformCandidate, p dnsServingPayload, previous *dnsroutesource.Context, probe dnsReadinessProbeFunc) (dnsServingPayload, []dnsReadinessFact, error) {
	approvals, err := platformconfig.DNSRouteSourceAuthorizations(c.Artifact)
	if err != nil || len(approvals) == 0 {
		return p, nil, err
	}
	snapshot, err := client.DNSRouteSources(ctx, id, c.Assignment, c.Release)
	if err != nil {
		return p, nil, err
	}
	now := time.Now().UTC()
	if snapshot.ObservedAt.After(now) || now.Sub(snapshot.ObservedAt) > time.Minute {
		return p, nil, errors.New("DNS route source observation is not fresh")
	}
	if previous != nil && !dnsroutesource.Monotonic(previous.Snapshot, snapshot) {
		return p, nil, errors.New("DNS routing ledger replay rejected")
	}
	plans, err := dnsroutesource.Build(c.Artifact, snapshot, s.platformDNSKeys(), now)
	if err != nil {
		return p, nil, err
	}
	union := &platformconfig.DNSReadinessPlan{}
	for _, requirement := range plans.Probes {
		union.Probes = append(union.Probes, requirement)
	}
	sort.Slice(union.Probes, func(i, j int) bool { return union.Probes[i].ID < union.Probes[j].ID })
	facts := collectDNSRouteSourceFacts(ctx, union, p.Policy.DNSReadiness, probe)
	facts = retainValidDNSReadinessFacts(union, p.Policy.DNSReadiness, s.platformDNSRouteFacts, facts, time.Now().UTC())
	boundDNSRouteSourceFacts(dnsServingPayload{routeSources: &dnsroutesource.Context{Snapshot: snapshot}}, facts, time.Now().UTC())
	valid := validDNSReadinessFacts(union, p.Policy.DNSReadiness, facts, time.Now().UTC())
	byID := map[string]dnsReadinessFact{}
	for _, f := range facts {
		if f.Ready && !plans.Matches(plans.Probes[f.ProbeID], f.Proof) {
			f.Ready = false
			f.Reason = "traffic_release_mismatch"
			delete(valid, f.ProbeID)
		}
		byID[f.ProbeID] = f
	}
	// Cache only current, authenticated source matches. A failed or revoked
	// physical proof cannot be revived on the next refresh.
	s.platformDNSRouteFacts = s.platformDNSRouteFacts[:0]
	for _, f := range byID {
		if _, ok := valid[f.ProbeID]; ok {
			s.platformDNSRouteFacts = append(s.platformDNSRouteFacts, f)
		}
	}
	selection := plans.Choose(func(id string) bool { _, ok := valid[id]; return ok })
	p.Plan, err = plans.Replay(selection)
	if err != nil {
		return p, nil, err
	}
	if err = platformconfig.ValidateDNSReadinessPlan(p.Plan, p.Policy.DNSReadiness); err != nil {
		return p, nil, err
	}
	p.routeSources, p.sourceBindings = selection, plans.Bindings
	p.indexSourceRequirements()
	selected := make([]dnsReadinessFact, 0, len(p.Plan.Probes))
	for _, requirement := range p.Plan.Probes {
		selected = append(selected, byID[requirement.ID])
	}
	if err = s.checkDNSRouteSources(ctx, client, id, c, selection); err != nil {
		return p, nil, err
	}
	if err = s.advanceDNSRouteCursor(snapshot); err != nil {
		return p, nil, err
	}
	return p, selected, nil
}
func (s *Service) checkDNSRouteSources(ctx context.Context, client platformconsumer.Client, id platformconsumer.Identity, c dnsPlatformCandidate, source *dnsroutesource.Context) error {
	if source == nil {
		return nil
	}
	next, err := client.DNSRouteSources(ctx, id, c.Assignment, c.Release)
	if err != nil {
		return err
	}
	if next.SelectionDigest != source.Snapshot.SelectionDigest {
		return platformconsumer.ErrAssignmentChanged
	}
	return nil
}

// Schedule each physical target once, then evaluate its unchanged observation
// against every authorized publication. Waiting duplicates must not occupy the
// bounded network worker pool or starve other targets during the probe window.
func collectDNSRouteSourceFacts(ctx context.Context, plan *platformconfig.DNSReadinessPlan, policy *platformconfig.DNSReadinessPolicy, probe dnsReadinessProbeFunc) []dnsReadinessFact {
	physical := &platformconfig.DNSReadinessPlan{}
	representatives := map[string]string{}
	for _, requirement := range plan.Probes {
		key := dnsroutesource.ProbeKey(requirement)
		if _, exists := representatives[key]; !exists {
			representatives[key] = requirement.ID
			physical.Probes = append(physical.Probes, requirement)
		}
	}
	// Transport results must be retained separately: a proof may mismatch the
	// representative's digest while satisfying another authorized publication.
	type observation struct {
		proof routeprobe.Proof
		err   error
	}
	results := make([]observation, len(physical.Probes))
	indices := map[string]int{}
	for i, p := range physical.Probes {
		indices[dnsroutesource.ProbeKey(p)] = i
	}
	wrapped := func(ctx context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
		proof, err := probe(ctx, host, path, address, state, timeout)
		key := address + "\x00" + host + "\x00" + path + "\x00" + state
		results[indices[key]] = observation{proof, err}
		return proof, err
	}
	physicalFacts := collectDNSReadinessFacts(ctx, physical, policy, wrapped)
	facts := make([]dnsReadinessFact, 0, len(plan.Probes))
	for _, requirement := range plan.Probes {
		i := indices[dnsroutesource.ProbeKey(requirement)]
		if physicalFacts[i].Reason == "not_observed" {
			facts = append(facts, dnsReadinessFact{ProbeID: requirement.ID, Reason: "not_observed"})
			continue
		}
		observed := results[i]
		facts = append(facts, evaluateDNSReadinessFact(requirement, policy, observed.proof, observed.err, time.Now().UTC()))
	}
	return facts
}

func dnsSameSourceContext(a, b *dnsroutesource.Context) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Snapshot.SelectionDigest == b.Snapshot.SelectionDigest && reflect.DeepEqual(a.Selections, b.Selections)
}

// The API, checkpoint and live state all replay the same signed requirements.
func (s *Service) verifiedDNSCheckpointPayload(c dnsServingCheckpoint) (dnsServingPayload, string, error) {
	p, routeID, err := s.verifyDNSServingRelease(c.Parent, c.Candidate)
	if err == nil {
		p, err = s.replayDNSRouteContext(p, c.Candidate, c.RouteSources, time.Now().UTC())
	}
	return p, routeID, err
}

func boundDNSRouteSourceFacts(p dnsServingPayload, facts []dnsReadinessFact, now time.Time) {
	if p.routeSources == nil {
		return
	}
	for i := range facts {
		f := &facts[i]
		until := p.routeSources.ProofDeadline(f.Proof.TrafficRelease)
		if !until.IsZero() {
			if f.Proof.ValidUntil.After(until) {
				f.Proof.ValidUntil = until
			}
			if !until.After(now) {
				f.Ready = false
				f.Reason = "proof_not_fresh"
			}
		}
	}
}
