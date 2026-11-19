package dnsserver

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformsafety"
	"fugue/internal/routeprobe"
)

func (s *Service) replayDNSRouteContext(p dnsServingPayload, c dnsPlatformCandidate, source *dnsroutesource.Context, now time.Time) (dnsServingPayload, error) {
	if source == nil {
		return p, nil
	}
	// The checkpoint authenticates the original observation. Retained-only LKG
	// proof deadlines are enforced independently; an unused expired LKG entry
	// cannot invalidate an active source during bounded offline recovery.
	plans, err := s.buildDNSRouteSources(context.Background(), c.Artifact, source.Snapshot, source.Snapshot.ObservedAt)
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
	p.sourceRecoveryKeys, err = dnsRouteRecoveryKeys(c.Artifact, plans)
	if err != nil {
		return p, err
	}
	p.indexSourceRequirements()
	return p, nil
}

func (s *Service) observeDNSRouteSources(ctx context.Context, client platformconsumer.Client, id platformconsumer.Identity, c dnsPlatformCandidate, p dnsServingPayload, previous *dnsroutesource.Context, probe dnsReadinessProbeFunc) (dnsServingPayload, []dnsReadinessFact, error) {
	var prior []model.PlatformDNSRouteSourceSnapshot
	if previous != nil {
		prior = []model.PlatformDNSRouteSourceSnapshot{previous.Snapshot}
	}
	return s.observeDNSRouteSourcesWithReader(ctx, c, p, previous, probe, func(ctx context.Context) (model.PlatformDNSRouteSourceSnapshot, error) {
		snapshot, err := client.DNSRouteSources(ctx, id, c.Assignment, c.Release, prior...)
		if err == nil {
			prior = []model.PlatformDNSRouteSourceSnapshot{snapshot}
		}
		return snapshot, err
	})
}

// Both selected and retained DNS use the same authenticated source compilation,
// recovery history and physical proof checks, but separate read authority.
func (s *Service) observeDNSRouteSourcesWithReader(ctx context.Context, c dnsPlatformCandidate, p dnsServingPayload, previous *dnsroutesource.Context, probe dnsReadinessProbeFunc, read func(context.Context) (model.PlatformDNSRouteSourceSnapshot, error)) (resultPayload dnsServingPayload, resultFacts []dnsReadinessFact, resultErr error) {
	ctx, finish := observeDNSSyncPhase(ctx, "route_sources")
	defer func() { finish(resultErr) }()
	originalRead := read
	read = func(ctx context.Context) (snapshot model.PlatformDNSRouteSourceSnapshot, err error) {
		ctx, finish := observeDNSSyncPhase(ctx, "source_authority_read")
		defer func() { finish(err) }()
		return originalRead(ctx)
	}
	approvals, err := platformconfig.DNSRouteSourceAuthorizations(c.Artifact)
	if err != nil || len(approvals) == 0 {
		return p, nil, err
	}
	snapshot, err := read(ctx)
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
	reuseDNSRouteSourceContent(&snapshot, previous)
	_, finishBuild := observeDNSSyncPhase(ctx, "source_plan_build")
	plans, err := s.buildDNSRouteSources(ctx, c.Artifact, snapshot, now)
	finishBuild(err)
	if err != nil {
		return p, nil, err
	}
	union := &platformconfig.DNSReadinessPlan{}
	for _, requirement := range plans.Probes {
		union.Probes = append(union.Probes, requirement)
	}
	sort.Slice(union.Probes, func(i, j int) bool { return union.Probes[i].ID < union.Probes[j].ID })
	preferred, availability := dnsProbePriorities(p, s.Config.DNSNodeID, union)
	source := &dnsroutesource.Context{Snapshot: snapshot}
	facts := collectDNSRouteSourceFacts(ctx, union, p.Policy.DNSReadiness, probe, plans.Matches, source.ProofDeadline, s.servingDNSProbes.shareHistory(s.probeHistoryStore()).bind(c.Assignment, s.Logger, s.Config.CachePath).prioritize(preferred, availability))
	// A physical request can satisfy a different authorized publication than
	// its scheduling representative. Record continuity under each immutable
	// requirement only after its complete publication binding is verified.
	// Do this before retaining old proofs: a timed-out refresh must reset
	// recovery even when an older proof still has an unexpired lease.
	if err := s.observeDNSRouteRecovery(c.Artifact, plans, facts, p.Policy.DNSReadiness, time.Now().UTC()); err != nil {
		return p, nil, err
	}

	facts = retainValidDNSReadinessFacts(union, p.Policy.DNSReadiness, s.platformDNSRouteFacts, facts, time.Now().UTC())
	boundDNSRouteSourceFacts(dnsServingPayload{routeSources: source}, facts, time.Now().UTC())
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
	p.sourceRecoveryKeys, err = dnsRouteRecoveryKeys(c.Artifact, plans)
	if err != nil {
		return p, nil, err
	}
	p.indexSourceRequirements()
	selected := make([]dnsReadinessFact, 0, len(p.Plan.Probes))
	for _, requirement := range p.Plan.Probes {
		selected = append(selected, byID[requirement.ID])
	}
	next, err := read(ctx)
	if err != nil {
		return p, selected, err
	}
	if next.SelectionDigest != selection.Snapshot.SelectionDigest {
		return p, selected, platformconsumer.ErrAssignmentChanged
	}
	if err = s.advanceDNSRouteCursor(snapshot); err != nil {
		return p, selected, err
	}
	return p, selected, nil
}
func (s *Service) checkDNSRouteSources(ctx context.Context, client platformconsumer.Client, id platformconsumer.Identity, c dnsPlatformCandidate, source *dnsroutesource.Context) (resultErr error) {
	ctx, finish := observeDNSSyncPhase(ctx, "source_recheck")
	defer func() { finish(resultErr) }()
	if source == nil {
		return nil
	}
	next, err := client.DNSRouteSources(ctx, id, c.Assignment, c.Release, source.Snapshot)
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
func collectDNSRouteSourceFacts(ctx context.Context, plan *platformconfig.DNSReadinessPlan, policy *platformconfig.DNSReadinessPolicy, probe dnsReadinessProbeFunc, matches func(platformconfig.DNSReadinessProbe, routeprobe.Proof) bool, sourceDeadline func(*model.TrafficReleaseBinding) time.Time, scheduler *dnsReadinessScheduler) []dnsReadinessFact {
	physical := &platformconfig.DNSReadinessPlan{}
	indices := make(map[string]int, len(plan.Probes))
	physicalIndex := make([]int, len(plan.Probes))
	variants := make([][]int, 0)
	for i, requirement := range plan.Probes {
		key := dnsroutesource.ProbeKey(requirement)
		index, exists := indices[key]
		if !exists {
			index = len(physical.Probes)
			indices[key] = index
			physical.Probes = append(physical.Probes, requirement)
			variants = append(variants, nil)
		}
		physicalIndex[i] = index
		variants[index] = append(variants[index], i)
	}
	// Each worker owns disjoint variant indices. Only its scheduling view
	// uses the representative's ID; export and recovery keep the real IDs.
	facts := make([]dnsReadinessFact, len(plan.Probes))
	if scheduler == nil {
		scheduler = &dnsReadinessScheduler{}
	}
	physicalFacts := scheduler.collectWithEvaluator(ctx, physical, policy, probe, func(index int, proof routeprobe.Proof, err error, now time.Time) dnsReadinessFact {
		representative := physical.Probes[index]
		scheduled := dnsReadinessFact{ProbeID: representative.ID, Reason: "traffic_release_mismatch"}
		var until time.Time
		var checkedDeadline bool
		for _, i := range variants[index] {
			requirement := plan.Probes[i]
			fact := evaluateDNSReadinessFact(requirement, policy, proof, err, now)
			facts[i] = fact
			if i == variants[index][0] && !fact.Ready {
				scheduled = fact // preserve transport/negative probe diagnostics
			}
			if !fact.Ready || !matches(requirement, proof) {
				continue
			}
			if !checkedDeadline && sourceDeadline != nil {
				until = sourceDeadline(proof.TrafficRelease)
				checkedDeadline = true
			}
			if !until.IsZero() && until.Before(fact.Proof.ValidUntil) {
				if !until.After(now) {
					continue
				}
				fact.Proof.ValidUntil = until
			}
			if !scheduled.Ready || fact.Proof.ValidUntil.Before(scheduled.Proof.ValidUntil) {
				scheduled = fact
				scheduled.ProbeID = representative.ID
			}
		}
		return scheduled
	})
	now := time.Now().UTC()
	for original, requirement := range plan.Probes {
		physicalFact := physicalFacts[physicalIndex[original]]
		if physicalFact.Reason == "not_observed" {
			facts[original] = dnsReadinessFact{ProbeID: requirement.ID, ObservationID: physicalFact.ObservationID, Reason: "not_observed"}
			continue
		}
		fact := &facts[original]
		fact.ObservationID = physicalFact.ObservationID
		if fact.Ready && !fact.Proof.ValidUntil.After(now) {
			fact.Ready, fact.Reason = false, "proof_not_fresh"
		}
	}
	return facts
}

func dnsSameSourceContext(a, b *dnsroutesource.Context) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Snapshot.SelectionDigest == b.Snapshot.SelectionDigest && reflect.DeepEqual(a.Selections, b.Selections) && reflect.DeepEqual(a.RetainedDNSLKG, b.RetainedDNSLKG)
}

// The API, checkpoint and live state all replay the same signed requirements.
func (s *Service) verifiedDNSCheckpointPayload(c dnsServingCheckpoint) (dnsServingPayload, string, error) {
	p, routeID, err := s.verifyDNSServingRelease(c.Parent, c.Candidate)
	if err == nil && c.RouteSources != nil && c.RouteSources.RetainedDNSLKG != nil {
		grant := c.RouteSources.RetainedDNSLKG
		if grant.VerifiedByReleaseID != c.Candidate.Release.ID || !platformsafety.EvaluatePlatformLKGSnapshot(*grant, c.Parent, s.platformDNSKeys(), c.RouteSources.Snapshot.ObservedAt).Pass {
			return p, routeID, errors.New("retained DNS checkpoint source authorization invalid")
		}
	}
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
