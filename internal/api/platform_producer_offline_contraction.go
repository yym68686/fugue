package api

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"reflect"
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
	"fugue/internal/routeprobe"
)

func checkOfflineContractionSurvivors(status model.PlatformConsumerConvergenceStatus, offline map[string]string, proveDNS func(model.PlatformConsumerEvidenceAssessment) error) error {
	if len(status.UnexpectedConsumers) != 0 {
		return errors.New("unexpected predecessor consumers")
	}
	seen, retained := 0, 0
	for _, a := range status.Assessments {
		if !a.Required {
			continue
		}
		seen++
		c := a.Expected
		if group := offline[c.NodeID]; group != "" &&
			c.Component == model.PlatformConsumerComponentEdgeWorker && !c.HostedRouteTLSOnly &&
			(c.ArtifactKind == model.PlatformArtifactKindEdgeRouteBundle || c.ArtifactKind == model.PlatformArtifactKindCaddyRouteConfig) &&
			c.Cohort == group && c.AuthorityID == platformcontrol.ConsumerAuthorityID(group) {
			continue
		}
		retained++
		if a.State == model.InvariantEvidenceStatePass {
			continue
		}
		if proveDNS != nil && offlineContractionRetainedDNS(status, a) {
			if err := proveDNS(a); err != nil {
				return fmt.Errorf("DNS survivor %s: %w", c.NodeID, err)
			}
			continue
		}
		return fmt.Errorf("predecessor %s survivor %s: %s", c.ArtifactKind, c.NodeID, strings.Join(a.Reasons, "; "))
	}
	if retained == 0 || seen != status.RequiredExpected {
		return errors.New("predecessor survivor roster is empty or incomplete")
	}
	return nil
}

// Match the existing DNS heartbeat and convergence diagnostics. New or
// combined failure reasons require the ordinary recovery path.
const (
	retainedDNSProbeError = "DNS serving readiness or listener probe failed"
	probeNotPassedReason  = "probe status is not passed"
)

func offlineContractionRetainedDNS(status model.PlatformConsumerConvergenceStatus, a model.PlatformConsumerEvidenceAssessment) bool {
	c, f := a.Expected, a.Observed
	// The normal evaluator must have accepted the identity, freshness,
	// exact predecessor fence, and all other receipt checks.
	return status.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle && status.ScopeKey == platformconfig.GlobalScopeKey &&
		c.Component == model.PlatformConsumerComponentDNSServer && c.ArtifactKind == status.ArtifactKind && c.ScopeKey == status.ScopeKey &&
		a.State == model.InvariantEvidenceStateFail && len(a.Reasons) == 1 && a.Reasons[0] == probeNotPassedReason &&
		f != nil && f.IdentityVerified && strings.HasPrefix(f.CredentialID, "kubernetes:") &&
		f.ProbeStatus == "failed" && f.LastError == retainedDNSProbeError && f.ServingLKG && !f.LKGExpired &&
		f.CandidateGeneration == "" && f.LKGGeneration == c.ExpectedGeneration
}

func newlyHeldEdges(before, after []platformconfig.HostedAddressHold) []platformconfig.HostedAddressHold {
	old := make(map[string]platformconfig.HostedAddressHold, len(before))
	for _, h := range before {
		if _, duplicate := old[h.EdgeID]; h.EdgeID == "" || duplicate {
			return nil
		}
		old[h.EdgeID] = h
	}
	var added []platformconfig.HostedAddressHold
	seen := map[string]bool{}
	for _, h := range after {
		if h.EdgeID == "" || seen[h.EdgeID] {
			return nil
		}
		seen[h.EdgeID] = true
		if p, found := old[h.EdgeID]; found {
			if p != h {
				return nil
			}
			delete(old, h.EdgeID)
		} else {
			added = append(added, h)
		}
	}
	if len(old) != 0 {
		return nil
	}
	return added
}

func sameTrafficProgram(before *model.PlatformArtifact, after model.PlatformArtifact) bool {
	if before == nil || before.ArtifactKind != after.ArtifactKind {
		return false
	}
	program := func(a model.PlatformArtifact) map[string]any {
		m := make(map[string]any, len(a.Content))
		for k, v := range a.Content {
			switch k {
			case "generation", "lineage":
				continue
			case "policy":
				if policy, ok := v.(map[string]any); ok {
					policy = maps.Clone(policy)
					delete(policy, "generation")
					// The exact DNS-only transformation of these rules is
					// checked independently; it does not change Worker routes/TLS.
					delete(policy, "dns_answer_rules")
					m[k] = policy
					continue
				}
			case "domain_states":
				// The TLS capture can refresh the time associated with
				// precisely the same certificate state during this run.
				if rows, ok := v.([]any); ok {
					x := make([]any, len(rows))
					for i, row := range rows {
						m, ok := row.(map[string]any)
						if !ok {
							x[i] = row
							continue
						}
						m = maps.Clone(m)
						for _, stamp := range []string{"verified_at", "tls_last_checked_at", "tls_ready_at", "observed_at"} {
							delete(m, stamp)
						}
						x[i] = m
					}
					m[k] = x
					continue
				}
			}
			m[k] = v
		}
		return m
	}
	return reflect.DeepEqual(program(*before), program(after))
}

// Preserve every DNS declaration except for deletion of an address that the
// predecessor's signed readiness plan ties to a newly held physical member.
// Inherited address reservations can still keep their declaration unchanged.
func onlyOfflineDNSRecordsChanged(before, after platformconfig.DNSArtifactPayload, omitted map[string]bool) bool {
	if before.ReadinessPlan == nil || after.ReadinessPlan == nil || len(before.Records) != len(after.Records) {
		return false
	}
	type key = [2]string
	removed := map[key]map[string]bool{}
	for _, r := range before.ReadinessPlan.Records {
		for _, t := range r.Targets {
			if !omitted[t.EdgeID] {
				continue
			}
			k := key{r.Hostname, t.Family}
			if removed[k] == nil {
				removed[k] = map[string]bool{}
			}
			h, _, err := net.SplitHostPort(t.Address)
			if err != nil {
				h = t.Address
			}
			removed[k][h] = true
		}
	}
	old := map[key]platformconfig.DNSIntent{}
	for _, r := range before.Records {
		old[key{r.Hostname, r.Type}] = r
	}
	for _, r := range after.Records {
		k := key{r.Hostname, r.Type}
		p, ok := old[k]
		if !ok {
			return false
		}
		delete(old, k)
		a, b := p.Values, r.Values
		p.Values, r.Values = nil, nil
		if !reflect.DeepEqual(p, r) {
			return false
		}
		filter := func(x []string, present []string) []string {
			return slices.DeleteFunc(slices.Clone(x), func(v string) bool { return removed[k][v] && !slices.Contains(present, v) })
		}
		if !slices.Equal(slices.Sorted(slices.Values(filter(a, b))), slices.Sorted(slices.Values(b))) {
			return false
		}
	}
	return len(old) == 0
}

// The signed plan, not the DNS declaration's reserved addresses, describes
// eligible routes. Preserve each surviving physical target, probe and quorum.
func heldDNSAddress(held map[string]platformconfig.HostedAddressHold, id, group, address string) bool {
	h, ok := held[id]
	host, _, _ := net.SplitHostPort(address)
	return ok && group == h.EdgeGroupID && (address == h.Address || host == h.Address)
}

// Runtime facts are validated against the live Service, the exact selected
// DNS Pod and the signed predecessor before they can reach this check. They
// prove only that the unchanged requirements survive the authorized subtraction.
// They do not change the DNS receipt or attest to the proposed publication.
func verifyOfflineDNSProjection(facts platformDNSRuntimeFactsResponse, a model.PlatformConsumerEvidenceAssessment, source platformconfig.HostedAddressSource, held map[string]platformconfig.HostedAddressHold, now time.Time) error {
	snapshot, pin := facts.Snapshot, source.Publication
	assignment := snapshot.Assignment
	if !facts.transportReady || a.Observed == nil || !strings.HasPrefix(a.Observed.CredentialID, "kubernetes:") ||
		facts.Backend.PodUID == "" || !strings.HasSuffix(a.Observed.CredentialID, ":"+facts.Backend.PodUID) ||
		snapshot.NodeID != a.Expected.NodeID || snapshot.ParentDigest != pin.ReleaseSetDigest || snapshot.RouteArtifactID != pin.RouteArtifactID ||
		assignment.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || assignment.ScopeKey != platformconfig.GlobalScopeKey ||
		assignment.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || assignment.ExpectedConsumerSetID != a.Observed.ExpectedConsumerSetID ||
		assignment.ReleaseSetID != pin.ReleaseSetID || assignment.ArtifactReleaseID != pin.ReleaseID || assignment.FencingToken != pin.FencingToken ||
		assignment.ArtifactID != pin.DNSArtifactID || assignment.ContentHash != pin.DNSArtifactDigest {
		return errors.New("live transport or exact predecessor binding unavailable")
	}
	if a.FreshUntil == nil || !a.FreshUntil.After(now) || !facts.heartbeatValidUntil.After(now) || !snapshot.CheckpointValidUntil.After(now) {
		return errors.New("predecessor DNS evidence expired")
	}
	plan := source.Prior.ReadinessPlan
	if plan == nil || !reflect.DeepEqual(plan, facts.readinessPlan) {
		return errors.New("runtime DNS requirements differ from signed predecessor")
	}
	readyIDs := make(map[string]bool, len(facts.ReadyProbeIDs))
	for _, id := range facts.ReadyProbeIDs {
		readyIDs[id] = true
	}
	byID := make(map[string]routeprobe.Proof, len(readyIDs))
	for _, fact := range snapshot.Facts {
		if p := fact.Proof; readyIDs[fact.ProbeID] && fact.Ready && p.ValidUntil.After(now) && p.TrafficRelease != nil {
			byID[fact.ProbeID] = p
		}
	}
	removedFailure := false
	for _, p := range plan.Probes {
		_, pass := byID[p.ID]
		if heldDNSAddress(held, p.EdgeID, p.EdgeGroupID, p.Address) {
			removedFailure = removedFailure || !pass
		} else if !pass {
			return fmt.Errorf("retained route proof missing or expired for Edge %s", p.EdgeID)
		}
	}
	if !removedFailure {
		return errors.New("old DNS failure is not attributable to a withdrawn route probe")
	}
	for _, r := range plan.Records {
		if !platformconfig.DNSReadinessQuorum(r, func(t platformconfig.DNSReadinessTarget) bool {
			if heldDNSAddress(held, t.EdgeID, t.EdgeGroupID, t.Address) {
				return false
			}
			return platformconfig.DNSReadinessTargetReady(t, func(id string) (string, *model.TrafficReleaseBinding, bool) {
				p, ok := byID[id]
				return p.GroupID, p.TrafficRelease, ok
			})
		}) {
			return errors.New("retained route evidence cannot satisfy original DNS quorum")
		}
	}
	return nil
}

func offlineDNSPlanContracts(before, after *platformconfig.DNSReadinessPlan, held map[string]platformconfig.HostedAddressHold) bool {
	if before == nil || after == nil || len(before.Records) != len(after.Records) {
		return false
	}
	probes := func(list []platformconfig.DNSReadinessProbe, prior bool) map[string]string {
		ids := make(map[string]string, len(list))
		for _, p := range list {
			if prior && heldDNSAddress(held, p.EdgeID, p.EdgeGroupID, p.Address) {
				continue
			}
			id := p.ID
			p.RouteDigest = "" // route behavior is checked separately
			k, err := platformconfig.DNSReadinessProbeID(p)
			if err != nil || id == "" || ids[id] != "" {
				return nil
			}
			ids[id] = k
		}
		return ids
	}
	old, next := probes(before.Probes, true), probes(after.Probes, false)
	if len(old) == 0 || !slices.Equal(slices.Sorted(maps.Values(old)), slices.Sorted(maps.Values(next))) {
		return false
	}
	normalize := func(r platformconfig.DNSReadinessRecord, ids map[string]string, prior bool) (platformconfig.DNSReadinessRecord, bool) {
		r.Targets = slices.Clone(r.Targets)
		if prior {
			r.Targets = slices.DeleteFunc(r.Targets, func(t platformconfig.DNSReadinessTarget) bool {
				return heldDNSAddress(held, t.EdgeID, t.EdgeGroupID, t.Address)
			})
		}
		for i := range r.Targets {
			keys := make([]string, 0, len(r.Targets[i].ProbeIDs))
			for _, id := range r.Targets[i].ProbeIDs {
				if ids[id] == "" {
					return r, false
				}
				keys = append(keys, ids[id])
			}
			slices.Sort(keys)
			r.Targets[i].ProbeIDs = keys
		}
		return r, platformconfig.DNSReadinessQuorum(r, func(platformconfig.DNSReadinessTarget) bool { return true })
	}
	for i, r := range before.Records {
		left, ok := normalize(r, old, true)
		right, pass := normalize(after.Records[i], next, false)
		if !ok || !pass || !reflect.DeepEqual(left, right) {
			return false
		}
	}
	return true
}

func offlineDNSQueryViewsContract(before, after []platformconfig.DNSQueryView, policy *platformconfig.DNSQueryPolicy, held map[string]platformconfig.HostedAddressHold) bool {
	return slices.EqualFunc(before, after, func(a, b platformconfig.DNSQueryView) bool {
		if a.NodeID != b.NodeID || a.EdgeGroupID != b.EdgeGroupID || a.Zone != b.Zone {
			return false
		}
		remove := func(c model.EdgeDNSAnswerCandidate) bool {
			h, ok := held[c.EdgeID]
			return ok && h.EdgeGroupID == c.EdgeGroupID && h.Address == c.IP
		}
		return slices.EqualFunc(a.Records, b.Records, func(old, next model.EdgeDNSRecord) bool {
			old.RecordGeneration = next.RecordGeneration
			dropped := make(map[string]bool)
			old.Candidates = slices.DeleteFunc(slices.Clone(old.Candidates), func(c model.EdgeDNSAnswerCandidate) bool {
				if remove(c) {
					dropped[c.IP] = true
					return true
				}
				return false
			})
			old.Values = slices.DeleteFunc(slices.Clone(old.Values), func(ip string) bool { return dropped[ip] })
			eligible := make(map[string]bool, len(old.Candidates))
			for _, c := range old.Candidates {
				eligible[c.EdgeID] = true
			}
			fallback := offlineDNSFallback(policy, a.NodeID, old.Name, old.Type, eligible)
			order, selection, converted, err := offlineDNSOrder(old.AnswerPolicy.PhysicalOrder, old.AnswerPolicy.PhysicalSelection, fallback, held)
			if err != nil {
				return false
			}
			old.AnswerPolicy.PhysicalOrder, old.AnswerPolicy.PhysicalSelection = order, selection
			if converted {
				old.AnswerPolicy.PolicyKind = model.DNSAnswerPolicyKindPhysicalOrder
				old.AnswerPolicy.RankingScope, old.AnswerPolicy.RankingVersion = "", ""
			}
			groups := make(map[string]bool)
			for _, c := range old.Candidates {
				groups[c.EdgeGroupID] = true
			}
			if len(dropped) != 0 {
				// Allowed groups are derived from the signed readiness plan.
				old.AnswerPolicy.AllowedEdgeGroups = nil
				next.AnswerPolicy.AllowedEdgeGroups = nil
				if !groups[old.AnswerPolicy.SelectedEdgeGroupID] {
					old.AnswerPolicy.SelectedEdgeGroupID = ""
				}
				if !groups[old.AnswerPolicy.ShadowSelectedEdgeGroupID] {
					old.AnswerPolicy.ShadowSelectedEdgeGroupID = ""
				}
			}
			old.ScopedCandidates = slices.Clone(old.ScopedCandidates)
			for i, scope := range old.ScopedCandidates {
				kept := slices.DeleteFunc(slices.Clone(scope.Candidates), remove)
				if len(kept) != len(scope.Candidates) &&
					!slices.ContainsFunc(kept, func(c model.EdgeDNSAnswerCandidate) bool { return c.EdgeGroupID == scope.SelectedEdgeGroupID }) {
					scope.SelectedEdgeGroupID = ""
				}
				scope.Candidates = kept
				old.ScopedCandidates[i] = scope
			}
			old.ScopedCandidates = slices.DeleteFunc(old.ScopedCandidates, func(s model.EdgeDNSScopedAnswerCandidates) bool {
				return len(s.Candidates) == 0
			})
			return reflect.DeepEqual(old, next)
		})
	})
}

// A signed physical order can be narrowed, not reranked. A measurement with
// removed participants can only be carried as an order, not as renewed quality
// or an edited business allocation. Preserve the original age and preserve the
// primary epoch only when that same Edge remains primary.
func offlineDNSOrder(order *model.DNSPhysicalOrder, selection *model.DNSPhysicalSelection, fallback []string, held map[string]platformconfig.HostedAddressHold) (*model.DNSPhysicalOrder, *model.DNSPhysicalSelection, bool, error) {
	if order == nil && selection == nil {
		return nil, nil, false, nil
	}
	var originals []string
	if order != nil {
		originals = order.OrderedEdgeIDs
	} else {
		originals = selection.OrderedEdgeIDs
	}
	kept := slices.DeleteFunc(slices.Clone(originals), func(id string) bool { _, drop := held[id]; return drop })
	if len(kept) == len(originals) {
		return order, selection, false, nil
	}
	if selection != nil && selection.BusinessAllocation != nil {
		return nil, nil, false, errors.New("physical DNS order cannot be narrowed without changing its signed allocation")
	}
	exhausted := len(kept) == 0
	if exhausted {
		kept = slices.Clone(fallback)
		if len(kept) == 0 {
			return nil, nil, false, errors.New("withdrawn DNS order has no signed eligible fallback")
		}
	}
	if order != nil {
		order = model.CloneDNSPhysicalOrder(order)
	} else {
		order = &model.DNSPhysicalOrder{Version: "physical-order-v1", PrimarySince: selection.PrimarySince, QualityObservedAt: selection.QualityObservedAt}
		if order.QualityObservedAt == nil && selection.QualityState == "measured" {
			at := selection.CapturedAt
			order.QualityObservedAt = &at
		}
	}
	order.OrderedEdgeIDs = kept
	if exhausted {
		order.QualityObservedAt = nil // the signed fallback is not a quality observation
	}
	if kept[0] != originals[0] {
		order.PrimarySince = nil
	}
	return order, nil, selection != nil, nil
}

func offlineDNSFallback(policy *platformconfig.DNSQueryPolicy, node, host, family string, eligible map[string]bool) []string {
	if policy == nil {
		return nil
	}
	var order []string
	if availability := platformconfig.DNSAvailabilityForHost(policy, host); availability != nil {
		order = availability.OrderedEdgeIDs
	} else if policy.OrderedProjection != nil && !slices.ContainsFunc(policy.PhysicalRoutes, func(r platformconfig.PhysicalQualityRoute) bool { return r.Hostname == host }) {
		selected := policy.OrderedProjection.DefaultOrder
		for _, v := range policy.OrderedProjection.Overrides {
			if v.NodeID == node && v.Hostname == host && v.Type == family {
				selected = v.Order
				break
			}
		}
		if model.ValidateDNSPhysicalOrder(&selected) == nil {
			order = selected.OrderedEdgeIDs
		}
	}
	return slices.DeleteFunc(slices.Clone(order), func(id string) bool { return !eligible[id] })
}

// Recompile the exact verified predecessor inputs. A fresh business capture
// can contain unrelated routes or regenerated DNS rankings; publishing it
// under an offline contraction would make the transaction more than subtraction.
func offlineContractionRequest(req platformconfig.CompileRequest, source platformconfig.HostedAddressSource, held map[string]platformconfig.HostedAddressHold) (platformconfig.CompileRequest, error) {
	prior := source.PriorRuntime
	prior.HostedAddressReservations = nil // recompilation binds the same custody to the exact predecessor
	prior.DNSEdgeEndpoints = slices.DeleteFunc(slices.Clone(prior.DNSEdgeEndpoints), func(e platformconfig.DNSEdgeEndpoint) bool {
		h, ok := held[e.EdgeID]
		return ok && h.EdgeGroupID == e.EdgeGroupID && slices.Contains(e.A, h.Address)
	})
	// Retain the current producer's evidence for any previously readmitted
	// members; the verified predecessor's historical evidence is immutable.
	prior.Facts = map[string]any{}
	for _, key := range []string{"configuration_producer", platformproducer.OfflineRecoveryFact} {
		if fact, ok := req.RuntimeSnapshot.Facts[key]; ok {
			prior.Facts[key] = fact
		}
	}
	policy := source.Prior.Policy
	policy.DNSAnswerRules = slices.Clone(policy.DNSAnswerRules)
	prior.DNSSelections = slices.Clone(prior.DNSSelections)
	byQuery := make(map[[3]string]int, len(prior.DNSSelections))
	eligibleByQuery := make(map[[3]string]map[string]bool, len(prior.DNSSelections))
	for i, f := range prior.DNSSelections {
		byQuery[[3]string{f.NodeID, f.Hostname, f.Type}] = i
	}
	for _, view := range source.Prior.QueryViews {
		for _, rr := range view.Records {
			key := [3]string{view.NodeID, rr.Name, rr.Type}
			if _, found := byQuery[key]; !found {
				continue
			}
			if _, duplicate := eligibleByQuery[key]; duplicate {
				return req, errors.New("verified predecessor has ambiguous DNS query views")
			}
			ids := make(map[string]bool, len(rr.Candidates))
			for _, c := range rr.Candidates {
				if !heldDNSAddress(held, c.EdgeID, c.EdgeGroupID, c.IP) {
					ids[c.EdgeID] = true
				}
			}
			eligibleByQuery[key] = ids
		}
	}
	for i, rule := range policy.DNSAnswerRules {
		key := [3]string{rule.NodeID, rule.Hostname, rule.Type}
		j, found := byQuery[key]
		if !found || len(eligibleByQuery[key]) == 0 {
			return req, errors.New("verified predecessor DNS rule has no captured selection")
		}
		f := &prior.DNSSelections[j]
		fallback := offlineDNSFallback(policy.DNSQueryPolicy, rule.NodeID, rule.Hostname, rule.Type, eligibleByQuery[key])
		order, selection, converted, err := offlineDNSOrder(rule.PhysicalOrder, f.PhysicalSelection, fallback, held)
		if err != nil {
			return req, err
		}
		policy.DNSAnswerRules[i].PhysicalOrder, f.PhysicalSelection = order, selection
		if converted {
			policy.DNSAnswerRules[i].SelectionMode = model.DNSAnswerPolicyKindPhysicalOrder
			f.RankingVersion, f.RankingScope, f.PhysicalEvidence, f.RetainedPhysicalEvidence = "", "", nil, nil
		}
	}
	if len(policy.DNSAnswerRules) != len(byQuery) {
		return req, errors.New("verified predecessor has unbound DNS selection data")
	}
	generation, err := platformconfig.PolicySnapshotGeneration(policy)
	if err != nil {
		return req, err
	}
	policy.Generation, prior.PolicyGeneration = generation, generation
	return platformconfig.CompileRequest{Intent: source.PriorIntent, Policy: policy, RuntimeSnapshot: prior, OmitOriginalInput: req.OmitOriginalInput}, nil
}

// Keep every existing hold in a contraction, even when a different member
// just became eligible for readmission. Only a separate ordinary publication
// may add members back. Use current evidence for every other signed hold.
func offlineContractionHolds(prior, effective []platformproducer.OfflineEdge, observations []platformproducer.OfflineRecoveryObservation) ([]platformproducer.OfflineEdge, []platformproducer.OfflineRecoveryObservation) {
	priorIDs := make(map[string]bool, len(prior))
	for _, h := range prior {
		priorIDs[h.EdgeID] = true
	}
	next := slices.Clone(prior)
	for _, h := range effective {
		if !priorIDs[h.EdgeID] {
			next = append(next, h)
		}
	}
	return next, slices.DeleteFunc(slices.Clone(observations), func(o platformproducer.OfflineRecoveryObservation) bool { return priorIDs[o.EdgeID] })
}

func (s *Server) prepareOfflineContraction(ctx context.Context, policy platformproducer.Policy, authority model.PlatformArtifactRelease) (*platformconfig.CompileResult, platformCompilationSource, error) {
	var provenance platformCompilationSource
	if policy.TargetScope != platformconfig.GlobalScopeKey || policy.PublicationRole != "" || policy.Mode != "serving" || policy.InputSource != "business-static-intent" || policy.DNSPolicyArtifactID == "" {
		return nil, provenance, nil
	}
	parent, _, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, policy.TargetScope, model.PlatformArtifactReleaseChannelFull)
	if err != nil || !found {
		return nil, provenance, err
	}
	if parent.Metadata[platformproducer.PolicyReleaseMetadata] != authority.ID || parent.Metadata[platformproducer.DNSPolicyIDMetadata] != policy.DNSPolicyArtifactID {
		return nil, provenance, nil // changing policy authority is a separate release
	}
	input, err := s.store.GetPlatformArtifact(policy.DNSPolicyArtifactID)
	if err != nil {
		return nil, provenance, err
	}
	if input.Content["offline_edges"] == nil {
		return nil, provenance, nil
	}
	if input.ContentHash != policy.DNSPolicyDigest || input.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(input) != nil {
		return nil, provenance, errors.New("offline contraction policy is not trusted")
	}
	static, err := s.loadStaticPlatformIntent(policy.StaticIntentArtifactID, policy.StaticIntentDigest)
	if err != nil {
		return nil, provenance, err
	}
	declaration, err := platformproducer.DecodeProjectionPolicy(input, static.Consumers, policy.HostedZoneTemplates)
	if err != nil {
		return nil, provenance, err
	}
	prior, err := platformproducer.PublicationOfflineHolds(parent, declaration.OfflineEdges)
	if err != nil || len(prior) == len(declaration.OfflineEdges) {
		return nil, provenance, err
	}
	nodes, _, err := s.store.ListEdgeNodes("")
	if err != nil {
		return nil, provenance, err
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	effective, observations, err := s.effectiveOfflineEdgeHolds(ctx, nodes, now, declaration.OfflineEdges, s.store.RetiredPlatformConsumerTargets)
	if err != nil {
		return nil, provenance, err
	}
	holds, observations := offlineContractionHolds(prior, effective, observations)
	if len(holds) == len(prior) {
		return nil, provenance, nil
	}
	// A stale recovery checkpoint for a reachable node is not proof of loss.
	// Leave the ordinary refresh/readmission path available in this case.
	stale := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		stale[n.ID] = observeEdgeNodeInventory(n, now).Reason == "heartbeat_stale"
	}
	if slices.ContainsFunc(holds[len(prior):], func(h platformproducer.OfflineEdge) bool { return !stale[h.EdgeID] }) {
		return nil, provenance, nil
	}
	effectiveAddresses := offlineHostedAddressHolds(holds)
	source, err := s.readHostedAddressSourceWith(ctx, policy, authority, effectiveAddresses, s.observeHostedDraftBase)
	if err != nil {
		return nil, provenance, err
	}
	if source.Publication.ReleaseSetID != parent.ID || source.Publication.ReleaseSetDigest != parent.ContentHash {
		return nil, provenance, errors.New("offline contraction predecessor changed during selection")
	}
	compiler := platformconfig.ConfigurationInputCompilerVersion
	if s.compilerInputOmitOriginal {
		compiler = platformconfig.CompactOnlyInputCompilerVersion
	}
	digest, err := platformconfig.Digest([]any{"offline-consumer-contraction/v2", compiler, authority.ID, source.Publication, effectiveAddresses})
	if err != nil {
		return nil, provenance, err
	}
	if failed, err := s.producedSourcePreviouslyFailed(policy.TargetScope, authority.ID, digest); err != nil || failed {
		if err == nil {
			err = errors.New("offline contraction candidate previously failed; no change to the serving release")
		}
		return nil, provenance, err
	}
	req := platformconfig.CompileRequest{OmitOriginalInput: s.compilerInputOmitOriginal}
	req.RuntimeSnapshot.Facts = map[string]any{"configuration_producer": map[string]any{
		"policy_release_id": authority.ID, "source_digest": digest, "static_intent_artifact_id": policy.StaticIntentArtifactID,
		"static_intent_digest": policy.StaticIntentDigest, "dns_policy_artifact_id": policy.DNSPolicyArtifactID, "dns_policy_digest": policy.DNSPolicyDigest,
	}}
	if err := setOfflineRecoveryFact(&req.RuntimeSnapshot, observations, now); err != nil {
		return nil, provenance, err
	}
	compiled, err := s.compileHostedOfflineContraction(ctx, req, source)
	if err != nil {
		return nil, provenance, err
	}
	provenance = platformCompilationSource{PolicyReleaseID: authority.ID, SourceDigest: digest, StaticIntentID: policy.StaticIntentArtifactID,
		StaticIntentDigest: policy.StaticIntentDigest, DNSPolicyID: policy.DNSPolicyArtifactID, DNSPolicyDigest: policy.DNSPolicyDigest,
		offlinePredecessor: model.PlatformPublicationPrecondition{ArtifactID: source.Publication.ReleaseSetID, ContentHash: source.Publication.ReleaseSetDigest,
			ReleaseID: source.Publication.ReleaseID, FencingToken: source.Publication.FencingToken}}
	if len(compiled.InputSnapshot.HostedAddressReservations) > 0 {
		provenance.hostedSource = source.Publication
	}
	return &compiled, provenance, nil
}

// Run only after reading the unchanged signed producer inputs and the exact
// verified predecessor. Complete a membership-only withdrawal before allowing
// an ordinary release to consume new business input. Readmission and unrelated
// stale receipts must still go through the ordinary convergence gate.
func (s *Server) compileHostedOfflineContraction(ctx context.Context, req platformconfig.CompileRequest, source platformconfig.HostedAddressSource) (platformconfig.CompileResult, error) {
	bad := func(reason string) error { return fmt.Errorf("offline contraction: %s", reason) }
	held := newlyHeldEdges(source.PriorHolds, source.EffectiveHolds)
	if len(held) == 0 {
		return platformconfig.CompileResult{}, bad("no newly effective signed offline hold")
	}
	now := time.Now().UTC()
	omitted := make(map[string]string, len(held))
	omittedIDs := make(map[string]bool, len(held))
	heldByID := make(map[string]platformconfig.HostedAddressHold, len(held))
	var exclusions []platformproducer.OfflineEdge
	for _, h := range held {
		n, _, err := s.store.GetEdgeNode(h.EdgeID)
		if err != nil || n.EdgeGroupID != h.EdgeGroupID || n.PublicIPv4 != h.Address || observeEdgeNodeInventory(n, now).Reason != "heartbeat_stale" {
			return platformconfig.CompileResult{}, bad("held Edge identity or stale heartbeat not confirmed")
		}
		omitted[h.EdgeID] = h.EdgeGroupID
		omittedIDs[h.EdgeID] = true
		heldByID[h.EdgeID] = h
		exclusions = append(exclusions, platformproducer.OfflineEdge{EdgeID: h.EdgeID, EdgeGroupID: h.EdgeGroupID, PublicIPv4: h.Address, Reason: "network_unavailable"})
	}
	slices.SortFunc(exclusions, func(a, b platformproducer.OfflineEdge) int {
		return strings.Compare(a.EdgeID, b.EdgeID)
	})
	parent, current, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, platformconfig.GlobalScopeKey, "full")
	if err != nil || !found || parent.ID != source.Publication.ReleaseSetID || parent.ContentHash != source.Publication.ReleaseSetDigest ||
		current.ID != source.Publication.ReleaseID || current.FencingToken != source.Publication.FencingToken {
		return platformconfig.CompileResult{}, bad("verified predecessor changed")
	}
	out, err := compileOfflineContractionCandidate(req, source, heldByID, omittedIDs, exclusions)
	if err != nil {
		return out, err
	}
	// Probe only online, still-required DNS executors. All candidate checks above
	// run first: the result removes no consumer and is not a candidate receipt.
	proofCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	proveDNS := func(a model.PlatformConsumerEvidenceAssessment) error {
		facts, err := s.readPlatformDNSRuntimeFacts(proofCtx, a.Expected.NodeID)
		if err != nil {
			return fmt.Errorf("live serving snapshot unavailable: %w", err)
		}
		return verifyOfflineDNSProjection(facts, a, source, heldByID, time.Now().UTC())
	}
	if gate := s.checkReleaseConsumerSetsExcept(ctx, parent, true, omitted, proveDNS); !gate.Pass {
		return platformconfig.CompileResult{}, bad(gate.Message)
	}
	return out, nil
}

func compileOfflineContractionCandidate(current platformconfig.CompileRequest, source platformconfig.HostedAddressSource, heldByID map[string]platformconfig.HostedAddressHold, omittedIDs map[string]bool, exclusions []platformproducer.OfflineEdge) (platformconfig.CompileResult, error) {
	bad := func(reason string) error { return fmt.Errorf("offline contraction: %s", reason) }
	req, err := offlineContractionRequest(current, source, heldByID)
	if err != nil {
		return platformconfig.CompileResult{}, fmt.Errorf("offline contraction inputs: %w", err)
	}
	out, err := platformconfig.CompileIndependentManagedBaseline(source, req)
	if err != nil {
		return out, err
	}
	candidate := out.DNSArtifact
	if candidate.ScopeKey == "" {
		candidate.ScopeKey = candidate.Scope.Key
	}
	nextDNS, err := platformconfig.DecodeDNSArtifact(candidate)
	priorPolicy := source.Prior.Policy
	priorPolicy.Generation = nextDNS.Policy.Generation
	// The request derived only these rule changes from the verified prior
	// observations; the compiled query views must independently agree.
	priorPolicy.DNSAnswerRules = req.Policy.DNSAnswerRules
	if err != nil {
		return platformconfig.CompileResult{}, fmt.Errorf("offline contraction DNS candidate: %w", err)
	}
	if !onlyOfflineDNSRecordsChanged(source.Prior, nextDNS, omittedIDs) || !reflect.DeepEqual(priorPolicy, nextDNS.Policy) ||
		!reflect.DeepEqual(source.Prior.ConsumerViews, nextDNS.ConsumerViews) {
		return platformconfig.CompileResult{}, bad("DNS declarations, policy or owners changed beyond the authorized withdrawal")
	}
	if !offlineDNSPlanContracts(source.Prior.ReadinessPlan, nextDNS.ReadinessPlan, heldByID) {
		return platformconfig.CompileResult{}, bad("DNS surviving requirements or quorum changed")
	}
	if !offlineDNSQueryViewsContract(source.Prior.QueryViews, nextDNS.QueryViews, source.Prior.Policy.DNSQueryPolicy, heldByID) || validateOfflineDNSCandidate(out.DNSArtifact, exclusions) != nil {
		return platformconfig.CompileResult{}, bad("DNS query views are not an exact withdrawal")
	}
	if !sameTrafficProgram(source.PriorRoute, out.RouteArtifact) || !sameTrafficProgram(source.PriorTLS, out.TLSArtifact) {
		return platformconfig.CompileResult{}, bad("route or TLS program changed alongside membership")
	}
	return out, nil
}
