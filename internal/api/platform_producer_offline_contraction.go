package api

import (
	"context"
	"errors"
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
)

func offlineContractionSurvivorsPass(status model.PlatformConsumerConvergenceStatus, offline map[string]string) bool {
	if len(status.UnexpectedConsumers) != 0 {
		return false
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
			c.AuthorityID == platformcontrol.ConsumerAuthorityID(group) {
			continue
		}
		retained++
		if a.State != model.InvariantEvidenceStatePass {
			return false
		}
	}
	return retained > 0 && seen == status.RequiredExpected
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
func offlineDNSPlanContracts(before, after *platformconfig.DNSReadinessPlan, held map[string]platformconfig.HostedAddressHold) bool {
	if before == nil || after == nil || len(before.Records) != len(after.Records) {
		return false
	}
	removed := func(id, group, address string) bool {
		h, ok := held[id]
		host, _, _ := net.SplitHostPort(address)
		return ok && group == h.EdgeGroupID && (address == h.Address || host == h.Address)
	}
	probes := func(list []platformconfig.DNSReadinessProbe, prior bool) map[string]string {
		ids := make(map[string]string, len(list))
		for _, p := range list {
			if prior && removed(p.EdgeID, p.EdgeGroupID, p.Address) {
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
				return removed(t.EdgeID, t.EdgeGroupID, t.Address)
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

func offlineDNSQueryViewsContract(before, after []platformconfig.DNSQueryView, held map[string]platformconfig.HostedAddressHold) bool {
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

// Run only after reading the unchanged signed producer inputs and the exact
// verified predecessor. Complete a membership-only withdrawal before allowing
// an ordinary release to consume new business input. Readmission and unrelated
// stale receipts must still go through the ordinary convergence gate.
func (s *Server) compileHostedOfflineContraction(ctx context.Context, req platformconfig.CompileRequest, source platformconfig.HostedAddressSource, capture *platformconfig.HostedTransitionCapture) (platformconfig.CompileResult, error) {
	bad := errors.New("offline contraction lacks exact authorized withdrawal and surviving consumer evidence")
	held := newlyHeldEdges(source.PriorHolds, source.EffectiveHolds)
	if len(held) == 0 {
		return platformconfig.CompileResult{}, bad
	}
	now := time.Now().UTC()
	omitted := make(map[string]string, len(held))
	omittedIDs := make(map[string]bool, len(held))
	heldByID := make(map[string]platformconfig.HostedAddressHold, len(held))
	var exclusions []platformproducer.OfflineEdge
	for _, h := range held {
		n, _, err := s.store.GetEdgeNode(h.EdgeID)
		if err != nil || n.EdgeGroupID != h.EdgeGroupID || n.PublicIPv4 != h.Address || observeEdgeNodeInventory(n, now).Reason != "heartbeat_stale" {
			return platformconfig.CompileResult{}, bad
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
		current.ID != source.Publication.ReleaseID || current.FencingToken != source.Publication.FencingToken ||
		!s.checkReleaseConsumerSetsExcept(ctx, parent, true, omitted).Pass {
		return platformconfig.CompileResult{}, bad
	}
	if capture != nil {
		req, err = platformconfig.IndependentManagedBaselineRequest(source, req, capture)
		if err != nil {
			return platformconfig.CompileResult{}, err
		}
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
	if err != nil || !onlyOfflineDNSRecordsChanged(source.Prior, nextDNS, omittedIDs) ||
		!reflect.DeepEqual(priorPolicy, nextDNS.Policy) || !reflect.DeepEqual(source.Prior.ConsumerViews, nextDNS.ConsumerViews) ||
		!offlineDNSPlanContracts(source.Prior.ReadinessPlan, nextDNS.ReadinessPlan, heldByID) ||
		!offlineDNSQueryViewsContract(source.Prior.QueryViews, nextDNS.QueryViews, heldByID) ||
		validateOfflineDNSCandidate(out.DNSArtifact, exclusions) != nil ||
		!sameTrafficProgram(source.PriorRoute, out.RouteArtifact) || !sameTrafficProgram(source.PriorTLS, out.TLSArtifact) {
		return platformconfig.CompileResult{}, bad
	}
	return out, nil
}
