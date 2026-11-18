package dnsserver

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"sync/atomic"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/passiveasn"
	"fugue/internal/platformcontrol"
	"github.com/miekg/dns"
)

type dnsPassiveASNDecision struct {
	Reason            string     `json:"reason"`
	Mode              string     `json:"mode,omitempty"`
	ASN               uint32     `json:"asn,omitempty"`
	Family            int        `json:"family,omitempty"`
	ObservationDigest string     `json:"observation_digest,omitempty"`
	EvidenceAt        *time.Time `json:"evidence_at,omitempty"`
	EvidenceKind      string     `json:"evidence_kind,omitempty"`
	Incumbent         string     `json:"incumbent_edge_id,omitempty"`
	ComparedChoice    string     `json:"choice_edge_id,omitempty"`
	SharedWindows     int        `json:"shared_windows,omitempty"`
	LeftWinWindows    int        `json:"left_win_windows,omitempty"`
	RightWinWindows   int        `json:"right_win_windows,omitempty"`
}

type dnsPassiveASNSelection struct {
	Decision dnsPassiveASNDecision        `json:"decision"`
	Chosen   *edgequality.PassiveEndpoint `json:"chosen,omitempty"`
	Scope    uint8                        `json:"scope,omitempty"`
	TTL      uint32                       `json:"ttl,omitempty"`
}

type dnsPassiveRuntime struct {
	view   *passiveasn.View
	budget *atomic.Uint64
	node   string
}

func (s *Service) runPassiveASNDNS(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for ctx.Err() == nil {
		if err := s.refreshPassiveASNDNS(ctx); err != nil && ctx.Err() == nil && s.Logger != nil {
			s.Logger.Printf("DNS passive ASN refresh: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) refreshPassiveASNDNS(ctx context.Context) error {
	st := s.currentDNSServingState()
	if st == nil || !st.passiveASNDynamic() {
		s.passiveASNView.Store(nil)
		return nil
	}
	run, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	client := s.platformConsumerClient()
	id, a, err := client.ObserveServingAssignment(run, model.PlatformConsumerComponentDNSServer, s.Config.DNSNodeID, platformcontrol.ConfiguredConsumerScope(s.Config.PlatformScopeKey), model.PlatformArtifactKindDNSAnswerBundle)
	if err != nil {
		return fmt.Errorf("discover serving assignment: %w", err)
	}
	if !passiveAssignmentMatches(st, a) {
		return nil
	}
	artifact, err := client.PassiveASN(run, id, a)
	if err != nil {
		return fmt.Errorf("fetch bound observation: %w", err)
	}
	view, err := passiveasn.Verify(artifact, a, s.Config.DNSNodeID, s.platformDNSKeys(), time.Now().UTC())
	if err != nil {
		return err
	}
	if previous := s.passiveASNView.Load(); previous != nil && previous.DNSDigest == view.DNSDigest && view.ObservedAt.Before(previous.ObservedAt) {
		return nil
	}
	if state := s.currentDNSServingState(); state != nil && passiveAssignmentMatches(state, a) {
		previous := s.passiveASNView.Swap(view)
		if s.Logger != nil && (previous == nil || previous.Digest != view.Digest) {
			s.Logger.Printf("DNS passive ASN loaded mode=%s bindings=%d comparisons=%d observation=%s",
				view.Mode, len(view.Bindings), len(view.Pairs), view.Digest)
		}
	}
	return nil
}

func passiveAssignmentMatches(st *dnsServingState, a model.PlatformConsumerAssignment) bool {
	c := st.record.Candidate
	return c.Artifact.ID == a.ArtifactID && c.Artifact.ContentHash == a.ContentHash && c.Assignment.ReleaseSetID == a.ReleaseSetID &&
		c.Assignment.ExpectedConsumerSetID == a.ExpectedConsumerSetID && c.Assignment.FencingToken == a.FencingToken
}

func (st *dnsServingState) passiveASNDynamic() bool {
	p := st.payload.Policy.DNSQueryPolicy
	return p != nil && p.DynamicQuality != nil && p.DynamicQuality.Mode == "all_dynamic"
}

func passiveECSPrefix(req *dns.Msg) netip.Prefix {
	opt := req.IsEdns0()
	if opt == nil {
		return netip.Prefix{}
	}
	optCount := 0
	for _, rr := range req.Extra {
		if rr.Header().Rrtype == dns.TypeOPT {
			optCount++
		}
	}
	if optCount != 1 {
		return netip.Prefix{}
	}
	var found *dns.EDNS0_SUBNET
	for _, option := range opt.Option {
		if ecs, ok := option.(*dns.EDNS0_SUBNET); ok {
			if found != nil || ecs == nil {
				return netip.Prefix{}
			}
			found = ecs
		}
	}
	if found == nil || found.SourceScope != 0 || found.SourceNetmask == 0 {
		return netip.Prefix{}
	}
	addr, ok := netip.AddrFromSlice(found.Address)
	if !ok {
		return netip.Prefix{}
	}
	addr = addr.Unmap()
	if (found.Family != 1 || !addr.Is4()) && (found.Family != 2 || !addr.Is6()) ||
		int(found.SourceNetmask) > addr.BitLen() {
		return netip.Prefix{}
	}
	p := netip.PrefixFrom(addr, int(found.SourceNetmask))
	if p != p.Masked() {
		return netip.Prefix{}
	}
	return p
}

func passiveEndpoint(c model.EdgeDNSAnswerCandidate) edgequality.PassiveEndpoint {
	return edgequality.PassiveEndpoint{EdgeID: c.EdgeID, GroupID: c.EdgeGroupID, Address: c.IP}
}

func (st *dnsServingState) passiveASNSelection(rt *dnsPassiveRuntime, req *dns.Msg, remote string, record model.EdgeDNSRecord, audit edgeDNSAnswerAudit, now time.Time, hasReadiness bool) *dnsPassiveASNSelection {
	if !st.passiveASNDynamic() || rt == nil {
		return nil
	}
	result := &dnsPassiveASNSelection{Decision: dnsPassiveASNDecision{Reason: "record_not_eligible"}}
	if record.Type != "A" && record.Type != "AAAA" {
		return nil
	}
	// Explicit service policy, fixed overrides, business allocation,
	// redundancy, multi-answer and failover retain their original semantics.
	if !hasReadiness || st.passiveFixed[record.Name+"\x00"+record.Type] || len(audit.Answered) != 1 || audit.SelectionResult == "availability_readiness_failover" || !record.AnswerPolicy.HealthRequired ||
		!record.AnswerPolicy.RouteReadyRequired || len(record.ScopedCandidates) != 0 || record.AnswerPolicy.ECSEnabled ||
		record.AnswerPolicy.Availability != nil ||
		record.AnswerPolicy.PolicyKind != model.DNSAnswerPolicyKindPhysicalQuality && record.AnswerPolicy.PolicyKind != model.DNSAnswerPolicyKindPhysicalOrder ||
		(record.AnswerPolicy.PhysicalSelection != nil && record.AnswerPolicy.PhysicalSelection.BusinessAllocation != nil) {
		return result
	}
	prefix := passiveECSPrefix(req)
	// Keep subnet cache entries isolated even when the supplemental
	// observation is cold, shadow-only, or not classifiable. Otherwise a
	// baseline answer with scope zero can suppress a scoped answer served
	// by the other authority. This does not trust the prefix for steering.
	if prefix.IsValid() {
		result.Scope = uint8(prefix.Bits())
	}
	view := rt.view
	c := st.record.Candidate
	if view == nil || view.NodeID != rt.node || view.DNSArtifactID != c.Artifact.ID || view.DNSDigest != c.Artifact.ContentHash ||
		view.ReleaseSetID != c.Assignment.ReleaseSetID || view.ConsumerSetID != c.Assignment.ExpectedConsumerSetID ||
		view.FencingToken != c.Assignment.FencingToken || !view.ExpiresAt.After(now) {
		result.Decision.Reason = "publication_unavailable"
		return result
	}
	mode := view.Mode
	result.Decision.Mode = mode
	ip, _, _ := net.SplitHostPort(remote)
	resolver, _ := netip.ParseAddr(ip)
	asn, reason := view.Classify(prefix, resolver.Unmap(), now)
	result.Decision.Reason, result.Decision.ObservationDigest = reason, view.Digest
	if asn == 0 {
		return result
	}
	if (record.Type == "A") != prefix.Addr().Is4() {
		result.Decision.Reason = "ecs_family_mismatch"
		return result
	}
	family := 6
	if record.Type == "A" {
		family = 4
	}
	result.Decision.ASN, result.Decision.Family = asn, family
	// Existing selector's signed eligible order is the upper bound, never a
	// restatement of the producer's broader inventory.
	ordered, _ := edgeDNSOrderedCandidatesWithDecision(record, dnsGeoHint{}, now, false)
	endpoints := []edgequality.PassiveEndpoint{passiveEndpoint(audit.Answered[0])}
	for _, candidate := range ordered {
		endpoint := passiveEndpoint(candidate)
		if !slices.ContainsFunc(endpoints, func(prior edgequality.PassiveEndpoint) bool { return prior.EdgeID == endpoint.EdgeID }) {
			endpoints = append(endpoints, endpoint)
		}
	}
	var explore uint64
	if mode == "active" && len(endpoints) > 1 && rt.budget != nil {
		if n := rt.budget.Add(1); n%200 == 0 {
			explore = n / 200
		}
	}
	choice := view.Choose(asn, family, endpoints, explore)
	result.Decision.Reason = choice.Reason
	result.Decision.Incumbent, result.Decision.ComparedChoice = endpoints[0].EdgeID, choice.Edge.EdgeID
	if !choice.EvidenceAt.IsZero() {
		result.Decision.EvidenceAt = &choice.EvidenceAt
		result.Decision.EvidenceKind = "recent"
		if now.Sub(choice.EvidenceAt) > edgequality.PassiveASNWindow {
			result.Decision.EvidenceKind = "historical"
		}
		result.Decision.SharedWindows, result.Decision.LeftWinWindows, result.Decision.RightWinWindows = choice.SharedWindows, choice.LeftWinWindows, choice.RightWinWindows
	}
	if mode == "active" && len(endpoints) > 1 {
		remaining := view.ExpiresAt.Sub(now) / time.Second
		if remaining >= 1 {
			result.TTL = uint32(remaining)
			if choice.Reason == "bounded_passive_asn_exploration" {
				result.TTL = min(result.TTL, 30)
			}
			if choice.Edge != endpoints[0] {
				result.Chosen = &choice.Edge
			}
		}
	}
	return result
}

func applyPassiveASNSelection(result *dnsPassiveASNSelection, record model.EdgeDNSRecord, rrs []dns.RR, audit *edgeDNSAnswerAudit, at time.Time) {
	if result == nil {
		return
	}
	audit.PassiveASN = &result.Decision
	if result.Decision.Mode != "active" || result.Scope == 0 || len(rrs) != 1 || len(audit.Answered) != 1 {
		return
	}
	if result.TTL > 0 {
		rrs[0].Header().Ttl = min(rrs[0].Header().Ttl, result.TTL)
	}
	if result.Chosen == nil {
		return
	}
	ordered, _ := edgeDNSOrderedCandidatesWithDecision(record, dnsGeoHint{}, at, false)
	for _, c := range ordered {
		if passiveEndpoint(c) != *result.Chosen {
			continue
		}
		switch r := rrs[0].(type) {
		case *dns.A:
			ip := net.ParseIP(c.IP).To4()
			if ip == nil {
				return
			}
			r.A = ip
		case *dns.AAAA:
			ip := net.ParseIP(c.IP)
			if ip == nil || ip.To4() != nil {
				return
			}
			r.AAAA = ip
		default:
			return
		}
		audit.Answered = []model.EdgeDNSAnswerCandidate{c}
		audit.SelectedEdgeGroupID = c.EdgeGroupID
		audit.SelectionResult = result.Decision.Reason
		if result.Decision.Reason == "bounded_passive_asn_exploration" {
			audit.ExplorationKind = result.Decision.Reason
		}
		return
	}
}
