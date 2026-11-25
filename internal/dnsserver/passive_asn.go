package dnsserver

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/passiveasn"
	"fugue/internal/platformcontrol"
	"github.com/miekg/dns"
)

type dnsPassiveASNDecision struct {
	Reason                  string     `json:"reason"`
	Mode                    string     `json:"mode,omitempty"`
	ASN                     uint32     `json:"asn,omitempty"`
	Family                  int        `json:"family,omitempty"`
	ObservationDigest       string     `json:"observation_digest,omitempty"`
	EvidenceAt              *time.Time `json:"evidence_at,omitempty"`
	EvidenceKind            string     `json:"evidence_kind,omitempty"`
	Incumbent               string     `json:"incumbent_edge_id,omitempty"`
	ComparedChoice          string     `json:"choice_edge_id,omitempty"`
	SharedWindows           int        `json:"shared_windows,omitempty"`
	LeftWinWindows          int        `json:"left_win_windows,omitempty"`
	RightWinWindows         int        `json:"right_win_windows,omitempty"`
	AggregateASNs           int        `json:"aggregate_asns,omitempty"`
	AggregateSupportingASNs int        `json:"aggregate_supporting_asns,omitempty"`
}

type dnsPassiveASNSelection struct {
	Decision dnsPassiveASNDecision        `json:"decision"`
	Chosen   *edgequality.PassiveEndpoint `json:"chosen,omitempty"`
	Scope    uint8                        `json:"scope,omitempty"`
	TTL      uint32                       `json:"ttl,omitempty"`
}

type dnsPassiveRuntime struct {
	view *passiveasn.View
	node string
}

func passiveHasECS(req *dns.Msg) bool {
	for _, rr := range req.Extra {
		if opt, ok := rr.(*dns.OPT); ok {
			for _, option := range opt.Option {
				if _, ecs := option.(*dns.EDNS0_SUBNET); ecs {
					return true
				}
			}
		}
	}
	return false
}

// Cohort allocation is independent of request count, hostname and executor.
// A 60-second assignment with a 30s response TTL lets recurring cohorts
// contribute control and challenger traffic over time. This samples 5% of
// eligible prefix/epochs, not a guaranteed percentage of HTTP requests.
func passiveExploration(prefix netip.Prefix, now time.Time) uint64 {
	if !prefix.IsValid() {
		return 0
	}
	coarseBits := 48
	if prefix.Addr().Is4() {
		coarseBits = 24
	}
	prefix = netip.PrefixFrom(prefix.Addr(), coarseBits).Masked()
	var key [24]byte
	addr := prefix.Addr().As16()
	copy(key[:16], addr[:])
	binary.BigEndian.PutUint64(key[16:], uint64(now.Unix()/60))
	sum := sha256.Sum256(key[:])
	if binary.BigEndian.Uint64(sum[:8])%20 != 0 {
		return 0
	}
	return 1 + (binary.BigEndian.Uint64(sum[8:16]) >> 1)
}

func (s *Service) runPassiveASNDNS(ctx context.Context) {
	// Detect a newly loaded binding locally rather than waiting a full minute
	// to fetch it. HTTP work remains serial and bounded even when DNS or the
	// publisher is recovering.
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var prior passiveASNBinding
	var attempted time.Time
	for ctx.Err() == nil {
		st := s.currentDNSServingState()
		if st == nil || !st.passiveASNDynamic() {
			s.passiveASNView.Store(nil)
			prior, attempted = passiveASNBinding{}, time.Time{}
		} else if binding := st.passiveASNBinding(); binding.refreshDue(prior, attempted, time.Now(), s.passiveASNView.Load()) {
			prior, attempted = binding, time.Now()
			if err := s.refreshPassiveASNDNS(ctx); err != nil && ctx.Err() == nil && s.Logger != nil {
				s.Logger.Printf("DNS passive ASN refresh: %v", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type passiveASNBinding struct {
	artifact, digest, release, consumers string
	fence                                int64
}

func (st *dnsServingState) passiveASNBinding() passiveASNBinding {
	c := st.record.Candidate
	return passiveASNBinding{c.Artifact.ID, c.Artifact.ContentHash, c.Assignment.ReleaseSetID, c.Assignment.ExpectedConsumerSetID, c.Assignment.FencingToken}
}

func (b passiveASNBinding) matchesView(view *passiveasn.View, now time.Time) bool {
	return view != nil && view.DNSArtifactID == b.artifact && view.DNSDigest == b.digest && view.ReleaseSetID == b.release &&
		view.ConsumerSetID == b.consumers && view.FencingToken == b.fence && view.ExpiresAt.After(now)
}

func (b passiveASNBinding) refreshDue(prior passiveASNBinding, attempted, now time.Time, view *passiveasn.View) bool {
	if b != prior || attempted.IsZero() {
		return true
	}
	interval := 15 * time.Second
	if b.matchesView(view, now) && view.Mode == "active" {
		interval = time.Minute
	}
	return now.Sub(attempted) >= interval
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

// Availability can supply the baseline for an otherwise dynamic query; it
// does not pin its healthy primary. Only compiler-owned dynamic baselines
// may supersede a bootstrap order override. Do not steer a real failover.
func passiveAvailabilityPrimary(policy model.DNSAnswerPolicy, selectionResult string) bool {
	if policy.Availability == nil || policy.Availability.Version != model.DNSAvailabilityVersion {
		return false
	}
	switch policy.PolicyKind {
	case model.DNSAnswerPolicyKindPhysicalOrder:
		return selectionResult == "physical_order_primary" &&
			(policy.Reason == "availability_only_quality_unavailable" || policy.Reason == "retained_previous_verified_order_quality_unavailable" ||
				policy.RankingVersion == model.DNSFleetDefaultRankingVersion)
	case model.DNSAnswerPolicyKindPhysicalQuality:
		return selectionResult == "physical_selected_primary" || selectionResult == "physical_learning_primary"
	}
	return false
}

func (st *dnsServingState) passiveASNSelection(rt *dnsPassiveRuntime, req *dns.Msg, remote string, record model.EdgeDNSRecord, audit edgeDNSAnswerAudit, now time.Time, hasReadiness bool) *dnsPassiveASNSelection {
	if !st.passiveASNDynamic() || rt == nil {
		return nil
	}
	result := &dnsPassiveASNSelection{Decision: dnsPassiveASNDecision{Reason: "record_not_eligible"}}
	if record.Type != "A" && record.Type != "AAAA" {
		return nil
	}
	policy := record.AnswerPolicy
	availabilityPrimary := passiveAvailabilityPrimary(policy, audit.SelectionResult)
	primary := audit.SelectionResult == "physical_order_primary" || audit.SelectionResult == "physical_selected_primary" || audit.SelectionResult == "physical_learning_primary"
	fleetDefault := policy.PolicyKind == model.DNSAnswerPolicyKindPhysicalOrder && policy.RankingVersion == model.DNSFleetDefaultRankingVersion
	// Explicit service/placement constraints, effective fixed orders, and
	// separate allocation rules retain their original semantics. Once the
	// signed primary is unavailable, readiness failover owns the answer.
	if !primary || !hasReadiness || st.passiveServicePolicy[record.Name] ||
		(st.passiveFixed[record.Name+"\x00"+record.Type] || policy.Availability != nil) && !availabilityPrimary ||
		len(audit.Answered) != 1 || !policy.HealthRequired || !policy.RouteReadyRequired ||
		len(record.ScopedCandidates) != 0 || policy.ECSEnabled ||
		policy.PolicyKind != model.DNSAnswerPolicyKindPhysicalQuality && policy.PolicyKind != model.DNSAnswerPolicyKindPhysicalOrder ||
		(policy.PhysicalSelection != nil && policy.PhysicalSelection.BusinessAllocation != nil) {
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
	if !st.passiveASNBinding().matchesView(view, now) || view.NodeID != rt.node {
		result.Decision.Reason = "publication_unavailable"
		return result
	}
	mode := view.Mode
	result.Decision.Mode = mode
	ip, _, _ := net.SplitHostPort(remote)
	resolver, _ := netip.ParseAddr(ip)
	asn, reason := view.Classify(prefix, resolver.Unmap(), now)
	result.Decision.Reason, result.Decision.ObservationDigest = reason, view.Digest
	noECS := !prefix.IsValid() && !passiveHasECS(req)
	if !noECS && asn == 0 && reason != "ecs_origin_unknown" {
		return result
	}
	if !noECS && (record.Type == "A") != prefix.Addr().Is4() {
		result.Decision.Reason = "ecs_family_mismatch"
		return result
	}
	family := 6
	if record.Type == "A" {
		family = 4
	}
	result.Decision.ASN, result.Decision.Family = asn, family
	if fleetDefault && asn == 0 {
		result.Decision.Incumbent, result.Decision.ComparedChoice = audit.Answered[0].EdgeID, audit.Answered[0].EdgeID
		return result
	}
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
	if mode == "active" && view.CohortExploration && len(endpoints) > 1 && !noECS {
		explore = passiveExploration(prefix, now)
	}
	var choice passiveasn.Choice
	if fleetDefault {
		choice = view.ChooseLocal(asn, family, endpoints, 0)
	} else {
		choice = view.Choose(asn, family, endpoints, explore)
	}
	if asn != 0 || choice.Reason != "retained_no_comparison" {
		result.Decision.Reason = choice.Reason
	}
	result.Decision.Incumbent, result.Decision.ComparedChoice = endpoints[0].EdgeID, choice.Edge.EdgeID
	if !choice.EvidenceAt.IsZero() {
		result.Decision.EvidenceAt = &choice.EvidenceAt
		result.Decision.EvidenceKind = "recent"
		if now.Sub(choice.EvidenceAt) > edgequality.PassiveASNWindow {
			result.Decision.EvidenceKind = "historical"
		}
		result.Decision.SharedWindows, result.Decision.LeftWinWindows, result.Decision.RightWinWindows = choice.SharedWindows, choice.LeftWinWindows, choice.RightWinWindows
		result.Decision.AggregateASNs, result.Decision.AggregateSupportingASNs = choice.AggregateASNs, choice.AggregateSupportingASNs
	}
	if mode == "active" && len(endpoints) > 1 && (asn != 0 || choice.Edge != endpoints[0]) {
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
	globalAggregate := result.Scope == 0 && result.Decision.ASN == 0 && result.Decision.Reason == "passive_aggregate_preferred"
	if result.Decision.Mode != "active" || result.Scope == 0 && !globalAggregate || len(rrs) != 1 || len(audit.Answered) != 1 {
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
