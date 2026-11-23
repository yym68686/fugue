package platformconfig

import (
	"errors"
	"reflect"
	"slices"

	"fugue/internal/model"
)

// This edit carries a negative-admission fallback, not newly measured
// quality. Its predecessor is the entire signed query and rule; its successor
// may not carry forward historical scores, a positive fact or a new epoch.
type HostedQueryFallback struct {
	Node       string              `json:"node"`
	Group      string              `json:"group"`
	Zone       string              `json:"zone"`
	BaseDigest string              `json:"base_digest"`
	Query      model.EdgeDNSRecord `json:"query"`
	Rule       DNSAnswerRule       `json:"rule"`
}
type hostedQueryKey struct{ node, group, zone, name, family string }

func (f HostedQueryFallback) key() hostedQueryKey {
	return hostedQueryKey{f.Node, f.Group, f.Zone, f.Query.Name, f.Query.Type}
}

func declaredHostedOrder(p *DNSQueryPolicy, k hostedQueryKey) ([]string, bool) {
	if p == nil {
		return nil, false
	}
	if a := DNSAvailabilityForHost(p, k.name); a != nil {
		return a.OrderedEdgeIDs, true
	}
	// A named measurement-only route cannot borrow the generic bootstrap
	// order. Its own signed fallback would have been found above.
	if p.OrderedProjection == nil || slices.ContainsFunc(p.PhysicalRoutes, func(r PhysicalQualityRoute) bool { return r.Hostname == k.name }) {
		return nil, false
	}
	order := p.OrderedProjection.DefaultOrder
	for _, v := range p.OrderedProjection.Overrides {
		if v.NodeID == k.node && v.Hostname == k.name && v.Type == k.family {
			order = v.Order
		}
	}
	return order.OrderedEdgeIDs, model.ValidateDNSPhysicalOrder(&order) == nil
}

var errHostedFallback = errors.New("held selection does not have a matching independent deterministic fallback")

func validHostedFallback(before model.EdgeDNSRecord, rule DNSAnswerRule, policy PolicySnapshot, h HostedHoldRequirements, f HostedQueryFallback) bool {
	after := f.Query
	prior, now := before.AnswerPolicy, after.AnswerPolicy
	if before.Type != h.Family || before.Name != h.Record || before.Type != after.Type || before.Name != after.Name || before.AppID != after.AppID || before.TenantID != after.TenantID || before.RecordKind != after.RecordKind || before.TTL != after.TTL ||
		prior.PolicyKind != model.DNSAnswerPolicyKindPhysicalQuality || prior.PhysicalSelection == nil || prior.PhysicalSelection.BusinessAllocation != nil ||
		now.PolicyKind != model.DNSAnswerPolicyKindPhysicalOrder || now.PhysicalSelection != nil || model.ValidateDNSPhysicalOrder(now.PhysicalOrder) != nil ||
		now.PhysicalOrder.PrimarySince != nil || now.PhysicalOrder.QualityObservedAt != nil || !reflect.DeepEqual(now.Availability, prior.Availability) ||
		!now.HealthRequired || !now.RouteReadyRequired || len(after.ScopedCandidates) != 0 || len(after.ValueExpirations) != 0 ||
		rule.SelectionMode != prior.PolicyKind || f.Rule.SelectionMode != now.PolicyKind || !reflect.DeepEqual(f.Rule.PhysicalOrder, now.PhysicalOrder) {
		return false
	}
	orig := f.Rule
	orig.PhysicalOrder, orig.SelectionMode = rule.PhysicalOrder, rule.SelectionMode
	if !reflect.DeepEqual(rule, orig) || rule.NodeID != f.Node || rule.Hostname != h.Record || rule.Type != h.Family {
		return false
	}
	// Preserve status, ownership, wire metadata and any future fields we
	// do not explicitly own. The fallback clears measurement, not constraints.
	wire := before
	wire.Values, wire.AnswerPolicy, wire.Candidates = after.Values, after.AnswerPolicy, after.Candidates
	if !reflect.DeepEqual(wire, after) || validateDNSPhysicalQuery(after) != nil {
		return false
	}
	endpoints := map[string]DNSReadinessTarget{}
	eligible := map[string]bool{}
	var groups []string
	for _, n := range h.After.Targets {
		if _, duplicate := endpoints[n.Address]; duplicate || n.Family != h.Family {
			return false
		}
		endpoints[n.Address] = n
		eligible[n.EdgeID] = true
		groups = append(groups, n.EdgeGroupID)
	}
	if eligible[prior.PhysicalSelection.PrimaryEdgeID] || !slices.ContainsFunc(h.Before.Targets, func(n DNSReadinessTarget) bool {
		return n.EdgeID == prior.PhysicalSelection.PrimaryEdgeID && n.Family == h.Family
	}) {
		return false
	}
	expected := prior
	expected.PhysicalOrder, expected.PhysicalSelection, expected.PolicyKind = now.PhysicalOrder, nil, model.DNSAnswerPolicyKindPhysicalOrder
	expected.RankingScope, expected.RankingVersion = "", ""
	expected.AllowedEdgeGroups = uniqueSorted(groups)
	expected.Reason = "physical_quality_learning_preserved_verified_order"
	if DNSAvailabilityForHost(policy.DNSQueryPolicy, h.Record) != nil {
		expected.Reason = "availability_only_quality_unavailable"
	}
	if !reflect.DeepEqual(expected, now) {
		return false
	}
	// A different rank/signal is not silently recast as measured traffic.
	// Only use the order originally declared for this (consumer, host, family).
	declared, ok := declaredHostedOrder(policy.DNSQueryPolicy, f.key())
	if !ok {
		return false
	}
	allowed := []string{}
	for _, id := range declared {
		if eligible[id] {
			allowed = append(allowed, id)
		}
	}
	if !slices.Equal(allowed, now.PhysicalOrder.OrderedEdgeIDs) {
		return false
	}
	seen := map[string]bool{}
	for _, c := range after.Candidates {
		n, ok := endpoints[c.IP]
		if !ok || seen[c.IP] || n.EdgeID != c.EdgeID || n.EdgeGroupID != c.EdgeGroupID || !reflect.DeepEqual(c, model.EdgeDNSAnswerCandidate{IP: c.IP, EdgeID: c.EdgeID, EdgeGroupID: c.EdgeGroupID}) {
			return false
		}
		seen[c.IP] = true
	}
	for _, ip := range after.Values {
		if !seen[ip] {
			return false
		}
		delete(seen, ip)
	}
	if len(after.Candidates) == 0 || len(after.Candidates) != len(endpoints) || len(seen) != 0 || !slices.IsSorted(after.Values) {
		return false
	}
	d, e := Digest([]any{before, rule})
	return e == nil && d == f.BaseDigest
}

func captureHostedFallbacks(p HostedScopeProposal, before, stage, desired DNSArtifactPayload) ([]HostedQueryFallback, error) {
	keys := map[hostedRecordKey]HostedHoldRequirements{}
	for _, v := range p.Hold {
		keys[hostedRecordKey{v.Record, v.Family}] = v
	}
	type pair struct {
		q model.EdgeDNSRecord
		r DNSAnswerRule
	}
	index := func(x DNSArtifactPayload) map[hostedQueryKey]pair {
		out := map[hostedQueryKey]pair{}
		rules := map[string]DNSAnswerRule{}
		for _, r := range x.Policy.DNSAnswerRules {
			rules[dnsQueryKey(r.NodeID, r.Hostname, r.Type)] = r
		}
		for _, v := range x.QueryViews {
			for _, q := range v.Records {
				if _, ok := keys[hostedRecordKey{q.Name, q.Type}]; ok {
					out[hostedQueryKey{v.NodeID, v.EdgeGroupID, v.Zone, q.Name, q.Type}] = pair{q, rules[dnsQueryKey(v.NodeID, q.Name, q.Type)]}
				}
			}
		}
		return out
	}
	from, to, future := index(before), index(stage), index(desired)
	var edits []HostedQueryFallback
	// Iterate the authenticated view order. Never range over a map while
	// constructing content-addressed input.
	for _, v := range before.QueryViews {
		for _, q := range v.Records {
			k := hostedQueryKey{v.NodeID, v.EdgeGroupID, v.Zone, q.Name, q.Type}
			a, ok := from[k]
			if !ok {
				continue
			}
			b, ok := to[k]
			if !ok {
				return nil, errHostedFallback
			}
			if a.q.AnswerPolicy.PolicyKind != model.DNSAnswerPolicyKindPhysicalQuality || b.q.AnswerPolicy.PolicyKind != model.DNSAnswerPolicyKindPhysicalOrder {
				continue
			}
			oldOrder, ok := declaredHostedOrder(before.Policy.DNSQueryPolicy, k)
			sourceOrder, ok2 := declaredHostedOrder(stage.Policy.DNSQueryPolicy, k)
			if !ok || !ok2 || !slices.Equal(oldOrder, sourceOrder) || !reflect.DeepEqual(b, future[k]) {
				return nil, errHostedFallback
			}
			d, e := Digest([]any{a.q, a.r})
			if e != nil {
				return nil, e
			}
			f := HostedQueryFallback{v.NodeID, v.EdgeGroupID, v.Zone, d, b.q, b.r}
			if !validHostedFallback(a.q, a.r, before.Policy, keys[hostedRecordKey{q.Name, q.Type}], f) {
				return nil, errHostedFallback
			}
			edits = append(edits, f)
		}
	}
	return edits, nil
}
