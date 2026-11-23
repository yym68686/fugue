package platformconfig

import (
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestHeldPrimaryFallbackRequiresTheSelectedIndependentOrder(t *testing.T) {
	r := physicalQueryFixture()
	r.Policy.DNSAnswerRules[0].ECSEnabled = false
	rebindPlacement(&r)
	built, e := Compile(r)
	if e != nil {
		t.Fatal(e)
	}
	v := compiledQueryViews(t, built)[0]
	src := v.Records[0]
	k := hostedQueryKey{v.NodeID, v.EdgeGroupID, v.Zone, src.Name, src.Type}
	if src.AnswerPolicy.PhysicalSelection == nil {
		t.Fatal("physical query missing")
	}
	rule := r.Policy.DNSAnswerRules[0]
	policy := r.Policy
	policy.DNSQueryPolicy = &DNSQueryPolicy{OrderedProjection: &DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}}}
	next := src
	next.Candidates = nil
	next.Values = nil
	var h HostedHoldRequirements
	h.Record, h.Family = src.Name, src.Type
	for _, n := range src.Candidates {
		tgt := DNSReadinessTarget{EdgeID: n.EdgeID, EdgeGroupID: n.EdgeGroupID, Address: n.IP, Family: src.Type}
		h.Before.Targets = append(h.Before.Targets, tgt)
		if n.EdgeID == "edge-a" {
			h.After.Targets = append(h.After.Targets, tgt)
			next.Values = append(next.Values, n.IP)
			next.Candidates = append(next.Candidates, model.EdgeDNSAnswerCandidate{IP: n.IP, EdgeID: n.EdgeID, EdgeGroupID: n.EdgeGroupID})
		}
	}
	order := &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a"}}
	next.AnswerPolicy.PhysicalOrder, next.AnswerPolicy.PhysicalSelection, next.AnswerPolicy.PolicyKind = order, nil, model.DNSAnswerPolicyKindPhysicalOrder
	next.AnswerPolicy.AllowedEdgeGroups = []string{h.After.Targets[0].EdgeGroupID}
	next.AnswerPolicy.RankingScope, next.AnswerPolicy.RankingVersion, next.AnswerPolicy.Reason = "", "", "physical_quality_learning_preserved_verified_order"
	nr := rule
	nr.SelectionMode, nr.PhysicalOrder = next.AnswerPolicy.PolicyKind, order
	d, _ := Digest([]any{src, rule})
	f := HostedQueryFallback{k.node, k.group, k.zone, d, next, nr}
	if !validHostedFallback(src, rule, policy, h, f) {
		t.Fatal("exact independent fallback refused")
	}
	b := DNSArtifactPayload{Policy: policy, QueryViews: []DNSQueryView{{NodeID: k.node, EdgeGroupID: k.group, Zone: k.zone, Records: []model.EdgeDNSRecord{src}}}}
	s := b
	s.Policy.DNSAnswerRules = []DNSAnswerRule{nr}
	s.QueryViews = []DNSQueryView{{NodeID: k.node, EdgeGroupID: k.group, Zone: k.zone, Records: []model.EdgeDNSRecord{next}}}
	edits, e := captureHostedFallbacks(HostedScopeProposal{Hold: []HostedHoldRequirements{h}}, b, s, s)
	if e != nil || len(edits) != 1 || edits[0].BaseDigest != f.BaseDigest {
		t.Fatal("did not capture candidate", e)
	}
	for _, name := range []string{"drift", "ttl", "wrong-order", "epoch", "positive", "held", "unselected", "measurement-only", "status", "canary", "duplicate", "ranking", "cooldown"} {
		t.Run(name, func(t *testing.T) {
			x := f
			p := policy
			q := *policy.DNSQueryPolicy
			p.DNSQueryPolicy = &q
			ord := *order
			x.Query.AnswerPolicy.PhysicalOrder = &ord
			x.Rule.PhysicalOrder = &ord
			switch name {
			case "drift":
				x.BaseDigest += "x"
			case "ttl":
				x.Rule.TTLSeconds++
			case "wrong-order":
				ord.OrderedEdgeIDs = []string{"foreign"}
			case "epoch":
				n := time.Now()
				ord.QualityObservedAt = &n
			case "positive":
				x.Query.Candidates = slices.Clone(x.Query.Candidates)
				x.Query.Candidates[0].Healthy = true
			case "held":
				x.Query.Values = src.Values
			case "unselected":
				q.OrderedProjection = nil
			case "measurement-only":
				q.PhysicalRoutes = []PhysicalQualityRoute{{Hostname: src.Name}}
			case "status":
				x.Query.Status = "unexpected"
			case "canary":
				x.Query.Candidates = slices.Clone(x.Query.Candidates)
				x.Query.Candidates[0].CanaryWeight = 100
			case "duplicate":
				x.Query.Values = append(slices.Clone(x.Query.Values), x.Query.Values[0])
			case "ranking":
				x.Query.AnswerPolicy.RankingVersion = "unexpected"
			case "cooldown":
				x.Query.AnswerPolicy.SwitchCooldownSec++
			}
			if validHostedFallback(src, rule, p, h, x) {
				t.Fatal("accepted independent change or manufactured evidence")
			}
		})
	}
}
