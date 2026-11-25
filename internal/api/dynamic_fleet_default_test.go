package api

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/passiveasn"
	"fugue/internal/platformconfig"
)

func fleetDefaultFixture(t *testing.T) (platformIntentProjectionResponse, platformconfig.DNSQueryPolicy, map[string]string, fleetDefaultInput, time.Time) {
	t.Helper()
	projection, nodes, policy, now := directQueryFixture()
	host := projection.Intent.DNS[0].Hostname
	order := model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{nodes[0].ID, nodes[1].ID}}
	policy.DynamicQuality = &platformconfig.DynamicQualityPolicy{FleetDefault: "active", FleetDefaultHosts: []string{host}}
	policy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: order}
	input := fleetDefaultInput{
		ready: map[string]map[fleetDefaultQuery]map[edgequality.PassiveEndpoint]time.Time{},
		ranks: map[string]fleetDefaultRankInput{},
		holds: map[edgequality.PassiveEndpoint]bool{},
	}
	rank := passiveFleetSnapshot{At: now.Add(-time.Hour)}
	var candidates []platformconfig.DNSSelectionCandidate
	for index, node := range nodes {
		endpoint := edgequality.PassiveEndpoint{EdgeID: node.ID, GroupID: node.EdgeGroupID, Address: node.PublicIPv4}
		rank.Ranks = append(rank.Ranks, passiveasn.FleetRank{Edge: endpoint, Family: 4, Tier: len(nodes) - index, EvidenceAt: rank.At})
		candidates = append(candidates, platformconfig.DNSSelectionCandidate{EdgeID: node.ID, EdgeGroupID: node.EdgeGroupID, IP: node.PublicIPv4})
	}
	input.ranks["A"] = fleetDefaultRankInput{historical: rank}
	for _, authority := range []string{"dns-a", "dns-b"} {
		projection.Policy.DNSAnswerRules = append(projection.Policy.DNSAnswerRules, platformconfig.DNSAnswerRule{
			NodeID: authority, Hostname: host, Type: "A", TTLSeconds: 60, SelectionMode: model.DNSAnswerPolicyKindPhysicalOrder, PhysicalOrder: &order,
		})
		projection.RuntimeSnapshot.DNSSelections = append(projection.RuntimeSnapshot.DNSSelections, platformconfig.DNSSelectionObservation{
			NodeID: authority, Hostname: host, Type: "A", Candidates: slices.Clone(candidates), ObservedAt: now, Reason: "bootstrap",
		})
		ready := map[edgequality.PassiveEndpoint]time.Time{}
		for _, row := range rank.Ranks {
			ready[row.Edge] = now.Add(time.Minute)
		}
		input.ready[authority] = map[fleetDefaultQuery]map[edgequality.PassiveEndpoint]time.Time{{host, "A"}: ready}
	}
	return projection, policy, map[string]string{host: "learning_evidence_unavailable"}, input, now
}

func TestFleetDefaultProjectionRespectsSignedCandidatesAndEvidence(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		wantChallenger, wantShadow bool
	}{
		{"active", true, false},
		{"shadow", false, true},
		{"unproven authority", false, false},
		{"expired challenger", false, false},
		{"excluded challenger", false, false},
		{"endpoint identity changed", false, false},
		{"recovery hold", false, false},
		{"other rollout hostname", false, false},
		{"fixed override", false, false},
		{"domain evidence", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, policy, states, input, now := fleetDefaultFixture(t)
			initial := p.Policy.DNSAnswerRules[0]
			query := fleetDefaultQuery{initial.Hostname, initial.Type}
			challenger := input.ranks["A"].historical.Ranks[1].Edge
			switch tc.name {
			case "shadow":
				policy.DynamicQuality.FleetDefault = ""
			case "unproven authority":
				delete(input.ready, "dns-b")
			case "expired challenger":
				input.ready["dns-b"][query][challenger] = now
			case "excluded challenger":
				p.RuntimeSnapshot.DNSSelections[0].Candidates = p.RuntimeSnapshot.DNSSelections[0].Candidates[:1]
			case "endpoint identity changed":
				p.RuntimeSnapshot.DNSSelections[0].Candidates[1].EdgeGroupID = "other-group"
			case "recovery hold":
				input.holds[challenger] = true
			case "other rollout hostname":
				policy.DynamicQuality.FleetDefaultHosts = []string{"other.example.test"}
			case "fixed override":
				policy.OrderedProjection.Overrides = []platformconfig.DNSOrderOverride{{
					NodeID: initial.NodeID, Hostname: initial.Hostname, Type: initial.Type, Order: *initial.PhysicalOrder,
				}}
			case "domain evidence":
				states[initial.Hostname] = "measured"
			}
			if err := applyFleetDefaults(&p, policy, states, input, now); err != nil {
				t.Fatal(err)
			}
			for _, rule := range p.Policy.DNSAnswerRules {
				got := rule.PhysicalOrder.OrderedEdgeIDs
				want := initial.PhysicalOrder.OrderedEdgeIDs
				if tc.wantChallenger {
					want = []string{challenger.EdgeID, want[0]}
				}
				if !slices.Equal(got, want) || rule.TTLSeconds != initial.TTLSeconds {
					t.Fatalf("%s: got order %v, want %v", rule.NodeID, got, want)
				}
			}
			if _, ok := p.RuntimeSnapshot.Facts["fleet_default_shadow"]; ok != tc.wantShadow {
				t.Fatal("unexpected shadow recommendation", p.RuntimeSnapshot.Facts)
			}
			if tc.wantChallenger {
				for _, fact := range p.RuntimeSnapshot.DNSSelections {
					if fact.Reason != model.DNSFleetDefaultHistorical || fact.RankingVersion != model.DNSFleetDefaultRankingVersion ||
						fleetDefaultEvidenceAt(fact.ShadowReason).IsZero() || fact.SourceDigest == "" {
						t.Fatal("missing reproducible fleet provenance", fact)
					}
				}
				if err := platformconfig.ValidateDNSAnswerRules(p.Policy.DNSAnswerRules); err != nil {
					t.Fatal(err)
				}
			}
			if tc.wantShadow && (!reflect.DeepEqual(initial, p.Policy.DNSAnswerRules[0]) || p.RuntimeSnapshot.DNSSelections[0].Reason != "bootstrap") {
				t.Fatal("shadow changed serving inputs")
			}
		})
	}
}

func TestGlobalFleetDefaultUsesSharedRankButRequiresEachHostsReadiness(t *testing.T) {
	p, policy, states, input, now := fleetDefaultFixture(t)
	policy.DynamicQuality.FleetDefaultHosts = nil
	first := p.Policy.DNSAnswerRules[0].Hostname
	second, limited := "other.example.test", "limited.example.test"
	winner := input.ranks["A"].historical.Ranks[1].Edge
	initial := p.Policy.DNSAnswerRules[0].PhysicalOrder.OrderedEdgeIDs[0]
	for _, host := range []string{second, limited} {
		states[host] = states[first]
		for _, rule := range slices.Clone(p.Policy.DNSAnswerRules) {
			if rule.Hostname == first {
				rule.Hostname = host
				p.Policy.DNSAnswerRules = append(p.Policy.DNSAnswerRules, rule)
			}
		}
		for _, fact := range slices.Clone(p.RuntimeSnapshot.DNSSelections) {
			if fact.Hostname == first {
				fact.Hostname = host
				p.RuntimeSnapshot.DNSSelections = append(p.RuntimeSnapshot.DNSSelections, fact)
			}
		}
		for node, byQuery := range input.ready {
			ready := map[edgequality.PassiveEndpoint]time.Time{}
			for edge, until := range byQuery[fleetDefaultQuery{first, "A"}] {
				if host != limited || node != "dns-b" || edge != winner {
					ready[edge] = until
				}
			}
			byQuery[fleetDefaultQuery{host, "A"}] = ready
		}
	}
	if err := applyFleetDefaults(&p, policy, states, input, now); err != nil {
		t.Fatal(err)
	}
	for _, rule := range p.Policy.DNSAnswerRules {
		want := winner.EdgeID
		if rule.Hostname == limited {
			want = initial
		}
		if got := rule.PhysicalOrder.OrderedEdgeIDs[0]; got != want {
			t.Errorf("%s on %s: got %s, want %s", rule.Hostname, rule.NodeID, got, want)
		}
	}
	for _, fact := range p.RuntimeSnapshot.DNSSelections {
		if fact.RankingVersion != model.DNSFleetDefaultRankingVersion ||
			!fleetDefaultEvidenceAt(fact.ShadowReason).Equal(input.ranks["A"].historical.At) {
			t.Errorf("%s on %s did not use shared platform evidence", fact.Hostname, fact.NodeID)
		}
	}
}

func TestFleetDefaultNeedsTwoIndependentLiveWindowsAndKeepsTies(t *testing.T) {
	p, _, _, input, now := fleetDefaultFixture(t)
	query := fleetDefaultQuery{p.Policy.DNSAnswerRules[0].Hostname, "A"}
	order := p.Policy.DNSAnswerRules[0].PhysicalOrder.OrderedEdgeIDs
	ranks := input.ranks["A"]
	ranks.recent = ranks.historical
	ranks.recent.At = now.Add(-time.Minute)
	ranks.previous = ranks.recent
	for _, tc := range []struct {
		name    string
		spacing time.Duration
		tie     bool
		want    string
	}{
		{"overlap", time.Minute, false, ""},
		{"disjoint", edgequality.PassiveASNWindow, false, order[1]},
		{"tie retains incumbent", edgequality.PassiveASNWindow, true, order[0]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			signed := ranks
			signed.previous.At = signed.recent.At.Add(-tc.spacing)
			if tc.tie {
				signed.recent.Ranks = slices.Clone(signed.recent.Ranks)
				signed.recent.Ranks[0].Tier = signed.recent.Ranks[1].Tier
			}
			input.ranks["A"] = signed
			got := input.choice(query, order[0], order, p.RuntimeSnapshot.DNSSelections, now)
			if got.edge != tc.want || got.edge != "" && got.reason != model.DNSFleetDefaultLive {
				t.Fatalf("got %+v, want %q", got, tc.want)
			}
			query6 := fleetDefaultQuery{query.host, "AAAA"}
			if got := input.choice(query6, order[0], order, p.RuntimeSnapshot.DNSSelections, now); got.edge != "" {
				t.Fatal("IPv4 evidence leaked to IPv6", got)
			}
		})
	}
}
