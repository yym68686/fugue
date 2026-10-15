package dnsserver

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

func narrowedPhysicalTransitionFixture(t *testing.T, reverse bool) (*dnsServingState, *dnsReleaseBridge, []dnsReadinessFact, time.Time) {
	t.Helper()
	base, now := decisionTestState(t)
	_, _, _, facts, _ := queryExecutionFixture()
	epoch := now.Add(-10 * time.Minute)
	makePolicy := func(kind string, order []string) model.DNSAnswerPolicy {
		p := model.DNSAnswerPolicy{PolicyKind: kind, HealthRequired: true, RouteReadyRequired: true}
		if kind == model.DNSAnswerPolicyKindPhysicalOrder {
			p.PhysicalOrder = &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: slices.Clone(order), PrimarySince: &epoch}
		} else {
			p.PhysicalSelection = model.CloneDNSPhysicalSelection(physicalSelectionTestRecord(now).AnswerPolicy.PhysicalSelection)
			p.PhysicalSelection.PrimaryEdgeID = order[0]
			p.PhysicalSelection.OrderedEdgeIDs = slices.Clone(order)
			p.PhysicalSelection.PrimarySince = &epoch
			p.RankingVersion, p.RankingScope = model.DNSPhysicalSelectionVersion, "global"
		}
		return p
	}
	before, after := base.payload, dnsServingPayload{}
	if err := json.Unmarshal(mustDNSJSON(t, before), &after); err != nil {
		t.Fatal(err)
	}
	oldKind, newKind := model.DNSAnswerPolicyKindPhysicalOrder, model.DNSAnswerPolicyKindPhysicalQuality
	if reverse {
		oldKind, newKind = newKind, oldKind
	}
	before.Queries[0].Records[0].AnswerPolicy = makePolicy(oldKind, []string{"edge-a", "edge-b"})
	after.Queries[0].Records[0].AnswerPolicy = makePolicy(newKind, []string{"edge-a"})
	for _, payload := range []*dnsServingPayload{&before, &after} {
		record := &payload.Queries[0].Records[0]
		record.AppID, record.TenantID = "application", "tenant"
		payload.Policy.DNSAnswerRules = []platformconfig.DNSAnswerRule{{NodeID: payload.Queries[0].NodeID, Hostname: record.Name, Type: record.Type,
			SelectionMode: record.AnswerPolicy.PolicyKind, PhysicalOrder: model.CloneDNSPhysicalOrder(record.AnswerPolicy.PhysicalOrder), TTLSeconds: record.TTL}}
	}
	checkpoint := base.record
	checkpoint.AppliedAt = now.Add(-time.Minute)
	checkpoint.Candidate.Release = model.PlatformArtifactRelease{ID: "previous", ReleasedAt: now.Add(-time.Minute)}
	old, err := buildDNSServingState(checkpoint, before, "route", "dns-a", "edge-group-a", nil, now)
	if err != nil {
		t.Fatal(err)
	}
	next := checkpoint.Candidate
	next.Release = model.PlatformArtifactRelease{ID: "successor", ReleasedAt: now}
	next.Artifact.ID, next.Artifact.ContentHash = "artifact-next", "sha256:next"
	return old, &dnsReleaseBridge{payload: after, candidate: next}, facts, now
}

func TestDNSRecordTransitionUsesNarrowerPhysicalSelectorWithExactSuccessorFacts(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "order_to_quality", true: "quality_to_order"}[reverse], func(t *testing.T) {
			old, bridge, facts, now := narrowedPhysicalTransitionFixture(t, reverse)
			oldRaw, newRaw := mustDNSJSON(t, old.payload), mustDNSJSON(t, bridge.payload)
			host := old.payload.Plan.Records[0].Hostname
			allowed := dnsTransitionRecords(old, bridge)
			if !allowed[host] {
				t.Fatal("same-primary selector restriction prevented forward recovery")
			}
			for _, permitted := range compatibleDNSReleaseProbes(old, bridge) {
				if permitted {
					t.Fatal("narrower selector allowed rebinding proofs into the broader old policy")
				}
			}
			makeNext := func(nextFacts []dnsReadinessFact) *dnsServingState {
				t.Helper()
				state, err := buildDNSServingState(dnsServingCheckpoint{AppliedAt: old.record.AppliedAt, Candidate: bridge.candidate}, bridge.payload, "route", "dns-a", "edge-group-a", nextFacts, now)
				if err != nil {
					t.Fatal(err)
				}
				return state
			}
			old.transition = &dnsRecordTransition{state: makeNext(facts), hosts: allowed}
			query := new(dns.Msg)
			query.SetQuestion(dns.Fqdn(host), dns.TypeA)
			answer := old.answer(query, "", now)
			if answer.Rcode != dns.RcodeSuccess || len(answer.Answer) != 1 || answer.Answer[0].(*dns.A).A.String() != "8.8.8.8" || answer.Answer[0].Header().Ttl > 30 {
				t.Fatal("complete successor did not serve its original narrowed selector", answer)
			}
			receipt := captureDecision(t, old, query, "", now)
			replay, err := ReplayDNSDecision(receipt)
			if err != nil || !replay.Matched || receipt.RCode != dns.RcodeSuccess || receipt.AnswerPublication == nil || receipt.AnswerPublication.ArtifactID != "artifact-next" {
				t.Fatal("forward answer lost its exact publication or replay", replay, err)
			}
			if got := old.answer(query, "", now.Add(time.Minute)); got.Rcode != dns.RcodeServerFailure {
				t.Fatal("transition extended expired proofs", got)
			}
			partial := append([]dnsReadinessFact(nil), facts...)
			for i := range partial {
				if partial[i].Proof.EdgeID == "edge-b" {
					partial[i].Ready = false
				}
			}
			old.transition.state = makeNext(partial)
			if got := old.answer(query, "", now); got.Rcode != dns.RcodeServerFailure {
				t.Fatal("one selected endpoint weakened the original two-edge quorum", got)
			}
			if !reflect.DeepEqual(oldRaw, mustDNSJSON(t, old.payload)) || !reflect.DeepEqual(newRaw, mustDNSJSON(t, bridge.payload)) || !old.record.Positive {
				t.Fatal("comparison or query modified a signed policy or positive checkpoint")
			}
		})
	}
}

func TestDNSNarrowedPhysicalTransitionRetainsAuthorizationBoundaries(t *testing.T) {
	for _, scenario := range []string{"reordered", "widened", "primary changed", "epoch changed", "business", "scope", "version", "endpoint", "tenant", "quorum", "rule mismatch", "duplicate rule", "missing rule", "older publication"} {
		t.Run(scenario, func(t *testing.T) {
			old, bridge, _, now := narrowedPhysicalTransitionFixture(t, false)
			record := &bridge.payload.Queries[0].Records[0]
			selection := record.AnswerPolicy.PhysicalSelection
			switch scenario {
			case "reordered":
				selection.PrimaryEdgeID, selection.OrderedEdgeIDs = "edge-b", []string{"edge-b", "edge-a"}
			case "widened":
				selection.OrderedEdgeIDs = []string{"edge-a", "edge-b", "edge-other"}
			case "primary changed":
				selection.PrimaryEdgeID, selection.OrderedEdgeIDs = "edge-b", []string{"edge-b"}
			case "epoch changed":
				selection.PrimarySince = &now
			case "business":
				selection.BusinessAllocation = &model.DNSBusinessAllocation{}
			case "scope":
				selection.Scope = "region:other"
			case "version":
				selection.Version = "future"
			case "endpoint":
				record.Candidates[0].IP = "192.0.2.99"
			case "tenant":
				record.TenantID = "foreign"
			case "quorum":
				bridge.payload.Plan.Records[0].MinimumHealthyEdges--
			case "rule mismatch":
				old.payload.Policy.DNSAnswerRules[0].PhysicalOrder.OrderedEdgeIDs = []string{"edge-b", "edge-a"}
			case "duplicate rule":
				old.payload.Policy.DNSAnswerRules = append(old.payload.Policy.DNSAnswerRules, old.payload.Policy.DNSAnswerRules[0])
			case "missing rule":
				bridge.payload.Policy.DNSAnswerRules = nil
			case "older publication":
				bridge.candidate.Release.ReleasedAt = old.record.Candidate.Release.ReleasedAt
			}
			if dnsTransitionRecords(old, bridge)[old.payload.Plan.Records[0].Hostname] {
				t.Fatal("narrowing bypassed an independent authorization boundary")
			}
		})
	}
}

func TestDNSNarrowedTransitionDoesNotReviveRemovedFallback(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		old, bridge, facts, now := narrowedPhysicalTransitionFixture(t, reverse)
		old.payload.Plan.Records[0].MinimumHealthyEdges = 1
		bridge.payload.Plan.Records[0].MinimumHealthyEdges = 1
		for index := range facts {
			if facts[index].Proof.EdgeID == "edge-a" {
				facts[index].Ready = false
			}
		}
		view := bridge.payload.Queries[0]
		records, err := materializeDNSQueries(view, bridge.payload.Plan, bridge.payload.Policy.DNSReadiness, facts, now)
		if err != nil || len(records[0].Values) == 0 {
			t.Fatal("fixture must have a ready quorum before selector exclusion", err)
		}
		next, err := buildDNSServingState(dnsServingCheckpoint{AppliedAt: old.record.AppliedAt, Candidate: bridge.candidate}, bridge.payload, "route", view.NodeID, view.EdgeGroupID, facts, now)
		if err != nil {
			t.Fatal(err)
		}
		hosts := dnsTransitionRecords(old, bridge)
		if !hosts[old.payload.Plan.Records[0].Hostname] {
			t.Fatal("fixture did not reach the forward transition")
		}
		old.transition = &dnsRecordTransition{state: next, hosts: hosts}
		query := new(dns.Msg)
		query.SetQuestion(dns.Fqdn(old.payload.Plan.Records[0].Hostname), dns.TypeA)
		if answer := old.answer(query, "", now); answer.Rcode != dns.RcodeServerFailure || len(answer.Answer) != 0 {
			t.Fatal("removed fallback was revived despite the successor selector", answer)
		}
	}
}
