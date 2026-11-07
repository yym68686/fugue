package dnsserver

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

func TestDNSAddressRecoveryIsIndependentOfSameNameMailAndText(t *testing.T) {
	for _, kind := range []string{"MX", "TXT"} {
		t.Run(kind, func(t *testing.T) {
			old, bridge, facts, now := narrowedPhysicalTransitionFixture(t, false)
			host := old.payload.Plan.Records[0].Hostname
			selection := bridge.payload.Queries[0].Records[0].AnswerPolicy.PhysicalSelection
			selection.OrderedEdgeIDs = []string{"edge-a", "edge-b"}
			intent := platformconfig.DNSIntent{Hostname: host, Type: "A", TTL: 60, AppID: "application", TenantID: "tenant"}
			old.payload.Records, bridge.payload.Records = []platformconfig.DNSIntent{intent}, []platformconfig.DNSIntent{intent}
			bridge.payload.Records = append(bridge.payload.Records, platformconfig.DNSIntent{Hostname: host, Type: kind, TTL: 60, Values: []string{"10 mail.example.test"}})
			bridge.payload.Queries[0].Records = append(bridge.payload.Queries[0].Records, model.EdgeDNSRecord{Name: host, Type: kind, TTL: 60, Values: []string{"10 mail.example.test"}})
			before, after := mustDNSJSON(t, old.payload), mustDNSJSON(t, bridge.payload)
			for _, probe := range old.payload.Plan.Probes {
				if !compatibleDNSReleaseProbes(old, bridge)[probe.ID] {
					t.Fatal("independent RRset vetoed identical address proof")
				}
			}
			assertDNSForwardOrderAnswer(t, old, bridge, facts, now, "8.8.8.8")
			if !reflect.DeepEqual(before, mustDNSJSON(t, old.payload)) || !reflect.DeepEqual(after, mustDNSJSON(t, bridge.payload)) {
				t.Fatal("comparison mutated signed inputs")
			}
		})
	}
}

func TestDNSForwardPermutationExecutesSuccessorWithoutRebindingOrBorrowingRecovery(t *testing.T) {
	old, bridge, facts, now := narrowedPhysicalTransitionFixture(t, false)
	selection := bridge.payload.Queries[0].Records[0].AnswerPolicy.PhysicalSelection
	selection.PrimaryEdgeID, selection.OrderedEdgeIDs, selection.PrimarySince = "edge-b", []string{"edge-b", "edge-a"}, &now
	before, after := mustDNSJSON(t, old.payload), mustDNSJSON(t, bridge.payload)
	if len(compatibleDNSReleaseProbes(old, bridge)) != 0 {
		t.Fatal("new selector rebound proofs into old checkpoint")
	}
	assertDNSForwardOrderAnswer(t, old, bridge, facts, now, "9.9.9.9")
	if !reflect.DeepEqual(before, mustDNSJSON(t, old.payload)) || !reflect.DeepEqual(after, mustDNSJSON(t, bridge.payload)) {
		t.Fatal("forward comparison changed signed policies")
	}
	// A valid new selector cannot bypass the signed recovery count/span gate.
	var next dnsServingPayload
	if err := json.Unmarshal(after, &next); err != nil {
		t.Fatal(err)
	}
	next.Queries[0].Records[0].AnswerPolicy.Availability = &model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
	state, err := buildDNSServingState(dnsServingCheckpoint{AppliedAt: now, Candidate: bridge.candidate}, next, "route", "dns-a", "edge-group-a", facts, now)
	if err != nil {
		t.Fatal(err)
	}
	old.transition.state = state
	query := new(dns.Msg)
	query.SetQuestion(dns.Fqdn(old.payload.Plan.Records[0].Hostname), dns.TypeA)
	if answer := old.answer(query, "", now); answer.Rcode != dns.RcodeServerFailure {
		t.Fatal("permutation bypassed recovery history", answer)
	}
}

func assertDNSForwardOrderAnswer(t *testing.T, old *dnsServingState, bridge *dnsReleaseBridge, facts []dnsReadinessFact, now time.Time, expected string) {
	t.Helper()
	host := old.payload.Plan.Records[0].Hostname
	hosts := dnsTransitionRecords(old, bridge)
	if !hosts[host] {
		t.Fatal("signed successor blocked despite unchanged address authority")
	}
	build := func(observations []dnsReadinessFact) *dnsServingState {
		t.Helper()
		state, err := buildDNSServingState(dnsServingCheckpoint{AppliedAt: old.record.AppliedAt, Candidate: bridge.candidate}, bridge.payload, "route", "dns-a", "edge-group-a", observations, now)
		if err != nil {
			t.Fatal(err)
		}
		return state
	}
	old.transition = &dnsRecordTransition{state: build(facts), hosts: hosts}
	query := new(dns.Msg)
	query.SetQuestion(dns.Fqdn(host), dns.TypeA)
	answer := old.answer(query, "", now)
	if answer.Rcode != dns.RcodeSuccess || len(answer.Answer) != 1 || answer.Answer[0].(*dns.A).A.String() != expected {
		t.Fatal("wrong successor answer", answer)
	}
	receipt := captureDecision(t, old, query, "", now)
	if replay, err := ReplayDNSDecision(receipt); err != nil || !replay.Matched || receipt.AnswerPublication == nil || receipt.AnswerPublication.ArtifactID != bridge.candidate.Artifact.ID {
		t.Fatal("lost exact answer publication/replay", replay, err)
	}
	if answer := old.answer(query, "", now.Add(time.Minute)); answer.Rcode != dns.RcodeServerFailure {
		t.Fatal("extended proof expiry", answer)
	}
	old.transition.state = build(facts[:1])
	if answer := old.answer(query, "", now); answer.Rcode != dns.RcodeServerFailure {
		t.Fatal("lowered original quorum", answer)
	}
}
