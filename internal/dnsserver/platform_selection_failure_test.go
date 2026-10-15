package dnsserver

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

// The proof quorum can be satisfied by an edge that the signed allocation
// excludes. Such a record exists, but has no currently authorized answer.
func businessUnavailableServingState(t *testing.T) (*dnsServingState, time.Time) {
	t.Helper()
	state, now := decisionTestState(t)
	zone := state.zones["example.test"]
	entries := zone.records["target.example.test"]
	entry := &entries[0]
	entry.record.AnswerPolicy = businessSelectionRecord(t, now).AnswerPolicy
	allocation := entry.record.AnswerPolicy.PhysicalSelection.BusinessAllocation
	allocation.State.BasisPoints = map[string]int{"edge-b": 10000}
	allocation.State.Stopped = map[string]string{"edge-a": "observed_transport_stop_threshold"}
	entry.plan.Records[0].MinimumHealthyEdges = 1
	for i := range entry.facts {
		if entry.facts[i].Proof.EdgeID == "edge-b" {
			entry.facts[i].Ready = false
			entry.facts[i].Reason = "probe_unavailable"
		}
	}
	zone.records["target.example.test"] = entries
	state.zones["example.test"] = zone
	materialized, err := materializeDNSQueries(platformconfig.DNSQueryView{Records: []model.EdgeDNSRecord{entry.record}}, &entry.plan, state.payload.Policy.DNSReadiness, entry.facts, now)
	if err != nil || len(materialized) != 1 || len(materialized[0].Candidates) != 1 || materialized[0].Candidates[0].EdgeID != "edge-a" {
		t.Fatalf("fixture must satisfy route quorum through the excluded edge: %+v %v", materialized, err)
	}
	return state, now
}

func TestPlatformDNSUnavailableSelectionIsFailureNotNoData(t *testing.T) {
	state, now := businessUnavailableServingState(t)
	query := new(dns.Msg)
	query.SetQuestion("target.example.test.", dns.TypeA)
	before, _ := json.Marshal(state.zones["example.test"].records["target.example.test"][0].record)
	got := state.answer(query, "", now)
	if got.Rcode != dns.RcodeServerFailure || len(got.Answer) != 0 || len(got.Ns) != 0 {
		t.Fatalf("unavailable authorized candidates must not produce cacheable NODATA: %s", got)
	}
	after, _ := json.Marshal(state.zones["example.test"].records["target.example.test"][0].record)
	if !bytes.Equal(before, after) {
		t.Fatal("answer mutated signed policy or candidates")
	}
	receipt := captureDecision(t, state, query, "198.51.100.1:53", now)
	if receipt.RCode != dns.RcodeServerFailure || len(receipt.Records) != 1 || receipt.Records[0].SelectionResult != "business_no_ready_endpoint" {
		t.Fatalf("failed selection evidence lost: %+v", receipt)
	}
	if result, err := ReplayDNSDecision(receipt); err != nil || !result.Matched {
		t.Fatalf("failure must replay exactly: %+v %v", result, err)
	}
}

func TestPlatformDNSUnavailableSelectionUsesOnlyAuthorizedTransition(t *testing.T) {
	state, now := businessUnavailableServingState(t)
	next, _ := decisionTestState(t)
	next.record.Candidate.Artifact.ContentHash = "sha256:successor"
	state.transition = &dnsRecordTransition{state: next, hosts: map[string]bool{"target.example.test": true}}
	query := new(dns.Msg)
	query.SetQuestion("target.example.test.", dns.TypeA)
	receipt := captureDecision(t, state, query, "198.51.100.1:53", now)
	if receipt.RCode != dns.RcodeSuccess || len(receipt.RRSet) != 1 || len(receipt.Records) != 2 || receipt.AnswerPublication == nil || receipt.AnswerPublication.Digest != "sha256:successor" {
		t.Fatalf("verified record transition must remain available: %+v", receipt)
	}
	if result, err := ReplayDNSDecision(receipt); err != nil || !result.Matched {
		t.Fatalf("transition must replay exactly: %+v %v", result, err)
	}
	state.transition.hosts["target.example.test"] = false
	if got := state.answer(query, "", now); got.Rcode != dns.RcodeServerFailure || len(got.Answer) != 0 {
		t.Fatal("unapproved successor was used", got)
	}
}

func TestPlatformDNSSelectionFailurePreservesActualNoData(t *testing.T) {
	state, now := businessUnavailableServingState(t)
	for _, qtype := range []uint16{dns.TypeAAAA, dns.TypeTXT, dns.TypeDNSKEY} {
		query := new(dns.Msg)
		query.SetQuestion("target.example.test.", qtype)
		got := state.answer(query, "", now)
		if got.Rcode != dns.RcodeSuccess || len(got.Answer) != 0 || len(got.Ns) != 1 {
			t.Fatalf("absent type must remain NODATA: %s", got)
		}
	}
	query := new(dns.Msg)
	query.SetQuestion("absent.example.test.", dns.TypeA)
	if got := state.answer(query, "", now); got.Rcode != dns.RcodeNameError {
		t.Fatal("NXDOMAIN changed", got)
	}
}

func TestPlatformDNSUnavailableSelectionRejectsExpiredSuccessor(t *testing.T) {
	state, now := businessUnavailableServingState(t)
	next, _ := decisionTestState(t)
	zone := next.zones["example.test"]
	entries := zone.records["target.example.test"]
	for i := range entries {
		for j := range entries[i].facts {
			entries[i].facts[j].Proof.ValidUntil = now.Add(-time.Second)
		}
	}
	zone.records["target.example.test"] = entries
	next.zones["example.test"] = zone
	state.transition = &dnsRecordTransition{state: next, hosts: map[string]bool{"target.example.test": true}}
	query := new(dns.Msg)
	query.SetQuestion("target.example.test.", dns.TypeA)
	got := state.answer(query, "", now)
	if got.Rcode != dns.RcodeServerFailure || len(got.Answer) != 0 {
		t.Fatal("expired successor admitted", got)
	}
}
