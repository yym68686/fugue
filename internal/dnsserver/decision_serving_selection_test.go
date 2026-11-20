package dnsserver

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"github.com/miekg/dns"
)

func readServingDecision(t *testing.T, journal *dnsDecisionJournal) (DNSDecisionReceipt, dnsDecisionReplay) {
	t.Helper()
	select {
	case pending := <-journal.queue:
		journal.append(pending)
	default:
		t.Fatal("no queued decision")
	}
	rows := *journal.records.Load()
	if len(rows) == 0 {
		t.Fatal("no persisted decision")
	}
	receipt := rows[len(rows)-1]
	var input dnsDecisionReplay
	if err := json.Unmarshal(receipt.ReplayInput, &input); err != nil {
		t.Fatal(err)
	}
	if result, err := ReplayDNSDecision(receipt); err != nil || !result.Matched {
		t.Fatalf("decision did not replay: %v", err)
	}
	return receipt, input
}

func TestDNSDecisionServingSelectionKeepsSameReadDuringPublication(t *testing.T) {
	base, now := decisionTestState(t)
	source := func(digest string, version int64) *dnsroutesource.Context {
		return &dnsroutesource.Context{Snapshot: model.PlatformDNSRouteSourceSnapshot{
			SelectionDigest: digest,
			Scopes: []model.PlatformDNSRouteSourceScope{{ScopeKey: "global", Lanes: []model.PlatformDNSRouteSourceLane{
				{ReleaseChannel: "full", Version: version, FencingToken: version},
			}}},
		}}
	}
	base.checkedAt = now.Add(-time.Second)
	base.record.RouteSources = source("base", 1)
	renewed := *base
	renewed.checkedAt = now
	renewed.record.RouteSources = source("renewed", 2)

	s := &Service{PlatformTokenFile: "test-only"}
	s.platformServing.publish(base, dnsServingWriterRetainedSource)
	s.platformRenewal.Store(&dnsServingRenewal{base: base, state: &renewed})
	journal := decisionTestJournal(t)
	s.decisionAudit.Store(journal)
	writer := &publishingDNSResponseWriter{publish: func() {
		s.platformServing.publish(&renewed, dnsServingWriterCandidate)
	}}
	query := new(dns.Msg)
	query.SetQuestion("static.example.test.", dns.TypeA)
	s.ServeDNS(writer, query)
	receipt, input := readServingDecision(t, journal)
	if receipt.RCode != dns.RcodeSuccess || !receipt.WriteSucceeded ||
		input.ServingSelection == nil || input.ServingSelection.Path != "renewal" ||
		input.ServingSelection.BaseWriter != dnsServingWriterRetainedSource ||
		input.ReturnedStage != 1 || len(input.Stages) != 1 ||
		input.Stages[0].RouteSourceDigest != "renewed" ||
		!input.Stages[0].ReadinessCheckedAt.Equal(now) {
		t.Fatalf("decision mixed the query's selected renewal with a later base write: %+v", input)
	}
	s.ServeDNS(writer, query)
	_, input = readServingDecision(t, journal)
	if input.ServingSelection == nil || input.ServingSelection.Path != "base" ||
		input.ServingSelection.BaseWriter != dnsServingWriterCandidate {
		t.Fatalf("next query did not report the new base: %+v", input.ServingSelection)
	}
	s.platformServing.publish(nil, dnsServingWriterClearFallback)
	s.ServeDNS(writer, query)
	receipt, input = readServingDecision(t, journal)
	if receipt.RCode != dns.RcodeServerFailure || !input.NoServingState || len(input.Stages) != 0 ||
		input.ServingSelection == nil || input.ServingSelection.Path != "absent" ||
		input.ServingSelection.BaseWriter != dnsServingWriterClearFallback || input.ReturnedStage != 0 {
		t.Fatalf("absent state was attributed to a stale renewal: %+v", input)
	}
}

func TestDNSDecisionServingSelectionIdentifiesReturnedTransitionStage(t *testing.T) {
	for _, success := range []bool{true, false} {
		name := "failed_transition"
		if success {
			name = "returned_transition"
		}
		t.Run(name, func(t *testing.T) {
			base, now := decisionTestState(t)
			next, _ := decisionTestState(t)
			next.checkedAt = now.Add(-time.Millisecond)
			// Keep the same publication metadata in both replay stages. The
			// returned answer must identify the stage, not compare digests.
			disableReadiness := func(state *dnsServingState) {
				entries := state.zones["example.test"].records["target.example.test"]
				for i := range entries {
					entries[i].facts = nil
				}
			}
			disableReadiness(base)
			wantStage := 2
			if !success {
				disableReadiness(next)
				wantStage = 1
			}
			base.transition = &dnsRecordTransition{state: next, hosts: map[string]bool{"target.example.test": true}}
			s := &Service{PlatformTokenFile: "test-only"}
			s.platformServing.publish(base, dnsServingWriterRecordTransition)
			journal := decisionTestJournal(t)
			s.decisionAudit.Store(journal)
			query := new(dns.Msg)
			query.SetQuestion("target.example.test.", dns.TypeA)
			s.ServeDNS(&captureDNSResponseWriter{}, query)
			receipt, input := readServingDecision(t, journal)
			if len(input.Stages) != 2 || input.ReturnedStage != wantStage ||
				input.Stages[0].Publication != input.Stages[1].Publication ||
				!input.Stages[1].ReadinessCheckedAt.Equal(next.checkedAt) {
				t.Fatalf("returned state confused with attempted transition: %+v", input)
			}
			input.ReturnedStage = 3 - wantStage
			receipt.ReplayInput, _ = json.Marshal(input)
			receipt.EvidenceDigest = dnsDecisionDigest(receipt)
			if _, err := ReplayDNSDecision(receipt); err == nil {
				t.Fatal("replay accepted the wrong returned stage")
			}
		})
	}
}
