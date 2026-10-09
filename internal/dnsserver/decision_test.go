package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

func decisionTestState(t *testing.T) (*dnsServingState, time.Time) {
	t.Helper()
	view, plan, policy, facts, now := queryExecutionFixture()
	view.Records = append(view.Records,
		model.EdgeDNSRecord{Name: "static.example.test", Type: "A", Values: []string{"192.0.2.12"}, TTL: 60},
		model.EdgeDNSRecord{Name: "lease.example.test", Type: "TXT", Values: []string{"lease"}, ValueExpirations: map[string]time.Time{"lease": now.Add(time.Minute)}, TTL: 60},
		model.EdgeDNSRecord{Name: "alias.example.test", Type: "CNAME", Values: []string{"static.example.test"}, TTL: 60},
		model.EdgeDNSRecord{Name: "*.wild.example.test", Type: "TXT", Values: []string{"wild"}, TTL: 60})
	payload := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}, Policy: platformconfig.PolicySnapshot{MaxStaleSeconds: 3600, DNSReadiness: &policy,
		DNSAuthorities:    []platformconfig.DNSAuthorityPolicy{{NodeID: "dns-a", Zone: "example.test", Nameservers: []string{"ns.example.test"}, TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600}},
		DNSClientPolicies: []platformconfig.DNSClientPolicy{{NodeID: "dns-a"}}}}
	state, err := buildDNSServingState(dnsServingCheckpoint{Positive: true, NodeID: "dns-a", GroupID: "edge-group-a", AppliedAt: now,
		Candidate: dnsPlatformCandidate{Artifact: model.PlatformArtifact{ID: "artifact-a", ContentHash: "sha256:loaded", GenerationSequence: 12}}}, payload, "route", "dns-a", "edge-group-a", facts, now)
	if err != nil {
		t.Fatal(err)
	}
	return state, now
}

func decisionTestJournal(t *testing.T) *dnsDecisionJournal {
	t.Helper()
	journal := &dnsDecisionJournal{nodeID: "dns-a", processID: "test-process", directory: t.TempDir(), queue: make(chan dnsDecisionPending, dnsDecisionQueue), write: func(filename string, data []byte) error { return os.WriteFile(filename, data, 0600) }}
	empty := []DNSDecisionReceipt{}
	journal.records.Store(&empty)
	return journal
}

func captureDecision(t *testing.T, state *dnsServingState, request *dns.Msg, remote string, now time.Time) DNSDecisionReceipt {
	t.Helper()
	capture := &dnsDecisionCapture{input: dnsDecisionReplay{Version: DNSDecisionSchema}, records: []DNSDecisionRecord{}}
	response := state.answerObserved(request, remote, now, capture)
	journal := decisionTestJournal(t)
	journal.enqueue(request, response, capture, decisionPublicationState(state, nil), "udp", now, true)
	select {
	case pending := <-journal.queue:
		journal.append(pending)
	default:
		t.Fatal("capture was dropped")
	}
	if journal.dropped.Load() != 0 {
		t.Fatal("journal dropped valid evidence")
	}
	return (*journal.records.Load())[0]
}

func TestDNSDecisionReplayActualAnswersAndPrivacy(t *testing.T) {
	state, now := decisionTestState(t)
	before, _ := json.Marshal(state.payload)
	for _, test := range []struct {
		name   string
		kind   uint16
		offset time.Duration
	}{
		{"target.example.test", dns.TypeA, 0}, {"static.example.test", dns.TypeA, 0}, {"lease.example.test", dns.TypeTXT, 0},
		{"alias.example.test", dns.TypeA, 0}, {"a.wild.example.test", dns.TypeTXT, 0}, {"absent.example.test", dns.TypeA, 0},
		{"example.test", dns.TypeNS, 0}, {"example.test", dns.TypeSOA, 0}, {"outside.test", dns.TypeA, 0},
		{"target.example.test", dns.TypeA, time.Hour}, {"static.example.test", dns.TypeA, 2 * time.Hour},
	} {
		t.Run(fmt.Sprintf("%s-%d-%s", test.name, test.kind, test.offset), func(t *testing.T) {
			request := new(dns.Msg)
			request.SetQuestion(dns.Fqdn(test.name), test.kind)
			request.SetEdns0(1232, false)
			request.IsEdns0().Option = append(request.IsEdns0().Option, &dns.EDNS0_SUBNET{Code: dns.EDNS0SUBNET, Family: 1, SourceNetmask: 24, Address: net.ParseIP("203.0.113.99")})
			receipt := captureDecision(t, state, request, "198.51.100.93:54321", now.Add(test.offset))
			result, err := ReplayDNSDecision(receipt)
			if err != nil || !result.Matched {
				t.Fatalf("replay failed: %v %+v", err, result)
			}
			if strings.Contains(string(receipt.ReplayInput), "203.0.113.") || strings.Contains(string(receipt.ReplayInput), "198.51.100.") {
				t.Fatal("client address retained")
			}
			baseline := state.answer(request, "198.51.100.93:54321", now.Add(test.offset))
			if !reflect.DeepEqual(dnsRRStrings(baseline.Answer), receipt.RRSet) || baseline.Rcode != receipt.RCode {
				t.Fatal("audit changed answer")
			}
		})
	}
	after, _ := json.Marshal(state.payload)
	if string(before) != string(after) {
		t.Fatal("audit mutated serving input")
	}
}

func TestDNSDecisionReplayReadinessFilteringAndTransition(t *testing.T) {
	state, now := decisionTestState(t)
	next, _ := decisionTestState(t)
	next.record.Candidate.Artifact.ContentHash = "sha256:successor"
	zone := state.zones["example.test"]
	entries := zone.records["target.example.test"]
	for index := range entries {
		entries[index].facts = nil
	}
	zone.records["target.example.test"] = entries
	state.zones["example.test"] = zone
	request := new(dns.Msg)
	request.SetQuestion("target.example.test.", dns.TypeA)
	failed := captureDecision(t, state, request, "198.51.100.1:53", now)
	if failed.RCode != dns.RcodeServerFailure || len(failed.Records) != 1 || len(failed.Records[0].Filtered) == 0 {
		t.Fatal("missing readiness failure")
	}
	if result, err := ReplayDNSDecision(failed); err != nil || !result.Matched {
		t.Fatal(err)
	}
	state.transition = &dnsRecordTransition{state: next, hosts: map[string]bool{"target.example.test": true}}
	receipt := captureDecision(t, state, request, "198.51.100.1:53", now)
	if receipt.RCode != dns.RcodeSuccess || len(receipt.Records) != 2 {
		t.Fatal("transition attempts not retained")
	}
	if receipt.Publication.Loaded.Digest != "sha256:loaded" || receipt.AnswerPublication == nil || receipt.AnswerPublication.Digest != "sha256:successor" {
		t.Fatal("transition answer confused with loaded checkpoint")
	}
	if result, err := ReplayDNSDecision(receipt); err != nil || !result.Matched {
		t.Fatal(err)
	}
}

func TestDNSDecisionJournalFailureAndOverflowCannotChangeServing(t *testing.T) {
	state, now := decisionTestState(t)
	service := NewService(config.DNSConfig{DNSNodeID: "dns-a"}, nil)
	service.platformServingBound.Store(true)
	service.platformServing.Store(state)
	journal := decisionTestJournal(t)
	service.decisionAudit.Store(journal)
	journal.write = func(string, []byte) error { return errors.New("disk unavailable") }
	request := new(dns.Msg)
	request.SetQuestion("target.example.test.", dns.TypeA)
	for index := 0; index < dnsDecisionQueue+3; index++ {
		writer := &captureDNSResponseWriter{}
		service.ServeDNS(writer, request)
		if writer.msg == nil || writer.msg.Rcode != dns.RcodeSuccess || len(writer.msg.Answer) != 1 {
			t.Fatal("audit affected wire response")
		}
	}
	if journal.dropped.Load() != 3 {
		t.Fatal("queue overflow not counted")
	}
	journal.append(<-journal.queue)
	if journal.persistenceErrors.Load() != 1 || len(*journal.records.Load()) != 1 {
		t.Fatal("persistence failure lost in-memory receipt")
	}
	requestHTTP := httptest.NewRequest(http.MethodGet, "/decisions?hostname=target.example.test&limit=1", nil)
	writerHTTP := httptest.NewRecorder()
	service.Handler().ServeHTTP(writerHTTP, requestHTTP)
	var response DNSDecisionSnapshot
	if json.Unmarshal(writerHTTP.Body.Bytes(), &response) != nil || writerHTTP.Code != 200 || len(response.Receipts) != 1 || response.Publication.Loaded == nil || response.Publication.LKG == nil {
		t.Fatal(writerHTTP.Body.String())
	}
	if !response.Receipts[0].WriteSucceeded || response.Receipts[0].ObservedAt.Before(now.Add(-time.Minute)) {
		t.Fatal("not actual receipt")
	}
}

func TestDNSDecisionJournalRecoveryAndTampering(t *testing.T) {
	state, now := decisionTestState(t)
	request := new(dns.Msg)
	request.SetQuestion("target.example.test.", dns.TypeA)
	receipt := captureDecision(t, state, request, "192.0.2.51:1234", now)
	journal := decisionTestJournal(t)
	raw, _ := json.Marshal(receipt)
	if err := os.WriteFile(filepath.Join(journal.directory, "00.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(journal.directory, "01.json"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	journal.recover()
	if journal.recoveryErrors.Load() != 1 || len(*journal.records.Load()) != 1 {
		t.Fatal("invalid recovery accounting")
	}
	if result, err := ReplayDNSDecision((*journal.records.Load())[0]); err != nil || !result.Matched {
		t.Fatal(err)
	}
	receipt.RCode = dns.RcodeRefused
	if _, err := ReplayDNSDecision(receipt); err == nil {
		t.Fatal("tampered receipt accepted")
	}
	receipt.EvidenceDigest = dnsDecisionDigest(receipt)
	if _, err := ReplayDNSDecision(receipt); err == nil {
		t.Fatal("contradictory expected result accepted")
	}
}

func TestDNSDecisionPublicationSeparatesDesiredLoadedAndLKG(t *testing.T) {
	state, now := decisionTestState(t)
	desired := DNSDecisionPublication{Digest: "sha256:other", ArtifactID: "new"}
	observed := &DNSDecisionPublicationState{ObservedAt: now, DesiredKnown: true, Desired: &desired, Rejected: true, Outcome: "candidate_rejected"}
	state.fallback = "candidate_rejected"
	result := decisionPublicationState(state, observed)
	if !result.ServingLKG || !result.Rejected || result.Desired.Digest == result.Loaded.Digest || result.LKG.Digest != result.Loaded.Digest {
		t.Fatal(result)
	}
	if observed.Loaded != nil {
		t.Fatal("mutated immutable observation")
	}
	state.record.Positive = false
	retained := decisionPublicationState(state, &result)
	if retained.LKG == nil || retained.LKG.Digest != result.LKG.Digest || !retained.ServingLKG {
		t.Fatal("readiness loss erased the retained positive LKG")
	}
	unknown := decisionPublicationState(state, nil)
	if unknown.DesiredKnown || unknown.Desired != nil || unknown.Loaded == nil {
		t.Fatal("invented desired publication")
	}
}

func TestDNSDecisionAbsentServingStateReplays(t *testing.T) {
	service := NewService(config.DNSConfig{DNSNodeID: "dns-a"}, nil)
	service.platformServingBound.Store(true)
	journal := decisionTestJournal(t)
	service.decisionAudit.Store(journal)
	request := new(dns.Msg)
	request.SetQuestion("target.example.test.", dns.TypeA)
	writer := &captureDNSResponseWriter{}
	service.ServeDNS(writer, request)
	if writer.msg == nil || writer.msg.Rcode != dns.RcodeServerFailure {
		t.Fatal("changed absent-state response")
	}
	journal.append(<-journal.queue)
	receipt := (*journal.records.Load())[0]
	if receipt.Publication.Loaded != nil || receipt.AnswerPublication != nil {
		t.Fatal("invented loaded state")
	}
	if replay, err := ReplayDNSDecision(receipt); err != nil || !replay.Matched {
		t.Fatal(err)
	}
}

func TestDNSDecisionBoundedRetentionAndShutdown(t *testing.T) {
	state, now := decisionTestState(t)
	request := new(dns.Msg)
	request.SetQuestion("static.example.test.", dns.TypeA)
	journal := decisionTestJournal(t)
	for index := 0; index < dnsDecisionRetention+3; index++ {
		capture := &dnsDecisionCapture{input: dnsDecisionReplay{Version: DNSDecisionSchema}, records: []DNSDecisionRecord{}}
		response := state.answerObserved(request, "", now, capture)
		journal.enqueue(request, response, capture, decisionPublicationState(state, nil), "udp", now, true)
		journal.append(<-journal.queue)
	}
	if len(*journal.records.Load()) != dnsDecisionRetention || journal.evicted.Load() != 3 {
		t.Fatal("retention not bounded")
	}
	files, _ := os.ReadDir(journal.directory)
	if len(files) != dnsDecisionRetention {
		t.Fatal("disk retention not bounded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { journal.run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("journal did not stop")
	}
}
