package dnsserver

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/miekg/dns"
)

func failureTestJournals(t *testing.T) (*dnsDecisionJournal, *dnsDecisionJournal) {
	t.Helper()
	recent, failures := decisionTestJournal(t), decisionTestJournal(t)
	failures.diskLimit = 4
	recent.failures = failures
	return recent, failures
}

func queueTestDecision(t *testing.T, journal *dnsDecisionJournal, state *dnsServingState, at time.Time, written bool) {
	t.Helper()
	request := new(dns.Msg)
	request.SetQuestion("static.example.test.", dns.TypeA)
	capture := &dnsDecisionCapture{input: dnsDecisionReplay{Version: DNSDecisionSchema}, records: []DNSDecisionRecord{}}
	response := state.answerObserved(request, "", at, capture)
	journal.enqueue(request, response, capture, decisionPublicationState(state, nil), "udp", at, written)
}

func TestDNSFailureReceiptSurvivesSuccessfulTrafficAndFullRecentQueue(t *testing.T) {
	state, now := decisionTestState(t)
	recent, failures := failureTestJournals(t)
	for i := 0; i < dnsDecisionQueue; i++ {
		queueTestDecision(t, recent, state, now, true)
	}
	queueTestDecision(t, recent, state, now.Add(2*time.Hour), true)
	if recent.dropped.Load() != 1 || len(failures.queue) != 1 {
		t.Fatal("recent overflow lost independent failure")
	}
	failures.append(<-failures.queue)
	receipt := (*failures.records.Load())[0]
	if receipt.RCode != dns.RcodeServerFailure || receipt.Publication.Loaded == nil || len(receipt.ReplayInput) == 0 {
		t.Fatalf("incomplete failure evidence: %+v", receipt)
	}
	if replay, err := ReplayDNSDecision(receipt); err != nil || !replay.Matched {
		t.Fatalf("failure replay: %+v %v", replay, err)
	}
	for len(recent.queue) > 0 {
		recent.append(<-recent.queue)
	}
	for i := 0; i < dnsDecisionRetention+3; i++ {
		queueTestDecision(t, recent, state, now, true)
		recent.append(<-recent.queue)
	}
	files, err := os.ReadDir(failures.directory)
	if err != nil || len(files) != 1 || failures.dropped.Load() != 0 {
		t.Fatal("success traffic altered failure history", err)
	}
	raw, err := os.ReadFile(filepath.Join(failures.directory, files[0].Name()))
	var persisted DNSDecisionReceipt
	if err != nil || json.Unmarshal(raw, &persisted) != nil || persisted.EvidenceDigest != receipt.EvidenceDigest {
		t.Fatal("failure evidence was overwritten")
	}
}

func TestDNSFailureReceiptDiskBoundAndRestartRotation(t *testing.T) {
	state, now := decisionTestState(t)
	recent, failures := failureTestJournals(t)
	for i := 0; i < 6; i++ {
		queueTestDecision(t, recent, state, now.Add(time.Duration(i)*time.Second), false)
		failures.append(<-failures.queue)
		recent.append(<-recent.queue)
	}
	files, _ := os.ReadDir(failures.directory)
	if len(files) != 4 {
		t.Fatal("failure disk history is unbounded")
	}
	restarted := decisionTestJournal(t)
	restarted.directory, restarted.diskLimit = failures.directory, failures.diskLimit
	restarted.recover()
	if len(*restarted.records.Load()) != 4 || restarted.nextSlot != 2 {
		t.Fatal("restart lost ring position")
	}
	recent.failures = restarted
	queueTestDecision(t, recent, state, now.Add(6*time.Second), false)
	restarted.append(<-restarted.queue)
	restarted.recover()
	receipts := *restarted.records.Load()
	if len(receipts) != 4 || !receipts[0].ObservedAt.Equal(now.Add(3*time.Second)) {
		t.Fatal("restart overwrote recent evidence instead of oldest")
	}
	if err := os.WriteFile(filepath.Join(restarted.directory, "00.json"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	restarted.recover()
	if restarted.recoveryErrors.Load() != 1 || len(*restarted.records.Load()) != 3 {
		t.Fatal("corrupt evidence accepted")
	}
}

func TestDNSFailureReceiptWorkersOwnMutableDescriptions(t *testing.T) {
	state, now := decisionTestState(t)
	recent, failures := failureTestJournals(t)
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); recent.run(ctx) }()
	go func() { defer workers.Done(); failures.run(ctx) }()
	for i := 0; i < 20; i++ {
		queueTestDecision(t, recent, state, now.Add(2*time.Hour), true)
	}
	deadline := time.Now().Add(3 * time.Second)
	for failures.persisted.Load() < 20 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	workers.Wait()
	if failures.persisted.Load() != 20 || failures.dropped.Load() != 0 || recent.dropped.Load() != 0 {
		t.Fatal("concurrent capture lost or mutated evidence")
	}
}
