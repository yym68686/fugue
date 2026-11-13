package dnsserver

import (
	"encoding/json"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"github.com/miekg/dns"
)

func physicalSelectionTestRecord(now time.Time) model.EdgeDNSRecord {
	digest := "sha256:" + strings.Repeat("a", 64)
	return model.EdgeDNSRecord{Name: "app.example.test", Type: "A", TTL: 60,
		AnswerPolicy: model.DNSAnswerPolicy{PolicyKind: model.DNSAnswerPolicyKindPhysicalQuality, HealthRequired: true, RouteReadyRequired: true,
			PhysicalSelection: &model.DNSPhysicalSelection{Version: model.DNSPhysicalSelectionVersion, PrimaryEdgeID: "edge-b", OrderedEdgeIDs: []string{"edge-b", "edge-a"},
				EvidenceDigest: digest, DNSReceiptID: "actual-answer-1", LoadedDigest: digest, PolicyDigest: digest, Scope: "global", CapturedAt: now}},
		Candidates: []model.EdgeDNSAnswerCandidate{
			{IP: "192.0.2.10", EdgeID: "edge-a", EdgeGroupID: "shared", Country: "aa", Score: 1, Weight: 9999, Healthy: true, RouteReady: true, TLSReady: true, DNSEligible: true},
			{IP: "192.0.2.11", EdgeID: "edge-b", EdgeGroupID: "shared", Country: "bb", Score: 999999, Priority: 9999, Healthy: true, RouteReady: true, TLSReady: true, DNSEligible: true},
			{IP: "192.0.2.12", EdgeID: "unranked", EdgeGroupID: "other", Healthy: true, RouteReady: true, TLSReady: true, DNSEligible: true},
		}}
}

// Run the actual loaded serving state, readiness filter, UDP writer and replay
// journal. Merely compiling an order does not prove it reaches authoritative
// DNS, nor that recovery restores it after a readiness failure.
func TestHistoricalPhysicalOrderPublishesOverUDPAndRecovers(t *testing.T) {
	base, now := decisionTestState(t)
	payload := base.payload
	payload.Plan.Records[0].MinimumHealthyEdges = 1
	selection := physicalSelectionTestRecord(now).AnswerPolicy.PhysicalSelection
	original := now.Add(-48 * time.Hour)
	selection.QualityState, selection.QualityObservedAt = "retained", &original
	selection.History = &model.PhysicalQualityHistoryUse{SourceArtifactID: "verified-original", InputDigest: selection.EvidenceDigest, EvidenceDigest: selection.EvidenceDigest,
		PolicyVersion: model.PhysicalComparableDeliveryPolicyVersion, ObservedAt: original, AgeSeconds: 172800, Reason: "historical_order_retained_no_fresh_comparable_evidence",
		OriginalOrder: []string{"edge-b", "edge-a"}, ReusedOrder: []string{"edge-b", "edge-a"}}
	for i := range payload.Queries {
		for j := range payload.Queries[i].Records {
			if payload.Queries[i].Records[j].Name == "target.example.test" {
				payload.Queries[i].Records[j].AnswerPolicy = physicalSelectionTestRecord(now).AnswerPolicy
				payload.Queries[i].Records[j].AnswerPolicy.PhysicalSelection = selection
			}
		}
	}
	service := NewService(config.DNSConfig{DNSNodeID: "dns-a"}, nil)
	service.platformServingBound.Store(true)
	journal := decisionTestJournal(t)
	service.decisionAudit.Store(journal)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	server := &dns.Server{PacketConn: packet, Handler: service, NotifyStartedFunc: func() { close(started) }}
	completed := make(chan error, 1)
	go func() { completed <- server.ActivateAndServe() }()
	<-started
	t.Cleanup(func() {
		_ = server.Shutdown()
		if err := <-completed; err != nil {
			t.Error(err)
		}
	})
	for _, phase := range []string{"historical", "failure", "recovered"} {
		facts := append([]dnsReadinessFact(nil), base.facts...)
		if phase == "failure" {
			for i := range facts {
				for _, probe := range payload.Plan.Probes {
					if facts[i].ProbeID == probe.ID && probe.EdgeID == "edge-b" {
						facts[i].Ready = false
					}
				}
			}
		}
		state, err := buildDNSServingState(base.record, payload, "route", "dns-a", "edge-group-a", facts, now)
		if err != nil {
			t.Fatal(err)
		}
		service.platformServing.Store(state)
		query := new(dns.Msg)
		query.SetQuestion("target.example.test.", dns.TypeA)
		answer, _, err := (&dns.Client{Net: "udp", Timeout: time.Second}).Exchange(query, packet.LocalAddr().String())
		want := "9.9.9.9"
		if phase == "failure" {
			want = "8.8.8.8"
		}
		if err != nil || answer.Rcode != dns.RcodeSuccess || len(answer.Answer) != 1 || !strings.Contains(answer.Answer[0].String(), want) {
			t.Fatalf("%s: %v %v", phase, answer, err)
		}
		select {
		case pending := <-journal.queue:
			journal.append(pending)
			receipts := *journal.records.Load()
			receipt := receipts[len(receipts)-1]
			if replay, err := ReplayDNSDecision(receipt); err != nil || !replay.Matched || !receipt.WriteSucceeded {
				t.Fatal("wire answer not replayable", err)
			}
		case <-time.After(time.Second):
			t.Fatal("UDP answer lacks durable decision evidence")
		}
	}
}

func TestPhysicalDNSOrderIgnoresLegacyScoringAndGeography(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	record := physicalSelectionTestRecord(now)
	before, _ := json.Marshal(record)
	for offset := 0; offset < 100; offset++ {
		ordered, decision := edgeDNSOrderedCandidatesWithDecision(record, dnsGeoHint{Country: "aa", EdgeGroupID: "other"}, now.Add(time.Duration(offset)*time.Hour), false)
		if len(ordered) != 2 || ordered[0].EdgeID != "edge-b" || ordered[1].EdgeID != "edge-a" || decision.ExplorationKind != "" {
			t.Fatal(ordered, decision)
		}
		if edgeDNSSelectionResult(decision, ordered[:1], nil) != "physical_selected_primary" {
			t.Fatal("incorrect physical primary receipt")
		}
	}
	after, _ := json.Marshal(record)
	if string(before) != string(after) {
		t.Fatal("selection mutated signed input")
	}
}

func TestPhysicalDNSFastFailoverToSameGroupSibling(t *testing.T) {
	record := physicalSelectionTestRecord(time.Now())
	record.Candidates[1].RouteReady = false
	ordered, decision := edgeDNSOrderedCandidatesWithDecision(record, dnsGeoHint{}, time.Now(), false)
	if len(ordered) != 1 || ordered[0].EdgeID != "edge-a" || edgeDNSSelectionResult(decision, ordered, nil) != "physical_readiness_failover" {
		t.Fatal(ordered, decision)
	}
	record.Candidates[0].RouteReady = false
	ordered, decision = edgeDNSOrderedCandidatesWithDecision(record, dnsGeoHint{}, time.Now(), false)
	if len(ordered) != 0 || edgeDNSSelectionResult(decision, ordered, nil) != "physical_no_ready_endpoint" {
		t.Fatal("fell back to legacy or unranked endpoint", ordered)
	}
}

func TestPhysicalDNSActualReceiptReplays(t *testing.T) {
	state, now := decisionTestState(t)
	payload := state.payload
	for viewIndex := range payload.Queries {
		for recordIndex := range payload.Queries[viewIndex].Records {
			record := &payload.Queries[viewIndex].Records[recordIndex]
			if record.Name == "target.example.test" {
				record.AnswerPolicy = physicalSelectionTestRecord(now).AnswerPolicy
			}
		}
	}
	state, err := buildDNSServingState(state.record, payload, "route", "dns-a", "edge-group-a", state.facts, now)
	if err != nil {
		t.Fatal(err)
	}
	request := new(dns.Msg)
	request.SetQuestion("target.example.test.", dns.TypeA)
	receipt := captureDecision(t, state, request, "198.51.100.2:1234", now)
	replayed, err := ReplayDNSDecision(receipt)
	if err != nil || !replayed.Matched || len(receipt.RRSet) != 1 || !strings.Contains(receipt.RRSet[0], "9.9.9.9") || !reflect.DeepEqual(replayed.RRSet, receipt.RRSet) {
		t.Fatal(replayed, err)
	}
}

func TestPhysicalDNSMaterializationRetainsSignedPrimaryWhenRejected(t *testing.T) {
	view, plan, policy, facts, now := queryExecutionFixture()
	plan.Records[0].MinimumHealthyEdges = 1
	view.Records[0].AnswerPolicy = physicalSelectionTestRecord(now).AnswerPolicy
	for index := range facts {
		for _, probe := range plan.Probes {
			if facts[index].ProbeID == probe.ID && probe.EdgeID == "edge-b" {
				facts[index].Ready = false
			}
		}
	}
	records, err := materializeDNSQueries(view, &plan, &policy, facts, now)
	if err != nil {
		t.Fatal(err)
	}
	ordered, decision := edgeDNSOrderedCandidatesWithDecision(records[0], dnsGeoHint{}, now, false)
	if len(ordered) != 1 || ordered[0].EdgeID != "edge-a" || edgeDNSSelectionResult(decision, ordered, nil) != "physical_readiness_failover" {
		t.Fatal("materialization changed signed authority", ordered, decision)
	}
}
