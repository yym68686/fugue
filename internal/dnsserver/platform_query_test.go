package dnsserver

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

func queryExecutionFixture() (platformconfig.DNSQueryView, platformconfig.DNSReadinessPlan, platformconfig.DNSReadinessPolicy, []dnsReadinessFact, time.Time) {
	plan, policy := readinessTestPlan()
	now := time.Now().UTC()
	facts := []dnsReadinessFact{}
	for _, p := range plan.Probes {
		facts = append(facts, dnsReadinessFact{ProbeID: p.ID, Ready: true, Proof: routeprobe.Proof{Digest: p.RouteDigest, Version: "serving", EdgeID: p.EdgeID, GroupID: p.EdgeGroupID, CheckedAt: now.Add(-time.Second), ValidUntil: now.Add(30 * time.Second)}})
	}
	record := model.EdgeDNSRecord{Name: plan.Records[0].Hostname, Type: "A", Values: []string{"8.8.8.8", "9.9.9.9"}, TTL: 60, AnswerPolicy: model.DNSAnswerPolicy{PolicyKind: "geo", ECSEnabled: true, HealthRequired: true, RouteReadyRequired: true}, Candidates: []model.EdgeDNSAnswerCandidate{
		{IP: "8.8.8.8", EdgeID: "edge-a", EdgeGroupID: "edge-group-a", Country: "aa", Weight: 100},
		{IP: "9.9.9.9", EdgeID: "edge-b", EdgeGroupID: "edge-group-b", Country: "bb", Weight: 100},
	}}
	return platformconfig.DNSQueryView{NodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test", Records: []model.EdgeDNSRecord{record, {Name: "_acme-challenge.example.test", Type: "TXT", Values: []string{"expired"}, TTL: 60, ValueExpirations: map[string]time.Time{"expired": now.Add(-time.Second)}}}}, plan, policy, facts, now
}

func TestDNSQueryExecutionRequiresFreshAllPathFactsAndPreservesStaticExpiry(t *testing.T) {
	view, plan, policy, facts, now := queryExecutionFixture()
	before, _ := json.Marshal(view)
	records, err := materializeDNSQueries(view, &plan, &policy, facts, now)
	if err != nil {
		t.Fatal(err)
	}
	if records[0].TTL != 30 || len(records[0].Candidates) != 2 || len(records[1].Values) != 0 {
		t.Fatal("fresh lease bound or ACME expiry lost")
	}
	answers, err := executeDNSQueryRecord(records[0], dnsGeoHint{Country: "bb", Source: "ecs"}, now)
	if err != nil || len(answers) != 1 || answers[0].(*dns.A).A.String() != "9.9.9.9" {
		t.Fatal("production geo selector not applied", err)
	}
	records[0].AnswerPolicy.ECSEnabled = false
	answers, err = executeDNSQueryRecord(records[0], dnsGeoHint{Country: "bb", Source: "ecs"}, now)
	if err != nil || answers[0].(*dns.A).A.String() != "8.8.8.8" {
		t.Fatal("disabled ECS affected selection", err)
	}
	shortQuorum := append([]dnsReadinessFact(nil), facts...)
	shortQuorum[2].Proof.ValidUntil = now.Add(5 * time.Second)
	bounded, err := materializeDNSQueries(view, &plan, &policy, shortQuorum, now)
	if err != nil || bounded[0].TTL != 5 {
		t.Fatal("answer TTL outlives required peer proof", err)
	}
	partial := append([]dnsReadinessFact(nil), facts...)
	partial[0].Ready = false
	records, err = materializeDNSQueries(view, &plan, &policy, partial, now)
	if err != nil || len(records[0].Values) != 0 {
		t.Fatal("one failed dependency satisfied quorum", err)
	}
	records, err = materializeDNSQueries(view, &plan, &policy, facts, now.Add(time.Minute))
	if err != nil || len(records[0].Values) != 0 {
		t.Fatal("expired facts renewed by query", err)
	}
	after, _ := json.Marshal(view)
	if string(before) != string(after) {
		t.Fatal("query mutated signed authorization")
	}
}

func TestDNSQueryUsesExistingLatencyScopedAndExplorationSelection(t *testing.T) {
	view, plan, policy, facts, now := queryExecutionFixture()
	records, err := materializeDNSQueries(view, &plan, &policy, facts, now)
	if err != nil {
		t.Fatal(err)
	}
	record := records[0]
	record.AnswerPolicy.PolicyKind = "latency_aware"
	record.AnswerPolicy.SelectedEdgeGroupID = "edge-group-b"
	record.Candidates[0].Score = 200
	record.Candidates[1].Score = 100
	for _, percent := range []int{0, 5, 50} {
		record.AnswerPolicy.ExplorationPercent = percent
		for step := 0; step < 20; step++ {
			at := now.Add(time.Duration(step) * 10 * time.Minute)
			hint := dnsGeoHint{Country: "aa", Source: "ecs"}
			expected, _, _ := edgeDNSAnswerCandidateDecision(record, hint, at, nil, nil)
			answers, err := executeDNSQueryRecord(record, hint, at)
			if err != nil || len(answers) != len(expected) {
				t.Fatal("selector output cardinality changed", err)
			}
			for i, answer := range answers {
				if answer.(*dns.A).A.String() != expected[i].IP {
					t.Fatal("latency/exploration order drift")
				}
			}
		}
	}
	record.AnswerPolicy.ExplorationPercent = 0
	record.ScopedCandidates = []model.EdgeDNSScopedAnswerCandidates{{ScopeKey: "country:aa", Country: "aa", PolicyKind: "latency_aware", SelectedEdgeGroupID: "edge-group-a", Candidates: record.Candidates}}
	expected, _, _ := edgeDNSAnswerCandidateDecision(record, dnsGeoHint{Country: "aa", Source: "ecs"}, now, nil, nil)
	answers, err := executeDNSQueryRecord(record, dnsGeoHint{Country: "aa", Source: "ecs"}, now)
	if err != nil || len(expected) == 0 || !reflect.DeepEqual(answers[0].(*dns.A).A.String(), expected[0].IP) {
		t.Fatal("scoped selection lost", err)
	}
}

func TestDNSQueryReceiptBindsExactReleaseAndConsumer(t *testing.T) {
	view, plan, policy, facts, now := queryExecutionFixture()
	service := &Service{Config: config.DNSConfig{DNSNodeID: view.NodeID, EdgeGroupID: view.EdgeGroupID}}
	candidate := dnsPlatformCandidate{Artifact: model.PlatformArtifact{ID: "artifact", ContentHash: "digest", Content: map[string]any{"query_views": []platformconfig.DNSQueryView{view}, "readiness_plan": plan, "policy": platformconfig.PolicySnapshot{DNSReadiness: &policy}}}}
	assignment := model.PlatformConsumerAssignment{ReleaseSetID: "release", ExpectedConsumerSetID: "expected", FencingToken: 7}
	factsReceipt := dnsReadinessReceipt{ArtifactID: "artifact", ArtifactDigest: "digest", ReleaseSetID: "release", ExpectedConsumerSetID: "expected", FencingToken: 7, NodeID: view.NodeID, Facts: facts, Status: DNSReadinessStatus{CheckedAt: now}}
	receipt, err := service.evaluatePlatformDNSQueries(candidate, assignment, &factsReceipt)
	if err != nil || receipt.Status.Serving || receipt.Status.Questions < 2 || receipt.Status.Answers < 1 || receipt.Status.ViewDigest == "" || len(receipt.Zones) != 1 || len(receipt.Zones[0].Records[1].Values) != 0 {
		t.Fatalf("invalid query receipt: %+v %v", receipt, err)
	}
	for name, mutate := range map[string]func(*dnsReadinessReceipt){
		"artifact": func(r *dnsReadinessReceipt) { r.ArtifactID = "other" },
		"digest":   func(r *dnsReadinessReceipt) { r.ArtifactDigest = "other" },
		"release":  func(r *dnsReadinessReceipt) { r.ReleaseSetID = "other" },
		"expected": func(r *dnsReadinessReceipt) { r.ExpectedConsumerSetID = "other" },
		"fence":    func(r *dnsReadinessReceipt) { r.FencingToken++ },
		"node":     func(r *dnsReadinessReceipt) { r.NodeID = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := factsReceipt
			mutate(&bad)
			if _, err := service.evaluatePlatformDNSQueries(candidate, assignment, &bad); err == nil {
				t.Fatal("unbound facts admitted to query")
			}
		})
	}
}
