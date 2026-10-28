package dnsserver

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
	"github.com/miekg/dns"
)

func TestRedundantAvailabilityReturnsCachedRetryAddresses(t *testing.T) {
	record := declaredOrderTestRecord()
	record.AnswerPolicy.Availability = &model.DNSAvailabilityPolicy{
		Version: model.DNSAvailabilityRedundantVersion, MaxAnswers: 2,
		OrderedEdgeIDs: []string{"edge-b", "edge-a"}, RecoverySuccesses: 2, RecoveryMinSeconds: 10,
	}
	answers, _, _ := rrForEdgeDNSRecordWithGeoAuditAt(record, record.Name, dns.TypeA, dnsGeoHint{}, nil, nil, time.Now())
	if len(answers) != 2 {
		t.Fatalf("caching client has no address to retry after primary exit: %v", answers)
	}
	// Existing policies must retain their single-answer semantics.
	record.AnswerPolicy.Availability.Version = model.DNSAvailabilityVersion
	record.AnswerPolicy.Availability.MaxAnswers = 0
	answers, _, _ = rrForEdgeDNSRecordWithGeoAuditAt(record, record.Name, dns.TypeA, dnsGeoHint{}, nil, nil, time.Now())
	if len(answers) != 1 {
		t.Fatal("legacy policy silently changed", answers)
	}
}

func redundantTestPolicy() *model.DNSAvailabilityPolicy {
	return &model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityRedundantVersion, MaxAnswers: 2,
		OrderedEdgeIDs: []string{"edge-b", "edge-a"}, RecoverySuccesses: 2, RecoveryMinSeconds: 10}
}

func TestRedundantAvailabilityIncludesOnlyAuthorizedHealthyDistinctEdges(t *testing.T) {
	for _, scenario := range []string{"quality_omits_backup", "same_edge", "same_ip", "route_failed", "tls_failed", "local_failed", "peer_failed", "outside_group", "business", "pinned", "aaaa"} {
		t.Run(scenario, func(t *testing.T) {
			record := physicalSelectionTestRecord(time.Now())
			record.AnswerPolicy.Availability = redundantTestPolicy()
			record.AnswerPolicy.PhysicalSelection.OrderedEdgeIDs = []string{"edge-b"}
			var local edgeDNSLiveHealthFunc
			var peer edgeDNSPeerHealthFunc
			kind := uint16(dns.TypeA)
			want := 1
			switch scenario {
			case "quality_omits_backup":
				want = 2
			case "same_edge":
				record.Candidates[0].EdgeID = "edge-b"
			case "same_ip":
				record.Candidates[0].IP = record.Candidates[1].IP
			case "route_failed":
				record.Candidates[0].RouteReady = false
			case "tls_failed":
				record.Candidates[0].TLSReady = false
			case "local_failed":
				local = func(_, ip string) bool { return ip != "192.0.2.10" }
			case "peer_failed":
				peer = func(c model.EdgeDNSAnswerCandidate) string {
					if c.EdgeID == "edge-a" {
						return "unhealthy"
					}
					return ""
				}
			case "outside_group":
				record.AnswerPolicy.AllowedEdgeGroups = []string{"other"}
				want = 0
			case "business":
				record.AnswerPolicy.PhysicalSelection.BusinessAllocation = &model.DNSBusinessAllocation{}
				want = 0
			case "pinned":
				record.AnswerPolicy.PolicyKind = model.DNSAnswerPolicyKindPinned
				want = 0
			case "aaaa":
				kind, want, record.Type = dns.TypeAAAA, 2, "AAAA"
				record.Candidates[0].IP, record.Candidates[1].IP = "2001:db8::1", "2001:db8::2"
			}
			before, _ := json.Marshal(record)
			answers, _, _ := rrForEdgeDNSRecordWithGeoAuditAt(record, record.Name, kind, dnsGeoHint{}, local, peer, time.Now())
			if len(answers) != want {
				t.Fatalf("%s: wanted %d answers, got %v", scenario, want, answers)
			}
			after, _ := json.Marshal(record)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("answering changed signed policy")
			}
		})
	}
}

func TestRedundantAvailabilityActualAnswerReplaysAndKeepsProofGates(t *testing.T) {
	for _, scenario := range []string{"ready", "recovering", "expired", "wrong_route", "failed", "outside_pool"} {
		t.Run(scenario, func(t *testing.T) {
			state, now := decisionTestState(t)
			payload := state.payload
			payload.Plan.Records[0].MinimumHealthyEdges = 1
			for i := range payload.Queries[0].Records {
				if payload.Queries[0].Records[i].Name == "target.example.test" {
					payload.Queries[0].Records[i].AnswerPolicy.Availability = redundantTestPolicy()
					if scenario == "outside_pool" {
						payload.Queries[0].Records[i].Candidates[1].EdgeID = "foreign"
					}
				}
			}
			for i := range state.facts {
				fact := &state.facts[i]
				at := fact.Proof.CheckedAt
				fact.ProbeHistory = []time.Time{at.Add(-10 * time.Second), at}
				if fact.Proof.EdgeID == "edge-b" {
					switch scenario {
					case "recovering":
						fact.ProbeHistory = []time.Time{at}
					case "expired":
						fact.Proof.ValidUntil = now.Add(-time.Second)
					case "wrong_route":
						fact.Proof.Digest = "foreign"
					case "failed":
						fact.Ready = false
					}
				}
			}
			state, err := buildDNSServingState(state.record, payload, "route", "dns-a", "edge-group-a", state.facts, now)
			if err != nil {
				t.Fatal(err)
			}
			query := new(dns.Msg)
			query.SetQuestion("target.example.test.", dns.TypeA)
			receipt := captureDecision(t, state, query, "198.51.100.1:53", now)
			want := 1
			if scenario == "ready" {
				want = 2
			}
			if receipt.RCode != dns.RcodeSuccess || len(receipt.RRSet) != want {
				t.Fatalf("proof gates changed: %+v", receipt)
			}
			replay, err := ReplayDNSDecision(receipt)
			if err != nil || !replay.Matched || !slices.Equal(replay.RRSet, receipt.RRSet) {
				t.Fatal("redundant answer cannot be replayed", replay, err)
			}
		})
	}
}
