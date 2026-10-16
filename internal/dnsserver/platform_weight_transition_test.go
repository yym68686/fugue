package dnsserver

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

func TestDNSCanaryWeightTransitionUsesSuccessorProofWithoutRebindingLKG(t *testing.T) {
	view, plan, readiness, facts, now := queryExecutionFixture()
	view.Records[0].AppID, view.Records[0].TenantID = "app-a", "tenant-a"
	previous := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}, Policy: platformconfig.PolicySnapshot{
		MaxStaleSeconds: 3600, DNSReadiness: &readiness,
		DNSClientPolicies: []platformconfig.DNSClientPolicy{{NodeID: view.NodeID}},
		DNSAuthorities:    []platformconfig.DNSAuthorityPolicy{{NodeID: view.NodeID, Zone: view.Zone, Nameservers: []string{"ns.example.test"}, TTLSeconds: 60}},
		TrafficConstraints: []platformconfig.TrafficPolicyConstraint{{ID: "traffic-a", AppID: "app-a", TenantID: "tenant-a", Mode: model.AppTrafficModeCanary,
			StableReleaseID: "stable-a", CandidateReleaseID: "candidate-a", StableWeight: 99, CandidateWeight: 1, UnavailableCandidate: "stable", StickyCookie: "release"}},
	}}
	raw := mustDNSJSON(t, previous)
	for _, scenario := range []string{"weight", "start", "abort", "promote", "foreign stable", "candidate", "stable", "tenant", "app", "fallback", "cookie", "mode", "enable", "disable", "invalid weights", "address", "quorum"} {
		t.Run(scenario, func(t *testing.T) {
			var previous, next dnsServingPayload
			if err := json.Unmarshal(raw, &previous); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &next); err != nil {
				t.Fatal(err)
			}
			constraint := &next.Policy.TrafficConstraints[0]
			constraint.StableWeight, constraint.CandidateWeight = 95, 5
			switch scenario {
			case "start":
				// Keep the retained single-release checkpoint distinct from the
				// successor's newly authorized candidate and its exact proofs.
				previous = dnsServingPayload{}
				if err := json.Unmarshal(raw, &previous); err != nil {
					t.Fatal(err)
				}
				previous.Policy.TrafficConstraints[0].Mode = model.AppTrafficModeSingle
				previous.Policy.TrafficConstraints[0].CandidateReleaseID = ""
				previous.Policy.TrafficConstraints[0].StableWeight, previous.Policy.TrafficConstraints[0].CandidateWeight = 100, 0
			case "abort", "promote", "foreign stable":
				constraint.Mode, constraint.StableWeight, constraint.CandidateWeight = model.AppTrafficModeSingle, 100, 0
				constraint.CandidateReleaseID = ""
				if scenario == "promote" {
					constraint.StableReleaseID = "candidate-a"
				}
				if scenario == "foreign stable" {
					constraint.StableReleaseID = "foreign"
				}
			case "candidate":
				constraint.CandidateReleaseID = "foreign"
			case "stable":
				constraint.StableReleaseID = "foreign"
			case "tenant":
				constraint.TenantID = "foreign"
			case "app":
				constraint.AppID = "foreign"
			case "fallback":
				constraint.UnavailableCandidate = "fail"
			case "cookie":
				constraint.StickyCookie = "other"
			case "mode":
				constraint.Mode = model.AppTrafficModeSingle
			case "enable":
				constraint.StableWeight, constraint.CandidateWeight = 0, 100
			case "disable":
				constraint.StableWeight, constraint.CandidateWeight = 100, 0
			case "invalid weights":
				constraint.StableWeight = 99
			case "address":
				next.Queries[0].Records[0].Candidates[0].IP = "192.0.2.99"
			case "quorum":
				next.Plan.Records[0].MinimumHealthyEdges++
			}
			remap := map[string]string{}
			for i := range next.Plan.Probes {
				probe := &next.Plan.Probes[i]
				oldID := probe.ID
				probe.RouteDigest = "sha256:" + strings.Repeat("f", 64)
				probe.ID, _ = platformconfig.DNSReadinessProbeID(*probe)
				remap[oldID] = probe.ID
			}
			for i := range next.Plan.Records {
				for j := range next.Plan.Records[i].Targets {
					for k, id := range next.Plan.Records[i].Targets[j].ProbeIDs {
						next.Plan.Records[i].Targets[j].ProbeIDs[k] = remap[id]
					}
				}
			}
			record := dnsServingCheckpoint{Positive: true, AppliedAt: now.Add(-time.Minute), Candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "old", ReleasedAt: now.Add(-time.Minute)}}}
			old := &dnsServingState{payload: previous, record: record}
			bridge := &dnsReleaseBridge{payload: next, candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "next", ReleasedAt: now}}}
			previousRaw := mustDNSJSON(t, previous)
			nextRaw := mustDNSJSON(t, next)
			hosts := dnsTransitionRecords(old, bridge)
			if hosts[plan.Records[0].Hostname] != (scenario == "weight" || scenario == "start" || scenario == "abort" || scenario == "promote" || scenario == "enable" || scenario == "disable" || scenario == "candidate") {
				t.Fatal("weight transition authorization", scenario, hosts)
			}
			if string(previousRaw) != string(mustDNSJSON(t, previous)) || string(nextRaw) != string(mustDNSJSON(t, next)) {
				t.Fatal("comparison mutated signed policies")
			}
			for _, allowed := range compatibleDNSReleaseProbes(old, bridge) {
				if allowed {
					t.Fatal("weight transition rebound old-publication proofs")
				}
			}
			if !hosts[plan.Records[0].Hostname] {
				return
			}
			old, err := buildDNSServingState(record, previous, "old-route", view.NodeID, view.EdgeGroupID, nil, now)
			if err != nil {
				t.Fatal(err)
			}
			nextFacts := append([]dnsReadinessFact(nil), facts...)
			for i := range nextFacts {
				nextFacts[i].ProbeID = remap[nextFacts[i].ProbeID]
				nextFacts[i].Proof.Digest = "sha256:" + strings.Repeat("f", 64)
			}
			successor, err := buildDNSServingState(dnsServingCheckpoint{AppliedAt: record.AppliedAt, Candidate: bridge.candidate}, next, "new-route", view.NodeID, view.EdgeGroupID, nextFacts, now)
			if err != nil {
				t.Fatal(err)
			}
			old.transition = &dnsRecordTransition{state: successor, hosts: hosts}
			query := new(dns.Msg)
			query.SetQuestion(dns.Fqdn(plan.Records[0].Hostname), dns.TypeA)
			if got := old.answer(query, "", now); got.Rcode != dns.RcodeSuccess || len(got.Answer) == 0 {
				t.Fatal("complete successor could not recover record", got)
			}
			if got := old.answer(query, "", now.Add(time.Minute)); got.Rcode != dns.RcodeServerFailure {
				t.Fatal("expired successor proof was extended")
			}
			if !reflect.DeepEqual(old.record, record) || successor.record.Positive {
				t.Fatal("transition replaced positive checkpoint")
			}
		})
	}
}
