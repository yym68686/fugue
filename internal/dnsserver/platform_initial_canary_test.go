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

func initialCanaryDNSFixture(t *testing.T) (*dnsServingState, *dnsReleaseBridge, []dnsReadinessFact, time.Time) {
	t.Helper()
	view, plan, readiness, facts, now := queryExecutionFixture()
	view.Records[0].AppID, view.Records[0].TenantID = "app-a", "tenant-a"
	previous := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}, Policy: platformconfig.PolicySnapshot{
		MaxStaleSeconds: 3600, DNSReadiness: &readiness,
		DNSClientPolicies: []platformconfig.DNSClientPolicy{{NodeID: view.NodeID}},
		DNSAuthorities: []platformconfig.DNSAuthorityPolicy{{NodeID: view.NodeID, Zone: view.Zone,
			Nameservers: []string{"ns.example.test"}, TTLSeconds: 60}},
		TrafficConstraints: []platformconfig.TrafficPolicyConstraint{{ID: "policy-a", AppID: "app-a", TenantID: "tenant-a",
			Mode: model.AppTrafficModeSingle, StableReleaseID: "stable-a", StableWeight: 100,
			UnavailableCandidate: "stable", StickyCookie: "Fugue-Release-Stickiness"}},
	}}
	var next dnsServingPayload
	if err := json.Unmarshal(mustDNSJSON(t, previous), &next); err != nil {
		t.Fatal(err)
	}
	policy := &next.Policy.TrafficConstraints[0]
	policy.Mode, policy.CandidateReleaseID = model.AppTrafficModeCanary, "candidate-a"
	policy.StableWeight, policy.CandidateWeight = 99, 1
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
	for i := range facts {
		facts[i].ProbeID = remap[facts[i].ProbeID]
		facts[i].Proof.Digest = "sha256:" + strings.Repeat("f", 64)
	}
	checkpoint := dnsServingCheckpoint{Positive: true, AppliedAt: now.Add(-time.Minute),
		Candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "old", ReleasedAt: now.Add(-time.Minute)}}}
	old, err := buildDNSServingState(checkpoint, previous, "old-route", view.NodeID, view.EdgeGroupID, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	bridge := &dnsReleaseBridge{payload: next, candidate: dnsPlatformCandidate{
		Artifact: model.PlatformArtifact{ID: "next-artifact"},
		Release:  model.PlatformArtifactRelease{ID: "next", ReleasedAt: now}}}
	return old, bridge, facts, now
}

func TestDNSInitialCanaryUsesCompleteSuccessorWithoutReplacingPositiveCheckpoint(t *testing.T) {
	old, bridge, facts, now := initialCanaryDNSFixture(t)
	host := old.payload.Plan.Records[0].Hostname
	before, after, checkpoint := mustDNSJSON(t, old.payload), mustDNSJSON(t, bridge.payload), old.record
	hosts := dnsTransitionRecords(old, bridge)
	if !hosts[host] {
		t.Fatal("starting an authorized canary invalidated unchanged DNS endpoint authority")
	}
	for _, permitted := range compatibleDNSReleaseProbes(old, bridge) {
		if permitted {
			t.Fatal("canary proofs were rebound into the old single-release policy")
		}
	}
	makeNext := func(observations []dnsReadinessFact) *dnsServingState {
		t.Helper()
		view := bridge.payload.Queries[0]
		next, err := buildDNSServingState(dnsServingCheckpoint{AppliedAt: checkpoint.AppliedAt, Candidate: bridge.candidate},
			bridge.payload, "next-route", view.NodeID, view.EdgeGroupID, observations, now)
		if err != nil {
			t.Fatal(err)
		}
		return next
	}
	old.transition = &dnsRecordTransition{state: makeNext(facts), hosts: hosts}
	query := new(dns.Msg)
	query.SetQuestion(dns.Fqdn(host), dns.TypeA)
	answer := old.answer(query, "", now)
	if answer.Rcode != dns.RcodeSuccess || len(answer.Answer) == 0 {
		t.Fatal("ready successor did not answer", answer)
	}
	receipt := captureDecision(t, old, query, "", now)
	replay, err := ReplayDNSDecision(receipt)
	if err != nil || !replay.Matched || receipt.AnswerPublication == nil || receipt.AnswerPublication.ArtifactID != "next-artifact" {
		t.Fatal("successor answer lost its exact publication or replay", replay, err)
	}
	if got := old.answer(query, "", now.Add(time.Minute)); got.Rcode != dns.RcodeServerFailure {
		t.Fatal("expired canary proof was extended", got)
	}
	old.transition.state = makeNext(facts[:1])
	if got := old.answer(query, "", now); got.Rcode != dns.RcodeServerFailure {
		t.Fatal("an incomplete successor quorum was accepted", got)
	}
	if !reflect.DeepEqual(checkpoint, old.record) || old.transition.state.record.Positive ||
		string(before) != string(mustDNSJSON(t, old.payload)) || string(after) != string(mustDNSJSON(t, bridge.payload)) {
		t.Fatal("volatile transition changed a signed policy or positive checkpoint")
	}
}

func TestDNSInitialCanaryPreservesIdentityAndExistingReleaseBoundaries(t *testing.T) {
	for _, scenario := range []string{"stable", "app", "tenant", "policy", "fallback", "cookie", "header", "no stable traffic", "no candidate traffic", "invalid total", "same release", "missing candidate", "prior candidate", "prior canary", "endpoint", "quorum", "duplicate previous", "duplicate next", "older publication"} {
		t.Run(scenario, func(t *testing.T) {
			old, bridge, _, _ := initialCanaryDNSFixture(t)
			before, after := &old.payload.Policy.TrafficConstraints[0], &bridge.payload.Policy.TrafficConstraints[0]
			switch scenario {
			case "stable":
				after.StableReleaseID = "other-stable"
			case "app":
				after.AppID = "other-app"
			case "tenant":
				after.TenantID = "other-tenant"
			case "policy":
				after.ID = "other-policy"
			case "fallback":
				after.UnavailableCandidate = "fail"
			case "cookie":
				after.StickyCookie = "other-cookie"
			case "header":
				after.StickyHeader = "other-header"
			case "no stable traffic":
				after.StableWeight, after.CandidateWeight = 0, 100
			case "no candidate traffic":
				after.StableWeight, after.CandidateWeight = 100, 0
			case "invalid total":
				after.StableWeight = 100
			case "same release":
				after.CandidateReleaseID = after.StableReleaseID
			case "missing candidate":
				after.CandidateReleaseID = ""
			case "prior candidate":
				before.CandidateReleaseID = after.CandidateReleaseID
			case "prior canary":
				before.Mode = model.AppTrafficModeCanary
				before.CandidateReleaseID = "previous-candidate"
				before.StableWeight, before.CandidateWeight = 99, 1
			case "endpoint":
				bridge.payload.Queries[0].Records[0].Candidates[0].IP = "192.0.2.99"
			case "quorum":
				bridge.payload.Plan.Records[0].MinimumHealthyEdges--
			case "duplicate previous":
				old.payload.Policy.TrafficConstraints = append(old.payload.Policy.TrafficConstraints, *before)
			case "duplicate next":
				bridge.payload.Policy.TrafficConstraints = append(bridge.payload.Policy.TrafficConstraints, *after)
			case "older publication":
				bridge.candidate.Release.ReleasedAt = old.record.Candidate.Release.ReleasedAt
			}
			if dnsTransitionRecords(old, bridge)[old.payload.Plan.Records[0].Hostname] != (scenario == "prior canary") {
				t.Fatal("initial canary bypassed an independent authorization boundary")
			}
		})
	}
}

func TestDNSReplacementCanaryPreservesStableAndDNSAuthority(t *testing.T) {
	for _, scenario := range []string{"replace", "stable", "tenant", "app", "policy", "fallback", "cookie", "header", "old inactive", "new inactive", "invalid total", "same release", "endpoint", "quorum", "duplicate previous", "duplicate next", "older publication"} {
		t.Run(scenario, func(t *testing.T) {
			old, bridge, _, _ := initialCanaryDNSFixture(t)
			before, after := &old.payload.Policy.TrafficConstraints[0], &bridge.payload.Policy.TrafficConstraints[0]
			before.Mode, before.CandidateReleaseID = model.AppTrafficModeCanary, "previous-candidate"
			before.StableWeight, before.CandidateWeight = 75, 25
			switch scenario {
			case "stable":
				after.StableReleaseID = "other"
			case "tenant":
				after.TenantID = "other"
			case "app":
				after.AppID = "other"
			case "policy":
				after.ID = "other"
			case "fallback":
				after.UnavailableCandidate = "fail"
			case "cookie":
				after.StickyCookie = "other"
			case "header":
				after.StickyHeader = "other"
			case "old inactive":
				before.StableWeight, before.CandidateWeight = 100, 0
			case "new inactive":
				after.StableWeight, after.CandidateWeight = 100, 0
			case "invalid total":
				after.StableWeight = 100
			case "same release":
				after.CandidateReleaseID = after.StableReleaseID
			case "endpoint":
				bridge.payload.Queries[0].Records[0].Candidates[0].IP = "192.0.2.99"
			case "quorum":
				bridge.payload.Plan.Records[0].MinimumHealthyEdges--
			case "duplicate previous":
				old.payload.Policy.TrafficConstraints = append(old.payload.Policy.TrafficConstraints, *before)
			case "duplicate next":
				bridge.payload.Policy.TrafficConstraints = append(bridge.payload.Policy.TrafficConstraints, *after)
			case "older publication":
				bridge.candidate.Release.ReleasedAt = old.record.Candidate.Release.ReleasedAt
			}
			if dnsTransitionRecords(old, bridge)[old.payload.Plan.Records[0].Hostname] != (scenario == "replace") {
				t.Fatal("replacement changed independent authority", scenario)
			}
			for _, allowed := range compatibleDNSReleaseProbes(old, bridge) {
				if allowed {
					t.Fatal("replacement proofs rebound to old publication")
				}
			}
		})
	}
}
