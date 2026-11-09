package dnsserver

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

func TestDNSOutageUsesExactLateTransitionProofWithinOriginalDeadline(t *testing.T) {
	for _, scenario := range []string{"exact", "foreign fence", "foreign parent", "foreign scope", "foreign route", "unreachable", "expired deadline", "missing deadline", "duplicate deadline"} {
		t.Run(scenario, func(t *testing.T) {
			s, old, now := runtimeFactsFixture(t)
			candidate := old.record.Candidate
			candidate.Release.ID = "successor-release"
			candidate.Release.FencingToken++
			pending := old.record
			pending.Positive, pending.Candidate = false, candidate
			facts := append([]dnsReadinessFact(nil), old.facts...)
			deadline := now.Add(10 * time.Second)
			for i := range facts {
				facts[i].Ready, facts[i].Reason = false, "traffic_release_mismatch"
				facts[i].Proof.ValidUntil = deadline
				if scenario == "expired deadline" {
					facts[i].Proof.ValidUntil = now.Add(-time.Second)
				}
			}
			if scenario == "missing deadline" {
				facts = nil
			}
			if scenario == "duplicate deadline" {
				facts = append(facts, facts...)
			}
			next, err := buildDNSServingState(pending, old.payload, old.routeID, s.Config.DNSNodeID, s.Config.EdgeGroupID, facts, now)
			if err != nil {
				t.Fatal(err)
			}
			host := old.payload.Plan.Records[0].Hostname
			old.transition = &dnsRecordTransition{state: next, hosts: map[string]bool{host: true}}
			before := mustDNSJSON(t, []any{old.record, old.facts, next.record, next.facts})
			probe := func(_ context.Context, hostname, path, address, state string, _ time.Duration) (routeprobe.Proof, error) {
				if scenario == "unreachable" {
					return routeprobe.Proof{}, errors.New("unreachable")
				}
				for _, requirement := range old.payload.Plan.Probes {
					if requirement.Hostname != hostname || requirement.Address != address {
						continue
					}
					binding := &model.TrafficReleaseBinding{ReleaseSetID: old.record.Parent.ID, ReleaseSetDigest: old.record.Parent.ContentHash,
						RouteArtifactID: old.routeID, PolicyDigest: old.payload.Lineage.PolicyDigest, IntentDigest: old.payload.Lineage.IntentDigest,
						InputSnapshotDigest: old.payload.Lineage.InputSnapshotDigest, ReleaseID: candidate.Release.ID,
						ReleaseChannel: candidate.Release.ReleaseChannel, FencingToken: candidate.Release.FencingToken, ScopeKey: "global"}
					switch scenario {
					case "foreign fence":
						binding.FencingToken++
					case "foreign parent":
						binding.ReleaseSetID = "foreign-parent"
					case "foreign scope":
						binding.ScopeKey = "foreign-scope"
					}
					proof := routeprobe.Proof{Digest: requirement.RouteDigest, EdgeID: requirement.EdgeID, GroupID: requirement.EdgeGroupID,
						State: requirement.State, Version: "late-successor", CheckedAt: time.Now().UTC(), ValidUntil: now.Add(time.Minute), TrafficRelease: binding}
					if scenario == "foreign route" {
						proof.Digest = "foreign-route"
					}
					return proof, nil
				}
				return routeprobe.Proof{}, errors.New("unknown requirement")
			}
			refreshed, err := s.refreshedDNSServingFacts(context.Background(), old, probe, "control_plane_unavailable", nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			q := new(dns.Msg)
			q.SetQuestion(dns.Fqdn(host), dns.TypeA)
			got := refreshed.answer(q, "", time.Now().UTC())
			if (got.Rcode == dns.RcodeSuccess && len(got.Answer) > 0) != (scenario == "exact") {
				t.Fatalf("late transition proof result: %s", got)
			}
			if scenario == "exact" {
				for _, fact := range refreshed.transition.state.facts {
					if !fact.Ready || fact.Proof.ValidUntil.After(deadline) || fact.Proof.TrafficRelease.ReleaseID != candidate.Release.ID {
						t.Fatal("transition changed proof identity or extended its deadline", fact)
					}
				}
				if got := refreshed.answer(q, "", deadline.Add(time.Millisecond)); got.Rcode != dns.RcodeServerFailure {
					t.Fatal("offline transition outlived its original deadline")
				}
				receipt := captureDecision(t, refreshed, q, "", time.Now().UTC())
				if replay, err := ReplayDNSDecision(receipt); err != nil || !replay.Matched {
					t.Fatal("late transition cannot replay", replay, err)
				}
				second, err := s.refreshedDNSServingFacts(context.Background(), refreshed, probe, "control_plane_unavailable", nil, nil)
				if err != nil || second.answer(q, "", deadline.Add(time.Millisecond)).Rcode != dns.RcodeServerFailure {
					t.Fatal("repeated offline observations renewed pending authority", err)
				}
			}
			if !reflect.DeepEqual(refreshed.record, old.record) || refreshed.transition.state.record.Positive ||
				string(before) != string(mustDNSJSON(t, []any{old.record, old.facts, next.record, next.facts})) {
				t.Fatal("offline observation mutated or promoted a publication")
			}
		})
	}
}
