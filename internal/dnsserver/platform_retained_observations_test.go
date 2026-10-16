package dnsserver

import (
	"reflect"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

func TestRetainedObservationRefreshRejectsSupersededOrUntrustedProofs(t *testing.T) {
	for _, name := range []string{"exact", "foreign_release", "changed_address", "changed_host", "duplicate", "negative", "expired", "future", "older", "wrong_digest", "wrong_identity", "bounded_policy", "independent_predecessor_copy"} {
		t.Run(name, func(t *testing.T) {
			parent, candidate := dnsServingFixture(t, true)
			s := NewService(config.DNSConfig{Zone: "example.test", DNSNodeID: "dns-a", EdgeGroupID: "edge-group-a", BundleSigningKey: "synthetic-dns-serving-secret", BundleSigningKeyID: "key"}, nil)
			p, routeID, err := s.verifyDNSServingRelease(parent, candidate)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			if name == "independent_predecessor_copy" {
				p.Plan.Probes[0].PreviousAuthority = &platformconfig.DNSPreviousAuthority{PublicationDigest: "previous", EdgeGroupID: "edge-group-a", RouteDigest: "previous-route"}
			}
			plan := *p.Plan
			plan.Probes = append([]platformconfig.DNSReadinessProbe(nil), p.Plan.Probes...)
			requirement := p.Plan.Probes[0]
			proof := routeprobe.Proof{Digest: requirement.RouteDigest, Version: "serving", EdgeID: requirement.EdgeID, GroupID: requirement.EdgeGroupID, State: requirement.State, CheckedAt: now.Add(-time.Second), ValidUntil: now.Add(59 * time.Second), TrafficRelease: &model.TrafficReleaseBinding{ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, RouteArtifactID: routeID, PolicyDigest: p.Lineage.PolicyDigest, IntentDigest: p.Lineage.IntentDigest, InputSnapshotDigest: p.Lineage.InputSnapshotDigest, ReleaseID: candidate.Release.ID, ReleaseChannel: candidate.Release.ReleaseChannel, FencingToken: candidate.Release.FencingToken, ScopeKey: "global"}}
			fact := dnsReadinessFact{ProbeID: requirement.ID, Ready: true, Proof: proof}
			previous := fact
			previous.Proof.CheckedAt, previous.Proof.ValidUntil = now.Add(-59*time.Second), now.Add(time.Second)
			checkpoint := dnsServingCheckpoint{NodeID: "dns-a", GroupID: "edge-group-a", Parent: parent, Candidate: candidate, AppliedAt: now.Add(-time.Minute), Positive: true}
			old, err := buildDNSServingState(checkpoint, p, routeID, "dns-a", "edge-group-a", []dnsReadinessFact{previous}, now.Add(-time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			old.transition = &dnsRecordTransition{}
			observations := []dnsReadinessFact{fact}
			switch name {
			case "foreign_release":
				binding := *fact.Proof.TrafficRelease
				binding.FencingToken++
				observations[0].Proof.TrafficRelease = &binding
			case "changed_address":
				plan.Probes[0].Address = "192.0.2.99"
			case "changed_host":
				plan.Probes[0].Hostname = "other.example.test"
			case "duplicate":
				observations = append(observations, fact)
			case "negative":
				observations[0].Ready = false
			case "expired":
				observations[0].Proof.ValidUntil = now
			case "future":
				observations[0].Proof.CheckedAt = now.Add(time.Second)
			case "older":
				observations[0] = previous
			case "wrong_digest":
				observations[0].Proof.Digest = "foreign-route"
			case "wrong_identity":
				observations[0].Proof.EdgeID = "foreign-edge"
			case "bounded_policy":
				observations[0].Proof.ValidUntil = now.Add(time.Hour)
			case "independent_predecessor_copy":
				copy := *plan.Probes[0].PreviousAuthority
				plan.Probes[0].PreviousAuthority = &copy
			}
			before := mustDNSJSON(t, old.facts)
			got := s.refreshRetainedDNSFactsFromObservations(old, &plan, observations, now)
			if name != "exact" && name != "bounded_policy" && name != "independent_predecessor_copy" {
				if got != nil {
					t.Fatal("invalid observation refreshed retained facts")
				}
				return
			}
			if got == nil || !reflect.DeepEqual(got.record, old.record) || got.checkedAt != old.checkedAt || got.transition != old.transition {
				t.Fatal("fresh exact proof did not preserve publication and scan boundaries")
			}
			if string(before) != string(mustDNSJSON(t, old.facts)) || got.facts[0].Proof.CheckedAt != proof.CheckedAt || got.facts[0].Proof.ValidUntil != proof.ValidUntil {
				t.Fatal("refresh mutated old facts or extended observed validity")
			}
			query := new(dns.Msg)
			query.SetQuestion("app.example.test.", dns.TypeA)
			got.transition = nil
			if got.answer(query, "", now.Add(2*time.Second)).Rcode != dns.RcodeSuccess || got.answer(query, "", now.Add(time.Minute)).Rcode != dns.RcodeServerFailure {
				t.Fatal("refreshed proof did not retain its real expiry")
			}
		})
	}
}
