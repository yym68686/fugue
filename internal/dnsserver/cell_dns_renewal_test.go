package dnsserver

import (
	"context"
	"reflect"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
	"fugue/internal/testfixture/celldns"
	"github.com/miekg/dns"
)

func TestCellDNSRetainsOriginalProofDeadlineWhileSourcePublicationAdvances(t *testing.T) {
	for _, transition := range []bool{false, true} {
		for _, scenario := range []string{"renewed source", "gray source", "different route", "different scope", "different state", "missing binding", "no previous facts", "expired previous facts", "unbound previous facts", "not positive"} {
			t.Run(scenario+"/transition="+map[bool]string{false: "false", true: "true"}[transition], func(t *testing.T) {
				req := celldns.Request(t)
				if transition {
					req = celldns.TransitionRequest(t)
				}
				compiled := celldns.Compile(t, req)
				keys := celldns.Keys()
				s := NewService(config.DNSConfig{DNSNodeID: "dns-a", EdgeGroupID: "cell-dns", PlatformScopeKey: platformconfig.AuthorityCellScope("cell-dns"), Zone: "example.test", BundleSigningKey: keys.PrimaryKey, BundleSigningKeyID: keys.PrimaryKeyID}, nil)
				now := time.Now().UTC()
				release := model.PlatformArtifactRelease{ID: "dns-release", ArtifactID: compiled.ReleaseArtifact.ID, ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: compiled.ReleaseArtifact.ScopeKey, Generation: compiled.ReleaseArtifact.Generation, ReleaseChannel: "full", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now.Add(-time.Minute)}
				assignment := model.PlatformConsumerAssignment{ExpectedConsumerSetID: "dns-set", Revision: 1, ArtifactReleaseID: release.ID, ReleaseSetID: compiled.ReleaseArtifact.ID, ArtifactID: compiled.DNSArtifact.ID, ArtifactKind: compiled.DNSArtifact.ArtifactKind, ScopeKey: compiled.DNSArtifact.ScopeKey, ExpectedGeneration: compiled.DNSArtifact.Generation, GenerationSequence: compiled.DNSArtifact.GenerationSequence, ContentHash: compiled.DNSArtifact.ContentHash, FencingToken: release.FencingToken, ReleaseChannel: release.ReleaseChannel}
				candidate := dnsPlatformCandidate{Artifact: compiled.DNSArtifact, Assignment: assignment, Release: release}
				payload, routeID, err := s.verifyDNSServingRelease(compiled.ReleaseArtifact, candidate)
				if err != nil {
					t.Fatal(err)
				}
				facts := make([]dnsReadinessFact, 0, len(payload.Plan.Probes))
				proofs := map[string]routeprobe.Proof{}
				for _, requirement := range payload.Plan.Probes {
					proof := routeprobe.Proof{Digest: requirement.RouteDigest, EdgeID: requirement.EdgeID, GroupID: requirement.EdgeGroupID, State: requirement.State, Version: "original-bundle", CheckedAt: now.Add(-time.Second), ValidUntil: now.Add(10 * time.Second), TrafficRelease: payload.cellBindings[requirement.EdgeGroupID]}
					if transition {
						previous := requirement.PreviousAuthority
						proof.Digest, proof.GroupID, proof.TrafficRelease = previous.RouteDigest, previous.EdgeGroupID, payload.previousBindings[previous.EdgeGroupID]
					}
					facts = append(facts, dnsReadinessFact{ProbeID: requirement.ID, Ready: true, Proof: proof})
					proofs[requirement.Hostname+requirement.Path+requirement.Address] = proof
				}
				if scenario == "expired previous facts" {
					for i := range facts {
						facts[i].Proof.ValidUntil = now.Add(-time.Second)
					}
				}
				if scenario == "unbound previous facts" {
					for i := range facts {
						binding := *facts[i].Proof.TrafficRelease
						binding.FencingToken++
						facts[i].Proof.TrafficRelease = &binding
					}
				}
				record := dnsServingCheckpoint{Schema: "fugue.dns.positive-checkpoint/v1", NodeID: "dns-a", GroupID: "cell-dns", Parent: compiled.ReleaseArtifact, Candidate: candidate, AppliedAt: now.Add(-time.Minute), Positive: scenario != "not positive"}
				old, err := buildDNSServingState(record, payload, routeID, "dns-a", "cell-dns", facts, now)
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "no previous facts" {
					old.facts = nil
				}
				probe := func(_ context.Context, host, path, address, _ string, _ time.Duration) (routeprobe.Proof, error) {
					proof := proofs[host+path+address]
					binding := *proof.TrafficRelease
					binding.FencingToken++
					binding.ReleaseID = "next-source-release"
					proof.TrafficRelease = &binding
					proof.Version, proof.CheckedAt, proof.ValidUntil = "new-bundle", now, now.Add(time.Minute)
					switch scenario {
					case "gray source":
						binding.ReleaseChannel = "gray"
						binding.CanaryRuleRef = "cohort=canary"
						binding.EdgeGroupIDs = []string{proof.GroupID}
					case "different route":
						proof.Digest = "changed"
					case "different scope":
						binding.ScopeKey = "authority-cell:cell-foreign"
					case "different state":
						proof.State = "disabled"
					case "missing binding":
						proof.TrafficRelease = nil
					}
					return proof, nil
				}
				refreshed, err := s.refreshedDNSServingFacts(context.Background(), old, probe, "control_plane_unavailable", nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				question := new(dns.Msg)
				question.SetQuestion("app.example.test.", dns.TypeA)
				answer := refreshed.answer(question, "", now)
				if scenario != "renewed source" && scenario != "gray source" {
					if answer.Rcode != dns.RcodeServerFailure {
						t.Fatal("invalid source or missing original evidence kept a positive answer", answer)
					}
					return
				}
				if answer.Rcode != dns.RcodeSuccess || len(answer.Answer) == 0 || answer.Answer[0].Header().Ttl > 10 {
					t.Fatal("source renewal discarded unexpired original evidence", answer)
				}
				if !reflect.DeepEqual(refreshed.record, old.record) {
					t.Fatal("source renewal rewrote the positive checkpoint")
				}
				for i, fact := range refreshed.facts {
					if !fact.Ready || !reflect.DeepEqual(fact.Proof, facts[i].Proof) {
						t.Fatal("unrecognized successor was admitted or original proof deadline renewed")
					}
				}
				if expired := refreshed.answer(question, "", now.Add(11*time.Second)); expired.Rcode != dns.RcodeServerFailure {
					t.Fatal("original proof expiry no longer stops serving", expired)
				}
			})
		}
	}
}
