package dnsserver

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
	"fugue/internal/testfixture/celldns"
	"github.com/miekg/dns"
)

func TestCellDNSServingUsesExactIndependentProofsAndOfflineRecovery(t *testing.T) {
	c := celldns.Compile(t, celldns.Request(t))
	keys := celldns.Keys()
	s := NewService(config.DNSConfig{DNSNodeID: "dns-a", EdgeGroupID: "cell-dns", PlatformScopeKey: platformconfig.AuthorityCellScope("cell-dns"), Zone: "example.test", BundleSigningKey: keys.PrimaryKey, BundleSigningKeyID: keys.PrimaryKeyID}, nil)
	now := time.Now().UTC()
	r := model.PlatformArtifactRelease{ID: "dns-release", ArtifactID: c.ReleaseArtifact.ID, ArtifactKind: c.ReleaseArtifact.ArtifactKind, ScopeKey: c.ReleaseArtifact.ScopeKey, Generation: c.ReleaseArtifact.Generation, ReleaseChannel: "full", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now}
	a := model.PlatformConsumerAssignment{ExpectedConsumerSetID: "dns-set", Revision: 1, ArtifactReleaseID: r.ID, ReleaseSetID: c.ReleaseArtifact.ID, ArtifactID: c.DNSArtifact.ID, ArtifactKind: c.DNSArtifact.ArtifactKind, ScopeKey: c.DNSArtifact.ScopeKey, ExpectedGeneration: c.DNSArtifact.Generation, GenerationSequence: c.DNSArtifact.GenerationSequence, ContentHash: c.DNSArtifact.ContentHash, FencingToken: r.FencingToken, ReleaseChannel: r.ReleaseChannel}
	candidate := dnsPlatformCandidate{Artifact: c.DNSArtifact, Assignment: a, Release: r}
	p, routeID, err := s.verifyDNSServingRelease(c.ReleaseArtifact, candidate)
	if err != nil || routeID != "" {
		t.Fatal("independent DNS rejected or acquired route authority", err)
	}
	facts := []dnsReadinessFact{}
	for _, req := range p.Plan.Probes {
		proof := routeprobe.Proof{Digest: req.RouteDigest, Version: "observed-bundle", EdgeID: req.EdgeID, GroupID: req.EdgeGroupID, CheckedAt: now, ValidUntil: now.Add(20 * time.Second), TrafficRelease: p.cellBindings[req.EdgeGroupID]}
		if !dnsProofMatchesRelease(proof, c.ReleaseArtifact, candidate, routeID, p) {
			t.Fatal("exact Cell proof rejected")
		}
		facts = append(facts, dnsReadinessFact{ProbeID: req.ID, Ready: true, Proof: proof})
		for _, field := range []string{"fence", "parent", "route digest", "scope", "node", "group"} {
			bad := proof
			b := *proof.TrafficRelease
			bad.TrafficRelease = &b
			switch field {
			case "fence":
				b.FencingToken++
			case "parent":
				b.ReleaseSetID = c.ReleaseArtifact.ID
			case "route digest":
				b.RouteArtifactDigest = c.DNSArtifact.ContentHash
			case "scope":
				b.ScopeKey = c.DNSArtifact.ScopeKey
			case "node":
				bad.EdgeID = "foreign"
			case "group":
				bad.GroupID = "cell-foreign"
			}
			if dnsProofMatchesRelease(bad, c.ReleaseArtifact, candidate, routeID, p) {
				t.Fatalf("accepted substituted %s", field)
			}
		}
	}
	checkpoint := dnsServingCheckpoint{Schema: "fugue.dns.positive-checkpoint/v1", NodeID: "dns-a", GroupID: "cell-dns", Parent: c.ReleaseArtifact, Candidate: candidate, AppliedAt: now, Positive: true}
	if err := s.signDNSCheckpoint(&checkpoint); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(checkpoint)
	recovered, err := s.decodeDNSCheckpoint(raw)
	if err != nil {
		t.Fatal("offline recovery lost embedded authorities", err)
	}
	st, err := buildDNSServingState(recovered, p, routeID, "dns-a", "cell-dns", facts, now)
	if err != nil {
		t.Fatal(err)
	}
	question := new(dns.Msg)
	question.SetQuestion("app.example.test.", dns.TypeA)
	answer := st.answer(question, "", now)
	if answer.Rcode != dns.RcodeSuccess || len(answer.Answer) == 0 || answer.Answer[0].Header().Ttl > 20 {
		t.Fatal("fresh referenced routes did not authorize bounded answer", answer)
	}
	if answer := st.answer(question, "", now.Add(21*time.Second)); answer.Rcode != dns.RcodeServerFailure || len(answer.Answer) != 0 {
		t.Fatal("expired Cell proofs revived", answer)
	}
	restarted, err := buildDNSServingState(recovered, p, routeID, "dns-a", "cell-dns", nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if answer := restarted.answer(question, "", now); answer.Rcode != dns.RcodeServerFailure {
		t.Fatal("checkpoint fabricated readiness after restart")
	}
	snapshot, err := dnsRuntimeFacts(st, now)
	if err != nil || !snapshot.Ready || snapshot.RouteArtifactID != "" || len(snapshot.Facts) != 2 {
		t.Fatal("independent runtime observations invalid", err)
	}
	// A re-signed DNS envelope cannot conceal an untrusted embedded Cell.
	// It also cannot classify an active route as an omitted inactive route:
	// canonical replay still requires its complete plan and quorum.
	missing := candidate
	encoded, _ := json.Marshal(candidate.Artifact)
	json.Unmarshal(encoded, &missing.Artifact)
	missing.Artifact.Content["readiness_plan"] = platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{}, Records: []platformconfig.DNSReadinessRecord{}}
	missing.Artifact = celldns.Sign(t, missing.Artifact, missing.Artifact.ID, 1)
	missing.Assignment.ContentHash = missing.Artifact.ContentHash
	if _, _, err := s.verifyDNSServingRelease(c.ReleaseArtifact, missing); err == nil {
		t.Fatal("active route disguised as an inactive omission")
	}

	copy := candidate
	encoded, _ = json.Marshal(candidate.Artifact)
	json.Unmarshal(encoded, &copy.Artifact)
	copy.Artifact.Content["cell_route_publications"].([]any)[0].(map[string]any)["route"].(map[string]any)["provenance"].(map[string]any)["signature"] = "forged"
	copy.Artifact = celldns.Sign(t, copy.Artifact, copy.Artifact.ID, 1)
	copy.Assignment.ContentHash = copy.Artifact.ContentHash
	if _, _, err := s.verifyDNSServingRelease(c.ReleaseArtifact, copy); err == nil {
		t.Fatal("untrusted embedded Cell signature accepted")
	}
}

func TestCellDNSOmittedInactiveRouteStaysAbsentAfterOfflineRecovery(t *testing.T) {
	request := celldns.Request(t)
	request.Policy.DNSAnswerRules, request.RuntimeSnapshot.DNSSelections = nil, nil
	request.Intent.DNS = append(request.Intent.DNS, platformconfig.DNSIntent{Hostname: "static.example.test", Type: "TXT", Values: []string{"retained"}, TTL: 60})
	for i := range request.CellRoutePublications {
		p := &request.CellRoutePublications[i]
		route := p.Route.Content["routes"].([]any)[0].(map[string]any)
		route["enabled"], route["status"] = false, "unavailable"
		p.Route = celldns.Sign(t, p.Route, p.Route.ID, 1)
		p.Reference.RouteArtifactDigest = p.Route.ContentHash
		request.Intent.CellRoutePublications[i] = p.Reference
	}
	c := celldns.Compile(t, request)
	keys := celldns.Keys()
	s := NewService(config.DNSConfig{DNSNodeID: "dns-a", EdgeGroupID: "cell-dns", PlatformScopeKey: platformconfig.AuthorityCellScope("cell-dns"), Zone: "example.test", BundleSigningKey: keys.PrimaryKey, BundleSigningKeyID: keys.PrimaryKeyID}, nil)
	now := time.Now().UTC()
	r := model.PlatformArtifactRelease{ID: "dns-release", ArtifactID: c.ReleaseArtifact.ID, ArtifactKind: c.ReleaseArtifact.ArtifactKind, ScopeKey: c.ReleaseArtifact.ScopeKey, Generation: c.ReleaseArtifact.Generation, ReleaseChannel: "full", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now}
	a := model.PlatformConsumerAssignment{ExpectedConsumerSetID: "dns-set", Revision: 1, ArtifactReleaseID: r.ID, ReleaseSetID: c.ReleaseArtifact.ID, ArtifactID: c.DNSArtifact.ID, ArtifactKind: c.DNSArtifact.ArtifactKind, ScopeKey: c.DNSArtifact.ScopeKey, ExpectedGeneration: c.DNSArtifact.Generation, GenerationSequence: c.DNSArtifact.GenerationSequence, ContentHash: c.DNSArtifact.ContentHash, FencingToken: r.FencingToken, ReleaseChannel: r.ReleaseChannel}
	candidate := dnsPlatformCandidate{Artifact: c.DNSArtifact, Assignment: a, Release: r}
	p, routeID, err := s.verifyDNSServingRelease(c.ReleaseArtifact, candidate)
	if err != nil {
		t.Fatal("consumer rejected omitted route", err)
	}
	checkpoint := dnsServingCheckpoint{Schema: "fugue.dns.positive-checkpoint/v1", NodeID: "dns-a", GroupID: "cell-dns", Parent: c.ReleaseArtifact, Candidate: candidate, AppliedAt: now, Positive: true}
	if err := s.signDNSCheckpoint(&checkpoint); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(checkpoint)
	recovered, err := s.decodeDNSCheckpoint(raw)
	if err != nil {
		t.Fatal("offline replay rejected", err)
	}
	st, err := buildDNSServingState(recovered, p, routeID, "dns-a", "cell-dns", nil, now)
	if err != nil {
		t.Fatal(err)
	}
	question := new(dns.Msg)
	question.SetQuestion("app.example.test.", dns.TypeA)
	if answer := st.answer(question, "", now); answer.Rcode != dns.RcodeNameError || len(answer.Answer) != 0 {
		t.Fatal("omitted inactive route returned an address", answer)
	}
	question.SetQuestion("static.example.test.", dns.TypeTXT)
	answer := st.answer(question, "", now)
	if answer.Rcode != dns.RcodeSuccess || len(answer.Answer) != 1 {
		t.Fatal("inactive route blocked unrelated DNS", answer)
	}
	if txt, ok := answer.Answer[0].(*dns.TXT); !ok || len(txt.Txt) != 1 || txt.Txt[0] != "retained" {
		t.Fatal("static answer changed", answer)
	}
}
