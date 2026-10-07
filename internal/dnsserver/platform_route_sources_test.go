package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/routeprobe"
	"fugue/internal/testfixture/celldns"
	"github.com/miekg/dns"
)

func TestDynamicDNSRouteSourcesObserveRecheckAndRecover(t *testing.T) {
	for _, scenario := range []string{"success", "selection-race", "negative-probe", "wrong-binding", "mixed-publications", "old-fence", "forged-checkpoint"} {
		t.Run(scenario, func(t *testing.T) {
			req, policies, activations := celldns.SourceAuthorizedRequest(t, true)
			compiled := celldns.Compile(t, req)
			keys := celldns.Keys()
			now := time.Now().UTC()
			snapshot := celldns.SourceSnapshot(t, req, compiled.DNSArtifact, policies, activations, now)
			// Advance all producers while the immutable DNS artifact stays unchanged.
			for i := range snapshot.Scopes {
				scope := &snapshot.Scopes[i]
				pub := &scope.Publications[0]
				pub.Release.ID += "-next"
				pub.Release.FencingToken++
				scope.Lanes[0].ActiveReleaseID = pub.Release.ID
				scope.Lanes[0].FencingToken = pub.Release.FencingToken
				scope.Lanes[0].Version++
			}
			snapshot.SelectionDigest, _ = dnsroutesource.SelectionDigest(snapshot)
			s := NewService(config.DNSConfig{DNSNodeID: "dns-a", EdgeGroupID: "cell-dns", PlatformScopeKey: platformconfig.AuthorityCellScope("cell-dns"), Zone: "example.test", CachePath: filepath.Join(t.TempDir(), "dns"), BundleSigningKey: keys.PrimaryKey, BundleSigningKeyID: keys.PrimaryKeyID}, nil)
			release := model.PlatformArtifactRelease{ID: "dns-full", ArtifactID: compiled.ReleaseArtifact.ID, ArtifactKind: compiled.ReleaseArtifact.ArtifactKind, ScopeKey: compiled.ReleaseArtifact.ScopeKey, Generation: compiled.ReleaseArtifact.Generation, ReleaseChannel: "full", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now}
			assignment := model.PlatformConsumerAssignment{ArtifactID: compiled.DNSArtifact.ID, ArtifactKind: compiled.DNSArtifact.ArtifactKind, ArtifactReleaseID: release.ID, ReleaseSetID: compiled.ReleaseArtifact.ID, ExpectedConsumerSetID: "dns-expected", Revision: 1, ScopeKey: compiled.DNSArtifact.ScopeKey, ExpectedGeneration: compiled.DNSArtifact.Generation, GenerationSequence: compiled.DNSArtifact.GenerationSequence, ContentHash: compiled.DNSArtifact.ContentHash, FencingToken: 1, ReleaseChannel: "full"}
			candidate := dnsPlatformCandidate{Artifact: compiled.DNSArtifact, Assignment: assignment, Release: release}
			payload, routeID, err := s.verifyDNSServingRelease(compiled.ReleaseArtifact, candidate)
			if err != nil {
				t.Fatal(err)
			}
			plans, err := dnsroutesource.Build(compiled.DNSArtifact, snapshot, keys, now)
			if err != nil {
				t.Fatal(err)
			}
			reads := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reads++
				next := snapshot
				if scenario == "selection-race" && reads > 1 {
					next.SelectionDigest = "changed"
				}
				json.NewEncoder(w).Encode(model.PlatformConsumerDNSRouteSourcesResponse{Assignment: assignment, Release: release, Snapshot: next})
			}))
			defer server.Close()
			client := platformconsumer.Client{BaseURL: server.URL}
			probe := func(ctx context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
				if scenario == "negative-probe" {
					return routeprobe.Proof{}, routeprobe.ErrUnavailable
				}
				for _, p := range plans.Probes {
					if p.Hostname != host || p.Path != path || p.Address != address {
						continue
					}
					legacy := p.PreviousAuthority != nil
					// Mix entire physical targets across sources, never paths on one target.
					wantLegacy := p.EdgeID == "edge-a"
					if scenario == "mixed-publications" && p.Path != "/" {
						wantLegacy = !wantLegacy
					}
					if legacy != wantLegacy {
						continue
					}
					group := p.EdgeGroupID
					if legacy {
						group = p.PreviousAuthority.EdgeGroupID
					}
					b := *plans.Bindings[p.CellPublicationDigest]
					if scenario == "wrong-binding" {
						b.FencingToken++
					}
					return routeprobe.Proof{EdgeID: p.EdgeID, GroupID: group, Digest: p.RouteDigest, Version: "observed", State: state, TrafficRelease: &b, CheckedAt: time.Now().Add(-time.Millisecond), ValidUntil: time.Now().Add(20 * time.Second)}, nil
				}
				return routeprobe.Proof{}, errors.New("missing physical probe")
			}
			if scenario == "old-fence" {
				var newer model.PlatformDNSRouteSourceSnapshot
				raw, _ := json.Marshal(snapshot)
				json.Unmarshal(raw, &newer)
				newer.Scopes[0].Lanes[0].Version++
				newer.Scopes[0].Lanes[0].FencingToken++
				if err := s.advanceDNSRouteCursor(newer); err != nil {
					t.Fatal(err)
				}
			}
			effective, facts, err := s.observeDNSRouteSources(context.Background(), client, platformconsumer.Identity{Token: "test"}, candidate, payload, nil, probe)
			if scenario == "selection-race" {
				if !errors.Is(err, platformconsumer.ErrAssignmentChanged) {
					t.Fatal("selection drift accepted", err)
				}
				return
			}
			if scenario == "old-fence" {
				if err == nil || !strings.Contains(err.Error(), "replay") {
					t.Fatal("durable source fence regressed", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			record := dnsServingCheckpoint{Schema: "fugue.dns.positive-checkpoint/v1", NodeID: "dns-a", GroupID: "cell-dns", Parent: compiled.ReleaseArtifact, Candidate: candidate, AppliedAt: now, Positive: true, RouteSources: effective.routeSources}
			if err := s.signDNSCheckpoint(&record); err != nil {
				t.Fatal(err)
			}
			st, err := buildDNSServingState(record, effective, routeID, "dns-a", "cell-dns", facts, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			question := new(dns.Msg)
			question.SetQuestion("app.example.test.", dns.TypeA)
			answer := st.answer(question, "", time.Now())
			if scenario == "negative-probe" || scenario == "wrong-binding" || scenario == "mixed-publications" {
				if answer.Rcode != dns.RcodeServerFailure {
					t.Fatal("invalid proof granted an answer", answer)
				}
				return
			}
			if answer.Rcode != dns.RcodeSuccess || len(answer.Answer) == 0 {
				t.Fatal("authorized successor did not converge", answer)
			}
			if scenario == "forged-checkpoint" {
				record.RouteSources.Selections = map[string]string{"foreign": "foreign"}
			}
			raw, _ := json.Marshal(record)
			decoded, err := s.decodeDNSCheckpoint(raw)
			if scenario == "forged-checkpoint" {
				if err == nil {
					t.Fatal("context tampering escaped checkpoint MAC")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			restored, _, err := s.verifiedDNSCheckpointPayload(decoded)
			if err != nil {
				t.Fatal(err)
			}
			restarted, err := buildDNSServingState(decoded, restored, "", "dns-a", "cell-dns", nil, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if restarted.answer(question, "", time.Now()).Rcode != dns.RcodeServerFailure {
				t.Fatal("restart fabricated positive proofs")
			}
			deadline := record.AppliedAt.Add(time.Duration(payload.Policy.MaxStaleSeconds) * time.Second)
			if dnsServingReady(st, deadline.Add(time.Second)) {
				t.Fatal("source context renewed checkpoint deadline")
			}
			if err := s.checkDNSRouteSources(context.Background(), client, platformconsumer.Identity{Token: "test"}, candidate, effective.routeSources); err != nil {
				t.Fatal(err)
			}
		})
	}
}
