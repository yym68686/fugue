package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformconsumer"
	"fugue/internal/routeprobe"
	"fugue/internal/testfixture/celldns"
	"github.com/miekg/dns"
)

// Normal dynamic-source observations and retained local probing must use the
// same DNS-authorized recovery history. A publication-specific raw probe ID
// cannot replace its continuous history when the control-plane read fails.
func TestRetainedLocalProbePreservesAuthorizedSourceHistory(t *testing.T) {
	for _, scenario := range []string{"local-probe", "assignment-race", "assignment-race-delayed-observation", "foreign-alias", "negative", "foreign-proof", "restart"} {
		t.Run(scenario, func(t *testing.T) { testRetainedSourceHistory(t, scenario) })
	}
}
func testRetainedSourceHistory(t *testing.T, scenario string) {
	request, policies, activations := celldns.SourceAuthorizedRequest(t, true)
	celldns.RetainGlobalOnly(&request)
	compiled := celldns.Compile(t, request)
	keys := celldns.Keys()
	now := time.Now().UTC()
	service := NewService(config.DNSConfig{DNSNodeID: "dns-a", EdgeGroupID: "cell-dns", PlatformScopeKey: compiled.DNSArtifact.ScopeKey, Zone: "example.test", CachePath: filepath.Join(t.TempDir(), "cache"), BundleSigningKey: keys.PrimaryKey, BundleSigningKeyID: keys.PrimaryKeyID}, nil)
	release := model.PlatformArtifactRelease{ID: "dns-full", ArtifactID: compiled.ReleaseArtifact.ID, ArtifactKind: compiled.ReleaseArtifact.ArtifactKind, ScopeKey: compiled.ReleaseArtifact.ScopeKey, Generation: compiled.ReleaseArtifact.Generation, ReleaseChannel: "full", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now.Add(-time.Minute)}
	assignment := model.PlatformConsumerAssignment{ArtifactID: compiled.DNSArtifact.ID, ArtifactKind: compiled.DNSArtifact.ArtifactKind, ArtifactReleaseID: release.ID, ReleaseSetID: compiled.ReleaseArtifact.ID, ExpectedConsumerSetID: "set", Revision: 1, ScopeKey: compiled.DNSArtifact.ScopeKey, ExpectedGeneration: compiled.DNSArtifact.Generation, GenerationSequence: compiled.DNSArtifact.GenerationSequence, ContentHash: compiled.DNSArtifact.ContentHash, FencingToken: 1, ReleaseChannel: "full"}
	candidate := dnsPlatformCandidate{Artifact: compiled.DNSArtifact, Assignment: assignment, Release: release}
	payload, routeID, err := service.verifyDNSServingRelease(compiled.ReleaseArtifact, candidate)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := celldns.SourceSnapshot(t, request, compiled.DNSArtifact, policies, activations, now)
	plans, err := dnsroutesource.Build(compiled.DNSArtifact, snapshot, keys, now)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(model.PlatformConsumerDNSRouteSourcesResponse{Assignment: assignment, Release: release, Snapshot: snapshot})
	}))
	defer server.Close()
	client := platformconsumer.Client{BaseURL: server.URL}
	identity := platformconsumer.Identity{Token: "scoped"}
	checked := now.Add(-12 * time.Second)
	probe := func(_ context.Context, host, path, address, state string, _ time.Duration) (routeprobe.Proof, error) {
		for _, p := range plans.Probes {
			if p.Hostname == host && p.Path == path && p.Address == address {
				group, digest := p.EdgeGroupID, p.RouteDigest
				if p.PreviousAuthority != nil {
					group, digest = p.PreviousAuthority.EdgeGroupID, p.PreviousAuthority.RouteDigest
				}
				return routeprobe.Proof{EdgeID: p.EdgeID, GroupID: group, Digest: digest, Version: "actual", State: state, TrafficRelease: plans.Bindings[p.CellPublicationDigest], CheckedAt: checked, ValidUntil: checked.Add(time.Minute)}, nil
			}
		}
		t.Error("unexpected probe")
		return routeprobe.Proof{}, routeprobe.ErrUnavailable
	}
	recovery := &model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 10}
	var facts []dnsReadinessFact
	for _, delta := range []time.Duration{-12 * time.Second, -6 * time.Second, -time.Second} {
		checked = now.Add(delta)
		service.servingDNSProbes.digest = ""
		payload, facts, err = service.observeDNSRouteSources(context.Background(), client, identity, candidate, payload, nil, probe)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range facts {
		if !dnsAvailabilityRecovered(recovery, f, time.Minute, now) {
			t.Fatal("baseline history not recovered")
		}
	}
	// Advance only the source release: semantic DNS authority is unchanged, but
	// all publication-specific probe IDs become new.
	for i := range snapshot.Scopes {
		scope := &snapshot.Scopes[i]
		p := &scope.Publications[0]
		p.Release.ID += "-next"
		p.Release.FencingToken++
		scope.Lanes[0].ActiveReleaseID = p.Release.ID
		scope.Lanes[0].FencingToken = p.Release.FencingToken
		scope.Lanes[0].Version++
	}
	snapshot.SelectionDigest, _ = dnsroutesource.SelectionDigest(snapshot)
	plans, err = dnsroutesource.Build(compiled.DNSArtifact, snapshot, keys, now)
	if err != nil {
		t.Fatal(err)
	}
	checked = time.Now().UTC()
	service.servingDNSProbes.digest = ""
	payload, facts, err = service.observeDNSRouteSources(context.Background(), client, identity, candidate, payload, payload.routeSources, probe)
	if err != nil {
		t.Fatal(err)
	}
	for vi := range payload.Queries {
		for ri := range payload.Queries[vi].Records {
			r := &payload.Queries[vi].Records[ri]
			if r.Name == "app.example.test" && r.Type == "A" {
				r.AnswerPolicy.Availability = recovery
			}
		}
	}
	record := dnsServingCheckpoint{Schema: "fugue.dns.positive-checkpoint/v1", NodeID: "dns-a", GroupID: "cell-dns", Parent: compiled.ReleaseArtifact, Candidate: candidate, AppliedAt: now.Add(-time.Minute), Positive: true, RouteSources: payload.routeSources}
	old, err := buildDNSServingState(record, payload, routeID, "dns-a", "cell-dns", facts, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	query := new(dns.Msg)
	query.SetQuestion("app.example.test.", dns.TypeA)
	if a := old.answer(query, "", time.Now()); a.Rcode != dns.RcodeSuccess {
		t.Fatal("renewed source baseline not serving", a)
	}
	checked = time.Now().UTC()
	if scenario == "foreign-alias" {
		service.dnsProbeHistories.mu.Lock()
		for _, f := range old.facts {
			entry := service.dnsProbeHistories.values[f.ProbeID]
			entry.recoveryKey = "foreign-candidate-history"
			service.dnsProbeHistories.values[f.ProbeID] = entry
		}
		service.dnsProbeHistories.mu.Unlock()
	}
	if scenario == "restart" {
		service = NewService(service.Config, nil)
	}
	activeProbe := probe
	if scenario == "negative" {
		activeProbe = func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
			return routeprobe.Proof{}, errors.New("authenticated failure")
		}
	}
	if scenario == "foreign-proof" {
		activeProbe = func(ctx context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
			proof, err := probe(ctx, host, path, address, state, timeout)
			binding := *proof.TrafficRelease
			binding.ReleaseID = "unapproved"
			proof.TrafficRelease = &binding
			return proof, err
		}
	}
	var refreshed *dnsServingState
	if scenario == "assignment-race" {
		local := collectDNSReadinessFacts(context.Background(), old.payload.Plan, old.payload.Policy.DNSReadiness, activeProbe, service.retainedDNSProbes.shareHistory(&service.dnsProbeHistories))
		refreshed = service.refreshRetainedDNSFactsFromObservations(old, old.payload.Plan, local, time.Now())
		if refreshed == nil {
			t.Fatal("fresh exact retained proof was not incorporated")
		}
	} else if scenario == "assignment-race-delayed-observation" {
		local := collectDNSReadinessFacts(context.Background(), old.payload.Plan, old.payload.Policy.DNSReadiness, activeProbe, service.retainedDNSProbes.shareHistory(&service.dnsProbeHistories))
		refreshAt := time.Now().UTC()
		barriers := make(map[string]time.Time, len(local))
		service.dnsProbeHistories.mu.Lock()
		for i := range local {
			key := old.payload.sourceRecoveryKeys[local[i].ProbeID]
			entry := service.dnsProbeHistories.values[key]
			// Simulate an overlapping scheduler completing a later successful
			// probe before the exact retained observation below is consumed.
			newerAt := local[i].Proof.CheckedAt.Add(time.Millisecond)
			entry.successes = append(slices.Clone(entry.successes), local[i].Proof.CheckedAt, newerAt)
			barriers[key] = newerAt.Add(time.Millisecond)
			if barriers[key].After(refreshAt) {
				refreshAt = barriers[key].Add(time.Millisecond)
			}
			if newerAt.After(entry.observedAt) {
				entry.observedAt = newerAt
			}
			service.dnsProbeHistories.values[key] = entry
			if after := newerAt.Add(time.Second); after.After(refreshAt) {
				refreshAt = after
			}
		}
		service.dnsProbeHistories.mu.Unlock()
		refreshed = service.refreshRetainedDNSFactsFromObservations(old, old.payload.Plan, local, refreshAt)
		if refreshed == nil {
			t.Fatal("fresh exact delayed retained proof was not incorporated")
		}
		query := new(dns.Msg)
		query.SetQuestion("app.example.test.", dns.TypeA)
		if answer := refreshed.answer(query, "", refreshAt); answer.Rcode != dns.RcodeSuccess || len(answer.Answer) == 0 {
			t.Fatal("valid chronological history prefix did not keep DNS serving", answer)
		}
		for i := range local {
			failed := local[i]
			failed.Ready, failed.Reason = false, "probe_unavailable"
			failed.Proof.CheckedAt = barriers[old.payload.sourceRecoveryKeys[failed.ProbeID]]
			service.retainedDNSProbes.shareHistory(&service.dnsProbeHistories).observeProbeHistoryKey(old.payload.sourceRecoveryKeys[failed.ProbeID], &failed, old.payload.Policy.DNSReadiness, failed.Proof.CheckedAt)
		}
		afterFailure := service.refreshRetainedDNSFactsFromObservations(old, old.payload.Plan, local, refreshAt.Add(time.Second))
		if afterFailure == nil || afterFailure.answer(query, "", refreshAt.Add(time.Second)).Rcode != dns.RcodeServerFailure {
			t.Fatal("delayed retained success crossed an intervening negative observation")
		}
		return
	} else {
		refreshed, err = service.refreshedDNSServingFacts(context.Background(), old, activeProbe, "control_plane_unavailable", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if scenario == "negative" || scenario == "foreign-proof" || scenario == "restart" {
		if a := refreshed.answer(query, "", time.Now()); a.Rcode != dns.RcodeServerFailure {
			t.Fatal("retained history bypassed negative, unauthorized or restarted proof", a)
		}
		return
	}
	if a := refreshed.answer(query, "", time.Now()); a.Rcode != dns.RcodeSuccess || len(a.Answer) == 0 {
		t.Fatal("local retained refresh replaced authorized recovery with new raw probe history", a)
	}
	for i, f := range refreshed.facts {
		if !dnsAvailabilityRecovered(recovery, f, time.Minute, time.Now()) || f.HistoryObservation == nil || f.HistoryObservation.KeyDigest != old.facts[i].HistoryObservation.KeyDigest {
			t.Fatal("retained proof lost its own authorized history", f.HistoryObservation)
		}
	}
	// New internal history keys are not trusted from checkpoints: replay derives
	// them from the signed DNS inputs and snapshot, independently of the cache.
	restored, _, restoreErr := service.verifiedDNSCheckpointPayload(old.record)
	if restoreErr != nil || !reflect.DeepEqual(restored.sourceRecoveryKeys, old.payload.sourceRecoveryKeys) {
		t.Fatal("restart did not rederive exact history identities", restoreErr)
	}
	if !refreshed.record.AppliedAt.Equal(old.record.AppliedAt) {
		t.Fatal("retained recovery renewed checkpoint")
	}
}
