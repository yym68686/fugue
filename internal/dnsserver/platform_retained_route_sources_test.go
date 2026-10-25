package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/dnsfacts"
	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
	"fugue/internal/routeprobe"
	"fugue/internal/testfixture/celldns"
	"github.com/miekg/dns"
)

// Exercise the actual sync/rejection/fallback path: a valid DNS successor has
// no ready source observation yet, while the independently authorized global
// producer has advanced. The positive old DNS must retain its own authority.
func TestPendingDNSSuccessorDoesNotStrandRetainedSource(t *testing.T) {
	for _, scenario := range []string{"authorized", "delayed candidate", "independent renewal", "selected renewal", "selected renewal race", "raced source before candidate", "raced source during fallback", "revoked grant", "corrupt grant", "expired grant", "changed grant", "unapproved producer", "negative proof"} {
		t.Run(scenario, func(t *testing.T) { testPendingDNSSuccessorRetainedSource(t, scenario) })
	}
}

func testPendingDNSSuccessorRetainedSource(t *testing.T, scenario string) {
	req, policies, activations := celldns.SourceAuthorizedRequest(t, true)
	celldns.RetainGlobalOnly(&req)
	compiled := celldns.Compile(t, req)
	now := time.Now().UTC()
	keys := celldns.Keys()
	release := model.PlatformArtifactRelease{ID: "dns-full", ArtifactID: compiled.ReleaseArtifact.ID, ArtifactKind: compiled.ReleaseArtifact.ArtifactKind, ScopeKey: compiled.ReleaseArtifact.ScopeKey, Generation: compiled.ReleaseArtifact.Generation, ReleaseChannel: "full", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now.Add(-time.Minute)}
	assignment := model.PlatformConsumerAssignment{ArtifactID: compiled.DNSArtifact.ID, ArtifactKind: compiled.DNSArtifact.ArtifactKind, ArtifactReleaseID: release.ID, ReleaseSetID: compiled.ReleaseArtifact.ID, ExpectedConsumerSetID: "dns-expected", Revision: 1, ScopeKey: compiled.DNSArtifact.ScopeKey, ExpectedGeneration: compiled.DNSArtifact.Generation, GenerationSequence: compiled.DNSArtifact.GenerationSequence, ContentHash: compiled.DNSArtifact.ContentHash, FencingToken: 1, ReleaseChannel: "full"}
	oldCandidate := dnsPlatformCandidate{Artifact: compiled.DNSArtifact, Assignment: assignment, Release: release}
	parent, candidate := compiled.ReleaseArtifact, oldCandidate
	snapshot := celldns.SourceSnapshot(t, req, compiled.DNSArtifact, policies, activations, now)
	plans, err := dnsroutesource.Build(compiled.DNSArtifact, snapshot, keys, now)
	if err != nil {
		t.Fatal(err)
	}
	lkg, err := platformsafety.SignPlatformLKGSnapshot(model.PlatformLKGSnapshot{ID: "dns-lkg", ArtifactID: parent.ID, ArtifactKind: parent.ArtifactKind, Scope: parent.Scope, ScopeKey: parent.ScopeKey, SchemaVersion: parent.SchemaVersion, Generation: parent.Generation, GenerationSequence: parent.GenerationSequence, ContentHash: parent.ContentHash, ArtifactProvenance: parent.Provenance, VerifiedByReleaseID: release.ID, VerificationEvidenceHash: "sha256:" + strings.Repeat("a", 64), ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now}, keys)
	if err != nil {
		t.Fatal(err)
	}
	consumerID, _ := platformcontrol.PlatformConsumerID(model.PlatformConsumerComponentDNSServer, "dns-a", "cell-dns")
	independent := scenario == "independent renewal"
	delayed := scenario == "delayed candidate" || scenario == "raced source before candidate" || independent
	var longRenewalLease atomic.Bool
	fallbackRace := scenario == "raced source during fallback"
	var sourceChanged atomic.Bool
	var staleSourceProbes atomic.Int32
	var selectedRenewal atomic.Bool
	var renewalObserving atomic.Bool
	var renewalSourceReads atomic.Int32
	pending, retainedReads := false, 0
	blockedCandidate, releaseCandidate := make(chan struct{}), make(chan struct{})
	var signalOnce, unblockOnce sync.Once
	unblock := func() { unblockOnce.Do(func() { close(releaseCandidate) }) }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if renewalObserving.Load() && strings.Contains(r.URL.Path, "/artifacts/") && !strings.HasSuffix(r.URL.Path, "/route-sources") && !strings.HasSuffix(r.URL.Path, "/retained-route-sources") {
			t.Error("independent renewal downloaded a candidate artifact")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		switch {
		case r.URL.Path == "/v1/platform-state/consumers/identity":
			json.NewEncoder(w).Encode(map[string]any{"token": "test", "expires_at": time.Now().Add(time.Minute), "component": "dns-server", "node_id": "dns-a", "authority_id": "cell-dns", "consumer_id": consumerID, "scope_key": assignment.ScopeKey, "artifact_kinds": []string{assignment.ArtifactKind}})
		case r.URL.Path == "/v1/platform-state/consumers/assignment":
			if scenario == "selected renewal race" && selectedRenewal.Load() && renewalSourceReads.Load() >= 2 {
				json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{})
				return
			}
			json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{candidate.Assignment}})
		case strings.HasSuffix(r.URL.Path, "/retained-route-sources"):
			retainedReads++
			if fallbackRace && retainedReads > 5 {
				sourceChanged.Store(false)
			}
			if !pending || !strings.Contains(r.URL.Path, "/"+oldCandidate.Artifact.ID+"/") || r.URL.Query().Get("expected_consumer_set_id") != assignment.ExpectedConsumerSetID {
				t.Error("retained observation borrowed successor identity")
			}
			if scenario == "revoked grant" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			responseGrant := lkg
			if scenario == "corrupt grant" {
				responseGrant.SnapshotProvenance.Signature = "invalid"
			}
			if scenario == "expired grant" {
				responseGrant.ExpiresAt = now.Add(-time.Minute)
				responseGrant, _ = platformsafety.SignPlatformLKGSnapshot(responseGrant, keys)
			}
			if scenario == "changed grant" && retainedReads%2 == 0 {
				responseGrant.ID = "replacement-lkg"
				responseGrant, _ = platformsafety.SignPlatformLKGSnapshot(responseGrant, keys)
			}
			responseSnapshot := snapshot
			if scenario == "raced source before candidate" && retainedReads == 2 {
				responseSnapshot.SelectionDigest = "sha256:" + strings.Repeat("f", 64)
			}
			if fallbackRace && retainedReads == 5 {
				// The pre-candidate retained refresh had three stable reads.
				// Its fallback's second read now detects a publication advance.
				responseSnapshot.SelectionDigest = "sha256:" + strings.Repeat("f", 64)
				sourceChanged.Store(true)
			}
			if scenario == "unapproved producer" {
				raw, _ := json.Marshal(snapshot)
				json.Unmarshal(raw, &responseSnapshot)
				responseSnapshot.Scopes[0].Publications[0].ProducerRelease.ID = "foreign-producer"
				responseSnapshot.SelectionDigest, _ = dnsroutesource.SelectionDigest(responseSnapshot)
			}
			verified := release
			verified.VerificationState, verified.VerifiedLKGGeneration = model.PlatformArtifactVerificationStateVerified, compiled.ReleaseArtifact.Generation
			verified.VerifiedAt = &now
			json.NewEncoder(w).Encode(map[string]any{"assignment": assignment, "release": verified, "snapshot": responseSnapshot, "retained_lkg": responseGrant})
		case strings.HasSuffix(r.URL.Path, "/route-sources"):
			if selectedRenewal.Load() {
				renewalSourceReads.Add(1)
			}
			if pending {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			json.NewEncoder(w).Encode(model.PlatformConsumerDNSRouteSourcesResponse{Assignment: assignment, Release: release, Snapshot: snapshot})
		case r.URL.Path == "/v1/platform-state/consumers/artifacts/"+candidate.Artifact.ID:
			json.NewEncoder(w).Encode(candidate)
		case r.URL.Path == "/v1/platform-state/consumers/artifacts/"+parent.ID:
			if pending && delayed {
				signalOnce.Do(func() { close(blockedCandidate) })
				<-releaseCandidate
			}
			json.NewEncoder(w).Encode(map[string]any{"artifact": parent, "assignment": candidate.Assignment, "release": candidate.Release})
		case r.URL.Path == "/v1/platform-state/consumers/trusted-heartbeat":
			var h platformcontrol.PlatformConsumerHeartbeatEnvelope
			json.NewDecoder(r.Body).Decode(&h)
			if pending && h.ProbeStatus == "passed" {
				t.Error("retained source refresh acknowledged rejected successor")
			}
			json.NewEncoder(w).Encode(model.PlatformConsumerHeartbeatResponse{Consumer: model.PlatformConsumerInstance{IdentityVerified: true, ConsumerID: h.ConsumerID, Sequence: h.Sequence, EvidenceHash: h.EvidenceHash, ExpectedConsumerSetID: h.ExpectedConsumerSetID}})
		default:
			t.Error("unexpected request", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	defer unblock()
	dir := t.TempDir()
	s := NewService(config.DNSConfig{APIURL: server.URL, DNSNodeID: "dns-a", EdgeGroupID: "cell-dns", PlatformScopeKey: assignment.ScopeKey, Zone: "example.test", CachePath: filepath.Join(dir, "dns"), BundleSigningKey: keys.PrimaryKey, BundleSigningKeyID: keys.PrimaryKeyID}, nil)
	s.PlatformTokenFile = filepath.Join(dir, "token")
	if err := os.WriteFile(s.PlatformTokenFile, []byte("pod-token"), 0600); err != nil {
		t.Fatal(err)
	}
	probe := func(_ context.Context, host, path, address, state string, _ time.Duration) (routeprobe.Proof, error) {
		if pending && scenario == "negative proof" {
			return routeprobe.Proof{}, errors.New("authenticated negative proof")
		}
		for _, p := range plans.Probes {
			if p.Hostname == host && p.Path == path && p.Address == address {
				group, digest := p.EdgeGroupID, p.RouteDigest
				if p.PreviousAuthority != nil {
					group, digest = p.PreviousAuthority.EdgeGroupID, p.PreviousAuthority.RouteDigest
				}
				until := time.Now().Add(30 * time.Second)
				if independent && pending && !longRenewalLease.Load() {
					until = time.Now().Add(2 * time.Second)
				}
				proof := routeprobe.Proof{EdgeID: p.EdgeID, GroupID: group, Digest: digest, Version: "observed", State: state, TrafficRelease: plans.Bindings[p.CellPublicationDigest], CheckedAt: time.Now(), ValidUntil: until}
				if fallbackRace && sourceChanged.Load() {
					staleSourceProbes.Add(1)
					binding := *proof.TrafficRelease
					binding.ReleaseID = "source-after-observed-race"
					proof.TrafficRelease = &binding
				}
				return proof, nil
			}
		}
		return routeprobe.Proof{}, errors.New("unexpected probe")
	}
	if err := s.syncPlatformDNSServingOnce(context.Background(), probe, probeDNSServingSnapshot); err != nil {
		t.Fatal("baseline", err)
	}
	old := s.platformServing.Load()
	question := new(dns.Msg)
	question.SetQuestion("app.example.test.", dns.TypeA)
	if a := old.answer(question, "", time.Now()); a.Rcode != dns.RcodeSuccess || len(a.Answer) == 0 {
		t.Fatal("baseline not serving", a)
	}
	checkpointBefore, err := os.ReadFile(s.Config.CachePath + ".platform-serving.json")
	if err != nil {
		t.Fatal(err)
	}
	if scenario == "selected renewal" || scenario == "selected renewal race" {
		selectedRenewal.Store(true)
		worker := s.newDNSRenewalWorker()
		s.platformConsumerMu.Lock()
		renewalObserving.Store(true)
		err := s.observeDNSRenewal(context.Background(), worker, probe)
		renewalObserving.Store(false)
		s.platformConsumerMu.Unlock()
		if scenario == "selected renewal race" {
			if !errors.Is(err, platformconsumer.ErrAssignmentChanged) || s.currentDNSServingState() != old {
				t.Fatal("deselected publication acquired new serving facts", err)
			}
		} else if err != nil || s.currentDNSServingState() == old || s.platformServing.Load() != old {
			t.Fatal("selected authority did not independently renew", err)
		}
		after, err := os.ReadFile(s.Config.CachePath + ".platform-serving.json")
		if err != nil || string(after) != string(checkpointBefore) {
			t.Fatal("selected renewal changed publication checkpoint", err)
		}
		return
	}
	// A different signed DNS candidate is not yet ready. Its source endpoint
	// cannot authorize old DNS, even when it happens to share the same policy.
	req.Intent.Generation += "-successor"
	next := celldns.Compile(t, req)
	next.DNSArtifact = celldns.Sign(t, next.DNSArtifact, "dns-successor", 2)
	parent = celldns.Sign(t, platformconfig.BuildReleaseSetArtifact(next.ReleaseSet, []string{next.DNSArtifact.ID}, req.CreatedAt), "dns-parent-successor", 2)
	candidate = oldCandidate
	candidate.Artifact = next.DNSArtifact
	candidate.Release.ID, candidate.Release.ArtifactID, candidate.Release.Generation = "dns-gray", parent.ID, parent.Generation
	candidate.Release.ReleaseChannel, candidate.Release.ReleasedAt = "gray", now
	candidate.Release.CanaryRuleRef = "cohort=complete"
	candidate.Assignment.ArtifactID, candidate.Assignment.ContentHash, candidate.Assignment.ExpectedGeneration = next.DNSArtifact.ID, next.DNSArtifact.ContentHash, next.DNSArtifact.Generation
	candidate.Assignment.GenerationSequence = 2
	candidate.Assignment.ArtifactReleaseID, candidate.Assignment.ReleaseSetID, candidate.Assignment.ExpectedConsumerSetID = "dns-gray", parent.ID, "successor-expected"
	candidate.Assignment.ReleaseChannel = "gray"
	raw, _ := json.Marshal(snapshot)
	json.Unmarshal(raw, &snapshot)
	for i := range snapshot.Scopes {
		scope := &snapshot.Scopes[i]
		pub := &scope.Publications[0]
		pub.Release.ID += "-next"
		pub.Release.FencingToken++
		scope.Lanes[0].ActiveReleaseID, scope.Lanes[0].FencingToken = pub.Release.ID, pub.Release.FencingToken
		scope.Lanes[0].Version++
	}
	snapshot.ObservedAt = time.Now().UTC()
	snapshot.SelectionDigest, _ = dnsroutesource.SelectionDigest(snapshot)
	plans, err = dnsroutesource.Build(compiled.DNSArtifact, snapshot, keys, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	pending = true
	if fallbackRace {
		if err := s.syncPlatformDNSServing(context.Background(), probe, probeDNSServingSnapshot); err == nil {
			t.Fatal("pending successor unexpectedly applied")
		}
		if staleSourceProbes.Load() != 0 || retainedReads <= 5 {
			t.Fatal("known source race fell through to stale local probes instead of bounded authority retry", staleSourceProbes.Load(), retainedReads)
		}
	} else if delayed {
		expires := time.Now().Add(2 * time.Second)
		agingFacts := append([]dnsReadinessFact(nil), old.facts...)
		for i := range agingFacts {
			agingFacts[i].Proof.ValidUntil = expires
		}
		old, err = buildDNSServingState(old.record, old.payload, old.routeID, "dns-a", "cell-dns", agingFacts, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		s.platformServing.Store(old)
		if answer := old.answer(question, "", time.Now()); answer.Rcode != dns.RcodeSuccess {
			t.Fatal("aging baseline is not serving", answer)
		}
		done := make(chan error, 1)
		go func() { done <- s.syncPlatformDNSServing(context.Background(), probe, probeDNSServingSnapshot) }()
		select {
		case <-blockedCandidate:
		case <-time.After(5 * time.Second):
			t.Fatal("candidate did not reach blocked read")
		}
		if independent {
			loaded := s.platformServing.Load()
			saved, err := os.ReadFile(s.Config.CachePath + ".platform-serving.json")
			if err != nil {
				t.Fatal(err)
			}
			worker := s.newDNSRenewalWorker()
			if worker == nil {
				t.Fatal("independent renewer missing")
			}
			longRenewalLease.Store(true)
			renewalObserving.Store(true)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := s.observeDNSRenewal(ctx, worker, probe); err != nil {
				t.Fatal("candidate lock blocked independent renewal", err)
			}
			if s.platformServing.Load() != loaded || s.currentDNSServingState() == loaded {
				t.Fatal("renewal installed a configuration or failed to expose new facts")
			}
			deadline := loaded.facts[0].Proof.ValidUntil
			if wait := time.Until(deadline.Add(25 * time.Millisecond)); wait > 0 {
				time.Sleep(wait)
			}
			if a := loaded.answer(question, "", time.Now()); a.Rcode != dns.RcodeServerFailure {
				t.Fatal("blocked candidate fixture still has live original proofs")
			}
			if a := dnsQuery(t, s, "app.example.test", dns.TypeA); a.Rcode != dns.RcodeSuccess || len(a.Answer) == 0 {
				t.Fatal("actual query lost independent renewal while candidate remained blocked", a)
			}
			response := httptest.NewRecorder()
			s.handleRuntimeFacts(response, httptest.NewRequest(http.MethodGet, "/runtime-facts", nil))
			var exported dnsfacts.Snapshot
			if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &exported) != nil || !exported.Ready || !reflect.DeepEqual(exported.Assignment, loaded.record.Candidate.Assignment) ||
				!exported.CheckpointValidUntil.Equal(loaded.record.AppliedAt.Add(time.Duration(loaded.payload.Policy.MaxStaleSeconds)*time.Second)) {
				t.Fatal("runtime facts disagreed with renewed queries or changed configuration lease", response.Code)
			}
			after, err := os.ReadFile(s.Config.CachePath + ".platform-serving.json")
			if err != nil || string(after) != string(saved) || !s.currentDNSServingState().record.AppliedAt.Equal(loaded.record.AppliedAt) {
				t.Fatal("renewal changed publication checkpoint or its original lease", err)
			}
			negative := func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
				return routeprobe.Proof{}, errors.New("authenticated endpoint failure")
			}
			if err := s.observeDNSRenewal(context.Background(), worker, negative); err != nil {
				t.Fatal("negative observation not processed", err)
			}
			if a := dnsQuery(t, s, "app.example.test", dns.TypeA); a.Rcode != dns.RcodeServerFailure {
				t.Fatal("independent renewal masked real endpoint failure", a)
			}
			renewalObserving.Store(false)
			unblock()
			if err := <-done; err == nil {
				t.Fatal("blocked pending candidate unexpectedly applied")
			}
			return
		}
		// Query during the ongoing candidate read after the original proofs expire.
		// No candidate failure has returned, so deferred fallback cannot help.
		if wait := time.Until(expires.Add(20 * time.Millisecond)); wait > 0 {
			time.Sleep(wait)
		}
		answer := s.platformServing.Load().answer(question, "", time.Now())
		unblock()
		err := <-done
		if answer.Rcode != dns.RcodeSuccess || len(answer.Answer) == 0 {
			t.Fatal("candidate processing starved retained proof refresh before fallback", answer)
		}
		if err == nil {
			t.Fatal("delayed pending successor unexpectedly applied")
		}
	} else if err := s.syncPlatformDNSServingOnce(context.Background(), probe, probeDNSServingSnapshot); err == nil {
		t.Fatal("pending successor unexpectedly applied")
	}
	retained := s.platformServing.Load()
	if scenario != "authorized" && !delayed && !fallbackRace {
		if scenario == "changed grant" {
			// A control-plane observation race is not a negative physical proof.
			// Original authorized proofs may serve only to their unchanged deadline.
			if retained.record.RouteSources.Snapshot.SelectionDigest != old.record.RouteSources.Snapshot.SelectionDigest || !retained.record.AppliedAt.Equal(old.record.AppliedAt) {
				t.Fatal("raced grant changed retained authority")
			}
			if a := retained.answer(question, "", time.Now().Add(31*time.Second)); a.Rcode != dns.RcodeServerFailure {
				t.Fatal("raced grant extended original proof deadline", a)
			}
		} else if a := retained.answer(question, "", time.Now()); a.Rcode != dns.RcodeServerFailure {
			t.Fatal("invalid retained source authorization granted DNS", a)
		}
		checkpointAfter, _ := os.ReadFile(s.Config.CachePath + ".platform-serving.json")
		if string(checkpointBefore) != string(checkpointAfter) {
			t.Fatal("rejected retained source replaced positive checkpoint")
		}
		return
	}
	if a := retained.answer(question, "", time.Now()); a.Rcode != dns.RcodeSuccess || len(a.Answer) == 0 {
		t.Fatalf("pending successor stranded positive DNS on an authorized source advance: %s; facts=%+v", a, retained.facts)
	}
	if retainedReads < 2 {
		t.Fatal("retained source ownership was not independently rechecked")
	}
	if !reflect.DeepEqual(retained.record.Candidate, oldCandidate) || !retained.record.AppliedAt.Equal(old.record.AppliedAt) {
		t.Fatal("retained refresh changed DNS intent or extended checkpoint age")
	}
	if retained.record.RouteSources.Snapshot.SelectionDigest != snapshot.SelectionDigest {
		t.Fatal("retained configuration did not acquire its own current source context")
	}
	checkpointAfter, _ := os.ReadFile(s.Config.CachePath + ".platform-serving.json")
	var refreshedCheckpoint dnsServingCheckpoint
	if err := json.Unmarshal(checkpointAfter, &refreshedCheckpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := s.decodeDNSCheckpoint(checkpointAfter); err != nil {
		t.Fatal("retained source checkpoint was not re-signed", err)
	}
	if !reflect.DeepEqual(refreshedCheckpoint.Candidate, oldCandidate) || !refreshedCheckpoint.AppliedAt.Equal(old.record.AppliedAt) || refreshedCheckpoint.RouteSources.Snapshot.SelectionDigest != snapshot.SelectionDigest {
		t.Fatal("durable retained checkpoint lost DNS identity, age or refreshed source")
	}
	previousCheckpoint, err := os.ReadFile(s.Config.CachePath + ".platform-serving.json.previous")
	if err != nil || string(previousCheckpoint) != string(checkpointBefore) {
		t.Fatal("previous positive checkpoint was not retained")
	}
	restarted := NewService(s.Config, nil)
	restarted.PlatformTokenFile = s.PlatformTokenFile
	if loaded, err := restarted.loadDNSServingCache(); !loaded || err != nil {
		t.Fatal("retained source checkpoint did not survive restart", err)
	}
	if a := restarted.platformServing.Load().answer(question, "", time.Now()); a.Rcode != dns.RcodeServerFailure {
		t.Fatal("restart resurrected cached positive readiness")
	}
	if err := restarted.syncPlatformDNSServingOnce(context.Background(), probe, probeDNSServingSnapshot); err == nil {
		t.Fatal("restart acknowledged pending successor")
	}
	if a := restarted.platformServing.Load().answer(question, "", time.Now()); a.Rcode != dns.RcodeSuccess || len(a.Answer) == 0 {
		t.Fatal("retained DNS could not recover through its own source authority after restart", a)
	}
	if !restarted.platformServing.Load().record.AppliedAt.Equal(old.record.AppliedAt) {
		t.Fatal("restart renewed old configuration age")
	}
}

func TestRetainedDNSSourceProofDeadlineIncludesDNSLKG(t *testing.T) {
	deadline := time.Now().UTC().Add(time.Minute)
	context := &dnsroutesource.Context{RetainedDNSLKG: &model.PlatformLKGSnapshot{ExpiresAt: deadline}}
	binding := &model.TrafficReleaseBinding{ReleaseID: "active-route", ScopeKey: "global"}
	got := context.ProofDeadline(binding)
	if !got.Equal(deadline) {
		t.Fatal("retained DNS source proof is not bounded by its positive LKG", got)
	}
}
