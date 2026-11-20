package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/dnsroutesource"
	"fugue/internal/dnsroutewire"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeprobe"
	"fugue/internal/testfixture/celldns"
)

func TestIsolatedDNSShadowRevalidatesSourceBetweenRecoveringSweeps(t *testing.T) {
	req, policies, activations := celldns.SourceAuthorizedRequest(t, true)
	celldns.RetainGlobalOnly(&req)
	compiled := celldns.Compile(t, req)
	keys := celldns.Keys()
	now := time.Now().UTC()
	snapshot := celldns.SourceSnapshot(t, req, compiled.DNSArtifact, policies, activations, now)
	assignment := model.PlatformConsumerAssignment{
		ArtifactID: compiled.DNSArtifact.ID, ArtifactKind: compiled.DNSArtifact.ArtifactKind, ArtifactReleaseID: "shadow",
		ReleaseSetID: compiled.ReleaseArtifact.ID, ExpectedConsumerSetID: "dns-expected", Revision: 1, ScopeKey: compiled.DNSArtifact.ScopeKey,
		ExpectedGeneration: compiled.DNSArtifact.Generation, GenerationSequence: compiled.DNSArtifact.GenerationSequence,
		ContentHash: compiled.DNSArtifact.ContentHash, FencingToken: 1, ReleaseChannel: "shadow",
	}
	release := model.PlatformArtifactRelease{
		ID: assignment.ArtifactReleaseID, ArtifactID: assignment.ReleaseSetID, ArtifactKind: compiled.ReleaseArtifact.ArtifactKind,
		ScopeKey: assignment.ScopeKey, Generation: compiled.ReleaseArtifact.Generation, ReleaseChannel: "shadow", FencingToken: assignment.FencingToken,
		Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now,
	}
	candidate := dnsPlatformCandidate{Artifact: compiled.DNSArtifact, Assignment: assignment, Release: release}
	parent := NewService(config.DNSConfig{DNSNodeID: "dns-a", EdgeGroupID: "cell-dns", PlatformScopeKey: assignment.ScopeKey,
		Zone: "example.test", CachePath: filepath.Join(t.TempDir(), "dns"), BundleSigningKey: keys.PrimaryKey, BundleSigningKeyID: keys.PrimaryKeyID}, nil)
	parent.PlatformTokenFile = "unused"
	worker := parent.newIsolatedDNSShadowValidator()
	_, payload, err := worker.decodePlatformDNSCandidate(candidate, assignment)
	if err != nil {
		t.Fatal(err)
	}
	payload, _, err = worker.verifyDecodedDNSRelease(compiled.ReleaseArtifact, candidate, false, payload)
	if err != nil {
		t.Fatal(err)
	}
	plans, err := dnsroutesource.Build(compiled.DNSArtifact, snapshot, keys, now)
	if err != nil {
		t.Fatal(err)
	}
	var reads, bodies, probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := reads.Add(1)
		response := model.PlatformConsumerDNSRouteSourcesResponse{Assignment: assignment, Release: release, Snapshot: snapshot}
		if n >= 6 {
			// A new expected-set revision is not the previous candidate,
			// even when the underlying DNS artifact and source bytes match.
			response.Assignment.Revision++
		}
		response.Snapshot.ObservedAt = time.Now().UTC()
		validator, err := dnsroutewire.Validator(response, nil)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		got := r.Header.Get("If-None-Match")
		if (n == 1 || n == 6) && got != "" || n != 1 && n != 6 && got != validator {
			t.Errorf("unexpected cache validator on read %d: %q", n, got)
		}
		if got != "" && got == validator {
			w.Header().Set("ETag", validator)
			w.Header().Set(dnsroutewire.ObservedAtHeader, response.Snapshot.ObservedAt.Format(time.RFC3339Nano))
			w.WriteHeader(http.StatusNotModified)
			return
		}
		bodies.Add(1)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	client := platformconsumer.Client{BaseURL: server.URL}
	id := platformconsumer.Identity{Token: "test"}
	probe := func(_ context.Context, host, path, address, state string, _ time.Duration) (routeprobe.Proof, error) {
		probes.Add(1)
		for _, p := range plans.Probes {
			if p.Hostname == host && p.Path == path && p.Address == address {
				checked := time.Now().UTC()
				return routeprobe.Proof{EdgeID: p.EdgeID, GroupID: p.PreviousAuthority.EdgeGroupID, Digest: p.PreviousAuthority.RouteDigest,
					Version: "observed", State: state, TrafficRelease: plans.Bindings[p.CellPublicationDigest], CheckedAt: checked, ValidUntil: checked.Add(time.Minute)}, nil
			}
		}
		return routeprobe.Proof{}, errors.New("unexpected physical probe")
	}
	unrecovered := errors.New("signed recovery not reached")
	err = worker.observeShadowDNSRouteSources(t.Context(), client, id, candidate, payload, probe, func(dnsServingPayload, []dnsReadinessFact) error { return unrecovered })
	if !errors.Is(err, unrecovered) || reads.Load() != 2 || bodies.Load() != 1 || probes.Load() == 0 {
		t.Fatalf("first sweep: %v; reads=%d bodies=%d probes=%d", err, reads.Load(), bodies.Load(), probes.Load())
	}
	cursor := worker.Config.CachePath + ".platform-source-cursor.json"
	if _, err := os.Stat(cursor); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unrecovered shadow wrote cursor", err)
	}
	firstProbes := probes.Load()
	evaluate := func(_ dnsServingPayload, facts []dnsReadinessFact) error {
		if len(facts) == 0 || probes.Load() <= firstProbes {
			return errors.New("shadow reused physical evidence in place of observing")
		}
		return nil
	}
	err = worker.observeShadowDNSRouteSources(t.Context(), client, id, candidate, payload, probe, evaluate)
	if err != nil || reads.Load() != 5 || bodies.Load() != 1 {
		t.Fatalf("second sweep did not revalidate each source read: %v; reads=%d bodies=%d", err, reads.Load(), bodies.Load())
	}
	candidate.Assignment.Revision++
	err = worker.observeShadowDNSRouteSources(t.Context(), client, id, candidate, payload, probe, evaluate)
	if err != nil || reads.Load() != 8 || bodies.Load() != 2 || parent.shadowDNSRouteSource != nil {
		t.Fatalf("new assignment did not fetch its own closure: %v; reads=%d bodies=%d", err, reads.Load(), bodies.Load())
	}
}

func TestIsolatedDNSShadowDefersOnlyAfterExactServingAdvance(t *testing.T) {
	now := time.Now().UTC()
	shadow := dnsPlatformCandidate{
		Assignment: model.PlatformConsumerAssignment{ReleaseSetID: "set", ArtifactID: "dns", ContentHash: "hash", ScopeKey: "authority-cell:cell-dns", ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ReleaseChannel: "shadow", ArtifactReleaseID: "shadow", FencingToken: 19, Revision: 2},
		Release:    model.PlatformArtifactRelease{ID: "shadow", ReleasedAt: now},
	}
	serving := shadow
	serving.Assignment.ArtifactReleaseID, serving.Assignment.ReleaseChannel, serving.Assignment.FencingToken = "serving", "full", 3
	serving.Release.ID, serving.Release.ReleasedAt = "serving", now.Add(time.Minute)
	for _, tc := range []struct {
		name       string
		adjust     func(*dnsPlatformCandidate)
		remoteDiff bool
		checks     int32
		deferProbe bool
	}{
		{name: "newer exact full", checks: 1, deferProbe: true},
		{name: "newer exact gray", adjust: func(c *dnsPlatformCandidate) { c.Assignment.ReleaseChannel = "gray" }, checks: 1, deferProbe: true},
		{name: "no longer selected after rollback", remoteDiff: true, checks: 1},
		{name: "different release set", adjust: func(c *dnsPlatformCandidate) { c.Assignment.ReleaseSetID = "other-set" }},
		{name: "different content", adjust: func(c *dnsPlatformCandidate) { c.Assignment.ContentHash = "other-hash" }},
		{name: "reissued shadow is newer", adjust: func(c *dnsPlatformCandidate) { c.Release.ReleasedAt = now.Add(-time.Minute) }},
		{name: "ambiguous order", adjust: func(c *dnsPlatformCandidate) { c.Release.ReleasedAt = now }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := serving
			if tc.adjust != nil {
				tc.adjust(&candidate)
			}
			remote := candidate.Assignment
			if tc.remoteDiff {
				remote.ArtifactReleaseID = "replacement"
			}
			var checks atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				checks.Add(1)
				if r.URL.Path != "/v1/platform-state/consumers/assignment" || r.URL.Query().Get("serving_only") != "true" {
					t.Error("shadow advance must check current serving selection")
				}
				json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{remote}})
			}))
			defer server.Close()
			parent := &Service{}
			parent.platformServing.Store(&dnsServingState{record: dnsServingCheckpoint{Positive: true, Candidate: candidate}})
			worker := &Service{shadowValidationParent: parent}
			got := worker.dnsShadowAlreadyAdvanced(t.Context(), platformconsumer.Client{BaseURL: server.URL}, platformconsumer.Identity{Token: "fixture"}, shadow)
			if got != tc.deferProbe || checks.Load() != tc.checks {
				t.Fatalf("defer=%v checks=%d, want defer=%v checks=%d", got, checks.Load(), tc.deferProbe, tc.checks)
			}
		})
	}
}

func TestIsolatedDNSShadowSucceedsWhileServingLockedAndPreservesCheckpoint(t *testing.T) {
	req := celldns.Request(t)
	req.Intent.DNS = []platformconfig.DNSIntent{{Hostname: "static.example.test", Type: "A", Values: []string{"192.0.2.10"}, TTL: 60, Status: "active"}}
	req.Policy.DNSAnswerRules = nil
	req.Policy.EdgeSelectionConstraints = nil
	req.RuntimeSnapshot.DNSSelections = nil
	c := celldns.Compile(t, req)
	keys := celldns.Keys()
	now := time.Now().UTC()
	release := model.PlatformArtifactRelease{ID: "shadow-release", ArtifactID: c.ReleaseArtifact.ID, ArtifactKind: c.ReleaseArtifact.ArtifactKind, ScopeKey: c.ReleaseArtifact.ScopeKey, Generation: c.ReleaseArtifact.Generation, ReleaseChannel: "shadow", FencingToken: 2, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now}
	assignment := model.PlatformConsumerAssignment{ArtifactID: c.DNSArtifact.ID, ArtifactKind: c.DNSArtifact.ArtifactKind, ArtifactReleaseID: release.ID, ReleaseSetID: c.ReleaseArtifact.ID, ExpectedConsumerSetID: "shadow-set", Revision: 1, ScopeKey: c.DNSArtifact.ScopeKey, ExpectedGeneration: c.DNSArtifact.Generation, GenerationSequence: c.DNSArtifact.GenerationSequence, ContentHash: c.DNSArtifact.ContentHash, FencingToken: 2, ReleaseChannel: "shadow"}
	var rejected atomic.Bool
	observed := make(chan struct{}, 8)
	var reports atomic.Int64
	var sequence atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/platform-state/consumers/identity":
			json.NewEncoder(w).Encode(map[string]any{"token": "scoped", "expires_at": time.Now().Add(time.Minute), "component": "dns-server", "node_id": "dns-a", "authority_id": "cell-dns", "consumer_id": "dns-server:cell-dns:dns-a", "scope_key": assignment.ScopeKey, "artifact_kinds": []string{assignment.ArtifactKind}})
		case "/v1/platform-state/consumers/assignment":
			if r.URL.Query().Get("serving_only") == "true" {
				t.Error("shadow read serving assignment")
			}
			json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{assignment}})
		case "/v1/platform-state/consumers/artifacts/dns-child":
			json.NewEncoder(w).Encode(dnsPlatformCandidate{Artifact: c.DNSArtifact, Assignment: assignment, Release: release})
		case "/v1/platform-state/consumers/artifacts/dns-parent":
			json.NewEncoder(w).Encode(dnsPlatformCandidate{Artifact: c.ReleaseArtifact, Assignment: assignment, Release: release})
		case "/v1/platform-state/consumers/shadow-heartbeat":
			var h platformcontrol.PlatformConsumerHeartbeatEnvelope
			if json.NewDecoder(r.Body).Decode(&h) != nil {
				t.Error("invalid heartbeat")
				w.WriteHeader(400)
				return
			}
			if h.ApplyStatus != "staged" || h.ProbeStatus != "shadow_validated" || h.ActualGeneration != "old-serving" || h.Sequence <= sequence.Load() {
				t.Error("shadow rewrote serving or replayed cursor")
			}
			sequence.Store(h.Sequence)
			reports.Add(1)
			observed <- struct{}{}
			if rejected.Load() {
				w.WriteHeader(503)
				return
			}
			json.NewEncoder(w).Encode(model.PlatformConsumerHeartbeatResponse{Consumer: model.PlatformConsumerInstance{IdentityVerified: true, ConsumerID: h.ConsumerID, Sequence: h.Sequence, ExpectedConsumerSetID: h.ExpectedConsumerSetID, EvidenceHash: h.EvidenceHash}})
		default:
			t.Errorf("unexpected shadow endpoint %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	dir := t.TempDir()
	s := NewService(config.DNSConfig{APIURL: server.URL, DNSNodeID: "dns-a", EdgeGroupID: "cell-dns", PlatformScopeKey: assignment.ScopeKey, Zone: "example.test", CachePath: filepath.Join(dir, "dns"), BundleSigningKey: keys.PrimaryKey, BundleSigningKeyID: keys.PrimaryKeyID}, log.New(io.Discard, "", 0))
	s.PlatformTokenFile = filepath.Join(dir, "token")
	if err := os.WriteFile(s.PlatformTokenFile, []byte("pod"), 0600); err != nil {
		t.Fatal(err)
	}
	s.platformServingBound.Store(true)
	old := &dnsServingState{record: dnsServingCheckpoint{Positive: true, AppliedAt: now, Candidate: dnsPlatformCandidate{Artifact: model.PlatformArtifact{Generation: "old-serving"}}}, payload: dnsServingPayload{Plan: &platformconfig.DNSReadinessPlan{}, Policy: platformconfig.PolicySnapshot{MaxStaleSeconds: 3600, DNSReadiness: &platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 30, MaxConcurrency: 1}}}}
	s.platformServing.Store(old)
	s.platformServingError = "retained source unavailable"
	retained := map[string][]byte{}
	for _, suffix := range []string{".platform-serving.json", ".platform-source-cursor.json", ".platform-shadow.json"} {
		path := s.Config.CachePath + suffix
		retained[path] = []byte("retained-state" + suffix)
		if err := os.WriteFile(path, retained[path], 0600); err != nil {
			t.Fatal(err)
		}
	}
	worker := s.newIsolatedDNSShadowValidator()
	if worker == nil || worker.Config.CachePath == s.Config.CachePath || worker.platformServing.Load() != nil {
		t.Fatal("not isolated")
	}
	s.platformConsumerMu.Lock()
	done := make(chan struct{})
	go func() { defer close(done); s.syncIsolatedDNSShadowOnce(context.Background(), worker) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		s.platformConsumerMu.Unlock()
		t.Fatal("serving synchronization blocked shadow")
	}
	s.platformConsumerMu.Unlock()
	if reports.Load() != 1 || s.Status().PlatformCandidate.State != "shadow_verified" {
		t.Fatal("successor not verified", s.Status().PlatformCandidate)
	}
	before := s.platformServing.Load()
	rejected.Store(true)
	s.syncIsolatedDNSShadowOnce(context.Background(), worker)
	if reports.Load() != 2 || s.Status().PlatformCandidate.State != "failed" || s.platformServing.Load() != before || s.platformServingError != "retained source unavailable" {
		t.Fatal("rejected shadow changed serving")
	}
	rejected.Store(false)
	restarted := s.newIsolatedDNSShadowValidator()
	s.syncIsolatedDNSShadowOnce(context.Background(), restarted)
	if reports.Load() != 3 || s.Status().PlatformCandidate.State != "shadow_verified" {
		t.Fatal("shadow cursor did not survive restart", s.Status().PlatformCandidate)
	}
	for len(observed) > 0 {
		<-observed
	}
	s.platformConsumerMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() { defer close(loopDone); s.runPlatformShadowConsumer(ctx) }()
	select {
	case <-observed:
	case <-time.After(3 * time.Second):
		cancel()
		s.platformConsumerMu.Unlock()
		t.Fatal("production loop did not validate successor while serving blocked")
	}
	cancel()
	s.platformConsumerMu.Unlock()
	select {
	case <-loopDone:
	case <-time.After(3 * time.Second):
		t.Fatal("isolated validator did not stop with parent")
	}
	if !s.isolatedShadowStarted.Load() {
		t.Fatal("production loop retained legacy shared validation")
	}
	for path, want := range retained {
		got, err := os.ReadFile(path)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("shadow changed retained file", path, err)
		}
	}
}

func TestIsolatedDNSShadowSourceCursorCannotAdvanceServingCursor(t *testing.T) {
	req, policies, activations := celldns.SourceAuthorizedRequest(t, true)
	c := celldns.Compile(t, req)
	keys := celldns.Keys()
	snapshot := celldns.SourceSnapshot(t, req, c.DNSArtifact, policies, activations, time.Now().UTC())
	s := NewService(config.DNSConfig{DNSNodeID: "dns-a", EdgeGroupID: "cell-dns", PlatformScopeKey: platformconfig.AuthorityCellScope("cell-dns"), CachePath: filepath.Join(t.TempDir(), "dns"), BundleSigningKey: keys.PrimaryKey, BundleSigningKeyID: keys.PrimaryKeyID}, log.New(io.Discard, "", 0))
	s.PlatformTokenFile = "unused"
	if err := s.advanceDNSRouteCursor(snapshot); err != nil {
		t.Fatal(err)
	}
	path := s.Config.CachePath + ".platform-source-cursor.json"
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(snapshot)
	var next model.PlatformDNSRouteSourceSnapshot
	if json.Unmarshal(raw, &next) != nil {
		t.Fatal("snapshot copy")
	}
	for i := range next.Scopes {
		for j := range next.Scopes[i].Lanes {
			next.Scopes[i].Lanes[j].FencingToken++
			next.Scopes[i].Lanes[j].Version++
		}
	}
	next.SelectionDigest, _ = dnsroutesource.SelectionDigest(next)
	worker := s.newIsolatedDNSShadowValidator()
	if err := worker.advanceDNSRouteCursor(next); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("candidate advanced serving source cursor", err)
	}
	if err := s.advanceDNSRouteCursor(snapshot); err != nil {
		t.Fatal("shadow prevented serving refresh", err)
	}
	if err := worker.advanceDNSRouteCursor(snapshot); err == nil {
		t.Fatal("candidate source cursor rollback accepted")
	}
}
