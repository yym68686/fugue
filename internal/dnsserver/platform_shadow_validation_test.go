package dnsserver

import (
	"context"
	"encoding/json"
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
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/testfixture/celldns"
)

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
