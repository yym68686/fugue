package dnsserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/dnsfacts"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
)

func runtimeFactsFixture(t *testing.T) (*Service, *dnsServingState, time.Time) {
	t.Helper()
	parent, candidate := dnsServingFixture(t, true)
	s := NewService(config.DNSConfig{DNSNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test", CachePath: filepath.Join(t.TempDir(), "cache"), BundleSigningKey: "synthetic-dns-serving-secret", BundleSigningKeyID: "key"}, nil)
	payload, routeID, err := s.verifyDNSServingRelease(parent, candidate)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	facts := []dnsReadinessFact{}
	for _, requirement := range payload.Plan.Probes {
		proof := routeprobe.Proof{Digest: requirement.RouteDigest, EdgeID: requirement.EdgeID, GroupID: requirement.EdgeGroupID, State: requirement.State,
			Version: "observed-serving", CheckedAt: now, ValidUntil: now.Add(30 * time.Second),
			TrafficRelease: &model.TrafficReleaseBinding{ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, RouteArtifactID: routeID,
				PolicyDigest: payload.Lineage.PolicyDigest, IntentDigest: payload.Lineage.IntentDigest, InputSnapshotDigest: payload.Lineage.InputSnapshotDigest,
				ReleaseID: candidate.Release.ID, ReleaseChannel: candidate.Release.ReleaseChannel, FencingToken: candidate.Release.FencingToken, ScopeKey: "global"}}
		facts = append(facts, dnsReadinessFact{ProbeID: requirement.ID, Ready: true, Proof: proof, Reason: "probe error with https://private.example.test and private-token"})
	}
	checkpoint := dnsServingCheckpoint{Schema: "fugue.dns.positive-checkpoint/v1", NodeID: "dns-a", GroupID: "edge-group-a", Parent: parent, Candidate: candidate, AppliedAt: now, Positive: true}
	st, err := buildDNSServingState(checkpoint, payload, routeID, "dns-a", "edge-group-a", facts, now)
	if err != nil {
		t.Fatal(err)
	}
	s.platformServing.Store(st)
	return s, st, now
}

func TestDNSRuntimeFactsNeverRenewEvidenceOrMutateServingState(t *testing.T) {
	_, st, now := runtimeFactsFixture(t)
	before, err := json.Marshal([]any{st.record, st.payload, st.facts, st.checkedAt})
	if err != nil {
		t.Fatal(err)
	}
	first, err := dnsRuntimeFacts(st, now)
	if err != nil || !first.Ready || len(first.Facts) == 0 {
		t.Fatal(first, err)
	}
	for _, f := range first.Facts {
		if !f.Ready {
			t.Fatal(f)
		}
	}
	later, err := dnsRuntimeFacts(st, now.Add(time.Second))
	if err != nil || !reflect.DeepEqual(first.Facts, later.Facts) || first.ObservedAt != later.ObservedAt || first.CheckpointValidUntil != later.CheckpointValidUntil {
		t.Fatal("read renewed original facts", later, err)
	}
	expired, err := dnsRuntimeFacts(st, now.Add(time.Minute))
	if err != nil || expired.Ready {
		t.Fatal("expired proofs remained ready", expired, err)
	}
	for i, f := range expired.Facts {
		if f.Ready || !reflect.DeepEqual(f.Proof, first.Facts[i].Proof) {
			t.Fatal("expired proof was renewed or erased", f)
		}
	}
	for _, scenario := range []string{"checkpoint expired", "foreign release", "future proof", "wrong digest", "duplicate", "negative"} {
		t.Run(scenario, func(t *testing.T) {
			copy := *st
			copy.facts = append([]dnsReadinessFact(nil), st.facts...)
			switch scenario {
			case "checkpoint expired":
				copy.record.AppliedAt = now.Add(-2 * time.Hour)
			case "foreign release":
				binding := *copy.facts[0].Proof.TrafficRelease
				binding.FencingToken++
				copy.facts[0].Proof.TrafficRelease = &binding
			case "future proof":
				copy.facts[0].Proof.CheckedAt = now.Add(time.Second)
			case "wrong digest":
				copy.facts[0].Proof.Digest = "sha256:" + strings.Repeat("0", 64)
			case "duplicate":
				copy.facts = append(copy.facts, copy.facts[0])
			case "negative":
				copy.facts[0].Ready = false
			}
			facts, err := dnsRuntimeFacts(&copy, now)
			if err != nil || facts.Ready {
				t.Fatal(scenario, facts, err)
			}
		})
	}
	after, _ := json.Marshal([]any{st.record, st.payload, st.facts, st.checkedAt})
	if !reflect.DeepEqual(before, after) {
		t.Fatal("observation modified serving, policy or proof cache")
	}
}

func TestDNSRuntimeFactsHTTPReadsOnlyTheCurrentVerifiedSnapshot(t *testing.T) {
	s, st, _ := runtimeFactsFixture(t)
	requests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests++ }))
	defer upstream.Close()
	s.Config.APIURL = upstream.URL
	s.PlatformTokenFile = filepath.Join(t.TempDir(), "does-not-exist")
	s.platformCandidate = PlatformCandidateStatus{State: "staged", ArtifactID: "unpublished-candidate"}
	r := httptest.NewRecorder()
	s.Handler().ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/runtime-facts", nil))
	var facts dnsfacts.Snapshot
	if err := json.Unmarshal(r.Body.Bytes(), &facts); err != nil {
		t.Fatal(err)
	}
	if r.Code != 200 || facts.Schema != dnsfacts.Schema || !facts.Ready || !reflect.DeepEqual(facts.Assignment, st.record.Candidate.Assignment) || facts.ParentDigest != st.record.Parent.ContentHash || r.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatal(r.Code, r.Body.String())
	}
	for _, secret := range []string{"private.example.test", "private-token", "origin:8080", "app.example.test", "unpublished-candidate"} {
		if strings.Contains(r.Body.String(), secret) {
			t.Fatal("runtime facts leaked configuration or raw errors", secret)
		}
	}
	entries, err := os.ReadDir(filepath.Dir(s.Config.CachePath))
	if err != nil || len(entries) != 0 || requests != 0 || s.platformServing.Load() != st {
		t.Fatal("read changed cache, publication, or contacted control plane", err)
	}
	s.listenerFailed.Store(true)
	r = httptest.NewRecorder()
	s.Handler().ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/runtime-facts", nil))
	if err := json.Unmarshal(r.Body.Bytes(), &facts); err != nil {
		t.Fatal(err)
	}
	if r.Code != 200 || facts.Ready {
		t.Fatal("failed listener reported serving", r.Code, r.Body.String())
	}
	s.listenerFailed.Store(false)
	s.Config.BundleRevokedKeyIDs = []string{"key"}
	r = httptest.NewRecorder()
	s.Handler().ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/runtime-facts", nil))
	if r.Code != 503 {
		t.Fatal("revoked artifact returned observations", r.Code)
	}
	s.Config.BundleRevokedKeyIDs = nil
	s.platformServing.Store(nil)
	r = httptest.NewRecorder()
	s.Handler().ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/runtime-facts", nil))
	if r.Code != 503 || strings.Contains(r.Body.String(), "unpublished-candidate") {
		t.Fatal("missing serving state used candidate", r.Code, r.Body.String())
	}
}
