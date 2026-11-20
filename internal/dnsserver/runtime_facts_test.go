package dnsserver

import (
	"context"
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
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
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
	if r.Code != 503 || r.Header().Get("X-Fugue-DNS-Observation-Rejection") != "checkpoint_validation" {
		t.Fatal("revoked artifact returned observations", r.Code)
	}
	s.Config.BundleRevokedKeyIDs = nil
	s.platformServing.Store(nil)
	r = httptest.NewRecorder()
	s.Handler().ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/runtime-facts", nil))
	if r.Code != 503 || r.Header().Get("X-Fugue-DNS-Observation-Rejection") != "state_missing" || r.Header().Get("Cache-Control") != "private, no-store" || strings.Contains(r.Body.String(), "unpublished-candidate") {
		t.Fatal("missing serving state used candidate", r.Code, r.Body.String())
	}
}

func TestDNSRuntimeFactsRequireAnAnswerFromTheExactSignedSelector(t *testing.T) {
	_, base, _ := runtimeFactsFixture(t)
	view, plan, policy, facts, now := queryExecutionFixture()
	plan.Records[0].MinimumHealthyEdges = 1
	payload := base.payload
	payload.Plan, payload.Policy.DNSReadiness = &plan, &policy
	payload.Queries = []platformconfig.DNSQueryView{view}
	for i := range facts {
		facts[i].Proof.TrafficRelease = base.facts[0].Proof.TrafficRelease
		facts[i].Ready = facts[i].Proof.EdgeID == "edge-b"
	}
	record := &view.Records[0]
	record.AnswerPolicy = physicalSelectionTestRecord(now).AnswerPolicy
	record.AnswerPolicy.PhysicalSelection.PrimaryEdgeID = "edge-a"
	record.AnswerPolicy.PhysicalSelection.OrderedEdgeIDs = []string{"edge-a"}

	build := func(payload dnsServingPayload) *dnsServingState {
		t.Helper()
		st, err := buildDNSServingState(base.record, payload, base.routeID, view.NodeID, view.EdgeGroupID, facts, now)
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	blocked := build(payload)
	if !dnsServingReady(blocked, now) {
		t.Fatal("the fallback's fresh proof must still satisfy the generic route quorum")
	}
	snapshot, err := dnsRuntimeFacts(blocked, now)
	if err != nil || snapshot.Ready {
		t.Fatalf("unranked proof appeared as an answer from the checkpoint: ready=%v err=%v", snapshot.Ready, err)
	}
	for _, fact := range snapshot.Facts {
		if fact.Ready != (fact.Proof.EdgeID == "edge-b") {
			t.Fatal("the transport verdict must not erase individual route evidence")
		}
	}

	// An independently proved successor can legitimately serve a query. It
	// still cannot certify the old checkpoint for promotion.
	readyView := view
	readyView.Records = append([]model.EdgeDNSRecord(nil), view.Records...)
	selector := *record.AnswerPolicy.PhysicalSelection
	selector.OrderedEdgeIDs = []string{"edge-a", "edge-b"}
	readyView.Records[0].AnswerPolicy.PhysicalSelection = &selector
	payload.Queries = []platformconfig.DNSQueryView{readyView}
	ready := build(payload)
	blocked.transition = &dnsRecordTransition{state: ready, hosts: map[string]bool{record.Name: true}}
	request := new(dns.Msg)
	request.SetQuestion(dns.Fqdn(record.Name), dns.TypeA)
	if reply := blocked.answer(request, "", now); reply.Rcode != dns.RcodeSuccess || len(reply.Answer) != 1 {
		t.Fatal("the authorized successor did not preserve live traffic", reply)
	}
	if checkDNSServingSnapshot(blocked, now, false) == nil || probeDNSServingSnapshot(blocked) == nil {
		t.Fatal("the old checkpoint borrowed a successor answer")
	}
	if got, err := dnsRuntimeFacts(ready, now); err != nil || !got.Ready {
		t.Fatalf("the ranked, fresh fallback did not satisfy the checkpoint: ready=%v err=%v", got.Ready, err)
	}

	// At a future renewal horizon, the shorter-lived selected route must
	// not borrow a longer-lived unranked route's validity.
	payload.Queries = []platformconfig.DNSQueryView{view}
	for i := range facts {
		facts[i].Ready = true
		if facts[i].Proof.EdgeID == "edge-a" {
			facts[i].Proof.ValidUntil = now.Add(10 * time.Second)
		}
	}
	expiring := build(payload)
	horizon := now.Add(12 * time.Second)
	if checkDNSServingSnapshot(expiring, now, false) != nil || !dnsServingReady(expiring, horizon) || checkDNSServingSnapshot(expiring, horizon, false) == nil {
		t.Fatal("future materialization did not detect loss of the only ranked route")
	}
}

func TestDNSRuntimeFactsRejectionClassificationPreservesFailClosed(t *testing.T) {
	for _, reason := range []string{"request_cancelled", "route_identity", "plan_mismatch", "policy_mismatch", "checkpoint_not_positive", "node_identity", "fact_snapshot"} {
		t.Run(reason, func(t *testing.T) {
			s, state, _ := runtimeFactsFixture(t)
			request := httptest.NewRequest(http.MethodGet, "/runtime-facts", nil)
			switch reason {
			case "request_cancelled":
				ctx, cancel := context.WithCancel(request.Context())
				cancel()
				request = request.WithContext(ctx)
			case "route_identity":
				state.routeID = "other"
			case "plan_mismatch":
				plan := *state.payload.Plan
				plan.Probes = nil
				state.payload.Plan = &plan
			case "policy_mismatch":
				state.payload.Policy.MaxStaleSeconds++
			case "checkpoint_not_positive":
				state.record.Positive = false
			case "node_identity":
				state.record.NodeID = "other"
			case "fact_snapshot":
				state.checkedAt = time.Time{}
			}
			response := httptest.NewRecorder()
			s.Handler().ServeHTTP(response, request)
			if response.Code != http.StatusServiceUnavailable || response.Header().Get("X-Fugue-DNS-Observation-Rejection") != reason || strings.Contains(response.Body.String(), "private-token") {
				t.Fatal("rejection classification changed readiness or leaked details", response.Code, response.Header(), response.Body.String())
			}
		})
	}
}

type runtimeFactsComparedContext struct {
	context.Context
	checks  int
	compare func()
}

func (c *runtimeFactsComparedContext) Err() error {
	c.checks++
	if c.checks == 2 {
		c.compare()
	}
	return c.Context.Err()
}

func TestDNSRuntimeFactsReplacementDiagnosticsDoNotChangeVerdict(t *testing.T) {
	for _, scenario := range []string{"replacement", "same_pointer", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			s, state, _ := runtimeFactsFixture(t)
			s.platformServing.publish(state, dnsServingWriterCheckpoint)
			other := *state
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			observed := &runtimeFactsComparedContext{Context: ctx, compare: func() {
				next := &other
				if scenario == "same_pointer" {
					next = state
				}
				s.platformServing.publish(next, dnsServingWriterCandidate)
				if scenario == "cancelled" {
					cancel()
				}
			}}
			response := httptest.NewRecorder()
			s.handleRuntimeFacts(response, httptest.NewRequest(http.MethodGet, "/runtime-facts", nil).WithContext(observed))
			header := response.Header()
			if observed.checks != 2 || header.Get("Cache-Control") != "private, no-store" {
				t.Fatalf("did not exercise the final comparison: checks=%d headers=%v", observed.checks, header)
			}
			if scenario == "replacement" {
				if response.Code != http.StatusServiceUnavailable || header.Get("X-Fugue-DNS-Observation-Rejection") != "state_replaced" ||
					header.Get(dnsfacts.ReplacementBaseHeader) != dnsfacts.ReplacementCandidate ||
					header.Get(dnsfacts.ReplacementSelectionHeader) != dnsfacts.ReplacementBaseToBase ||
					header.Get(dnsfacts.ReplacementRenewalHeader) != dnsfacts.ReplacementUnchanged ||
					strings.Contains(response.Body.String(), dnsfacts.Schema) {
					t.Fatalf("replacement did not fail closed with fixed diagnostics: %d %v %s", response.Code, header, response.Body.String())
				}
				return
			}
			if header.Get(dnsfacts.ReplacementBaseHeader) != "" || header.Get(dnsfacts.ReplacementSelectionHeader) != "" || header.Get(dnsfacts.ReplacementRenewalHeader) != "" {
				t.Fatal("replacement metadata was emitted without a state_replaced verdict", header)
			}
			if scenario == "same_pointer" && response.Code != http.StatusOK || scenario == "cancelled" && (response.Code != http.StatusServiceUnavailable || header.Get("X-Fugue-DNS-Observation-Rejection") != "request_cancelled") {
				t.Fatal("diagnostics changed existing precedence or pointer comparison", response.Code, header)
			}
		})
	}
}
