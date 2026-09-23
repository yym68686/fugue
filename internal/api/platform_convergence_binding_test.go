package api

import (
	"context"
	"fmt"
	"fugue/internal/store"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestReleaseConvergenceBindsAllMembersAndAdvancesExpectationRevision(t *testing.T) {
	testReleaseConvergenceBinding(t, "")
}
func TestReleaseConvergencePostgresIntegration(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL for disposable Postgres")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires a disposable loopback fugue_test database")
	}
	testReleaseConvergenceBinding(t, address)
}
func testReleaseConvergenceBinding(t *testing.T, address string) {
	state, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	if address != "" {
		state = store.New("", address)
		if err := state.Init(); err != nil {
			t.Fatal(err)
		}
		state.ConfigurePlatformArtifactSigning(server.bundleKeyring())
		server.store = state
	}
	identityKeyring := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "component-key", Keys: map[string]string{"component-key": "synthetic-component-key"}}
	server.auth.PlatformComponentIdentityKeyring = identityKeyring
	server.heartbeatAuditKeyring = trustedHeartbeatAuditTestKeyring()
	now := time.Now().UTC()
	node := model.EdgeNode{ID: "edge-node", EdgeGroupID: "edge-group-a", LastHeartbeatAt: &now, LastSeenAt: &now, Healthy: true, Status: model.EdgeHealthHealthy}
	if _, _, err := state.CreateEdgeNodeToken(node); err != nil {
		t.Fatal(err)
	}
	if _, err := state.UpdateDNSHeartbeat(model.DNSNode{ID: "dns-node", EdgeGroupID: "edge-group-a", Zone: "example.test", Healthy: true}); err != nil {
		t.Fatal(err)
	}
	// Advance shadow's independent fence counter before this ReleaseSet.
	seedResponse := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, map[string]any{"intent": map[string]any{"generation": "seed-intent", "scope": "global", "routes": []any{map[string]any{"hostname": "seed.example.test", "upstream_url": "http://origin:8080", "enabled": true}}}, "policy": map[string]any{"generation": "seed-policy", "scope": "global"}})
	if seedResponse.Code != 201 {
		t.Fatal(seedResponse.Body.String())
	}
	var seed platformConfigCompileResponse
	mustDecodeJSON(t, seedResponse, &seed)
	seedRelease := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+seed.ReleaseArtifact.ID+"/release", admin, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", IdempotencyKey: "seed"})
	if seedRelease.Code != 200 {
		t.Fatal(seedRelease.Body.String())
	}
	response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, map[string]any{"intent": map[string]any{"generation": "binding-intent", "scope": "global", "routes": []any{map[string]any{"hostname": "app.example.test", "upstream_url": "http://origin:8080", "enabled": true}}}, "policy": map[string]any{"generation": "binding-policy", "scope": "global", "traffic_rollout_cohorts": []any{map[string]any{"id": "complete", "edge_group_ids": []string{"edge-group-a"}}}}})
	if response.Code != 201 {
		t.Fatal(response.Body.String())
	}
	var compiled platformConfigCompileResponse
	mustDecodeJSON(t, response, &compiled)
	release := func(channel string) model.PlatformArtifactRelease {
		t.Helper()
		r := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+compiled.ReleaseArtifact.ID+"/release", admin, model.PlatformArtifactReleaseRequest{ReleaseChannel: channel, Reason: "consumer binding regression", IdempotencyKey: channel, CanaryRuleRef: "cohort=complete"})
		if r.Code != 200 {
			t.Fatal(r.Code, r.Body.String())
		}
		var body model.PlatformArtifactReleaseResponse
		mustDecodeJSON(t, r, &body)
		return body.Release
	}
	shadow := release("shadow")
	partial, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: compiled.ReleaseArtifact.ID, ArtifactReleaseID: shadow.ID, ArtifactKind: compiled.RouteArtifact.ArtifactKind, ScopeKey: "global", Scope: compiled.RouteArtifact.Scope, Generation: compiled.RouteArtifact.Generation, Revision: 1, Topology: platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{node}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = state.CreatePlatformExpectedConsumerSet(partial); err != nil {
		t.Fatal(err)
	}
	if gate := server.validateReleaseSetConvergence(context.Background(), compiled.ReleaseArtifact); gate.Pass || !strings.Contains(gate.Message, "missing") {
		t.Fatal("partial member expectations passed", gate)
	}
	prepare := func(r model.PlatformArtifactRelease) []model.PlatformExpectedConsumerSet {
		t.Helper()
		out := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/release-set/prepare-consumers", admin, map[string]any{"release_set_id": compiled.ReleaseArtifact.ID, "artifact_release_id": r.ID})
		if out.Code != 201 {
			t.Fatal(out.Code, out.Body.String())
		}
		var body struct {
			Sets []model.PlatformExpectedConsumerSet `json:"expected_consumer_sets"`
		}
		mustDecodeJSON(t, out, &body)
		return body.Sets
	}
	// A legacy heartbeat can claim all status strings yet has no verified
	// identity and must not count as positive release evidence.
	if _, err := state.UpsertPlatformConsumerHeartbeat(model.PlatformConsumerHeartbeatRequest{ConsumerID: partial.Consumers[0].ConsumerID, Component: partial.Consumers[0].Component, NodeID: partial.Consumers[0].NodeID, ArtifactKind: partial.ArtifactKind, ScopeKey: partial.ScopeKey, ReleaseSetID: partial.ReleaseSetID, ExpectedConsumerSetID: partial.ID, FencingToken: shadow.FencingToken, GenerationSequence: compiled.RouteArtifact.GenerationSequence, DesiredGeneration: partial.ExpectedGeneration, ActualGeneration: partial.ExpectedGeneration, LKGGeneration: "lkg", ApplyStatus: "applied", ProbeStatus: "passed", ProtocolVersion: "v1", SchemaVersion: "v1"}); err != nil {
		t.Fatal(err)
	}
	consumers, err := state.ListPlatformConsumers(partial.ArtifactKind, partial.ScopeKey)
	if err != nil {
		t.Fatal(err)
	}
	if status := platformcontrol.EvaluateConsumerConvergence(partial, consumers, time.Now(), server.platformConvergenceBinding(partial)); status.Pass || status.RequiredPassing != 0 {
		t.Fatal("unverified legacy receipt passed", status)
	}
	sets := prepare(shadow)
	for _, set := range sets {
		reads := map[string]int{}
		reader := newConsumerArtifactReader(func(id string) (model.PlatformArtifact, error) {
			reads[id]++
			return state.GetPlatformArtifact(id)
		})
		binding := server.platformConvergenceBindingWithReader(set, reader)
		if binding == nil || !reflect.DeepEqual(binding, server.platformConvergenceBinding(set)) {
			t.Fatal("read reuse changed convergence binding", binding)
		}
		for _, artifact := range []model.PlatformArtifact{compiled.ReleaseArtifact, compiled.RouteArtifact, compiled.DNSArtifact, compiled.TLSArtifact} {
			if reads[artifact.ID] != 1 {
				t.Fatalf("convergence reread artifact %s: %v", artifact.ArtifactKind, reads)
			}
		}
	}
	again := prepare(shadow)
	if len(sets) != 3 || !reflect.DeepEqual(sets, again) {
		t.Fatal("same-release preparation was not idempotent")
	}
	report := func(set model.PlatformExpectedConsumerSet, r model.PlatformArtifactRelease, sequence int64) {
		t.Helper()
		child, err := server.consumerAssignmentChild(compiled.ReleaseArtifact, set.ArtifactKind)
		if err != nil {
			t.Fatal(err)
		}
		for _, expected := range set.Consumers {
			at := time.Now().UTC()
			claims := platformcontrol.PlatformComponentIdentityClaims{Version: "v1", CredentialID: "credential", TokenID: "token", Component: expected.Component, NodeID: expected.NodeID, ScopeKey: set.ScopeKey, ArtifactKinds: []string{set.ArtifactKind}, IssuedAtUnix: at.Unix(), ExpiresAtUnix: at.Add(time.Minute).Unix()}
			h, err := platformcontrol.BindPlatformConsumerHeartbeatToExpectedSet(claims, set, platformcontrol.PlatformConsumerHeartbeatEnvelope{Sequence: sequence, GenerationSequence: child.GenerationSequence, FencingToken: r.FencingToken, IssuedAt: at, Nonce: fmt.Sprintf("%032d", sequence), ProtocolVersion: "v1", SchemaVersion: "v1", ActualGeneration: set.ExpectedGeneration, LKGGeneration: "lkg", ApplyStatus: "applied", ProbeStatus: "passed"})
			if err != nil {
				t.Fatal(err)
			}
			h.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
			if err != nil {
				t.Fatal(err)
			}
			token, err := platformcontrol.IssuePlatformComponentIdentity(identityKeyring, claims, at, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			// Wrong authority must not consume the durable sequence cursor.
			for _, field := range []string{"fence", "sequence"} {
				bad := h
				if field == "fence" {
					bad.FencingToken += 100
				} else {
					bad.GenerationSequence += 100
				}
				bad.EvidenceHash, _ = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(bad)
				rejected := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", token, bad)
				if rejected.Code != 409 {
					t.Fatal("wrong authority accepted", field, rejected.Code, rejected.Body.String())
				}
			}
			accepted := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", token, h)
			if accepted.Code != 200 {
				t.Fatal(accepted.Code, accepted.Body.String())
			}
		}
	}
	for _, set := range sets {
		report(set, shadow, 1)
	}
	if gate := server.validateReleaseSetConvergence(context.Background(), compiled.ReleaseArtifact); !gate.Pass {
		t.Fatal("exact complete shadow evidence rejected", gate)
	}
	gray := release("gray")
	if gray.FencingToken >= shadow.FencingToken {
		t.Fatal("fixture did not exercise independent lower lane fence")
	}
	if gray.ID == shadow.ID || gray.LaneKey == shadow.LaneKey {
		t.Fatal("release authority did not change")
	}
	if gate := server.validateReleaseSetConvergence(context.Background(), compiled.ReleaseArtifact); gate.Pass {
		t.Fatal("old shadow expectations authorized new gray release")
	}
	if server.platformConvergenceBinding(sets[0]) != nil {
		t.Fatal("old active channel retained current convergence authority")
	}
	graySets := prepare(gray)
	for _, set := range graySets {
		for _, prior := range sets {
			if set.Revision <= prior.Revision {
				t.Fatal("new release expectation revision did not advance")
			}
		}
	}
	if gate := server.validateReleaseSetConvergence(context.Background(), compiled.ReleaseArtifact); gate.Pass {
		t.Fatal("old receipts matched new expectations")
	}
	for _, set := range graySets {
		report(set, gray, 2)
	}
	if gate := server.validateReleaseSetConvergence(context.Background(), compiled.ReleaseArtifact); !gate.Pass {
		t.Fatal("current release exact receipts rejected", gate)
	}
	// Topology changes get new immutable revisions in the same release.
	second := node
	second.ID = "edge-new"
	if _, _, err := state.CreateEdgeNodeToken(second); err != nil {
		t.Fatal(err)
	}
	changed := prepare(gray)
	if gate := server.validateReleaseSetConvergence(context.Background(), compiled.ReleaseArtifact); gate.Pass {
		t.Fatal("old cohort authorized a new required node")
	}
	for _, set := range changed {
		report(set, gray, 3)
	}
	if gate := server.validateReleaseSetConvergence(context.Background(), compiled.ReleaseArtifact); !gate.Pass {
		t.Fatal("current topology receipts rejected", gate)
	}
	if !reflect.DeepEqual(changed, prepare(gray)) {
		t.Fatal("changed topology preparation not idempotent")
	}
	for _, prior := range sets {
		stored, err := state.GetPlatformExpectedConsumerSet(prior.ID)
		if err != nil || !reflect.DeepEqual(stored, prior) {
			t.Fatal("historical expectation mutated", err)
		}
	}
}
