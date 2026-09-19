package api

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"fugue/internal/edgecontrol"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/trafficbinding"
)

func TestTrafficRouteSourceRequiresPreparedReleaseAndPreservesOtherGroups(t *testing.T) {
	state, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	compile := func(gen, host string) platformConfigCompileResponse {
		t.Helper()
		r := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, platformConfigCompileRequest{
			Intent: platformconfig.PlatformIntent{Generation: gen, Scope: "global", Routes: []platformconfig.RouteIntent{{Hostname: host, UpstreamURL: "http://origin:8080", Enabled: true, CachePolicyID: "assets", CacheNamespace: "assets"}}, CachePolicies: []model.CachePolicy{{ID: "assets", Kind: model.CachePolicyKindStaticAssets, HostnameScope: host, TTLSeconds: 60}}},
			Policy: platformconfig.PolicySnapshot{Generation: "policy", Scope: "global", TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "first", EdgeGroupIDs: []string{"edge-group-test-a"}}}},
		})
		if r.Code != 201 {
			t.Fatal(r.Code, r.Body.String())
		}
		var c platformConfigCompileResponse
		mustDecodeJSON(t, r, &c)
		return c
	}
	baseline := compile("baseline", "baseline.example.test")
	seedVerifiedPlatformArtifactAPI(t, server, admin, baseline.RouteArtifact.ID)
	compiled := compile("candidate", "candidate.example.test")
	principal := model.Principal{ActorType: "test", ActorID: "test"}
	_, gray, _, _, err := state.ReleasePlatformArtifact(compiled.ReleaseArtifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "gray", CanaryRuleRef: "cohort=first", IdempotencyKey: "first"}, principal)
	if err != nil {
		t.Fatal(err)
	}
	server.auth.EdgeRouteIntentIdentityKeyring = edgeRouteIntentTestKeyring()
	token, err := platformcontrol.IssuePlatformComponentIdentity(edgeRouteIntentTestKeyring(), *edgeRouteIntentTestClaims(model.PlatformConsumerComponentEdgeControl, "global", []string{model.PlatformArtifactKindEdgeRouteIntent}), time.Now().UTC(), 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		query  string
		status int
	}{
		{"", 503}, {"?edge_group_id=edge-group-test-a", 503}, {"?edge_group_id=INVALID", 400}, {"?edge_group_id=edge-group-test-a&edge_group_id=edge-group-test-b", 400},
		{"?edge_group_id=edge-group-test-b", 200},
	} {
		r := performJSONRequest(t, server, http.MethodGet, "/v1/edge/route-intents"+tc.query, token, nil)
		if r.Code != tc.status {
			t.Fatal(tc.query, r.Code, r.Body.String())
		}
		if r.Code == 200 {
			var snap model.EdgeRouteIntentSnapshot
			mustDecodeJSON(t, r, &snap)
			if snap.Generation != baseline.RouteArtifact.Generation || snap.TrafficRelease != nil {
				t.Fatal("unselected group changed source")
			}
		}
	}
	var revision int64
	prepare := func(release model.PlatformArtifactRelease, group string, report bool) {
		t.Helper()
		topology := platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: "edge-a", EdgeGroupID: group}}, DNSNodes: []model.DNSNode{{ID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: group, Zone: "example.test"}}}
		for _, child := range []model.PlatformArtifact{compiled.RouteArtifact, compiled.DNSArtifact, compiled.TLSArtifact} {
			revision++
			set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: compiled.ReleaseArtifact.ID, ArtifactReleaseID: release.ID, ArtifactKind: child.ArtifactKind, ScopeKey: "global", Generation: child.Generation, Revision: revision, Topology: topology})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = state.CreatePlatformExpectedConsumerSet(set); err != nil {
				t.Fatal(err)
			}
			if !report {
				continue
			}
			for _, member := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
				now := time.Now().UTC()
				keys := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "key", Keys: map[string]string{"key": "synthetic-source-identity"}}
				claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: "test", Component: member.Component, NodeID: member.NodeID, ScopeKey: "global", ArtifactKinds: []string{child.ArtifactKind}}
				token, err := platformcontrol.IssuePlatformComponentIdentity(keys, claims, now, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				claims, err = platformcontrol.ParsePlatformComponentIdentity(keys, token, now)
				if err != nil {
					t.Fatal(err)
				}
				h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: member.ConsumerID, Component: member.Component, NodeID: member.NodeID, ArtifactKind: child.ArtifactKind, ScopeKey: "global", ReleaseSetID: compiled.ReleaseArtifact.ID, ExpectedConsumerSetID: set.ID, FencingToken: release.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", Sequence: revision, IssuedAt: now, Nonce: fmt.Sprintf("%032d", revision), GenerationSequence: child.GenerationSequence, DesiredGeneration: child.Generation, ActualGeneration: child.Generation, LKGGeneration: child.Generation, ApplyStatus: "applied", ProbeStatus: "passed"}
				h.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = state.AcceptTrustedPlatformConsumerHeartbeat(claims, set.ID, h, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	prepare(gray, "edge-group-test-a", true)
	assertSource := func(release model.PlatformArtifactRelease) {
		t.Helper()
		r := performJSONRequest(t, server, http.MethodGet, "/v1/edge/route-intents?edge_group_id=edge-group-test-a", token, nil)
		if r.Code != 200 {
			t.Fatal(r.Code, r.Body.String())
		}
		var snapshot model.EdgeRouteIntentSnapshot
		mustDecodeJSON(t, r, &snapshot)
		if r.Header().Get("X-Fugue-Route-Intent-Source") != "traffic-release" || snapshot.TrafficRelease == nil || snapshot.TrafficRelease.ReleaseID != release.ID || snapshot.Generation != compiled.RouteArtifact.Generation {
			t.Fatal("wrong release selected")
		}
		ledger := edgecontrol.NewMemoryGroupShadowLedger()
		compiler := edgecontrol.GroupShadowCompiler{Inventory: projectionInventory{time.Now().UTC()}, Ledger: ledger}
		batch, err := compiler.Reconcile(context.Background(), snapshot, []string{"edge-group-test-a"})
		if err != nil || batch.Succeeded != 1 {
			t.Fatalf("group execution rejected source: %+v %v", batch, err)
		}
		head, _, err := ledger.Head(context.Background(), "edge-group-test-a")
		if err != nil || head.Bundle == nil || trafficbinding.ValidateGroup(head.Bundle.TrafficRelease, "edge-group-test-a", true) != nil {
			t.Fatal("group lost authorized provenance")
		}
		keys := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "dns", Keys: map[string]string{"dns": "synthetic-dns-serving-selection"}}
		server.auth.PlatformComponentIdentityKeyring = keys
		dnsToken, err := platformcontrol.IssuePlatformComponentIdentity(keys, platformcontrol.PlatformComponentIdentityClaims{CredentialID: "dns-reader", Component: model.PlatformConsumerComponentDNSServer, NodeID: "dns-a", ScopeKey: "global", ArtifactKinds: []string{model.PlatformArtifactKindDNSAnswerBundle}}, time.Now().UTC(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		response := performJSONRequest(t, server, http.MethodGet, "/v1/platform-state/consumers/assignment?serving_only=true", dnsToken, nil)
		var serving model.PlatformConsumerAssignmentResponse
		mustDecodeJSON(t, response, &serving)
		if response.Code != 200 || len(serving.Assignments) != 1 || serving.Assignments[0].ArtifactReleaseID != release.ID {
			t.Fatal("DNS and Group Authority selected different releases", response.Code, response.Body.String())
		}
	}
	assertSource(gray)
	shadow := compile("shadow", "shadow.example.test")
	if _, _, _, _, err = state.ReleasePlatformArtifact(shadow.ReleaseArtifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, principal); err != nil {
		t.Fatal(err)
	}
	assertSource(gray)
	// A latest topology removing this group cannot resurrect an older set.
	prepare(gray, "edge-group-test-b", false)
	if _, found, err := server.edgeRouteIntentSnapshotFromTrafficRelease("edge-group-test-a"); !found || err == nil {
		t.Fatal("removed group resurrected")
	}
	prepare(gray, "edge-group-test-a", true)
	_, _, _, _, err = state.VerifyPlatformArtifactReleaseLKG(gray.ID, model.PlatformArtifactVerifyLKGRequest{FencingToken: gray.FencingToken, AllowInitialLKG: true, Reason: "verified initial gray", Evidence: model.PlatformArtifactVerificationEvidence{ConsumerConvergence: true, LocalProbe: true, PlatformEvidence: true, WatchWindow: true, BaselineMonotonic: true, DatabaseRollbackCompatible: true, EvidenceRefs: []string{"synthetic-bootstrap"}}}, principal)
	if err != nil {
		t.Fatal(err)
	}
	// Member LKGs must not leak the gray artifact into the unselected group.
	unselected := performJSONRequest(t, server, http.MethodGet, "/v1/edge/route-intents?edge_group_id=edge-group-test-b", token, nil)
	var unchanged model.EdgeRouteIntentSnapshot
	mustDecodeJSON(t, unselected, &unchanged)
	if unselected.Code != 200 || unchanged.Generation != baseline.RouteArtifact.Generation || unchanged.TrafficRelease != nil {
		t.Fatal("gray verification changed unselected serving", unselected.Body.String())
	}
	_, full, _, _, err := state.ReleasePlatformArtifact(compiled.ReleaseArtifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "full", IdempotencyKey: "full"}, principal)
	if err != nil {
		t.Fatal("full release failed", err)
	}
	prepare(full, "edge-group-test-a", false)
	assertSource(full)
	compiled = compile("next-candidate", "next.example.test")
	_, newGray, _, _, err := state.ReleasePlatformArtifact(compiled.ReleaseArtifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "gray", CanaryRuleRef: "cohort=first", IdempotencyKey: "new-canary"}, principal)
	if err != nil {
		t.Fatal(err)
	}
	prepare(newGray, "edge-group-test-a", false)
	assertSource(newGray)
	server.bundleRevokedKeyIDs = append(server.bundleRevokedKeyIDs, compiled.RouteArtifact.Provenance.KeyID)
	result := performJSONRequest(t, server, http.MethodGet, "/v1/edge/route-intents?edge_group_id=edge-group-test-a", token, nil)
	if result.Code != 503 {
		t.Fatal("invalid selected release fell back", result.Code, result.Body.String())
	}
}
