package api

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

func TestServingProducerProgressesRecoversAndDoesNotRetryFailedSource(t *testing.T) {
	path := t.TempDir() + "/state.json"
	state := store.New(path)
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	config := ServerConfig{BundleSigningKey: "synthetic-serving-key", BundleSigningKeyID: "key"}
	server := NewServer(state, auth.New(state, ""), nil, config)
	seedInventory := func() {
		server.clusterNodeInventoryCache = newExpiringResponseCache[[]clusterNodeSnapshot](time.Hour)
		server.clusterNodeInventoryCache.set(clusterNodeInventoryCacheKey, []clusterNodeSnapshot{
			{node: model.ClusterNode{Name: "edge-a"}, labels: map[string]string{runtimepkg.EdgeRoleLabelKey: runtimepkg.NodeRoleLabelValue}},
			{node: model.ClusterNode{Name: "dns-a"}, labels: map[string]string{runtimepkg.DNSRoleLabelKey: runtimepkg.NodeRoleLabelValue}},
		})
	}
	seedInventory()

	principal := platformProducerPrincipal()
	ctx := context.Background()
	save := func(kind, scope, gen string, value any) model.PlatformArtifact {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var content map[string]any
		if err = json.Unmarshal(raw, &content); err != nil {
			t.Fatal(err)
		}
		a, err := state.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: kind, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: scope}, Generation: gen, Content: content})
		if err != nil {
			t.Fatal(err)
		}
		a, err = state.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "test", Pass: true}})
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	input, consumers, nodes := pinnedDNSFixture()
	minimum, stale := 1, 120
	rules := []platformconfig.RoutePolicyConstraint{}
	states := []platformconfig.DNSRouteStateConstraint{}
	input.MinimumHealthyEdges = &minimum
	input.MaxStaleSeconds = &stale
	input.RouteConstraints = &rules
	input.DNSRouteStateConstraints = &states
	input.DNSPlacementMode = platformconfig.DNSPlacementConsumerReadiness
	input.DNSQueryPolicy = &platformconfig.DNSQueryPolicy{RankingMode: "disabled", PreferenceMode: "runtime_locality", MinimumTTLSeconds: 60, MaximumTTLSeconds: 120}
	source := save(model.PlatformArtifactKindPolicySnapshot, "global", input.Generation, input)
	base := save(model.PlatformArtifactKindPlatformIntent, "global", "static", platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Scope: "global", Generation: "static", ApplicationDomains: &platformconfig.ApplicationDomainsIntent{AppBaseDomain: "example.test", ReservedHostnames: []string{}, DefaultDNSTTL: 60}, Routes: []platformconfig.RouteIntent{{Hostname: "static.example.test", UpstreamURL: "http://origin:8080", Enabled: true}}, DNSConsumers: consumers})
	p := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "producer", Mode: "serving", InputSource: "business-static-intent", TargetScope: "global", IntervalSeconds: 30, RefreshSeconds: 300, RequireApplicationDomains: true, RequireRouteDefaults: true, RequireDNSQueryPolicy: true, StaticIntentArtifactID: base.ID, StaticIntentDigest: base.ContentHash, DNSPolicyArtifactID: source.ID, DNSPolicyDigest: source.ContentHash, Serving: &platformproducer.ServingPolicy{CanaryRuleRef: "cohort=complete", GrayMinSeconds: 1, FullMinSeconds: 1, RolloutTimeoutSeconds: 60}}
	activate := func() model.PlatformArtifactRelease {
		a := save(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, p.Generation, p)
		_, r, _, _, err := state.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, principal)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	authority := activate()
	if _, _, err := state.CreateEdgeNodeToken(model.EdgeNode{ID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8"}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.UpdateDNSHeartbeat(nodes[0]); err != nil {
		t.Fatal(err)
	}
	reconcile := func() {
		t.Helper()
		if _, err := server.reconcilePlatformConfiguration(ctx); err != nil {
			t.Fatal(err)
		}
	}
	active := func(channel string) (model.PlatformArtifact, model.PlatformArtifactRelease) {
		t.Helper()
		a, r, found, err := state.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", channel)
		if err != nil || !found {
			t.Fatal("active", channel, err)
		}
		return a, r
	}
	verify := func(r model.PlatformArtifactRelease, initial bool) {
		t.Helper()
		_, _, _, _, err := state.VerifyPlatformArtifactReleaseLKG(r.ID, model.PlatformArtifactVerifyLKGRequest{FencingToken: r.FencingToken, AllowInitialLKG: initial, Reason: "explicit bootstrap", Evidence: model.PlatformArtifactVerificationEvidence{ConsumerConvergence: true, LocalProbe: true, PlatformEvidence: true, WatchWindow: true, BaselineMonotonic: true, DatabaseRollbackCompatible: true, EvidenceRefs: []string{"synthetic-bootstrap"}}}, principal)
		if err != nil {
			t.Fatal(err)
		}
	}
	// All aging and corruption is confined to this disposable file store.
	edit := func(change func(*model.State)) {
		t.Helper()
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var st model.State
		if err = json.Unmarshal(raw, &st); err != nil {
			t.Fatal(err)
		}
		change(&st)
		raw, err = json.Marshal(st)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	age := func(id string, seconds int) {
		edit(func(st *model.State) {
			for i := range st.PlatformArtifactReleases {
				if st.PlatformArtifactReleases[i].ID == id {
					st.PlatformArtifactReleases[i].ReleasedAt = time.Now().Add(-time.Duration(seconds) * time.Second)
				}
			}
		})
	}
	report := func(parent model.PlatformArtifact, r model.PlatformArtifactRelease) {
		t.Helper()
		reportServingProducerAPI(t, server, parent, r)
	}
	reconcile()
	first, shadow := active("shadow")
	if lkg, err := state.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global"); err != nil || lkg != nil {
		t.Fatal("producer bootstrapped LKG", err)
	}
	if _, _, found, err := state.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "gray"); err != nil || found {
		t.Fatal("producer bootstrapped serving", err)
	}
	report(first, shadow)
	_, gray, _, _, err := state.ReleasePlatformArtifact(first.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "gray", CanaryRuleRef: "cohort=complete"}, principal)
	if err != nil {
		t.Fatal(err)
	}
	report(first, gray)
	verify(gray, true)
	_, full, _, _, err := state.ReleasePlatformArtifact(first.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "full"}, principal)
	if err != nil {
		t.Fatal(err)
	}
	report(first, full)
	verify(full, false)
	// A policy revision creates a new source and starts real gray publication.
	p.Generation = "producer-two"
	authority = activate()
	reconcile()
	candidate, nextGray := active("gray")
	_, shadow = active("shadow")
	if nextGray.ArtifactID == first.ID {
		t.Fatal("new source not staged")
	}
	age(full.ID, 10)
	age(shadow.ID, 5)
	age(nextGray.ID, 2)
	reconcile()
	_, unchanged := active("full")
	if unchanged.ID != full.ID {
		t.Fatal("shadow facts promoted full")
	}
	report(candidate, nextGray)
	server = NewServer(state, auth.New(state, ""), nil, config)
	seedInventory()
	reconcile()
	_, nextFull := active("full")
	if nextFull.ArtifactID != candidate.ID {
		t.Fatal("restart lost gray cursor")
	}
	age(nextFull.ID, 2)
	reconcile()
	lkg, err := state.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil || lkg.ArtifactID != first.ID {
		t.Fatal("old gray facts verified full", err)
	}
	report(candidate, nextFull)
	reconcile()
	lkg, err = state.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil || lkg.ArtifactID != candidate.ID {
		t.Fatal("full was not verified", err)
	}
	// Refresh under the same source creates another candidate. A failed full
	// parent cannot prepare topology; timeout must still restore verified LKG.
	age(shadow.ID, 301)
	reconcile()
	failed, badGray := active("gray")
	_, shadow = active("shadow")
	age(nextFull.ID, 10)
	age(shadow.ID, 5)
	age(badGray.ID, 2)
	report(failed, badGray)
	reconcile()
	_, badFull := active("full")
	if badFull.ArtifactID != failed.ID {
		t.Fatal("refresh did not promote")
	}
	age(badGray.ID, 120)
	age(badFull.ID, 61)
	invalid, err := state.ValidatePlatformArtifact(failed.ID, []model.PlatformArtifactValidationResult{{Name: "failed candidate validation", Pass: false}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.preparePlatformReleaseSetConsumers(ctx, principal, invalid, badFull); err == nil {
		t.Fatal("invalid parent unexpectedly prepared")
	}

	reconcile()
	_, recovered := active("full")
	if recovered.ArtifactID != candidate.ID || recovered.ID == nextFull.ID {
		t.Fatal("failed full was not recovered")
	}
	blocked, err := server.producedSourcePreviouslyFailed("global", authority.ID, failed.Metadata[platformproducer.SourceDigestMetadata])
	if err != nil || !blocked {
		t.Fatal("failed source not blocked", err)
	}
	// Invalid failed shadow should not prevent a later changed policy from
	// recovering; the same authority/source stays frozen without another rollout.
	reconcile()
	_, still := active("gray")
	if still.ID != badGray.ID {
		t.Fatal("same source retried")
	}
	p.Generation = "producer-three"
	activate()
	reconcile()
	_, fresh := active("gray")
	if fresh.ID == badGray.ID {
		t.Fatal("new authority remained blocked")
	}
}

func reportServingProducerAPI(t *testing.T, s *Server, parent model.PlatformArtifact, r model.PlatformArtifactRelease) {
	t.Helper()
	sets, err := s.preparePlatformReleaseSetConsumers(context.Background(), platformProducerPrincipal(), parent, r)
	if err != nil {
		t.Fatal(err)
	}
	for _, set := range sets {
		child, err := s.store.GetPlatformArtifactByIdentity(set.ArtifactKind, set.ScopeKey, set.ExpectedGeneration)
		if err != nil {
			t.Fatal(err)
		}
		previous, err := s.store.ListPlatformConsumers(child.ArtifactKind, "global")
		if err != nil {
			t.Fatal(err)
		}
		for _, member := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
			seq := int64(1)
			for _, old := range previous {
				if old.ConsumerID == member.ConsumerID {
					seq = max(seq, old.Sequence+1)
				}
			}
			now := time.Now().UTC()
			keys := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "key", Keys: map[string]string{"key": "synthetic-producer-identity"}}
			claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: "test", Component: member.Component, NodeID: member.NodeID, ScopeKey: "global", ArtifactKinds: []string{child.ArtifactKind}}
			token, err := platformcontrol.IssuePlatformComponentIdentity(keys, claims, now, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			claims, err = platformcontrol.ParsePlatformComponentIdentity(keys, token, now)
			if err != nil {
				t.Fatal(err)
			}
			h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: member.ConsumerID, Component: member.Component, NodeID: member.NodeID, ArtifactKind: child.ArtifactKind, ScopeKey: "global", ReleaseSetID: parent.ID, ExpectedConsumerSetID: set.ID, FencingToken: r.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", Sequence: seq, IssuedAt: now, Nonce: model.NewID("nonce"), GenerationSequence: child.GenerationSequence, DesiredGeneration: child.Generation, ActualGeneration: child.Generation, LKGGeneration: child.Generation, ApplyStatus: "applied", ProbeStatus: "passed", CompatibilityCapabilities: []string{platformcontrol.TrafficReleaseCapabilityV1}}
			if r.ReleaseChannel == "shadow" {
				h.ActualGeneration = ""
				h.LKGGeneration = ""
				h.CandidateGeneration = child.Generation
				h.ApplyStatus = "staged"
				h.ProbeStatus = "shadow_validated"
			}
			h.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.store.AcceptTrustedPlatformConsumerHeartbeat(claims, set.ID, h, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
				t.Fatal(err)
			}
		}
	}
}
