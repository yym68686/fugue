package api

import (
	"context"
	"encoding/json"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"net/http"
	"testing"
	"time"
)

func TestPinnedDNSReferencesPersistThroughOperatorReplay(t *testing.T) {
	state, s, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	p, c, nodes := pinnedDNSFixture()
	p.DNSQueryPolicy = &platformconfig.DNSQueryPolicy{RankingMode: "active", PreferenceMode: "runtime_locality", ECSEnabled: true, ExplorationPercent: 5, SwitchCooldownSeconds: 1800, MinimumTTLSeconds: 60, MaximumTTLSeconds: 120}
	minimum, stale := 2, 120
	rules := []platformconfig.RoutePolicyConstraint{}
	states := []platformconfig.DNSRouteStateConstraint{}
	p.MinimumHealthyEdges = &minimum
	p.MaxStaleSeconds = &stale
	p.RouteConstraints = &rules
	p.DNSRouteStateConstraints = &states
	now := time.Now().UTC()
	baseIntent := platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Scope: "global", Generation: "base-dns", DNSConsumers: c, ApplicationDomains: &platformconfig.ApplicationDomainsIntent{AppBaseDomain: "example.test", CustomDomainBaseDomain: "dns.example.test", ReservedHostnames: []string{"api.example.test"}, DefaultDNSTTL: 180}}
	base, err := state.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPlatformIntent, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: "global"}, Generation: baseIntent.Generation, Content: mustPlatformIntentContent(baseIntent)})
	if err != nil {
		t.Fatal(err)
	}
	base, err = state.ValidatePlatformArtifact(base.ID, []model.PlatformArtifactValidationResult{{Name: "base", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(p)
	var content map[string]any
	json.Unmarshal(raw, &content)
	a, err := state.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: "global"}, Generation: p.Generation, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	response := performJSONRequest(t, s, http.MethodPost, "/v1/admin/artifacts/"+a.ID+"/validate", admin, model.PlatformArtifactValidateRequest{DryRun: false})
	if response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	control := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "dns-control", Mode: "shadow", RequireApplicationDomains: true, RequireRouteDefaults: true, RequireDNSQueryPolicy: true, InputSource: "business-static-intent", TargetScope: "global", IntervalSeconds: 60, RefreshSeconds: 600, StaticIntentArtifactID: base.ID, StaticIntentDigest: base.ContentHash, DNSPolicyArtifactID: a.ID, DNSPolicyDigest: a.ContentHash, HostedZoneTemplates: []platformproducer.HostedZoneTemplate{{NodeID: "dns-a", TemplateZone: "example.test"}}}
	raw, _ = json.Marshal(control)
	// Reset the decoded map so the two different policy schemas cannot mix.
	content = map[string]any{}
	json.Unmarshal(raw, &content)
	owner, err := state.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: platformproducer.Scope}, Generation: control.Generation, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	owner, err = state.ValidatePlatformArtifact(owner.ID, []model.PlatformArtifactValidationResult{{Name: "control", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	_, release, _, _, err := state.ReleasePlatformArtifact(owner.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, platformProducerPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	defaults, present, err := p.RouteDefaults()
	if err != nil || !present {
		t.Fatal(err)
	}
	defaults.DNSQueryPolicy = p.DNSQueryPolicy
	draft := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Generation: "intent", Scope: "global"}, Policy: defaults}
	if err = projectPinnedDNSInputs(&draft, c, p, control.HostedZoneTemplates, nodes, nil, now); err != nil {
		t.Fatal(err)
	}
	if err = projectDNSReadinessWithPolicy(&draft, []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "1.1.1.1"}}, now, &p); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	draft.RuntimeSnapshot.Facts = map[string]any{"configuration_producer": map[string]any{"policy_release_id": release.ID, "source_digest": digest, "static_intent_artifact_id": base.ID, "static_intent_digest": base.ContentHash, "dns_policy_artifact_id": a.ID, "dns_policy_digest": a.ContentHash}}
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: draft.Intent, Policy: draft.Policy, RuntimeSnapshot: draft.RuntimeSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.materializePlatformCompilation(context.Background(), compiled, platformProducerPrincipal(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.ReleaseArtifact.Metadata[platformproducer.DNSPolicyIDMetadata] != a.ID || first.ReleaseArtifact.Metadata[platformproducer.DNSPolicyDigestMetadata] != a.ContentHash {
		t.Fatal("DNS source binding not signed")
	}
	if _, _, _, _, err = state.ReleaseProducedPlatformArtifact(first.ReleaseArtifact.ID, release.ID, "", platformProducerPrincipal()); err != nil {
		t.Fatal("valid complete DNS source rejected", err)
	}
	operator := platformProducerPrincipal()
	operator.ActorID = "operator-replay"
	second, err := s.materializePlatformCompilation(context.Background(), compiled, operator, &platformConfigStoredInputs{Intent: first.IntentArtifact, Policy: first.PolicyArtifact})
	if err != nil || second.ReleaseArtifact.ID != first.ReleaseArtifact.ID {
		t.Fatal("DNS references not replayable", err)
	}
	// An explicit reference cannot silently run legacy capture with missing data.
	bad := control
	bad.DNSPolicyArtifactID = "missing"
	if _, err = s.capturePlatformIntentForProducer(context.Background(), platformProducerPrincipal(), bad); err == nil {
		t.Fatal("missing DNS reference fell back")
	}
	missingDefaults := control
	missingDefaults.DNSPolicyArtifactID = ""
	missingDefaults.DNSPolicyDigest = ""
	if _, err = s.capturePlatformIntentForProducer(context.Background(), platformProducerPrincipal(), missingDefaults); err == nil {
		t.Fatal("required defaults missing but capture allowed")
	}
	s.bundleRevokedKeyIDs = append(s.bundleRevokedKeyIDs, a.Provenance.KeyID)
	if _, err = s.capturePlatformIntentForProducer(context.Background(), platformProducerPrincipal(), control); err == nil {
		t.Fatal("revoked DNS source accepted")
	}
}
