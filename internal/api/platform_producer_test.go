package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/store"
)

func activateTestProducer(t *testing.T, s *Server, generation, mode string) model.PlatformArtifactRelease {
	t.Helper()
	base, err := s.store.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPlatformIntent, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: "global"}, Generation: generation + "-base", Content: mustPlatformIntentContent(platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Scope: "global", Generation: generation + "-base"})})
	if err != nil {
		t.Fatal(err)
	}
	base, err = s.store.ValidatePlatformArtifact(base.ID, []model.PlatformArtifactValidationResult{{Name: "pinned", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	a := createTestProjectionPolicy(t, s, base, generation, mode)
	_, r, _, _, err := s.store.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, platformProducerPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func createTestProjectionPolicy(t *testing.T, s *Server, base model.PlatformArtifact, generation, mode string) model.PlatformArtifact {
	t.Helper()
	raw, _ := json.Marshal(platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: generation, Mode: mode, InputSource: "business-static-intent", StaticIntentArtifactID: base.ID, StaticIntentDigest: base.ContentHash, TargetScope: "global", IntervalSeconds: 30, RefreshSeconds: 300})
	var content map[string]any
	json.Unmarshal(raw, &content)
	a, err := s.store.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: platformproducer.Scope}, Generation: generation, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	if err = validatePlatformPolicyArtifact(a); err != nil {
		t.Fatal(err)
	}
	a, err = s.store.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "producer-policy", Pass: true}})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestPlatformProducerPublishesAndReusesShadow(t *testing.T) {
	state, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	if _, _, err := state.CreateEdgeNodeToken(model.EdgeNode{ID: "edge-a", EdgeGroupID: "group-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.UpdateDNSHeartbeat(model.DNSNode{ID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: "group-a", Zone: "example.test"}); err != nil {
		t.Fatal(err)
	}
	count := 0
	desired := "one"
	capture := func(ctx context.Context, p model.Principal) (platformIntentProjectionResponse, error) {
		count++
		now := time.Now().UTC()
		return platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Generation: "intent-" + desired, Scope: "global", Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", UpstreamURL: "http://" + desired + ":8080", Enabled: true}}}, Policy: platformconfig.PolicySnapshot{Generation: "policy", Scope: "global"}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now}, BusinessSnapshotRevision: desired}, nil
	}
	if _, err := server.reconcilePlatformConfigurationWithCapture(context.Background(), capture); err != nil || count != 0 {
		t.Fatal("missing policy activated producer", err)
	}
	authority := activateTestProducer(t, server, "producer-one", "shadow")
	if _, err := server.reconcilePlatformConfigurationWithCapture(context.Background(), capture); err != nil {
		t.Fatal(err)
	}
	parent, first, found, err := state.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "shadow")
	if err != nil || !found || parent.Metadata[platformproducer.PolicyReleaseMetadata] != authority.ID {
		t.Fatal("no authorized shadow produced", err)
	}
	sets, err := state.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{ArtifactReleaseID: first.ID})
	if err != nil || len(sets) != 3 {
		t.Fatal("consumer preparation missing", err)
	}
	for _, set := range sets {
		if set.RequiredCardinality != 1 {
			t.Fatal("consumer ownership not declared", set)
		}
	}
	server = NewServer(state, auth.New(state, ""), nil, ServerConfig{BundleSigningKey: server.bundleSigningKey, BundleSigningKeyID: server.bundleSigningKeyID})
	if _, err := server.reconcilePlatformConfigurationWithCapture(context.Background(), capture); err != nil {
		t.Fatal(err)
	}
	_, repeated, _, _ := state.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "shadow")
	if repeated.ID != first.ID {
		t.Fatal("timestamp-only recapture advanced shadow")
	}
	desired = "two"
	if _, err := server.reconcilePlatformConfigurationWithCapture(context.Background(), capture); err != nil {
		t.Fatal(err)
	}
	_, second, _, _ := state.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "shadow")
	if second.ID == first.ID || second.FencingToken != first.FencingToken+1 {
		t.Fatal("business change was not published")
	}
	produced, err := state.GetPlatformArtifact(second.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	content, err := state.GetPlatformArtifactContent(produced.Metadata["input_snapshot_digest"])
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := platformconfig.DecodeRuntimeSnapshotContent(content.Content)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := state.GetPlatformArtifact(produced.Metadata["intent_generation"])
	if err != nil {
		t.Fatal(err)
	}
	policy, err := state.GetPlatformArtifact(produced.Metadata["policy_generation"])
	if err != nil {
		t.Fatal(err)
	}
	var desiredIntent platformconfig.PlatformIntent
	var desiredPolicy platformconfig.PolicySnapshot
	if decodeCompilerArtifactContent(intent, &desiredIntent) != nil || decodeCompilerArtifactContent(policy, &desiredPolicy) != nil {
		t.Fatal("cannot decode stored input")
	}
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: desiredIntent, Policy: desiredPolicy, RuntimeSnapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	operator := platformProducerPrincipal()
	operator.ActorID = "operator-replay"
	replay, err := server.materializePlatformCompilation(context.Background(), compiled, operator, &platformConfigStoredInputs{Intent: intent, Policy: policy})
	if err != nil || replay.ReleaseArtifact.ID != produced.ID || replay.ReleaseArtifact.CreatedByID != platformproducer.Actor {
		t.Fatal("operator replay altered producer identity", err)
	}
	if _, err := server.reconcilePlatformConfigurationWithCapture(context.Background(), func(context.Context, model.Principal) (platformIntentProjectionResponse, error) {
		return platformIntentProjectionResponse{}, errors.New("capture unavailable")
	}); err == nil {
		t.Fatal("capture failure hidden")
	}
	_, held, _, _ := state.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "shadow")
	if held.ID != second.ID {
		t.Fatal("capture failure replaced shadow")
	}
	activateTestProducer(t, server, "producer-paused", "paused")
	before := count
	if _, err := server.reconcilePlatformConfigurationWithCapture(context.Background(), capture); err != nil || count != before {
		t.Fatal("paused producer captured inputs", err)
	}
	for _, kind := range []string{model.PlatformArtifactKindReleaseSet, model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig, model.PlatformArtifactKindPolicySnapshot} {
		if lkg, err := state.GetPlatformLKG(kind, "global"); err != nil || lkg != nil {
			t.Fatal("shadow producer changed LKG", kind, err)
		}
		for _, channel := range []string{"gray", "full"} {
			if _, _, found, err := state.GetActivePlatformArtifact(kind, "global", channel); err != nil || found {
				t.Fatal("producer activated serving", kind, channel, err)
			}
		}
	}
}

func TestProducerPreservesInputAuthorsAndRejectsConflictingContent(t *testing.T) {
	state, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	request := platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: "authored-intent", Scope: "global", Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", UpstreamURL: "http://origin:8080", Enabled: true}}}, Policy: platformconfig.PolicySnapshot{Generation: "authored-policy", Scope: "global"}}
	compiled, err := platformconfig.Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	creator := platformProducerPrincipal()
	creator.ActorID = "operator"
	original, err := server.materializePlatformCompilation(context.Background(), compiled, creator, nil)
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := server.ensurePlatformProducerInputs(context.Background(), compiled, platformProducerPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	if inputs.Intent.ID != original.IntentArtifact.ID || inputs.Policy.ID != original.PolicyArtifact.ID || inputs.Intent.CreatedByID != creator.ActorID || inputs.Policy.CreatedByID != creator.ActorID {
		t.Fatal("producer replaced immutable authors")
	}
	request.Intent.Routes[0].UpstreamURL = "http://changed:8080"
	changed, err := platformconfig.Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = server.ensurePlatformProducerInputs(context.Background(), changed, platformProducerPrincipal()); !errors.Is(err, store.ErrConflict) {
		t.Fatal("same input generation accepted other content", err)
	}
	a, err := state.GetPlatformArtifact(original.IntentArtifact.ID)
	if err != nil || a.ContentHash != original.IntentArtifact.ContentHash || a.CreatedByID != creator.ActorID {
		t.Fatal("rejected producer changed input", err)
	}
}

func TestPlatformProducerRechecksPolicyAfterCapture(t *testing.T) {
	for _, change := range []string{"paused", "manual target", "invalid compile", "canceled"} {
		t.Run(change, func(t *testing.T) {
			state, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
			activateTestProducer(t, server, "producer-enabled", "shadow")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			capture := func(context.Context, model.Principal) (platformIntentProjectionResponse, error) {
				if change == "paused" {
					activateTestProducer(t, server, "producer-pause", "paused")
				}
				if change == "canceled" {
					cancel()
				}
				if change == "manual target" {
					compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: "manual", Scope: "global"}, Policy: platformconfig.PolicySnapshot{Generation: "manual", Scope: "global"}})
					if err != nil {
						t.Fatal(err)
					}
					result, err := server.materializePlatformCompilation(context.Background(), compiled, platformProducerPrincipal(), nil)
					if err != nil {
						t.Fatal(err)
					}
					if _, _, _, _, err = state.ReleasePlatformArtifact(result.ReleaseArtifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, platformProducerPrincipal()); err != nil {
						t.Fatal(err)
					}
				}
				now := time.Now().UTC()
				p := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Generation: "capture", Scope: "global"}, Policy: platformconfig.PolicySnapshot{Generation: "capture-policy", Scope: "global"}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now}}
				if change == "invalid compile" {
					p.Intent.Routes = []platformconfig.RouteIntent{{Hostname: "bad", Enabled: true}}
				}
				return p, nil
			}
			if _, err := server.reconcilePlatformConfigurationWithCapture(ctx, capture); err == nil {
				t.Fatal("changed authority or failed capture published")
			}
			a, _, found, _ := state.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "shadow")
			if change == "manual target" {
				if !found || a.Metadata[platformproducer.PolicyReleaseMetadata] != "" {
					t.Fatal("manual publication overwritten")
				}
			} else if found {
				t.Fatal("rejected producer changed target")
			}
		})
	}
}

func TestProducerPolicyAPIRejectsInvalidModes(t *testing.T) {
	_, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	for _, mode := range []string{"full", "shadow"} {
		r := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts", admin, model.PlatformArtifactCreateRequest{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: platformproducer.Scope}, Generation: "policy-" + mode, Content: map[string]any{"schema_version": platformproducer.Schema, "generation": "policy-" + mode, "mode": mode, "input_source": "business-static-intent", "static_intent_artifact_id": "synthetic-base", "static_intent_digest": "sha256:" + strings.Repeat("a", 64), "target_scope": "global", "interval_seconds": 30, "refresh_seconds": 300}})
		if r.Code != 201 {
			t.Fatal(r.Body.String())
		}
		var created model.PlatformArtifactResponse
		mustDecodeJSON(t, r, &created)
		r = performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+created.Artifact.ID+"/validate", admin, model.PlatformArtifactValidateRequest{DryRun: false})
		if mode == "full" && r.Code != 409 || mode == "shadow" && r.Code != 200 {
			t.Fatal(r.Code, r.Body.String())
		}
		if mode == "shadow" {
			if _, _, _, _, err := server.store.ReleasePlatformArtifact(created.Artifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "full", SoftOverride: true, Reason: "unsupported producer mode"}, platformProducerPrincipal()); !errors.Is(err, store.ErrConflict) {
				t.Fatal("producer policy entered serving", err)
			}
		}
	}
}

func TestPlatformProducerBackgroundCancellationAndIndependentWriter(t *testing.T) {
	state, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	started := make(chan struct{}, 1)
	server.log = log.New(producerLeadershipLog{started}, "", 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { server.StartBackgroundPlatformConfiguration(ctx); close(done) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("producer never acquired leadership")
	}
	if acquired, err := state.WithAdvisoryLock(context.Background(), "unrelated-config-writer", nil); !acquired || err != nil {
		t.Fatal("producer blocked unrelated writer", err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled background producer did not stop")
	}
	if acquired, err := state.WithAdvisoryLock(context.Background(), platformproducer.Actor, nil); !acquired || err != nil {
		t.Fatal("canceled producer retained leadership", err)
	}
}

type producerLeadershipLog struct{ started chan struct{} }

func (w producerLeadershipLog) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "leadership acquired") {
		select {
		case w.started <- struct{}{}:
		default:
		}
	}
	return len(p), nil
}
