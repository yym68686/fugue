package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

func configureTestPlatformArtifactSigning(s *Store) {
	s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring(
		"platform-artifact-test-signing-key",
		"platform-artifact-test",
		"",
		"",
		nil,
	))
}

func TestPlatformArtifactReleaseRollbackConsumerAndLKG(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	scope := model.PlatformArtifactScope{ScopeType: "global"}
	first, err := s.CreatePlatformArtifact(model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy,
		Scope:        scope,
		Generation:   "rank_gen_1",
		Content:      map[string]any{"weights": map[string]any{"ttfb": 1}},
	})
	if err != nil {
		t.Fatalf("create first artifact: %v", err)
	}
	content, err := s.GetPlatformArtifactContent(first.ContentHash)
	if err != nil {
		t.Fatalf("get content-addressed artifact content: %v", err)
	}
	if content.ContentHash != first.ContentHash || content.SizeBytes <= 0 || content.Content["weights"] == nil {
		t.Fatalf("unexpected content-addressed artifact content: %+v", content)
	}
	second, err := s.CreatePlatformArtifact(model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy,
		Scope:        scope,
		Generation:   "rank_gen_2",
		Content:      map[string]any{"weights": map[string]any{"ttfb": 2}},
	})
	if err != nil {
		t.Fatalf("create second artifact: %v", err)
	}
	passResults := []model.PlatformArtifactValidationResult{{Name: "schema", Pass: true, Severity: model.RobustnessSeverityInfo}}
	first, err = s.ValidatePlatformArtifact(first.ID, passResults)
	if err != nil {
		t.Fatalf("validate first artifact: %v", err)
	}
	second, err = s.ValidatePlatformArtifact(second.ID, passResults)
	if err != nil {
		t.Fatalf("validate second artifact: %v", err)
	}
	if first.Status != model.PlatformArtifactStatusValidated || second.Status != model.PlatformArtifactStatusValidated {
		t.Fatalf("expected validated artifacts, got first=%s second=%s", first.Status, second.Status)
	}
	seedVerifiedPlatformLKG(t, s, first)
	_, release, message, lkg, err := s.ReleasePlatformArtifact(second.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		IdempotencyKey: "release-rank-gen-2",
	}, testPlatformPrincipal())
	if err != nil {
		t.Fatalf("release second artifact: %v", err)
	}
	if release.Generation != second.Generation || message.Generation != second.Generation {
		t.Fatalf("release/message generation mismatch: release=%+v message=%+v", release, message)
	}
	if release.VerificationState != model.PlatformArtifactVerificationStateServingUnverified ||
		release.PinnedRollbackGeneration != first.Generation {
		t.Fatalf("expected serving-unverified release pinned to first generation, got %+v", release)
	}
	if lkg == nil || lkg.Generation != first.Generation {
		t.Fatalf("full release must retain the previous verified LKG until verification, got %+v", lkg)
	}
	_, verifiedRelease, verifiedMessage, lkg, err := s.VerifyPlatformArtifactReleaseLKG(
		release.ID,
		completePlatformVerificationRequest(release.FencingToken, false),
		testPlatformPrincipal(),
	)
	if err != nil {
		t.Fatalf("verify second artifact LKG: %v", err)
	}
	if verifiedRelease.VerificationState != model.PlatformArtifactVerificationStateVerified ||
		verifiedMessage.MessageType != model.PlatformReleaseMessageTypeVerifiedLKG ||
		lkg == nil ||
		lkg.Generation != second.Generation ||
		!lkg.ExpiresAt.After(time.Now()) {
		t.Fatalf("expected verified LKG for second generation, release=%+v message=%+v lkg=%+v", verifiedRelease, verifiedMessage, lkg)
	}
	active, activeRelease, found, err := s.GetActivePlatformArtifact(model.PlatformArtifactKindEdgeRankingPolicy, "global", model.PlatformArtifactReleaseChannelFull)
	if err != nil {
		t.Fatalf("get active artifact: %v", err)
	}
	if !found || active.ID != second.ID || activeRelease.ID != release.ID {
		t.Fatalf("expected second active artifact, found=%t artifact=%+v release=%+v", found, active, activeRelease)
	}
	consumer, err := s.UpsertPlatformConsumerHeartbeat(model.PlatformConsumerHeartbeatRequest{
		ConsumerID:        "edge-worker-1",
		Component:         "edge-worker",
		ArtifactKind:      model.PlatformArtifactKindEdgeRankingPolicy,
		ScopeKey:          "global",
		DesiredGeneration: second.Generation,
		ActualGeneration:  first.Generation,
		LKGGeneration:     second.Generation,
		ApplyStatus:       "drifted",
		ProbeStatus:       "unknown",
	})
	if err != nil {
		t.Fatalf("upsert consumer heartbeat: %v", err)
	}
	if consumer.DesiredGeneration != second.Generation || consumer.ActualGeneration != first.Generation {
		t.Fatalf("unexpected consumer generation state: %+v", consumer)
	}
	consumers, err := s.ListPlatformConsumers(model.PlatformArtifactKindEdgeRankingPolicy, "global")
	if err != nil {
		t.Fatalf("list consumers: %v", err)
	}
	if len(consumers) != 1 || consumers[0].ConsumerID != "edge-worker-1" {
		t.Fatalf("expected one consumer, got %+v", consumers)
	}
	target, rollbackRelease, rollbackMessage, rollbackLKG, err := s.RollbackPlatformArtifact(second.ID, model.PlatformArtifactRollbackRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		ToGeneration:   first.Generation,
		Reason:         "test rollback",
	}, testPlatformPrincipal())
	if err != nil {
		t.Fatalf("rollback artifact: %v", err)
	}
	if target.ID != first.ID || rollbackRelease.Generation != first.Generation || rollbackRelease.RollbackTargetGeneration != second.Generation || rollbackMessage.MessageType != model.PlatformReleaseMessageTypeRollback {
		t.Fatalf("unexpected rollback state: target=%+v release=%+v message=%+v", target, rollbackRelease, rollbackMessage)
	}
	if rollbackLKG == nil || rollbackLKG.Generation != second.Generation {
		t.Fatalf("rollback release must retain the second verified LKG until rollback verification, got %+v", rollbackLKG)
	}
	_, rollbackRelease, rollbackMessage, rollbackLKG, err = s.VerifyPlatformArtifactReleaseLKG(
		rollbackRelease.ID,
		completePlatformVerificationRequest(rollbackRelease.FencingToken, false),
		testPlatformPrincipal(),
	)
	if err != nil {
		t.Fatalf("verify rollback generation: %v", err)
	}
	if rollbackRelease.VerificationState != model.PlatformArtifactVerificationStateVerified ||
		rollbackMessage.MessageType != model.PlatformReleaseMessageTypeVerifiedLKG ||
		rollbackLKG == nil ||
		rollbackLKG.Generation != first.Generation {
		t.Fatalf("expected verified rollback LKG for first generation, release=%+v message=%+v lkg=%+v", rollbackRelease, rollbackMessage, rollbackLKG)
	}
}

func TestPlatformStatePeriodicPullSurvivesReleaseMessageLoss(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	artifact, err := s.CreatePlatformArtifact(model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   "dns_gen_periodic_pull",
		Content:      map[string]any{"records": []any{map[string]any{"name": "api.fugue.pro"}}},
	})
	if err != nil {
		t.Fatalf("create artifact: %v", err)
	}
	passResults := []model.PlatformArtifactValidationResult{{Name: "schema", Pass: true, Severity: model.RobustnessSeverityInfo}}
	artifact, err = s.ValidatePlatformArtifact(artifact.ID, passResults)
	if err != nil {
		t.Fatalf("validate artifact: %v", err)
	}
	seedVerifiedPlatformLKG(t, s, artifact)
	_, release, _, _, err := s.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelFull}, testPlatformPrincipal())
	if err != nil {
		t.Fatalf("release full artifact: %v", err)
	}
	if _, _, _, _, err := s.VerifyPlatformArtifactReleaseLKG(release.ID, completePlatformVerificationRequest(release.FencingToken, false), testPlatformPrincipal()); err != nil {
		t.Fatalf("verify full artifact: %v", err)
	}
	if err := s.withLockedState(true, func(state *model.State) error {
		state.PlatformReleaseMessages = nil
		return nil
	}); err != nil {
		t.Fatalf("drop release messages: %v", err)
	}

	active, release, found, err := s.GetActivePlatformArtifact(model.PlatformArtifactKindDNSAnswerBundle, "global", model.PlatformArtifactReleaseChannelFull)
	if err != nil {
		t.Fatalf("periodic pull active artifact: %v", err)
	}
	if !found || active.Generation != artifact.Generation || release.Generation != artifact.Generation {
		t.Fatalf("expected active artifact despite message loss, found=%t active=%+v release=%+v", found, active, release)
	}
	messages, err := s.ListPlatformReleaseMessages(model.PlatformArtifactKindDNSAnswerBundle, "global", time.Now().Add(-time.Hour), 10)
	if err != nil {
		t.Fatalf("list release messages: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("test setup should simulate lost release messages, got %+v", messages)
	}
	lkg, err := s.GetPlatformLKG(model.PlatformArtifactKindDNSAnswerBundle, "global")
	if err != nil {
		t.Fatalf("get LKG: %v", err)
	}
	if lkg == nil || lkg.Generation != artifact.Generation {
		t.Fatalf("expected full-release LKG to survive message loss, got %+v", lkg)
	}
}

func TestPlatformGrayReleaseAbortDoesNotOverwriteFullLKG(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	scope := model.PlatformArtifactScope{ScopeType: "global"}
	stable, err := s.CreatePlatformArtifact(model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle,
		Scope:        scope,
		Generation:   "route_gen_stable",
		Content:      map[string]any{"routes": []any{map[string]any{"hostname": "api.fugue.pro"}}},
	})
	if err != nil {
		t.Fatalf("create stable artifact: %v", err)
	}
	canary, err := s.CreatePlatformArtifact(model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle,
		Scope:        scope,
		Generation:   "route_gen_canary",
		Content:      map[string]any{"routes": []any{map[string]any{"hostname": "api.fugue.pro"}}},
	})
	if err != nil {
		t.Fatalf("create canary artifact: %v", err)
	}
	passResults := []model.PlatformArtifactValidationResult{{Name: "schema", Pass: true, Severity: model.RobustnessSeverityInfo}}
	stable, err = s.ValidatePlatformArtifact(stable.ID, passResults)
	if err != nil {
		t.Fatalf("validate stable artifact: %v", err)
	}
	canary, err = s.ValidatePlatformArtifact(canary.ID, passResults)
	if err != nil {
		t.Fatalf("validate canary artifact: %v", err)
	}
	seedVerifiedPlatformLKG(t, s, stable)
	if _, release, _, lkg, err := s.ReleasePlatformArtifact(stable.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelFull}, testPlatformPrincipal()); err != nil {
		t.Fatalf("release stable full artifact: %v", err)
	} else if lkg == nil || lkg.Generation != stable.Generation {
		t.Fatalf("expected seeded stable LKG during full release, got %+v", lkg)
	} else if _, _, _, verifiedLKG, err := s.VerifyPlatformArtifactReleaseLKG(release.ID, completePlatformVerificationRequest(release.FencingToken, false), testPlatformPrincipal()); err != nil {
		t.Fatalf("verify stable full artifact: %v", err)
	} else if verifiedLKG == nil || verifiedLKG.Generation != stable.Generation {
		t.Fatalf("expected verified stable full LKG, got %+v", verifiedLKG)
	}
	if _, _, _, lkg, err := s.ReleasePlatformArtifact(canary.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelGray, CanaryRuleRef: "edge=bwg"}, testPlatformPrincipal()); err != nil {
		t.Fatalf("release canary gray artifact: %v", err)
	} else if lkg == nil || lkg.Generation != stable.Generation {
		t.Fatalf("gray release must retain the stable full LKG, got %+v", lkg)
	}
	if _, rollbackRelease, _, lkg, err := s.RollbackPlatformArtifact(canary.ID, model.PlatformArtifactRollbackRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelGray,
		ToGeneration:   stable.Generation,
		Reason:         "abort gray release",
	}, testPlatformPrincipal()); err != nil {
		t.Fatalf("rollback gray artifact: %v", err)
	} else if lkg == nil || lkg.Generation != stable.Generation {
		t.Fatalf("gray rollback must retain the stable full LKG, got %+v", lkg)
	} else if rollbackRelease.CanaryRuleRef != "edge=bwg" {
		t.Fatalf("gray rollback must inherit the active canary scope, got %+v", rollbackRelease)
	}
	activeFull, _, found, err := s.GetActivePlatformArtifact(model.PlatformArtifactKindEdgeRouteBundle, "global", model.PlatformArtifactReleaseChannelFull)
	if err != nil {
		t.Fatalf("get active full artifact: %v", err)
	}
	if !found || activeFull.Generation != stable.Generation {
		t.Fatalf("expected full channel to remain on stable generation, found=%t active=%+v", found, activeFull)
	}
	fullLKG, err := s.GetPlatformLKG(model.PlatformArtifactKindEdgeRouteBundle, "global")
	if err != nil {
		t.Fatalf("get full LKG: %v", err)
	}
	if fullLKG == nil || fullLKG.Generation != stable.Generation {
		t.Fatalf("expected full LKG to remain stable after gray abort, got %+v", fullLKG)
	}
}

func TestPlatformBadFullReleaseDoesNotOverwriteVerifiedLKG(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	stable := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindEdgeRouteBundle, "stable", map[string]any{"routes": []any{"stable"}})
	candidate := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindEdgeRouteBundle, "candidate", map[string]any{"routes": []any{"candidate"}})
	seedVerifiedPlatformLKG(t, s, stable)

	_, release, _, lkg, err := s.ReleasePlatformArtifact(candidate.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
	}, testPlatformPrincipal())
	if err != nil {
		t.Fatalf("release candidate: %v", err)
	}
	if lkg == nil || lkg.Generation != stable.Generation {
		t.Fatalf("candidate release overwrote stable LKG before verification: %+v", lkg)
	}
	incomplete := completePlatformVerificationRequest(release.FencingToken, false)
	incomplete.Evidence.PublicSynthetic = false
	if _, _, _, _, err := s.VerifyPlatformArtifactReleaseLKG(release.ID, incomplete, testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected incomplete evidence conflict, got %v", err)
	}
	lkg, err = s.GetPlatformLKG(candidate.ArtifactKind, candidate.ScopeKey)
	if err != nil {
		t.Fatalf("get LKG after rejected verification: %v", err)
	}
	if lkg == nil || lkg.Generation != stable.Generation {
		t.Fatalf("bad candidate must not overwrite verified LKG, got %+v", lkg)
	}
}

func TestPlatformReleaseFencingAndIdempotency(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	stable := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindDNSAnswerBundle, "stable", map[string]any{"records": []any{"stable"}})
	first := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindDNSAnswerBundle, "first", map[string]any{"records": []any{"first"}})
	second := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindDNSAnswerBundle, "second", map[string]any{"records": []any{"second"}})
	seedVerifiedPlatformLKG(t, s, stable)

	req := model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		IdempotencyKey: "dns-first-release",
	}
	_, firstRelease, _, _, err := s.ReleasePlatformArtifact(first.ID, req, testPlatformPrincipal())
	if err != nil {
		t.Fatalf("release first candidate: %v", err)
	}
	_, idempotentRelease, _, _, err := s.ReleasePlatformArtifact(first.ID, req, testPlatformPrincipal())
	if err != nil {
		t.Fatalf("repeat idempotent release: %v", err)
	}
	if idempotentRelease.ID != firstRelease.ID || idempotentRelease.FencingToken != firstRelease.FencingToken {
		t.Fatalf("idempotent release created a second ledger entry: first=%+v repeated=%+v", firstRelease, idempotentRelease)
	}
	_, secondRelease, _, _, err := s.ReleasePlatformArtifact(second.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		IdempotencyKey: "dns-second-release",
	}, testPlatformPrincipal())
	if err != nil {
		t.Fatalf("release second candidate: %v", err)
	}
	if secondRelease.FencingToken <= firstRelease.FencingToken {
		t.Fatalf("fencing token must increase monotonically: first=%d second=%d", firstRelease.FencingToken, secondRelease.FencingToken)
	}
	if _, _, _, _, err := s.VerifyPlatformArtifactReleaseLKG(firstRelease.ID, completePlatformVerificationRequest(firstRelease.FencingToken, false), testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale release verification must fail, got %v", err)
	}
	if _, _, _, lkg, err := s.VerifyPlatformArtifactReleaseLKG(secondRelease.ID, completePlatformVerificationRequest(secondRelease.FencingToken, false), testPlatformPrincipal()); err != nil {
		t.Fatalf("verify current release: %v", err)
	} else if lkg == nil || lkg.Generation != second.Generation {
		t.Fatalf("expected current fenced release to become LKG, got %+v", lkg)
	}
}

func TestPlatformInitialLKGRequiresExplicitShadowSeed(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	artifact := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindGatePolicyRegistry, "initial", map[string]any{"version": "v1", "policies": []any{}})
	if _, _, _, _, err := s.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelFull}, testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
		t.Fatalf("initial full release without verified rollback must fail, got %v", err)
	}
	_, shadowRelease, _, _, err := s.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelShadow}, testPlatformPrincipal())
	if err != nil {
		t.Fatalf("release initial shadow: %v", err)
	}
	if _, _, _, _, err := s.VerifyPlatformArtifactReleaseLKG(shadowRelease.ID, completePlatformVerificationRequest(shadowRelease.FencingToken, false), testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
		t.Fatalf("initial shadow verification without explicit approval must fail, got %v", err)
	}
	if _, _, _, lkg, err := s.VerifyPlatformArtifactReleaseLKG(shadowRelease.ID, completePlatformVerificationRequest(shadowRelease.FencingToken, true), testPlatformPrincipal()); err != nil {
		t.Fatalf("verify explicit initial shadow seed: %v", err)
	} else if lkg == nil || lkg.Generation != artifact.Generation {
		t.Fatalf("expected explicit initial shadow LKG, got %+v", lkg)
	}
}

func TestPlatformArtifactContentHashSurvivesJSONRoundTrip(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	artifact := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindGatePolicyRegistry, "canonical", map[string]any{
		"version":    "v1",
		"updated_at": time.Date(2026, 7, 10, 7, 0, 0, 0, time.UTC),
		"policies": []any{map[string]any{
			"id":    "test.gate",
			"mode":  "shadow",
			"scope": "cluster",
		}},
	})
	reloaded, err := s.GetPlatformArtifact(artifact.ID)
	if err != nil {
		t.Fatalf("reload artifact: %v", err)
	}
	if reloaded.ContentHash != artifact.ContentHash {
		t.Fatalf("content hash changed across persistence: created=%s reloaded=%s", artifact.ContentHash, reloaded.ContentHash)
	}
	if reloaded.SchemaVersion != model.PlatformArtifactSchemaVersionV1 ||
		reloaded.GenerationSequence != 1 ||
		reloaded.Provenance.Signature == "" {
		t.Fatalf("artifact provenance did not survive persistence: %+v", reloaded)
	}
	if _, _, _, _, err := s.ReleasePlatformArtifact(reloaded.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
	}, testPlatformPrincipal()); err != nil {
		t.Fatalf("canonical artifact should pass release integrity check: %v", err)
	}
}

func TestPlatformArtifactGenerationSequenceIsMonotonicAndRollbackOnlyExemption(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	first := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindTrafficSafetyPolicy, "sequence-first", map[string]any{"min_healthy_edges": 1})
	second := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindTrafficSafetyPolicy, "sequence-second", map[string]any{"min_healthy_edges": 2})
	if first.GenerationSequence != 1 || second.GenerationSequence != 2 {
		t.Fatalf("expected monotonic artifact sequences 1 and 2, got first=%d second=%d", first.GenerationSequence, second.GenerationSequence)
	}
	request := model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		IdempotencyKey: "sequence-second-release",
	}
	_, secondRelease, _, _, err := s.ReleasePlatformArtifact(second.ID, request, testPlatformPrincipal())
	if err != nil {
		t.Fatalf("release second generation: %v", err)
	}
	_, repeatedRelease, _, _, err := s.ReleasePlatformArtifact(second.ID, request, testPlatformPrincipal())
	if err != nil {
		t.Fatalf("idempotent retry must precede monotonic rejection: %v", err)
	}
	if repeatedRelease.ID != secondRelease.ID {
		t.Fatalf("idempotent retry created a different release: first=%+v repeated=%+v", secondRelease, repeatedRelease)
	}
	if _, _, _, _, err := s.ReleasePlatformArtifact(first.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
	}, testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
		t.Fatalf("ordinary release of an older sequence must fail, got %v", err)
	}
	if _, rollbackRelease, _, _, err := s.RollbackPlatformArtifact(second.ID, model.PlatformArtifactRollbackRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		ToGeneration:   first.Generation,
		Reason:         "test explicit rollback sequence exemption",
	}, testPlatformPrincipal()); err != nil {
		t.Fatalf("explicit rollback of an older signed generation must pass: %v", err)
	} else if rollbackRelease.Generation != first.Generation {
		t.Fatalf("rollback did not target the requested older generation: %+v", rollbackRelease)
	}
}

func TestPlatformArtifactCreationFailsClosedWithoutSigningKey(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	if _, err := s.CreatePlatformArtifact(model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindReleaseGuardPolicy,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   "unsigned",
		Content:      map[string]any{"version": "v1"},
	}); !errors.Is(err, platformsafety.ErrPlatformSigningKeyUnavailable) {
		t.Fatalf("artifact creation without a signing key must fail closed, got %v", err)
	}
}

func TestPlatformFullReleaseRejectsExpiredOrUnreadableRollbackLKG(t *testing.T) {
	t.Parallel()

	t.Run("expired", func(t *testing.T) {
		s := New(filepath.Join(t.TempDir(), "store.json"))
		configureTestPlatformArtifactSigning(s)
		if err := s.Init(); err != nil {
			t.Fatalf("init store: %v", err)
		}
		stable := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindEdgeRankingPolicy, "stable-expired", map[string]any{"weights": map[string]any{"ttfb": 1}})
		candidate := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindEdgeRankingPolicy, "candidate-expired", map[string]any{"weights": map[string]any{"ttfb": 2}})
		seedVerifiedPlatformLKG(t, s, stable)
		if err := s.withLockedState(true, func(state *model.State) error {
			for index := range state.PlatformLKGSnapshots {
				if state.PlatformLKGSnapshots[index].ArtifactKind == stable.ArtifactKind &&
					state.PlatformLKGSnapshots[index].ScopeKey == stable.ScopeKey {
					state.PlatformLKGSnapshots[index].ExpiresAt = time.Now().UTC().Add(-time.Minute)
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("expire LKG: %v", err)
		}
		if _, _, _, _, err := s.ReleasePlatformArtifact(candidate.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelFull}, testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
			t.Fatalf("full release with expired rollback LKG must fail, got %v", err)
		}
	})

	t.Run("artifact-missing", func(t *testing.T) {
		s := New(filepath.Join(t.TempDir(), "store.json"))
		configureTestPlatformArtifactSigning(s)
		if err := s.Init(); err != nil {
			t.Fatalf("init store: %v", err)
		}
		stable := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindTrafficSafetyPolicy, "stable-missing", map[string]any{"min_healthy_edges": 1})
		candidate := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindTrafficSafetyPolicy, "candidate-missing", map[string]any{"min_healthy_edges": 2})
		seedVerifiedPlatformLKG(t, s, stable)
		if err := s.withLockedState(true, func(state *model.State) error {
			artifacts := make([]model.PlatformArtifact, 0, len(state.PlatformArtifacts))
			for _, artifact := range state.PlatformArtifacts {
				if artifact.ID != stable.ID {
					artifacts = append(artifacts, artifact)
				}
			}
			state.PlatformArtifacts = artifacts
			return nil
		}); err != nil {
			t.Fatalf("remove rollback artifact: %v", err)
		}
		if _, _, _, _, err := s.ReleasePlatformArtifact(candidate.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelFull}, testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
			t.Fatalf("full release with unreadable rollback artifact must fail, got %v", err)
		}
	})

	t.Run("snapshot-signature-invalid", func(t *testing.T) {
		s := New(filepath.Join(t.TempDir(), "store.json"))
		configureTestPlatformArtifactSigning(s)
		if err := s.Init(); err != nil {
			t.Fatalf("init store: %v", err)
		}
		stable := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindDNSAnswerBundle, "stable-signature-invalid", map[string]any{"records": []any{"stable"}})
		candidate := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindDNSAnswerBundle, "candidate-signature-invalid", map[string]any{"records": []any{"candidate"}})
		seedVerifiedPlatformLKG(t, s, stable)
		if err := s.withLockedState(true, func(state *model.State) error {
			for index := range state.PlatformLKGSnapshots {
				if state.PlatformLKGSnapshots[index].ArtifactKind == stable.ArtifactKind &&
					state.PlatformLKGSnapshots[index].ScopeKey == stable.ScopeKey {
					state.PlatformLKGSnapshots[index].SnapshotProvenance.Signature = "invalid"
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("corrupt LKG signature: %v", err)
		}
		if _, _, _, _, err := s.ReleasePlatformArtifact(candidate.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelFull}, testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
			t.Fatalf("full release with signature-invalid rollback LKG must fail, got %v", err)
		}
	})

	t.Run("artifact-content-corrupt", func(t *testing.T) {
		s := New(filepath.Join(t.TempDir(), "store.json"))
		configureTestPlatformArtifactSigning(s)
		if err := s.Init(); err != nil {
			t.Fatalf("init store: %v", err)
		}
		stable := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindEdgeRankingPolicy, "stable-corrupt", map[string]any{"weights": map[string]any{"ttfb": 1}})
		candidate := createValidatedPlatformArtifact(t, s, model.PlatformArtifactKindEdgeRankingPolicy, "candidate-corrupt", map[string]any{"weights": map[string]any{"ttfb": 2}})
		seedVerifiedPlatformLKG(t, s, stable)
		if err := s.withLockedState(true, func(state *model.State) error {
			for index := range state.PlatformArtifacts {
				if state.PlatformArtifacts[index].ID == stable.ID {
					state.PlatformArtifacts[index].Content["tampered"] = true
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("corrupt rollback artifact: %v", err)
		}
		if _, _, _, _, err := s.ReleasePlatformArtifact(candidate.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelFull}, testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
			t.Fatalf("full release with corrupt rollback artifact must fail, got %v", err)
		}
	})
}

func testPlatformPrincipal() model.Principal {
	return model.Principal{ActorType: model.ActorTypeSystem, ActorID: "test"}
}

func completePlatformVerificationRequest(fencingToken int64, allowInitial bool) model.PlatformArtifactVerifyLKGRequest {
	return model.PlatformArtifactVerifyLKGRequest{
		FencingToken:    fencingToken,
		Reason:          "test verification completed",
		AllowInitialLKG: allowInitial,
		Evidence: model.PlatformArtifactVerificationEvidence{
			ConsumerConvergence:        true,
			LocalProbe:                 true,
			PublicSynthetic:            true,
			WatchWindow:                true,
			BaselineMonotonic:          true,
			DatabaseRollbackCompatible: true,
			EvidenceRefs:               []string{"test:evidence"},
		},
	}
}

func createValidatedPlatformArtifact(t *testing.T, s *Store, kind, generation string, content map[string]any) model.PlatformArtifact {
	t.Helper()
	artifact, err := s.CreatePlatformArtifact(model.PlatformArtifact{
		ArtifactKind: kind,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   generation,
		Content:      content,
	})
	if err != nil {
		t.Fatalf("create platform artifact %s: %v", generation, err)
	}
	artifact, err = s.ValidatePlatformArtifact(artifact.ID, []model.PlatformArtifactValidationResult{{
		Name:     "schema",
		Pass:     true,
		Severity: model.RobustnessSeverityInfo,
	}})
	if err != nil {
		t.Fatalf("validate platform artifact %s: %v", generation, err)
	}
	return artifact
}

func seedVerifiedPlatformLKG(t *testing.T, s *Store, artifact model.PlatformArtifact) *model.PlatformLKGSnapshot {
	t.Helper()
	_, release, _, existing, err := s.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		IdempotencyKey: "seed-" + artifact.Generation,
	}, testPlatformPrincipal())
	if err != nil {
		t.Fatalf("release initial shadow %s: %v", artifact.Generation, err)
	}
	if existing != nil {
		t.Fatalf("expected no LKG before initial seed, got %+v", existing)
	}
	_, _, _, lkg, err := s.VerifyPlatformArtifactReleaseLKG(
		release.ID,
		completePlatformVerificationRequest(release.FencingToken, true),
		testPlatformPrincipal(),
	)
	if err != nil {
		t.Fatalf("verify initial LKG %s: %v", artifact.Generation, err)
	}
	return lkg
}
