package store

import (
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPlatformArtifactReleaseRollbackConsumerAndLKG(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
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
	_, release, message, lkg, err := s.ReleasePlatformArtifact(second.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelFull}, model.Principal{ActorType: model.ActorTypeSystem, ActorID: "test"})
	if err != nil {
		t.Fatalf("release second artifact: %v", err)
	}
	if release.Generation != second.Generation || message.Generation != second.Generation {
		t.Fatalf("release/message generation mismatch: release=%+v message=%+v", release, message)
	}
	if lkg == nil || lkg.Generation != second.Generation || !lkg.ExpiresAt.After(time.Now()) {
		t.Fatalf("expected fresh LKG for second generation, got %+v", lkg)
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
	}, model.Principal{ActorType: model.ActorTypeSystem, ActorID: "test"})
	if err != nil {
		t.Fatalf("rollback artifact: %v", err)
	}
	if target.ID != first.ID || rollbackRelease.Generation != first.Generation || rollbackRelease.RollbackTargetGeneration != second.Generation || rollbackMessage.MessageType != model.PlatformReleaseMessageTypeRollback {
		t.Fatalf("unexpected rollback state: target=%+v release=%+v message=%+v", target, rollbackRelease, rollbackMessage)
	}
	if rollbackLKG == nil || rollbackLKG.Generation != first.Generation {
		t.Fatalf("expected rollback LKG for first generation, got %+v", rollbackLKG)
	}
}

func TestPlatformStatePeriodicPullSurvivesReleaseMessageLoss(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
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
	if _, _, _, _, err := s.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelFull}, model.Principal{ActorType: model.ActorTypeSystem, ActorID: "test"}); err != nil {
		t.Fatalf("release artifact: %v", err)
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
	if _, _, _, lkg, err := s.ReleasePlatformArtifact(stable.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelFull}, model.Principal{ActorType: model.ActorTypeSystem, ActorID: "test"}); err != nil {
		t.Fatalf("release stable full artifact: %v", err)
	} else if lkg == nil || lkg.Generation != stable.Generation {
		t.Fatalf("expected stable full LKG, got %+v", lkg)
	}
	if _, _, _, lkg, err := s.ReleasePlatformArtifact(canary.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelGray, CanaryRuleRef: "edge=bwg"}, model.Principal{ActorType: model.ActorTypeSystem, ActorID: "test"}); err != nil {
		t.Fatalf("release canary gray artifact: %v", err)
	} else if lkg != nil {
		t.Fatalf("gray release must not replace full LKG, got %+v", lkg)
	}
	if _, _, _, lkg, err := s.RollbackPlatformArtifact(canary.ID, model.PlatformArtifactRollbackRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelGray,
		ToGeneration:   stable.Generation,
		Reason:         "abort gray release",
	}, model.Principal{ActorType: model.ActorTypeSystem, ActorID: "test"}); err != nil {
		t.Fatalf("rollback gray artifact: %v", err)
	} else if lkg != nil {
		t.Fatalf("gray rollback must not replace full LKG, got %+v", lkg)
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
