package store

import (
	"context"
	"testing"
	"time"

	"fugue/internal/model"
)

// Two old SHARE guards could coexist, then deadlock when both transactions
// inserted an LKG. The second guard must wait before either upsert starts.
func TestActivationPostgresSelectorGuardCannotDeadlockDuringUpsert(t *testing.T) {
	s, _, _ := inputActivationFixture(t, 0)
	db := s.db
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	first, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Rollback()
	selector := compilerInputActivationTuple{table: "fugue_platform_lkg_snapshots", key: compilerInputLKGSelectorKey(model.PlatformArtifactKindReleaseSet, "global")}
	if err := readCompilerInputActivationTuple(ctx, first, &selector, true); err != nil {
		t.Fatal(err)
	}
	second, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Rollback()
	if _, err := second.ExecContext(ctx, `SET LOCAL lock_timeout='50ms'`); err != nil {
		t.Fatal(err)
	}
	other := selector
	if err := readCompilerInputActivationTuple(ctx, second, &other, true); stringSQLState(err) != "55P03" {
		t.Fatalf("two selector guards could both reach a write upgrade: %v", err)
	}
	if err := second.Rollback(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	snapshot := model.PlatformLKGSnapshot{ID: "neutral-lkg", ArtifactID: "activation-root", ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: "global", SchemaVersion: "v1", Generation: "neutral-generation", ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now}
	if _, err := pgUpsertPlatformLKGSnapshot(ctx, first, snapshot); err != nil {
		t.Fatal("guard could not upgrade to an upsert", err)
	}
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}
	second, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Rollback()
	if err := readCompilerInputActivationTuple(ctx, second, &other, true); err != nil {
		t.Fatal("guard did not recover after predecessor commit", err)
	}
	if _, err := pgUpsertPlatformLKGSnapshot(ctx, second, snapshot); err != nil {
		t.Fatal("successor upsert blocked", err)
	}
	if err := second.Commit(); err != nil {
		t.Fatal(err)
	}
}
