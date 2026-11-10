package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

func TestPlatformInputPostgresLKGWriterLocksExactPresentSelector(t *testing.T) {
	s, _, _ := inputActivationFixture(t, 1)
	if _, err := s.ValidatePlatformArtifact("activation-root", []model.PlatformArtifactValidationResult{{Name: "schema", Pass: true}}); err != nil {
		t.Fatal(err)
	}
	insertTaskConfigurationLKG(t, s, "activation-root")
	lkg, err := s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil || lkg == nil {
		t.Fatal(lkg, err)
	}
	otherLKG := *lkg
	artifact, err := s.GetPlatformArtifact("activation-root")
	if err != nil {
		t.Fatal(err)
	}
	artifact.ID, artifact.Generation, artifact.ScopeKey = "other-root", "other-root", "other-scope"
	artifact, err = platformsafety.SignPlatformArtifact(artifact, s.platformArtifactSigningKeyring())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgInsertPlatformArtifact(t.Context(), s.db, artifact); err != nil {
		t.Fatal(err)
	}
	otherLKG.ID, otherLKG.ScopeKey = "other-lkg", "other-scope"
	otherLKG.ArtifactID, otherLKG.Generation = artifact.ID, artifact.Generation
	otherLKG.ArtifactProvenance, otherLKG.ContentHash = artifact.Provenance, artifact.ContentHash
	otherLKG, err = platformsafety.SignPlatformLKGSnapshot(otherLKG, s.platformArtifactSigningKeyring())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgUpsertPlatformLKGSnapshot(t.Context(), s.db, otherLKG); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	prepare := func(scope string) (*compilerInputActivationProof, []compilerInputPinBinding) {
		t.Helper()
		lkg, err := s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, scope)
		if err != nil || lkg == nil {
			t.Fatal(lkg, err)
		}
		proof, err := s.prepareOrdinaryCompilerInputProof(ctx, nil, nil, nil, nil, []PlatformInputReadSelection{{LKGKind: model.PlatformArtifactKindReleaseSet, LKGScope: scope, LKG: lkg}})
		if err != nil {
			t.Fatal(err)
		}
		proof.lkgReadOnly = false // Exercise publication admission, not a reader.
		proof, pins, err := s.prepareCompilerInputActivationPins(ctx, proof)
		if err != nil {
			t.Fatal(err)
		}
		return proof, pins
	}
	first, pins := prepare("global")
	other, otherPins := prepare("other-scope")
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- s.withPreparedCompilerInputActivation(ctx, first, pins, func(ctx context.Context, tx *sql.Tx) error {
			close(entered)
			select {
			case <-release:
				_, err := tx.ExecContext(ctx, `UPDATE fugue_platform_lkg_snapshots SET updated_at=now() WHERE scope_key='global'`)
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := s.withPreparedCompilerInputActivation(ctx, other, otherPins, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE fugue_platform_lkg_snapshots SET updated_at=now() WHERE scope_key='other-scope'`)
		return err
	}); err != nil {
		t.Fatal("unrelated LKG writer serialized behind publication", err)
	}
	for _, query := range []string{
		`SELECT id FROM fugue_platform_lkg_snapshots WHERE scope_key='global' FOR SHARE`,
		`UPDATE fugue_platform_lkg_snapshots SET updated_at=now() WHERE scope_key='global'`,
		`DELETE FROM fugue_platform_lkg_snapshots WHERE scope_key='global'`,
		`TRUNCATE fugue_platform_lkg_snapshots`,
		`DROP INDEX idx_fugue_platform_lkg_kind_scope`,
	} {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.ExecContext(ctx, `SET LOCAL lock_timeout='50ms'`); err == nil {
			_, err = tx.ExecContext(ctx, query)
		}
		_ = tx.Rollback()
		if stringSQLState(err) != "55P03" {
			t.Fatalf("publication did not fence %q: %v", query, err)
		}
	}
}
