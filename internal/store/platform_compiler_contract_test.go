package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

func installCompilerContractTest(t *testing.T, s *Store) {
	t.Helper()
	for range 2 {
		if err := schemamigrate.MigrateCompilerContract(t.Context(), s.databaseURL); err != nil {
			t.Fatal(err)
		}
	}
}

func enforceCompilerContractTest(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.db.Exec(`UPDATE fugue_platform_compiler_contract SET epoch=epoch+1,phase='enforced',plan_sha256=$1,enforced_at=now() WHERE singleton`, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
}

func TestCompilerContractPostgresOverridesCapabilityAndTraversesChildren(t *testing.T) {
	s, _, _ := inputActivationFixturePhase(t, 0, false)
	installCompilerContractTest(t, s)
	activationInsertSigned(t, s, "old-child", model.PlatformArtifactKindEdgeRouteBundle, map[string]any{"neutral": true}, map[string]string{"compiler_version": "platform-config-compiler/v36"})
	activationInsertSigned(t, s, "wrapper", model.PlatformArtifactKindReleaseSet, map[string]any{"artifact_ids": []string{"old-child"}}, nil)
	roots := []inputarchive.HotRoot{{Source: "activation", ID: "neutral", Reference: inputarchive.HotReference{Kind: "artifact", ID: "wrapper"}}}
	proof, err := s.prepareOrdinaryCompilerInputProof(t.Context(), roots, nil, nil, nil, nil)
	if err != nil || !proof.capabilityOnly {
		t.Fatal("observe rollout changed existing availability", err)
	}
	enforceCompilerContractTest(t, s)
	if err := s.withPreparedCompilerInputActivation(t.Context(), proof, nil, func(context.Context, *sql.Tx) error { return nil }); !errors.Is(err, ErrConflict) {
		t.Fatal("pre-transition proof admitted after epoch change", err)
	}
	if _, err := s.prepareOrdinaryCompilerInputProof(t.Context(), roots, nil, nil, nil, nil); !errors.Is(err, ErrCompilerContractRetired) {
		t.Fatal("nested legacy contract escaped complete admission", err)
	}
}

func TestCompilerContractPostgresLedgerCannotBeDisarmedOrTruncated(t *testing.T) {
	s, _, _ := inputActivationFixturePhase(t, 0, false)
	installCompilerContractTest(t, s)
	enforceCompilerContractTest(t, s)
	for _, q := range []string{
		`UPDATE fugue_platform_compiler_contract SET phase='observe',epoch=epoch+1,plan_sha256='',enforced_at=NULL`,
		`DELETE FROM fugue_platform_compiler_contract`,
		`TRUNCATE fugue_platform_compiler_contract`,
		`UPDATE fugue_platform_compiler_contract SET compiler_version='platform-config-compiler/v39',epoch=epoch+1`,
	} {
		if _, err := s.db.Exec(q); err == nil {
			t.Fatal("ledger was disarmed", q)
		}
	}
}

func TestCompilerContractPostgresOldWriterIsFenced(t *testing.T) {
	s, _, _ := inputActivationFixturePhase(t, 0, false)
	installCompilerContractTest(t, s)
	activationInsertSigned(t, s, "legacy", model.PlatformArtifactKindEdgeRouteBundle, map[string]any{"neutral": true}, map[string]string{"compiler_version": "platform-config-compiler/v36"})
	enforceCompilerContractTest(t, s)
	for _, q := range []string{
		`UPDATE fugue_platform_artifacts SET metadata_json=jsonb_build_object('compiler_version','platform-config-compiler/v37') WHERE id='activation-root'`,
		`INSERT INTO fugue_platform_artifact_releases(id,artifact_id,artifact_kind,scope_key,generation,release_channel,status,released_at,created_at,updated_at) VALUES('old-release','legacy','edge_route_bundle','global','legacy','full','active',now(),now(),now())`,
	} {
		if _, err := s.db.Exec(q); err == nil {
			t.Fatal("old direct writer escaped database fence")
		}
	}
	// A non-compiler artifact remains compatible with the independent ledger.
	if _, err := s.db.Exec(`UPDATE fugue_platform_artifacts SET updated_at=now() WHERE id='activation-root'`); err != nil {
		t.Fatal(err)
	}
}

func TestCompilerContractPostgresEnforcedNonCompilerProofStillCommits(t *testing.T) {
	s, roots, _ := inputActivationFixturePhase(t, 0, false)
	installCompilerContractTest(t, s)
	enforceCompilerContractTest(t, s)
	proof, err := s.prepareOrdinaryCompilerInputProof(t.Context(), roots, nil, nil, nil, nil)
	if err != nil || proof.capabilityOnly || !proof.closure.MetadataComplete {
		t.Fatal("contract enforcement lost complete proof", err)
	}
	if err = s.withPreparedCompilerInputActivation(t.Context(), proof, nil, func(context.Context, *sql.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestCompilerContractPostgresTransitionWaitsForObserveWriter(t *testing.T) {
	s, _, _ := inputActivationFixturePhase(t, 0, false)
	installCompilerContractTest(t, s)
	tx, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE fugue_platform_artifacts SET updated_at=now() WHERE id='activation-root'`); err != nil {
		t.Fatal(err)
	}
	// An old writer has passed observe admission but has not committed. The
	// epoch transition must wait for that writer, rather than overtake it.
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	_, err = s.db.ExecContext(ctx, `UPDATE fugue_platform_compiler_contract SET epoch=epoch+1,phase='enforced',plan_sha256=$1,enforced_at=now() WHERE singleton`, strings.Repeat("a", 64))
	if err == nil {
		t.Fatal("transition overtook an admitted observe writer")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	enforceCompilerContractTest(t, s)
}
