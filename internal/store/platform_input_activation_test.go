package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
	"fugue/internal/schemamigrate"
)

func inputActivationFixture(t *testing.T, inputs int) (*Store, []inputarchive.HotRoot, []compilerInputPinBinding) {
	return inputActivationFixturePhase(t, inputs, true)
}

func inputActivationFixturePhase(t *testing.T, inputs int, enforced bool) (*Store, []inputarchive.HotRoot, []compilerInputPinBinding) {
	t.Helper()
	db, address, _ := hotInventoryTestDatabase(t)
	// The same fixed SQL as declarative CI; never grant SET on a GUC to the
	// application role or use its credentials to install privileged code.
	if _, err := db.Exec(schemamigrate.CompilerInputZeroTempSQL); err != nil {
		t.Fatal("install disposable admin-owned zero-temp guard", err)
	}
	s := &Store{db: db, databaseURL: address}
	s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("neutral-test-key", "neutral", "", "", nil))
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED", "true")
	children, pins := []string{}, []compilerInputPinBinding{}
	for i := 0; i < inputs; i++ {
		raw := []byte(fmt.Sprintf(`{"neutral":%d}`, i))
		sum := sha256.Sum256(raw)
		digest := "sha256:" + hex.EncodeToString(sum[:])
		if _, err := db.Exec(`INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,storage_location,created_at) VALUES($1,$2,$3,$4,1,'hot',now())`, digest, inputFormatV1, len(raw), inputChunkBytes); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO fugue_platform_input_hot_chunks(content_hash,ordinal,chunk_hash,data) VALUES($1,0,$1,$2)`, digest, raw); err != nil {
			t.Fatal(err)
		}
		pin, err := RunCompilerInputLocalPin(t.Context(), address, CompilerInputLocalPinOptions{Key: fmt.Sprintf("activation/neutral/%03d", i), Digest: digest, MaxRawBytes: 1024, Apply: true})
		if err != nil || !pin.Verified {
			t.Fatal(pin, err)
		}
		pins = append(pins, compilerInputPinBinding{Key: pin.Key, Token: pin.Token, Digest: digest})
		id := fmt.Sprintf("activation-artifact-%03d", i)
		activationInsertSigned(t, s, id, model.PlatformArtifactKindEdgeRouteBundle, map[string]any{"lineage": map[string]any{"input_snapshot_digest": digest}}, map[string]string{"input_snapshot_digest": digest})
		children = append(children, id)
	}
	if len(children) == 0 {
		activationInsertSigned(t, s, "activation-root", model.PlatformArtifactKindEdgeRouteBundle, map[string]any{"neutral": true}, nil)
	} else {
		activationInsertSigned(t, s, "activation-root", model.PlatformArtifactKindReleaseSet, map[string]any{"artifact_ids": children}, nil)
	}
	if enforced {
		enforceCompilerInputHistoryTestPhase(t, s)
	}
	return s, []inputarchive.HotRoot{{Source: "activation", ID: "neutral", Reference: inputarchive.HotReference{Kind: "artifact", ID: "activation-root"}}}, pins
}

// Test-only historical mode selection by the disposable schema owner. There is
// no production enabling transition in this binary.
func enforceCompilerInputHistoryTestPhase(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.db.Exec(`UPDATE fugue_platform_input_history_admission SET phase='enforced',plan_sha256=$1,enforced_at=now() WHERE singleton AND phase='capability'`, strings.Repeat("a", 64)); err != nil {
		t.Fatal("select enforced fixture phase", err)
	}
}

func activationInsertSigned(t *testing.T, s *Store, id, kind string, content map[string]any, metadata map[string]string) {
	t.Helper()
	a, err := normalizePlatformArtifactForStore(model.PlatformArtifact{ID: id, ArtifactKind: kind, Generation: id, Content: content, Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*)+1 FROM fugue_platform_artifacts WHERE artifact_kind=$1 AND scope_key=$2`, a.ArtifactKind, a.ScopeKey).Scan(&a.GenerationSequence); err != nil {
		t.Fatal(err)
	}
	a, err = platformsafety.SignPlatformArtifact(a, s.platformArtifactSigningKeyring())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgInsertPlatformArtifact(t.Context(), s.db, a); err != nil {
		t.Fatal(err)
	}
}

func activationExpectedSet() model.PlatformExpectedConsumerSet {
	now := time.Now().UTC()
	return model.PlatformExpectedConsumerSet{ID: "activation-expected", ReleaseSetID: "activation-root", ArtifactReleaseID: "activation-release", ArtifactKind: model.PlatformArtifactKindReleaseSet,
		ScopeKey: "global", ExpectedGeneration: "activation-root", TopologyRevision: "neutral", Revision: 1,
		HeartbeatDeadline: now.Add(time.Minute), ConvergenceDeadline: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now}
}

func TestActivationPostgresZeroTempRejectsMissingOrChangedGuard(t *testing.T) {
	s, _, _ := inputActivationFixture(t, 0)
	for name, query := range map[string]string{
		"missing": `DROP FUNCTION fugue_input_safety.zero_temp_v1()`,
		"body": `CREATE OR REPLACE FUNCTION fugue_input_safety.zero_temp_v1() RETURNS void
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS 'BEGIN NULL; END'`,
		"invoker":       `ALTER FUNCTION fugue_input_safety.zero_temp_v1() SECURITY INVOKER`,
		"path":          `ALTER FUNCTION fugue_input_safety.zero_temp_v1() SET search_path=public`,
		"schema-create": `GRANT CREATE ON SCHEMA fugue_input_safety TO PUBLIC`,
	} {
		t.Run(name, func(t *testing.T) {
			tx, err := s.db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.Exec(query); err != nil {
				t.Fatal(err)
			}
			if err := schemamigrate.EnforceCompilerInputZeroTemp(t.Context(), tx); err == nil {
				t.Fatal("changed guard accepted")
			}
		})
	}
}

func installActivationRoot(ctx context.Context, tx *sql.Tx) error {
	now := time.Now().UTC()
	_, err := pgInsertPlatformArtifactRelease(ctx, tx, model.PlatformArtifactRelease{ID: "activation-release", ArtifactID: "activation-root",
		ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: "global", Generation: "activation-root", ReleaseChannel: "shadow", Status: "active",
		ReleasedAt: now, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		return err
	}
	_, err = pgInsertPlatformExpectedConsumerSet(ctx, tx, activationExpectedSet())
	return err
}

func TestActivationPostgresCompleteSignedClosureAndDurableRoot(t *testing.T) {
	s, roots, pins := inputActivationFixture(t, 2)
	proof, err := s.prepareCompilerInputActivation(t.Context(), roots)
	if err != nil || len(proof.inputs) != 2 || len(proof.closure.Nodes) != 5 {
		t.Fatal(proof, err)
	}
	// A caller's omitted dependency is never an accepted one-page proof.
	called := false
	if err := s.withPreparedCompilerInputActivation(t.Context(), proof, pins[:1], func(context.Context, *sql.Tx) error { called = true; return nil }); err == nil || called {
		t.Fatal("partial pin set admitted", err)
	}
	err = s.withPreparedCompilerInputActivation(t.Context(), proof, pins, func(ctx context.Context, tx *sql.Tx) error {
		// These are actual expected-consumer rows retained by hot inventory,
		// not a fake success marker. Full entry-point authority is still
		// outside this internal storage transaction test.
		return installActivationRoot(ctx, tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	// Release temporary preparation pins after atomic installation; a new
	// connection and full inventory must retain every input via the real root.
	for _, pin := range pins {
		_, err := RunCompilerInputLocalPin(t.Context(), s.databaseURL, CompilerInputLocalPinOptions{Key: pin.Key, Digest: pin.Digest, Apply: true, Release: true, ExpectedToken: pin.Token})
		if err != nil {
			t.Fatal(err)
		}
	}
	other, err := openPostgresDatabase(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	tx, err := other.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	report, err := compilerInputHotInventoryFromSnapshot(t.Context(), tx)
	if err != nil || !report.Closure.MetadataComplete {
		t.Fatal(report, err)
	}
	for _, pin := range pins {
		found := false
		for _, node := range report.Closure.Nodes {
			if node.Identity.Kind == "input" && node.Identity.ID == pin.Digest {
				found = true
			}
		}
		if !found {
			t.Fatal("installed retention root lost descendant", pin.Digest)
		}
	}
}

func TestActivationPostgresRefusesChangedAuthorityAndGraph(t *testing.T) {
	for _, change := range []string{"body-same-hash", "metadata-lineage", "provenance", "root-members", "same-value-update", "deleted-child", "pin-pending", "epoch", "manifest", "keyring", "keyring-aba", "expired", "horizon", "physical-rewrite"} {
		t.Run(change, func(t *testing.T) {
			s, roots, pins := inputActivationFixture(t, 1)
			proof, err := s.prepareCompilerInputActivation(t.Context(), roots)
			if err != nil {
				t.Fatal(err)
			}
			query := map[string]string{
				"body-same-hash":    `UPDATE fugue_platform_artifacts SET content_json='{"neutral":"tampered"}' WHERE id='activation-artifact-000'`,
				"metadata-lineage":  `UPDATE fugue_platform_artifacts SET metadata_json='{}' WHERE id='activation-artifact-000'`,
				"provenance":        `UPDATE fugue_platform_artifacts SET provenance_json='{}' WHERE id='activation-artifact-000'`,
				"root-members":      `UPDATE fugue_platform_artifacts SET content_json='{"artifact_ids":["missing"]}' WHERE id='activation-root'`,
				"same-value-update": `UPDATE fugue_platform_artifacts SET content_json=content_json WHERE id='activation-root'`,
				"deleted-child":     `DELETE FROM fugue_platform_artifacts WHERE id='activation-artifact-000'`,
				"pin-pending":       `UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL`,
				"epoch":             `UPDATE fugue_platform_input_archive_control SET epoch=epoch+1`,
				"manifest":          `UPDATE fugue_platform_input_manifests SET storage_location='legacy'`,
				"physical-rewrite":  `VACUUM FULL fugue_platform_artifacts`,
			}[change]
			if query != "" {
				if _, err := s.db.Exec(query); err != nil {
					t.Fatal(err)
				}
			}
			switch change {
			case "keyring", "keyring-aba":
				s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("different", "different", "", "", nil))
				if change == "keyring-aba" {
					s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("neutral-test-key", "neutral", "", "", nil))
				}
			case "expired":
				proof.expires = time.Now().Add(-time.Second)
			case "horizon":
				proof.horizon -= 1 << 31
			}
			called := false
			if err := s.withPreparedCompilerInputActivation(t.Context(), proof, pins, func(context.Context, *sql.Tx) error { called = true; return nil }); err == nil || called {
				t.Fatal("changed proof admitted", change, err)
			}
		})
	}
}

func TestActivationPostgresChecksActualBytesAndEverySignature(t *testing.T) {
	for _, change := range []string{"hash", "signature", "missing", "lineage", "oversize", "no-key"} {
		t.Run(change, func(t *testing.T) {
			s, roots, _ := inputActivationFixture(t, 1)
			query := map[string]string{
				"hash":      `UPDATE fugue_platform_artifacts SET content_json='{"neutral":"tampered"}' WHERE id='activation-artifact-000'`,
				"signature": `UPDATE fugue_platform_artifacts SET provenance_json='{}' WHERE id='activation-artifact-000'`,
				"missing":   `DELETE FROM fugue_platform_artifacts WHERE id='activation-artifact-000'`,
				"lineage":   `UPDATE fugue_platform_artifacts SET metadata_json=jsonb_build_object('input_snapshot_digest','sha256:'||repeat('a',64)) WHERE id='activation-artifact-000'`,
				"oversize":  `UPDATE fugue_platform_artifacts SET content_json=jsonb_build_object('neutral',repeat('x',8388609)) WHERE id='activation-artifact-000'`,
			}[change]
			if query != "" {
				if _, err := s.db.Exec(query); err != nil {
					t.Fatal(err)
				}
			}
			if change == "no-key" {
				s.ConfigurePlatformArtifactSigning(bundleauth.Keyring{})
			}
			if proof, err := s.prepareCompilerInputActivation(t.Context(), roots); err == nil || proof != nil {
				t.Fatal("invalid signed dependency prepared", change, err)
			}
		})
	}
}

func TestActivationPostgresAtomicAllPagesAndCallbackRollback(t *testing.T) {
	s, roots, pins := inputActivationFixture(t, 33)
	proof, err := s.prepareCompilerInputActivation(t.Context(), roots)
	if err != nil || len(proof.inputs) != 33 {
		t.Fatal(proof, err)
	}
	if _, err := s.db.Exec(`UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL WHERE pin_key=$1`, pins[32].Key); err != nil {
		t.Fatal(err)
	}
	called := false
	if err := s.withPreparedCompilerInputActivation(t.Context(), proof, pins, func(context.Context, *sql.Tx) error { called = true; return nil }); err == nil || called {
		t.Fatal("first page became partial activation", err)
	}
	// Restore through the real full readback protocol, not an asserted label.
	if _, err := RunCompilerInputLocalPin(t.Context(), s.databaseURL, CompilerInputLocalPinOptions{Key: pins[32].Key, Digest: pins[32].Digest, MaxRawBytes: 1024, Apply: true}); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("neutral callback refusal")
	err = s.withPreparedCompilerInputActivation(t.Context(), proof, pins, func(ctx context.Context, tx *sql.Tx) error {
		if err := installActivationRoot(ctx, tx); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_expected_consumer_sets`).Scan(&count); err != nil || count != 0 {
		t.Fatal("refused callback committed activation", count, err)
	}
}

func TestActivationPostgresShortLocksAndRevocationDuringApply(t *testing.T) {
	s, roots, pins := inputActivationFixture(t, 1)
	proof, err := s.prepareCompilerInputActivation(t.Context(), roots)
	if err != nil {
		t.Fatal(err)
	}
	err = s.withPreparedCompilerInputActivation(t.Context(), proof, pins, func(ctx context.Context, tx *sql.Tx) error {
		// Pin unregistration and dependency changes cannot interleave.
		other, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer other.Rollback()
		if _, err = other.Exec(`SET LOCAL lock_timeout='40ms'`); err != nil {
			return err
		}
		if _, err = other.Exec(`UPDATE fugue_platform_artifacts SET content_json=content_json WHERE id='activation-artifact-000'`); err == nil {
			t.Fatal("dependency changed while activation transaction open")
		}
		_ = other.Rollback()
		if _, err = RunCompilerInputLocalPin(ctx, s.databaseURL, CompilerInputLocalPinOptions{Key: pins[0].Key, Digest: pins[0].Digest, Release: true, Apply: true, ExpectedToken: pins[0].Token}); err == nil {
			t.Fatal("preparation pin released before activation committed")
		}
		if err := installActivationRoot(ctx, tx); err != nil {
			return err
		}
		// Ordinary entry-point integrity checks remain callable without
		// recursive keyring-lock deadlock; revocation forces final rollback.
		_ = s.platformArtifactSigningKeyring()
		s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("changed", "changed", "", "", nil))
		return nil
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatal("key revocation did not roll back", err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_expected_consumer_sets`).Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
}

func TestActivationPostgresNoInputAndTypedGenerationBinding(t *testing.T) {
	s, _, _ := inputActivationFixture(t, 0)
	roots := []inputarchive.HotRoot{{Source: "activation", ID: "neutral", Reference: inputarchive.HotReference{Kind: "generation", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ScopeKey: "global", ID: "activation-root"}}}
	proof, err := s.prepareCompilerInputActivation(t.Context(), roots)
	if err != nil || len(proof.inputs) != 0 {
		t.Fatal(proof, err)
	}
	if err := s.withPreparedCompilerInputActivation(t.Context(), proof, nil, func(context.Context, *sql.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	wrong := append([]inputarchive.HotRoot(nil), roots...)
	wrong[0].Reference.ScopeKey = "different"
	if _, err := s.prepareCompilerInputActivation(t.Context(), wrong); err == nil {
		t.Fatal("generation guessed across scope")
	}
	roots[0].Reference.ExpectedHash = "sha256:" + strings.Repeat("a", 64)
	if _, err := s.prepareCompilerInputActivation(t.Context(), roots); err == nil {
		t.Fatal("conflicting expected hash ignored")
	}
}

func TestActivationPostgresMixedBatchHasOneByteBudget(t *testing.T) {
	s, _, _ := inputActivationFixture(t, 0)
	// Four signed artifacts fit the artifact-only limit by a small margin.
	// A task in the SAME resolver batch must consume that remaining margin,
	// rather than receiving a second metadata allowance.
	var artifactBytes int
	for i := 0; i < 4; i++ {
		activationInsertSigned(t, s, fmt.Sprintf("budget-artifact-%d", i), model.PlatformArtifactKindEdgeRouteBundle,
			map[string]any{"neutral": strings.Repeat("x", (8<<20)-8192)}, nil)
		var size int
		if err := s.db.QueryRow(`SELECT octet_length(content_json::text)+octet_length(scope_json::text)+octet_length(validation_results_json::text)+octet_length(metadata_json::text)+octet_length(provenance_json::text) FROM fugue_platform_artifacts WHERE id=$1`, fmt.Sprintf("budget-artifact-%d", i)).Scan(&size); err != nil {
			t.Fatal(err)
		}
		artifactBytes += size
	}
	if artifactBytes >= 32<<20 || artifactBytes+(64<<10)-256 <= 32<<20 {
		t.Fatal("fixture does not straddle mixed batch limit", artifactBytes)
	}
	if _, err := s.db.Exec(`INSERT INTO fugue_node_updaters(id,status,token_prefix,token_hash,created_at,updated_at) VALUES('budget-updater','ready','neutral','neutral-test-hash',now(),now())`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO fugue_node_update_tasks(id,node_updater_id,task_type,payload_json,status,created_at,updated_at) VALUES('budget-task','budget-updater','neutral',jsonb_build_object('neutral',repeat('x',65000)),'pending',now(),now())`); err != nil {
		t.Fatal(err)
	}
	roots := []inputarchive.HotRoot{}
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("budget-artifact-%d", i)
		roots = append(roots, inputarchive.HotRoot{Source: "activation", ID: id, Reference: inputarchive.HotReference{Kind: "artifact", ID: id}})
	}
	if _, err := s.prepareCompilerInputActivation(t.Context(), roots); err != nil {
		t.Fatal("artifact-only fixture must fit", err)
	}
	roots = append(roots, inputarchive.HotRoot{Source: "activation", ID: "budget-task", Reference: inputarchive.HotReference{Kind: "task", ID: "budget-task"}})
	if proof, err := s.prepareCompilerInputActivation(t.Context(), roots); err == nil || proof != nil {
		t.Fatal("mixed metadata exceeded single byte budget without refusal")
	}
}
