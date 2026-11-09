package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/inputarchive"
	"fugue/internal/schemamigrate"
)

func pendingExternalRootFixture(t *testing.T) (*Store, inputarchive.PendingRootRegistration, []compilerInputPinBinding) {
	t.Helper()
	s, _, pins := inputActivationFixturePhase(t, 1, false)
	if _, err := s.db.Exec(schemamigrate.CompilerInputExternalRootsSQL); err != nil {
		t.Fatal(err)
	}
	if err := schemamigrate.VerifyCompilerInputExternalRoots(t.Context(), s.db); err != nil {
		t.Fatal(err)
	}
	root, err := s.GetPlatformArtifact("activation-root")
	if err != nil {
		t.Fatal(err)
	}
	r := inputarchive.PendingRootRegistration{Schema: inputarchive.PendingRootSchema, Owner: "authority/neutral", Generation: "generation-neutral",
		SourceDigest: "sha256:" + strings.Repeat("a", 64), References: []inputarchive.HotReference{{Kind: "artifact", ID: root.ID, ExpectedHash: root.ContentHash}}}
	return s, r, pins
}

func externalRootCount(t *testing.T, s *Store) int {
	t.Helper()
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_external_roots`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestPendingExternalRootPostgresCrashRetentionIdempotenceAndNativeFK(t *testing.T) {
	s, r, pins := pendingExternalRootFixture(t)
	releaseHistoryFixturePins(t, s, pins)
	// No old chunks body can be consulted. A pending root conservatively
	// protects cold metadata; it does not restore or mint a verified pin.
	if _, err := s.db.Exec(`DROP TABLE fugue_platform_input_chunks`); err != nil {
		t.Fatal(err)
	}
	first, err := s.RetainPendingCompilerInputExternalRoot(t.Context(), r)
	if err != nil || !first.Matches(r) {
		t.Fatal(first, err)
	}
	db, err := openPostgresDatabase(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fresh := &Store{db: db, databaseURL: s.databaseURL}
	fresh.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
	second, err := fresh.RetainPendingCompilerInputExternalRoot(t.Context(), r)
	if err != nil || second != first || externalRootCount(t, s) != 1 {
		t.Fatal("lost acknowledgement duplicated or changed root", first, second, err)
	}
	strict, err := fresh.prepareCompilerInputActivation(t.Context(), []inputarchive.HotRoot{{Source: "activation", ID: "neutral", Reference: r.References[0]}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := fresh.prepareCompilerInputActivationPins(t.Context(), strict); !errors.Is(err, ErrConflict) {
		t.Fatal("pending retention receipt substituted for full-readback pin", err)
	}
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	roots, revision, err := compilerInputExternalRoots(t.Context(), tx, compilerInputHotRootLimit, nil)
	if err != nil || revision != first.RegistryRevision || len(roots) != 1 || roots[0].Reference != r.References[0] {
		t.Fatal("fresh census lost unpublished root", roots, revision, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`DELETE FROM fugue_platform_artifacts WHERE id='activation-root'`,
		`DELETE FROM fugue_platform_artifacts WHERE id='activation-artifact-000'`,
		`DELETE FROM fugue_platform_input_manifests WHERE content_hash='` + pins[0].Digest + `'`,
		`UPDATE fugue_platform_input_external_roots SET generation='replacement'`,
		`DELETE FROM fugue_platform_input_external_roots`,
		`TRUNCATE fugue_platform_input_external_roots CASCADE`,
		`DELETE FROM fugue_platform_input_external_root_refs`,
		`TRUNCATE fugue_platform_input_external_root_refs`,
	} {
		if _, err := db.Exec(q); err == nil {
			t.Fatal("pending root or descendant was unprotected", q)
		}
	}
	for _, query := range []string{
		`DELETE FROM fugue_platform_input_external_root_refs`,
		`DELETE FROM fugue_platform_artifacts WHERE id='activation-root'`,
		`DELETE FROM fugue_platform_artifacts WHERE id='activation-artifact-000'`,
		`DELETE FROM fugue_platform_input_manifests WHERE content_hash='` + pins[0].Digest + `'`,
	} {
		replica, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := replica.Exec(`SET LOCAL session_replication_role='replica'`); err != nil {
			t.Fatal("disposable owner could not exercise replica guard", err)
		}
		if _, err := replica.Exec(query); err == nil {
			t.Fatal("replica session erased pending protection", query)
		}
		if err := replica.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
	r.SourceDigest = "sha256:" + strings.Repeat("b", 64)
	if _, err := fresh.RetainPendingCompilerInputExternalRoot(t.Context(), r); !errors.Is(err, ErrConflict) || externalRootCount(t, s) != 1 {
		t.Fatal("same owner/generation changed its source", err)
	}
}

func TestPendingExternalRootPostgresNativeDriftAndWrongAuthorityRefuseBeforeCommit(t *testing.T) {
	for _, change := range []string{"dependency", "epoch", "policy", "expired", "intent", "activation"} {
		t.Run(change, func(t *testing.T) {
			s, r, _ := pendingExternalRootFixture(t)
			proof, err := s.preparePendingCompilerInputExternalRoot(t.Context(), r)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "dependency":
				_, err = s.db.Exec(`UPDATE fugue_platform_artifacts SET updated_at=updated_at WHERE id='activation-artifact-000'`)
			case "epoch":
				_, err = s.db.Exec(`UPDATE fugue_platform_input_archive_control SET epoch=epoch+1`)
			case "policy":
				s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("different-key", "other", "", "", nil))
			case "expired":
				proof.expires = time.Now().Add(-time.Second)
			case "intent":
				proof.pendingRoot.registration.SourceDigest = "sha256:" + strings.Repeat("b", 64)
			case "activation":
				err := s.withPreparedCompilerInputActivation(t.Context(), proof, nil, func(context.Context, *sql.Tx) error { t.Fatal("pending retention authorized activation"); return nil })
				if !errors.Is(err, ErrInvalidInput) {
					t.Fatal(err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if receipt, err := s.commitPendingCompilerInputExternalRoot(t.Context(), proof); err == nil || receipt.RegistryRevision != 0 || externalRootCount(t, s) != 0 {
				t.Fatal("drift installed or acknowledged retention prefix", receipt, err)
			}
		})
	}
}

func TestPendingExternalRootPostgresNewRecordInvalidatesFullCensusEvenForSameInput(t *testing.T) {
	s, r, _ := pendingExternalRootFixture(t)
	installHistoryPhaseRoot(t, s)
	prepared, err := s.prepareCompilerInputHistoryEnforcement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RetainPendingCompilerInputExternalRoot(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if plan, err := s.enforcePreparedCompilerInputHistory(t.Context(), prepared); !errors.Is(err, ErrConflict) || plan != "" || historyPhase(t, s) != "capability" {
		t.Fatal("new root was hidden by overlapping input set", plan, err)
	}
}

func TestPendingExternalRootPostgresBoundsNeverEvictAndBadSchemaNeverEmpty(t *testing.T) {
	s, r, _ := pendingExternalRootFixture(t)
	for i := 0; i < compilerInputExternalOwnerLimit; i++ {
		r.Generation = fmt.Sprintf("generation-%03d", i)
		if _, err := s.RetainPendingCompilerInputExternalRoot(t.Context(), r); err != nil {
			t.Fatal(i, err)
		}
	}
	r.Generation = "one-too-many"
	if _, err := s.RetainPendingCompilerInputExternalRoot(t.Context(), r); !errors.Is(err, ErrConflict) || externalRootCount(t, s) != compilerInputExternalOwnerLimit {
		t.Fatal("owner cap evicted protection or accepted overflow", err)
	}
	// A direct multi-row/alternate caller cannot bypass the persistent cap.
	if _, err := s.db.Exec(`INSERT INTO fugue_platform_input_external_roots(registration_sha256,owner_key,generation,source_digest,source_epoch,registration_json)
 SELECT repeat('b',64),owner_key,'bypassed-new-generation',source_digest,source_epoch,registration_json FROM fugue_platform_input_external_roots LIMIT 1`); err == nil || externalRootCount(t, s) != compilerInputExternalOwnerLimit {
		t.Fatal("native budget guard accepted overflow", err)
	}
	for _, change := range []string{
		`ALTER TABLE fugue_platform_input_external_roots DISABLE TRIGGER fugue_external_roots_guard`,
		`ALTER TABLE fugue_platform_input_external_root_refs DISABLE TRIGGER fugue_external_root_refs_budget`,
		`ALTER TABLE fugue_platform_input_external_root_refs DROP CONSTRAINT fugue_platform_input_external_root_refs_content_hash_fkey`,
	} {
		tx, err := s.db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(change); err != nil {
			t.Fatal(err)
		}
		if _, _, err := compilerInputExternalRoots(t.Context(), tx, compilerInputHotRootLimit, nil); err == nil {
			t.Fatal("corrupt registry treated as empty/valid", change)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
}
