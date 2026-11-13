package store

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/schemamigrate"
)

func requireInlineFenceRejection(t *testing.T, err error) {
	t.Helper()
	var state interface{ SQLState() string }
	if !errors.As(err, &state) || state.SQLState() != "55000" {
		t.Fatal("old inline mutation was not rejected by the database", err)
	}
}

func TestCompilerInputInlineFenceRejectsPreparedAndReconnectingOldCredentials(t *testing.T) {
	s := archiveTestStore(t)
	resetArchiveFenceFixture(t, s)
	role := fmt.Sprintf("fugue_test_inline_%d", time.Now().UnixNano())
	if _, err := s.db.Exec(`CREATE ROLE ` + role + ` LOGIN; GRANT SELECT,INSERT,UPDATE,DELETE ON fugue_platform_artifact_contents,fugue_platform_input_chunks TO ` + role); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := s.db.Exec(`REVOKE ALL ON fugue_platform_artifact_contents,fugue_platform_input_chunks FROM ` + role + `; DROP ROLE ` + role); err != nil {
			t.Error(err)
		}
	})
	u, err := url.Parse(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.User(role)
	open := func() *sql.DB {
		db, err := openPostgresDatabase(u.String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		if err := db.PingContext(t.Context()); err != nil {
			t.Fatal("disposable non-owner writer login", err)
		}
		return db
	}
	legacy := open()
	// This is the pre-typing contract, including its original conflict path.
	const oldInsert = `INSERT INTO fugue_platform_artifact_contents(content_hash,content_json,size_bytes,created_at,updated_at)
 VALUES($1,$2::jsonb,$3,now(),now()) ON CONFLICT(content_hash) DO UPDATE SET content_json=EXCLUDED.content_json`
	prepared, err := legacy.PrepareContext(t.Context(), oldInsert)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: role, PolicyGeneration: "neutral-policy"}
	raw, _ := platformconfig.EncodeRuntimeSnapshot(snapshot)
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	if _, err := prepared.ExecContext(t.Context(), digest, raw, len(raw)); err != nil {
		t.Fatal("unfenced old writer compatibility", err)
	}
	var kind, tuple, version string
	if err := s.db.QueryRow(`SELECT content_kind,ctid::text,xmin::text FROM fugue_platform_artifact_contents WHERE content_hash=$1`, digest).Scan(&kind, &tuple, &version); err != nil || kind != "" {
		t.Fatal("old untyped input was silently classified", kind, err)
	}
	if _, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, false); err != nil {
		t.Fatal(err)
	}
	replica, err := s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replica.ExecContext(t.Context(), `SET session_replication_role=replica`); err != nil {
		t.Fatal(err)
	}
	_, err = replica.ExecContext(t.Context(), oldInsert, digest, raw, len(raw))
	requireInlineFenceRejection(t, err)
	_, err = replica.ExecContext(t.Context(), `TRUNCATE fugue_platform_artifact_contents`)
	requireInlineFenceRejection(t, err)
	if _, err := replica.ExecContext(t.Context(), `RESET session_replication_role`); err != nil {
		t.Fatal(err)
	}
	if err := replica.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = prepared.ExecContext(t.Context(), digest, raw, len(raw))
	requireInlineFenceRejection(t, err)
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	// The identical role can reconnect. Table permissions alone do not undo
	// the durable database fence, and no session flag grants a bypass.
	reconnected := open()
	_, err = reconnected.ExecContext(t.Context(), oldInsert, digest, raw, len(raw))
	requireInlineFenceRejection(t, err)
	_, err = reconnected.ExecContext(t.Context(), `UPDATE fugue_platform_artifact_contents SET content_json=content_json WHERE content_hash=$1`, digest)
	requireInlineFenceRejection(t, err)
	_, err = reconnected.ExecContext(t.Context(), `DELETE FROM fugue_platform_artifact_contents WHERE content_hash=$1`, digest)
	requireInlineFenceRejection(t, err)
	_, err = reconnected.ExecContext(t.Context(), `INSERT INTO fugue_platform_artifact_contents(content_hash,content_json,size_bytes,created_at,updated_at,content_kind) VALUES($1,$2::jsonb,$3,now(),now(),'compiler-input-v1')`, digest, raw, len(raw))
	requireInlineFenceRejection(t, err)
	var unchanged bool
	if err := s.db.QueryRow(`SELECT content_kind='' AND ctid::text=$2 AND xmin::text=$3 FROM fugue_platform_artifact_contents WHERE content_hash=$1`, digest, tuple, version).Scan(&unchanged); err != nil || !unchanged {
		t.Fatal("fence changed or relabeled the original protected bytes", err)
	}
	now := time.Now().UTC()
	// Deliberately use snapshot-looking keys. Explicit call-site typing, not
	// a payload heuristic, permits ordinary artifacts with this content.
	artifact := model.PlatformArtifact{Content: map[string]any{"intent_generation": "ordinary-artifact", "policy_generation": "neutral-policy"}, CreatedAt: now, UpdatedAt: now}
	artifact.ContentHash, err = platformArtifactContentHash(artifact.Content)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgUpsertPlatformArtifactContent(t.Context(), s.db, buildPlatformArtifactContent(artifact)); err != nil {
		t.Fatal("modern immutable artifact writer was blocked", err)
	}
	if _, err := pgUpsertPlatformArtifactContent(t.Context(), s.db, buildPlatformArtifactContent(artifact)); err != nil {
		t.Fatal("modern content reuse was blocked", err)
	}
	s.storagePolicy.value.CompilerInputChunks = false
	snapshot.IntentGeneration += "-modern"
	modernDigest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err := s.EnsurePlatformCompilerInputContext(t.Context(), snapshot, modernDigest); err != nil {
		t.Fatal("modern policy-disabled writer did not use fenced hot storage", err)
	}
	var jsonRows int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_artifact_contents WHERE content_hash=$1`, modernDigest).Scan(&jsonRows); err != nil || jsonRows != 0 {
		t.Fatal("modern writer fell back into inline storage", jsonRows, err)
	}
	if _, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, true); err != nil {
		t.Fatal(err)
	}
	if _, err := reconnected.ExecContext(t.Context(), oldInsert, digest, raw, len(raw)); err != nil {
		t.Fatal("explicit pre-retirement unfence did not restore the old contract", err)
	}
}

func TestCompilerInputInlineFenceInstallRollsBackWhenOldTransactionHasNotDrained(t *testing.T) {
	s := archiveTestStore(t)
	resetArchiveFenceFixture(t, s)
	holder, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	if _, err := holder.Exec(`LOCK TABLE fugue_platform_artifact_contents IN ROW EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	report, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, false)
	if err == nil || report.Applied || report.WritesFenced {
		t.Fatal("non-drained old writer was ignored", report, err)
	}
	var fenced, leaked bool
	if err := s.db.QueryRow(`SELECT writes_fenced, to_regprocedure('fugue_platform_input_inline_write_fence()') IS NOT NULL FROM fugue_platform_input_archive_control WHERE singleton`).Scan(&fenced, &leaked); err != nil || fenced || leaked {
		t.Fatal("failed arm left partial durable isolation", fenced, leaked, err)
	}
	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, false); err != nil {
		t.Fatal("naturally drained writer still blocked explicit arm", err)
	}
}

func TestCompilerInputInlineFenceRejectsAlteredDefinitionAndUpgradesLegacyFence(t *testing.T) {
	for name, mutate := range map[string]string{
		"missing":           `DROP TRIGGER fugue_platform_input_inline_write_fence ON fugue_platform_artifact_contents; DROP FUNCTION fugue_platform_input_inline_write_fence()`,
		"disabled":          `ALTER TABLE fugue_platform_artifact_contents DISABLE TRIGGER fugue_platform_input_inline_write_fence`,
		"replica-only":      `ALTER TABLE fugue_platform_artifact_contents ENABLE REPLICA TRIGGER fugue_platform_input_inline_write_fence`,
		"function-replaced": `CREATE OR REPLACE FUNCTION fugue_platform_input_inline_write_fence() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$`,
		"truncate-disabled": `ALTER TABLE fugue_platform_artifact_contents DISABLE TRIGGER fugue_platform_input_inline_truncate_fence`,
		"default-forged":    `ALTER TABLE fugue_platform_artifact_contents ALTER COLUMN content_kind SET DEFAULT 'platform-artifact-v1'`,
	} {
		t.Run(name, func(t *testing.T) {
			s := archiveTestStore(t)
			resetArchiveFenceFixture(t, s)
			defer func() {
				if _, err := s.db.Exec(`ALTER TABLE fugue_platform_artifact_contents ALTER COLUMN content_kind SET DEFAULT ''`); err != nil {
					t.Error(err)
				}
			}()
			if _, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, false); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(mutate); err != nil {
				t.Fatal(err)
			}
			inline, err := compilerInputArchiveInlineWriteFenceState(t.Context(), s.db)
			if err != nil || inline.installed() {
				t.Fatal("altered inline isolation accepted", inline, err)
			}
			for _, disarm := range []bool{false, true} {
				report, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, disarm)
				if name == "missing" {
					if err != nil || !report.Applied {
						t.Fatal("explicit legacy fence upgrade/unfence failed", report, err)
					}
					continue
				}
				if !errors.Is(err, ErrConflict) || report.Applied {
					t.Fatal("altered inline guard silently accepted or repaired", report, err)
				}
			}
		})
	}
}

func TestCompilerInputInlineTypingMigrationPreservesUnknownBytes(t *testing.T) {
	s := boundedContentTestStore(t)
	tx, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`CREATE TEMP TABLE fugue_platform_artifact_contents(content_hash TEXT PRIMARY KEY,content_json JSONB NOT NULL,size_bytes BIGINT NOT NULL,created_at TIMESTAMPTZ NOT NULL,updated_at TIMESTAMPTZ NOT NULL) ON COMMIT DROP;
 INSERT INTO fugue_platform_artifact_contents VALUES('neutral-input','{"unknown":true}',16,now(),now())`); err != nil {
		t.Fatal(err)
	}
	var tuple, version string
	if err := tx.QueryRow(`SELECT ctid::text,xmin::text FROM fugue_platform_artifact_contents`).Scan(&tuple, &version); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := tx.Exec(schemamigrate.ArtifactContentKindSQL); err != nil {
			t.Fatal(err)
		}
	}
	var preserved bool
	if err := tx.QueryRow(`SELECT ctid::text=$1 AND xmin::text=$2 AND content_kind='' AND content_json='{"unknown":true}'::jsonb FROM fugue_platform_artifact_contents`, tuple, version).Scan(&preserved); err != nil || !preserved {
		t.Fatal("typing migration rewrote or classified unknown historical content", err)
	}
}
