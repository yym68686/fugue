package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/platformconfig"
	"fugue/internal/schemamigrate"
)

func TestLocalInputPinRejectsInvalidIntentBeforeDatabaseAccess(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	valid := CompilerInputLocalPinOptions{Key: "rollback/neutral-1", Digest: digest, MaxRawBytes: 1024}
	for _, change := range []func(*CompilerInputLocalPinOptions){
		func(o *CompilerInputLocalPinOptions) { o.Key = "" },
		func(o *CompilerInputLocalPinOptions) { o.Key = "bad\nkey" },
		func(o *CompilerInputLocalPinOptions) { o.Digest = "invalid" },
		func(o *CompilerInputLocalPinOptions) { o.MaxRawBytes = 0 },
		func(o *CompilerInputLocalPinOptions) { o.MaxRawBytes = inputMaxBytes + 1 },
		func(o *CompilerInputLocalPinOptions) { o.ExpectedToken = 1 },
		func(o *CompilerInputLocalPinOptions) { o.Release = true },
		func(o *CompilerInputLocalPinOptions) { o.Release, o.Apply, o.ExpectedToken = true, true, 1 },
	} {
		o := valid
		change(&o)
		r, err := RunCompilerInputLocalPin(t.Context(), "invalid", o)
		if !errors.Is(err, ErrInvalidInput) || r.Applied || r.Verified || r.RetirementAuthorized {
			t.Fatal("invalid pin reached database or granted authority", r, err)
		}
	}
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED", "false")
	valid.Apply = true
	if _, err := RunCompilerInputLocalPin(t.Context(), "invalid", valid); !errors.Is(err, ErrConflict) {
		t.Fatal("pin apply did not require explicit opt-in", err)
	}
}

func TestLocalInputPinPostgresVerifiedProtectionAndExactRelease(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	for _, migrate := range []func(context.Context, string) error{schemamigrate.MigrateCompilerInputCodec, schemamigrate.MigrateCompilerInputShared, schemamigrate.MigrateCompilerInputSeparated} {
		if err := migrate(t.Context(), address); err != nil {
			t.Fatal(err)
		}
	}
	digest := seedHotInventoryHistory(t, db)
	o := CompilerInputLocalPinOptions{Key: "rollback/neutral-1", Digest: digest, MaxRawBytes: 1024}
	if r, err := RunCompilerInputLocalPin(t.Context(), address, o); !errors.Is(err, ErrConflict) || r.Applied {
		t.Fatal("old chunks accepted as an independent pin", r, err)
	}
	if _, err := db.Exec(`INSERT INTO fugue_platform_input_hot_chunks SELECT * FROM fugue_platform_input_chunks WHERE content_hash=$1`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE fugue_platform_input_manifests SET storage_location='hot' WHERE content_hash=$1`, digest); err != nil {
		t.Fatal(err)
	}
	r, err := RunCompilerInputLocalPin(t.Context(), address, o)
	if err != nil || r.Applied || r.Verified || r.State != "unverified-preview" || r.RetirementAuthorized {
		t.Fatal("preview was not read-only metadata", r, err)
	}
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED", "true")
	o.Apply = true
	r, err = RunCompilerInputLocalPin(t.Context(), address, o)
	if err != nil || !r.Applied || !r.Verified || r.State != "verified" || r.Token < 1 || r.RetirementAuthorized {
		t.Fatal("complete local pin did not verify", r, err)
	}
	token := r.Token
	r, err = RunCompilerInputLocalPin(t.Context(), address, o)
	if err != nil || r.Token != token || !r.Verified {
		t.Fatal("exact retry changed fencing identity", r, err)
	}
	o.Key = "consumer/neutral-2"
	r2, err := RunCompilerInputLocalPin(t.Context(), address, o)
	if err != nil || r2.Token == token || !r2.Verified {
		t.Fatal("independent pin failed", r2, err)
	}
	roots, err := RunCompilerInputHotRootsInventory(t.Context(), address)
	if err != nil || roots.RootCounts["local-input-pin"] != 2 || roots.ReactivationProtected || roots.RetirementAuthorized {
		t.Fatal("durable pins not retained as roots, or exaggerated proof", roots, err)
	}
	for _, source := range roots.UncoveredSources {
		if source == "manual-input-pins-registry" {
			t.Fatal("installed manual registry was reported missing")
		}
	}
	s := &Store{db: db}
	if _, err = s.maintainCompilerInput(t.Context(), CompilerInputMaintenanceOptions{Action: "migrate", Digest: digest, MaxInputBytes: 1024, Apply: true}); !errors.Is(err, ErrConflict) {
		t.Fatal("representation migration ignored local pin", err)
	}
	if _, err = db.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest); err == nil {
		t.Fatal("active pin did not protect manifest identity")
	}
	release := CompilerInputLocalPinOptions{Key: "rollback/neutral-1", Digest: digest, Apply: true, Release: true, ExpectedToken: token + 1}
	if _, err = RunCompilerInputLocalPin(t.Context(), address, release); !errors.Is(err, ErrConflict) {
		t.Fatal("stale token released a pin", err)
	}
	release.ExpectedToken = token
	if r, err = RunCompilerInputLocalPin(t.Context(), address, release); err != nil || !r.Applied || r.State != "released" || r.Verified {
		t.Fatal("exact release failed", r, err)
	}
	pinned, err := compilerInputLocalPinsSource(t.Context(), db, digest)
	if err != nil || !pinned {
		t.Fatal("one release removed another owner's pin", pinned, err)
	}
	o.Key = release.Key
	if _, err = RunCompilerInputLocalPin(t.Context(), address, o); !errors.Is(err, ErrConflict) {
		t.Fatal("released key silently reused", err)
	}
	release.Key, release.ExpectedToken = "consumer/neutral-2", r2.Token
	if _, err = db.Exec(`UPDATE fugue_platform_input_archive_control SET retirement_started_at=now() WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	if _, err = RunCompilerInputLocalPin(t.Context(), address, release); !errors.Is(err, ErrConflict) {
		t.Fatal("retirement allowed unpin", err)
	}
	if _, err = db.Exec(`UPDATE fugue_platform_input_archive_control SET retirement_started_at=NULL WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	if _, err = RunCompilerInputLocalPin(t.Context(), address, release); err != nil {
		t.Fatal(err)
	}
	if pinned, err = compilerInputLocalPinsSource(t.Context(), db, digest); err != nil || pinned {
		t.Fatal("released tombstone still pinned input", pinned, err)
	}
	// A released audit tombstone must not retain the manifest forever.
	if _, err = db.Exec(`DELETE FROM fugue_platform_input_chunks WHERE content_hash=$1`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`DELETE FROM fugue_platform_input_hot_chunks WHERE content_hash=$1`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest); err != nil {
		t.Fatal("released tombstone prevented later retirement", err)
	}
}

func TestLocalInputPinPostgresFailureRemainsPendingAndYieldsWriter(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	digest := seedHotInventoryHistory(t, db)
	if _, err := db.Exec(`INSERT INTO fugue_platform_input_hot_chunks SELECT * FROM fugue_platform_input_chunks WHERE content_hash=$1`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE fugue_platform_input_manifests SET storage_location='hot' WHERE content_hash=$1`, digest); err != nil {
		t.Fatal(err)
	}
	// Self-consistent per-chunk corruption must still fail original digest.
	corruptHash := sha256.Sum256([]byte(`{"a":2}`))
	if _, err := db.Exec(`UPDATE fugue_platform_input_hot_chunks SET data=$2,chunk_hash=$3 WHERE content_hash=$1`, digest, []byte(`{"a":2}`), sharedHashBytes(corruptHash[:])); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED", "true")
	o := CompilerInputLocalPinOptions{Key: "rollback/corrupt", Digest: digest, MaxRawBytes: 1024, Apply: true}
	r, err := RunCompilerInputLocalPin(t.Context(), address, o)
	if err == nil || !r.Applied || r.Verified || r.State != "pending" || r.Token < 1 {
		t.Fatal("failure did not retain visible protective pending pin", r, err)
	}
	roots, err := RunCompilerInputHotRootsInventory(t.Context(), address)
	if err != nil || !hotInventoryHasRoot(roots, "local-input-pin", digest) {
		t.Fatal("pending pin omitted from roots", err)
	}
	if _, err = db.Exec(`UPDATE fugue_platform_input_hot_chunks SET data=$2,chunk_hash=$1 WHERE content_hash=$1`, digest, []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if retry, err := RunCompilerInputLocalPin(t.Context(), address, o); err != nil || !retry.Verified || retry.Token != r.Token {
		t.Fatal("exact pending retry failed", retry, err)
	}
	lock, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err = lock.Exec(`SELECT pg_advisory_xact_lock(315609238744290)`); err != nil {
		t.Fatal(err)
	}
	o.Key = "rollback/busy"
	started := time.Now()
	if r, err = RunCompilerInputLocalPin(t.Context(), address, o); err == nil || r.Applied || time.Since(started) > 2*time.Second {
		t.Fatal("pin did not yield to production writer", r, err)
	}
	var rows int
	if err = db.QueryRow(`SELECT count(*) FROM fugue_platform_input_local_pins WHERE pin_key=$1`, o.Key).Scan(&rows); err != nil || rows != 0 {
		t.Fatal("failed registration committed state", rows, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = RunCompilerInputLocalPin(ctx, address, o); err == nil {
		t.Fatal("cancelled registration succeeded")
	}
}

func TestLocalInputPinPostgresSharedFormatsProtectRollbackAndCollector(t *testing.T) {
	for _, separated := range []bool{false, true} {
		t.Run(map[bool]string{false: "shared", true: "separated"}[separated], func(t *testing.T) {
			db, address, _ := hotInventoryTestDatabase(t)
			for _, migrate := range []func(context.Context, string) error{schemamigrate.MigrateCompilerInputCodec, schemamigrate.MigrateCompilerInputShared, schemamigrate.MigrateCompilerInputSeparated} {
				if err := migrate(t.Context(), address); err != nil {
					t.Fatal(err)
				}
			}
			s := &Store{db: db}
			s.storagePolicy.shared = compilerInputCodecPolicy{Version: 1, Enabled: !separated}
			s.storagePolicy.separated = compilerInputCodecPolicy{Version: 1, Enabled: separated}
			snapshot := separatedFixture(t)
			digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
			if err := s.pgEnsureChunkedInput(t.Context(), snapshot, digest); err != nil {
				t.Fatal(err)
			}
			t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED", "true")
			r, err := RunCompilerInputLocalPin(t.Context(), address, CompilerInputLocalPinOptions{Key: "current/neutral", Digest: digest, MaxRawBytes: inputMaxBytes, Apply: true})
			if err != nil || !r.Verified {
				t.Fatal(r, err)
			}
			if _, err = s.maintainCompilerInput(t.Context(), CompilerInputMaintenanceOptions{Action: "rollback", Digest: digest, Apply: true, MaxInputBytes: 1 << 20}); !errors.Is(err, ErrConflict) {
				t.Fatal("pinned shared bytes were rolled back", err)
			}
			if _, err = s.maintainCompilerInput(t.Context(), CompilerInputMaintenanceOptions{Action: "collect", Apply: true}); err != nil {
				t.Fatal("physical orphan collector failed", err)
			}
			if read, err := RunCompilerInputLocalReadback(t.Context(), address, CompilerInputLocalReadbackOptions{Digest: digest, MaxRawBytes: inputMaxBytes}); err != nil || !read.Verified {
				t.Fatal("collector lost pinned representation closure", read, err)
			}
		})
	}
}

func TestLocalInputPinPostgresReadbackDoesNotHoldWriterAndRechecksEpoch(t *testing.T) {
	for _, cancelRead := range []bool{false, true} {
		t.Run(map[bool]string{false: "epoch-change", true: "cancelled-read"}[cancelRead], func(t *testing.T) {
			db, address, _ := hotInventoryTestDatabase(t)
			digest := seedHotInventoryHistory(t, db)
			for _, statement := range []string{`INSERT INTO fugue_platform_input_hot_chunks SELECT * FROM fugue_platform_input_chunks WHERE content_hash=$1`, `UPDATE fugue_platform_input_manifests SET storage_location='hot' WHERE content_hash=$1`} {
				if _, err := db.Exec(statement, digest); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED", "true")
			barrier, err := db.Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer barrier.Close()
			if _, err = barrier.ExecContext(t.Context(), `SELECT pg_advisory_lock($1)`, compilerInputArchiveReadLeaseLockID); err != nil {
				t.Fatal(err)
			}
			defer barrier.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, compilerInputArchiveReadLeaseLockID)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			type result struct {
				r CompilerInputLocalPinReport
				e error
			}
			done := make(chan result, 1)
			go func() {
				r, e := RunCompilerInputLocalPin(ctx, address, CompilerInputLocalPinOptions{Key: "reactivation/neutral", Digest: digest, MaxRawBytes: 1024, Apply: true})
				done <- result{r, e}
			}()
			deadline := time.Now().Add(2 * time.Second)
			for {
				var registered bool
				if err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM fugue_platform_input_local_pins WHERE pin_key='reactivation/neutral' AND state='pending')`).Scan(&registered); err != nil {
					t.Fatal(err)
				}
				if registered {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("pin registration did not complete ahead of readback")
				}
				time.Sleep(10 * time.Millisecond)
			}
			writer, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Rollback()
			var acquired bool
			if err = writer.QueryRow(`SELECT pg_try_advisory_xact_lock(315609238744290)`).Scan(&acquired); err != nil || !acquired {
				t.Fatal("full readback held global production writer", acquired, err)
			}
			if !cancelRead {
				if _, err = writer.Exec(`UPDATE fugue_platform_input_archive_control SET epoch=epoch+1 WHERE singleton`); err != nil {
					t.Fatal(err)
				}
			}
			if err = writer.Commit(); err != nil {
				t.Fatal(err)
			}
			if cancelRead {
				cancel()
			}
			if _, err = barrier.ExecContext(t.Context(), `SELECT pg_advisory_unlock($1)`, compilerInputArchiveReadLeaseLockID); err != nil {
				t.Fatal(err)
			}
			out := <-done
			if out.e == nil || !out.r.Applied || out.r.Verified || out.r.State != "pending" {
				t.Fatal("changed epoch/cancelled read promoted a pin", out.r, out.e)
			}
			var state string
			if err = db.QueryRow(`SELECT state FROM fugue_platform_input_local_pins WHERE pin_key='reactivation/neutral'`).Scan(&state); err != nil || state != "pending" {
				t.Fatal("failed verification lost protective pin", state, err)
			}
		})
	}
}

func TestLocalInputPinPostgresSchemaBarrierFailsClosed(t *testing.T) {
	for name, change := range map[string]string{
		"missing-reference":  `ALTER TABLE fugue_platform_input_local_pins DROP CONSTRAINT fugue_platform_input_local_pins_protected_content_hash_fkey`,
		"missing-protection": `ALTER TABLE fugue_platform_input_local_pins DROP CONSTRAINT fugue_platform_input_local_pins_protection_check`,
		"nullable-state":     `ALTER TABLE fugue_platform_input_local_pins ALTER COLUMN state DROP NOT NULL`,
		"wrong-protection":   `ALTER TABLE fugue_platform_input_local_pins DROP CONSTRAINT fugue_platform_input_local_pins_protection_check; ALTER TABLE fugue_platform_input_local_pins ADD CONSTRAINT fugue_platform_input_local_pins_protection_check CHECK (true)`,
	} {
		t.Run(name, func(t *testing.T) { testLocalInputPinPostgresSchemaBarrierFailsClosed(t, change) })
	}
}

func testLocalInputPinPostgresSchemaBarrierFailsClosed(t *testing.T, change string) {
	db, address, _ := hotInventoryTestDatabase(t)
	digest := seedHotInventoryHistory(t, db)
	for _, statement := range []string{`INSERT INTO fugue_platform_input_hot_chunks SELECT * FROM fugue_platform_input_chunks WHERE content_hash=$1`, `UPDATE fugue_platform_input_manifests SET storage_location='hot' WHERE content_hash=$1`} {
		if _, err := db.Exec(statement, digest); err != nil {
			t.Fatal(err)
		}
	}
	if err := schemamigrate.MigrateCompilerInputArchive(t.Context(), address); err != nil {
		t.Fatal("idempotent additive migration failed", err)
	}
	if _, err := db.Exec(change); err != nil {
		t.Fatal(err)
	}
	if err := schemamigrate.MigrateCompilerInputArchive(t.Context(), address); err == nil {
		t.Fatal("migration blessed a lookalike table missing the reference barrier")
	}
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED", "true")
	for _, apply := range []bool{false, true} {
		r, err := RunCompilerInputLocalPin(t.Context(), address, CompilerInputLocalPinOptions{Key: "rollback/neutral", Digest: digest, MaxRawBytes: 1024, Apply: apply})
		if err == nil || r.Applied || r.Verified {
			t.Fatal("pin ignored missing schema barrier", r, err)
		}
	}
}
