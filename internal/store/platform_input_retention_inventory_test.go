package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"fugue/internal/schemamigrate"
)

func retentionInventoryTx(t *testing.T, db *sql.DB, readOnly bool) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: readOnly})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

func TestRetentionInventoryPostgresDeduplicatesNativeRootsAndUnknownComplement(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	digest := seedHotInventoryHistory(t, db)
	unknown := "sha256:" + strings.Repeat("b", 64)
	inline := "sha256:" + strings.Repeat("c", 64)
	if _, err := db.Exec(`INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,created_at)
 VALUES($1,$2,7,262144,1,now())`, unknown, inputFormatV1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO fugue_platform_artifact_contents(content_hash,content_json,size_bytes,created_at,updated_at)
 VALUES($1,'{}',9,now(),now()),($2,'{}',7,now(),now())`, inline, digest); err != nil {
		t.Fatal(err)
	}
	// An archive's stored verified label is neither a cold-retention policy
	// disposition nor permission to delete its source.
	if _, err := db.Exec(`INSERT INTO fugue_platform_input_archives(content_hash,archive_format,source_format,raw_bytes,segment_bytes,segment_count,object_prefix,backend_hash,state,source_epoch,verified_digest,fencing_token,created_at,verified_at,updated_at)
 VALUES($1,'compiler-input-archive-v1',$2,7,65536,1,'neutral/archive',$1,'verified',0,$1,1,now(),now(),now())`, unknown, inputFormatV1); err != nil {
		t.Fatal(err)
	}
	report, err := RunCompilerInputRetentionInventory(t.Context(), address)
	if err != nil {
		t.Fatal(err)
	}
	if !report.UniverseComplete || !report.Native.Closure.MetadataComplete || !report.Native.CoherentMetadata || report.ClassificationComplete || report.ExecutionAuthorized || len(report.AuditSHA256) != 64 || len(report.Entries) != 3 || len(report.UncoveredSources) == 0 {
		t.Fatalf("incorrect audit scope: %+v", report)
	}
	if report.Totals["H"].Inputs != 1 || report.Totals["H"].LogicalBytes != 7 || report.Totals["U"].Inputs != 2 || report.Totals["U"].LogicalBytes != 16 || report.Totals["C"].Inputs != 0 || report.Totals["D"].Inputs != 0 {
		t.Fatal("double counted roots/inline duplicate or inferred deletion", report.Totals)
	}
	for _, entry := range report.Entries {
		if entry.Digest == digest && (entry.Class != "H" || len(entry.NativeRoots) < 2 || !entry.InlineCopyPresent || entry.LocalRepresentation != "legacy-chunks") {
			t.Fatal("lost retained authorities or representation", entry)
		}
		if entry.Digest == inline && (entry.Class != "U" || !entry.InlineCopyPresent || entry.LocalRepresentation != "legacy-inline") {
			t.Fatal("missed inline-only universe member", entry)
		}
	}
	if report.Native.BodyVerification || report.Native.Closure.LocalCopiesVerified || report.Native.ReactivationProtected || report.Native.RetirementAuthorized {
		t.Fatal("metadata audit certified byte verification or retirement")
	}
}

func TestRetentionInventoryPostgresUniverseAndClosureShareSnapshot(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	seedHotInventoryHistory(t, db)
	tx := retentionInventoryTx(t, db, true)
	before, err := compilerInputRetentionInventorySnapshot(t.Context(), tx)
	if err != nil {
		t.Fatal(err)
	}
	newDigest := "sha256:" + strings.Repeat("d", 64)
	if _, err := db.Exec(`INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,created_at) VALUES($1,$2,11,262144,1,now())`, newDigest, inputFormatV1); err != nil {
		t.Fatal(err)
	}
	after, err := compilerInputRetentionInventorySnapshot(t.Context(), tx)
	if err != nil || before.AuditSHA256 != after.AuditSHA256 || len(after.Entries) != 1 {
		t.Fatal("audit spliced an independently updated universe", before.AuditSHA256, after.AuditSHA256, err)
	}
	fresh, err := RunCompilerInputRetentionInventory(t.Context(), address)
	if err != nil || len(fresh.Entries) != 2 || fresh.Totals["U"].Inputs != 1 || fresh.AuditSHA256 == before.AuditSHA256 {
		t.Fatal("fresh audit omitted newly unknown input", fresh, err)
	}
}

func TestRetentionInventoryPostgresRefusesPartialClosureAndUniverse(t *testing.T) {
	for _, change := range []string{"missing-input", "bad-inline-size", "universe-budget", "not-repeatable-read"} {
		t.Run(change, func(t *testing.T) {
			db, address, _ := hotInventoryTestDatabase(t)
			digest := seedHotInventoryHistory(t, db)
			var err error
			switch change {
			case "missing-input":
				_, err = db.Exec(`UPDATE fugue_platform_artifacts SET metadata_json=jsonb_build_object('input_snapshot_digest','sha256:'||repeat('e',64))`)
			case "bad-inline-size":
				_, err = db.Exec(`INSERT INTO fugue_platform_artifact_contents(content_hash,content_json,size_bytes,created_at,updated_at) VALUES($1,'{}',8,now(),now())`, digest)
			case "universe-budget":
				_, err = db.Exec(`INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,created_at)
 SELECT 'sha256:'||lpad(to_hex(i),64,'0'),$1,1,262144,1,now() FROM generate_series(1,$2) i`, inputFormatV1, compilerInputRetentionUniverseLimit)
			}
			if err != nil {
				t.Fatal(err)
			}
			var report CompilerInputRetentionInventory
			if change == "not-repeatable-read" {
				tx, e := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
				if e != nil {
					t.Fatal(e)
				}
				defer tx.Rollback()
				report, err = compilerInputRetentionInventorySnapshot(t.Context(), tx)
			} else {
				report, err = RunCompilerInputRetentionInventory(t.Context(), address)
			}
			if err == nil || report.UniverseComplete || report.ClassificationComplete || report.ExecutionAuthorized || len(report.Entries) != 0 || len(report.Totals) != 0 || report.AuditSHA256 != "" {
				t.Fatal("partial audit produced an executable complement or digest", report, err)
			}
		})
	}
}

func TestRetentionInventoryPostgresV4LegacyLocationDoesNotMeanLegacyChunks(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	seedHotInventoryHistory(t, db)
	for _, migrate := range []func(context.Context, string) error{schemamigrate.MigrateCompilerInputCodec, schemamigrate.MigrateCompilerInputShared, schemamigrate.MigrateCompilerInputSeparated} {
		if err := migrate(t.Context(), address); err != nil {
			t.Fatal(err)
		}
	}
	digest := "sha256:" + strings.Repeat("f", 64)
	if _, err := db.Exec(`INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,created_at,storage_location) VALUES($1,$2,13,262144,1,now(),'legacy')`, digest, inputFormatSeparatedV4); err != nil {
		t.Fatal(err)
	}
	report, err := RunCompilerInputRetentionInventory(t.Context(), address)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range report.Entries {
		if entry.Digest == digest {
			if entry.LocalRepresentation != "separated-components" || entry.Location != "legacy" || entry.Class != "U" {
				t.Fatal("v4 falsely depends on old chunks or became discardable", entry)
			}
			return
		}
	}
	t.Fatal("v4 universe entry missing")
}
