package store

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/platformconfig"
)

func TestCompilerInputPostgresBatchCountAndLateFailureRollback(t *testing.T) {
	state := boundedContentTestStore(t)
	// Statement triggers count actual PostgreSQL round trips; row triggers verify
	// that a later failed batch cannot commit an earlier successful batch.
	_, err := state.db.Exec(`CREATE TABLE fugue_test_input_statements(n integer);
 CREATE FUNCTION fugue_test_input_statement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO fugue_test_input_statements VALUES(1); RETURN NULL; END $$;
 CREATE TRIGGER fugue_test_input_statement AFTER INSERT ON fugue_platform_input_chunks FOR EACH STATEMENT EXECUTE FUNCTION fugue_test_input_statement();`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := state.db.Exec(`DROP TRIGGER fugue_test_input_statement ON fugue_platform_input_chunks; DROP FUNCTION fugue_test_input_statement(); DROP TABLE fugue_test_input_statements;`)
		if err != nil {
			t.Error(err)
		}
	})
	snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: fmt.Sprintf("batch-test-%d", time.Now().UnixNano()), PolicyGeneration: "policy"}
	for i := 0; i < 20; i++ {
		snapshot.Origins = append(snapshot.Origins, platformconfig.OriginObservation{Ref: fmt.Sprintf("origin-%d", i), RuntimeID: strings.Repeat("payload-", 40000)})
	}
	digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := state.db.Exec(`DELETE FROM fugue_platform_input_chunks WHERE content_hash=$1`, digest); err != nil {
			t.Error(err)
		}
		if _, err := state.db.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest); err != nil {
			t.Error(err)
		}
	})
	if err := state.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	var statements, chunks int
	if err := state.db.QueryRow(`SELECT (SELECT count(*) FROM fugue_test_input_statements),(SELECT count(*) FROM fugue_platform_input_chunks WHERE content_hash=$1)`, digest).Scan(&statements, &chunks); err != nil {
		t.Fatal(err)
	}
	if chunks <= 16 || statements != (chunks+15)/16 {
		t.Fatalf("unbounded per-chunk write round trips: %d statements for %d chunks", statements, chunks)
	}
	t.Logf("stored %d chunks using %d INSERT statements", chunks, statements)
	restored, err := state.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal(err)
	}
	got, err := platformconfig.RuntimeSnapshotDigest(restored)
	if err != nil || got != digest {
		t.Fatal("stored bytes changed", err)
	}
	// Integrity still compares actual stored bytes, not only claimed hashes.
	for _, mode := range []string{"raise", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			snapshot.IntentGeneration = fmt.Sprintf("batch-failure-%s-%d", mode, time.Now().UnixNano())
			hash, err := platformconfig.RuntimeSnapshotDigest(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			action := `RAISE EXCEPTION 'synthetic second-batch failure';`
			if mode == "corrupt" {
				action = `NEW.data=set_byte(NEW.data,0,33); NEW.chunk_hash='sha256:'||encode(sha256(NEW.data),'hex');`
			}
			_, err = state.db.Exec(fmt.Sprintf(`CREATE FUNCTION fugue_test_input_late_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.content_hash='%s' AND NEW.ordinal=16 THEN %s END IF; RETURN NEW; END $$; CREATE TRIGGER fugue_test_input_late_failure BEFORE INSERT ON fugue_platform_input_chunks FOR EACH ROW EXECUTE FUNCTION fugue_test_input_late_failure();`, hash, action))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_, err := state.db.Exec(`DROP TRIGGER fugue_test_input_late_failure ON fugue_platform_input_chunks; DROP FUNCTION fugue_test_input_late_failure();`)
				if err != nil {
					t.Error(err)
				}
			}()
			if err := state.EnsurePlatformCompilerInput(snapshot, hash); err == nil {
				t.Fatal("late failed/corrupt chunk accepted")
			}
			var leaked int
			if err := state.db.QueryRow(`SELECT (SELECT count(*) FROM fugue_platform_input_chunks WHERE content_hash=$1)+(SELECT count(*) FROM fugue_platform_input_manifests WHERE content_hash=$1)`, hash).Scan(&leaked); err != nil || leaked != 0 {
				t.Fatal("earlier batch escaped rollback", leaked, err)
			}
		})
	}
}

func BenchmarkCompilerInputNewWrite(b *testing.B) {
	state := boundedContentTestStore(b)
	snapshot := platformconfig.RuntimeSnapshot{PolicyGeneration: "policy"}
	for i := 0; i < 32; i++ {
		snapshot.Origins = append(snapshot.Origins, platformconfig.OriginObservation{Ref: fmt.Sprintf("origin-%d", i), RuntimeID: strings.Repeat("synthetic-input-", 65536)})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		snapshot.IntentGeneration = fmt.Sprintf("batch-benchmark-%d-%d", time.Now().UnixNano(), i)
		digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		err = state.EnsurePlatformCompilerInput(snapshot, digest)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		var size, chunks int64
		if err := state.db.QueryRow(`SELECT size_bytes,chunk_count FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest).Scan(&size, &chunks); err != nil {
			b.Fatal(err)
		}
		b.SetBytes(size)
		b.ReportMetric(float64(chunks), "chunks/op")
		if _, err := state.db.Exec(`DELETE FROM fugue_platform_input_chunks WHERE content_hash=$1`, digest); err != nil {
			b.Fatal(err)
		}
		if _, err := state.db.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}
