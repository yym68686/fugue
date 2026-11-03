package schemamigrate

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCompilerInputArchiveSchemaCompatibilityAndFences(t *testing.T) {
	dsn := os.Getenv("FUGUE_MEMORY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires disposable PostgreSQL")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("disposable loopback only")
	}
	ctx := context.Background()
	for _, migrate := range []func(context.Context, string) error{
		MigrateBoundedContent,
		MigrateCompilerInputCodec,
		MigrateCompilerInputShared,
		MigrateCompilerInputSeparated,
		MigrateCompilerInputArchive,
		MigrateCompilerInputArchive,
	} {
		if err = migrate(ctx, dsn); err != nil {
			t.Fatal(err)
		}
	}
	db, err := openDatabase(dsn)
	if err != nil {
		t.Fatal(err)
	}
	var hotTable, locationColumn bool
	if err = db.QueryRow(`SELECT to_regclass('fugue_platform_input_hot_chunks') IS NOT NULL,EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='fugue_platform_input_manifests' AND column_name='storage_location')`).Scan(&hotTable, &locationColumn); err != nil || !hotTable || !locationColumn {
		t.Fatal("archive migration did not install the separate hot relation locator", hotTable, locationColumn, err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	defer func() {
		_, _ = db.Exec(`DELETE FROM fugue_platform_input_archives WHERE content_hash=$1`, digest)
		_, _ = db.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest)
		_ = db.Close()
	}()
	var epoch int64
	var fenced bool
	if err = db.QueryRow(`SELECT epoch,writes_fenced FROM fugue_platform_input_archive_control WHERE singleton`).Scan(&epoch, &fenced); err != nil || epoch != 0 || fenced {
		t.Fatal("archive migration changed behavior instead of adding an inert control row", err)
	}
	if _, err = db.Exec(`INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,created_at) VALUES($1,'runtime-snapshot-json-v1',1,1,1,now())`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO fugue_platform_input_archives(content_hash,archive_format,source_format,raw_bytes,segment_bytes,segment_count,object_prefix,backend_hash,state,source_epoch,verified_digest,fencing_token,created_at,verified_at,updated_at) VALUES($1,'compiler-input-archive-v1','runtime-snapshot-json-v1',1,65536,1,'input-archive/test',$1,'verified',0,$1,1,now(),now(),now())`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO fugue_platform_input_archive_segments(content_hash,ordinal,object_key,raw_bytes,stored_bytes,segment_hash,stored_hash,uploaded_at,verified_at) VALUES($1,0,'input-archive/test/0.gz',1,16,$1,$1,now(),now())`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO fugue_platform_input_archive_leases(content_hash,owner,source_epoch,fencing_token,expires_at,updated_at) VALUES($1,'test-worker',0,1,now()+interval '1 minute',now())`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest); err == nil {
		t.Fatal("archive metadata must pin the source identity")
	}
	if _, err = db.Exec(`INSERT INTO fugue_platform_input_archive_control(singleton,epoch,writes_fenced,updated_at) VALUES(false,0,false,now())`); err == nil {
		t.Fatal("multiple archive-control rows accepted")
	}
	if _, err = db.Exec(`UPDATE fugue_platform_input_archives SET state='unknown' WHERE content_hash=$1`, digest); err == nil {
		t.Fatal("unknown archive state accepted")
	}
	if _, err = db.Exec(`UPDATE fugue_platform_input_archives SET verified_digest='' WHERE content_hash=$1`, digest); err == nil {
		t.Fatal("verified archive without digest receipt accepted")
	}
	if _, err = db.Exec(`UPDATE fugue_platform_input_archive_segments SET stored_bytes=17825793 WHERE content_hash=$1`, digest); err == nil {
		t.Fatal("segment over the bounded stored-byte maximum accepted")
	}
	var triggerCount int
	if err = db.QueryRow(`SELECT count(*) FROM pg_trigger WHERE tgrelid='fugue_platform_input_chunks'::regclass AND (tgname='fugue_input_archive_write_fence' OR tgname='fugue_input_archive_source_pin')`).Scan(&triggerCount); err != nil || triggerCount != 0 {
		t.Fatal("additive archive schema must not add row-level overhead to normal input writes", err)
	}

	// A pre-existing incompatible table must make the additive migration fail
	// atomically rather than leave a partially installed ledger behind.
	for i, ddl := range []string{
		`CREATE TABLE fugue_platform_input_archive_segments(content_hash text,ordinal text)`,
		`CREATE TABLE fugue_platform_input_archive_control(singleton text)`,
	} {
		schema := fmt.Sprintf("archive_schema_test_%d_%d", time.Now().UnixNano(), i)
		if _, err = db.Exec("CREATE SCHEMA " + schema); err != nil {
			t.Fatal(err)
		}
		defer db.Exec("DROP SCHEMA " + schema + " CASCADE")
		isolated := *u
		q := isolated.Query()
		q.Set("search_path", schema)
		isolated.RawQuery = q.Encode()
		if err = MigrateBoundedContent(ctx, isolated.String()); err != nil {
			t.Fatal(err)
		}
		bad, err := openDatabase(isolated.String())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = bad.Exec(ddl); err != nil {
			bad.Close()
			t.Fatal(err)
		}
		if err = MigrateCompilerInputArchive(ctx, isolated.String()); err == nil {
			bad.Close()
			t.Fatal("malformed archive schema accepted")
		}
		var exists bool
		queryErr := bad.QueryRow(`SELECT to_regclass('fugue_platform_input_archives') IS NOT NULL`).Scan(&exists)
		if queryErr != nil || exists {
			bad.Close()
			t.Fatal("failed archive migration partially committed", queryErr)
		}
		bad.Close()
	}
}
