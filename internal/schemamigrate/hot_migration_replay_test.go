package schemamigrate

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestHotMigrationsReplayWithLiveWriters(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires disposable local PostgreSQL")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.HasPrefix(u.Path, "/fugue_test") {
		t.Fatal("disposable local PostgreSQL required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := fmt.Sprintf("hot_replay_%d", time.Now().UnixNano())
	if _, err := db.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP SCHEMA " + schema + " CASCADE")
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	local, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	exec := func(sql string) {
		t.Helper()
		if _, err := local.Exec(sql); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE TABLE fugue_platform_artifacts(id text, content_hash text, content_json jsonb NOT NULL, metadata_json jsonb)")
	exec("CREATE TABLE fugue_platform_artifact_contents(content_hash text PRIMARY KEY, content_json jsonb, content_kind text)")
	roots := []string{"fugue_platform_artifact_releases", "fugue_platform_release_lanes", "fugue_platform_lkg_snapshots", "fugue_platform_lkg_snapshot_history", "fugue_platform_consumer_instances", "fugue_platform_shadow_consumers"}
	for _, table := range roots {
		exec("CREATE TABLE " + table + "(id text)")
	}
	exec("CREATE TABLE fugue_platform_input_manifests(content_hash text PRIMARY KEY)")
	migrate := func() {
		t.Helper()
		for _, fn := range []func(context.Context, string) error{MigratePlatformArtifactContent, MigrateCompilerContract, MigrateCompilerInputArchive} {
			if err := fn(context.Background(), u.String()); err != nil {
				t.Fatal(err)
			}
		}
	}
	migrate() // cold installation must still build the actual guards
	busy, err := local.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Rollback()
	if _, err := busy.Exec("SELECT epoch FROM fugue_platform_input_archive_control WHERE singleton"); err != nil {
		t.Fatal(err)
	}
	if _, err := busy.Exec("LOCK TABLE fugue_platform_artifacts,fugue_platform_compiler_contract," + strings.Join(roots, ",") + " IN ROW EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	if _, err := busy.Exec("SELECT singleton FROM fugue_platform_compiler_contract FOR SHARE"); err != nil {
		t.Fatal(err)
	}
	migrate() // would have timed out against an ordinary open writer transaction
	if err := busy.Rollback(); err != nil {
		t.Fatal(err)
	}
	// A familiar name and function are not sufficient: scoped UPDATE OF,
	// disabled triggers, and late/statement triggers must all be repaired.
	exec("ALTER TABLE fugue_platform_artifacts DISABLE TRIGGER fugue_compiler_contract_root")
	exec("DROP TRIGGER fugue_compiler_contract_root ON fugue_platform_consumer_instances")
	exec("CREATE TRIGGER fugue_compiler_contract_root BEFORE INSERT OR UPDATE OF id ON fugue_platform_consumer_instances FOR EACH ROW EXECUTE FUNCTION fugue_guard_compiler_contract_root()")
	exec("ALTER TABLE fugue_platform_consumer_instances ENABLE ALWAYS TRIGGER fugue_compiler_contract_root")
	exec("DROP TRIGGER fugue_compiler_contract_control ON fugue_platform_compiler_contract")
	exec("CREATE TRIGGER fugue_compiler_contract_control AFTER UPDATE OR DELETE ON fugue_platform_compiler_contract FOR EACH STATEMENT EXECUTE FUNCTION fugue_guard_compiler_contract_control()")
	migrate()
	var rootsOK int
	if err := local.QueryRow("SELECT count(*) FROM pg_trigger WHERE tgrelid IN (SELECT oid FROM pg_class WHERE relnamespace=current_schema()::regnamespace) AND tgname='fugue_compiler_contract_root' AND tgtype=23 AND tgenabled='A' AND tgattr=''::int2vector AND tgqual IS NULL").Scan(&rootsOK); err != nil || rootsOK != len(roots)+1 {
		t.Fatalf("wrong repaired root triggers: %d, %v", rootsOK, err)
	}
	if _, err := local.Exec("DELETE FROM fugue_platform_compiler_contract"); err == nil {
		t.Fatal("control guard was not repaired")
	}
	if _, err := local.Exec("INSERT INTO fugue_platform_artifacts(id,content_hash) VALUES('bad','missing')"); err == nil {
		t.Fatal("body reference guard was not retained")
	}
}
