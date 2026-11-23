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

func TestHostedProposalStorageRejectsLegacyPublish(t *testing.T) {
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
	schema := fmt.Sprintf("proposal_guard_%d", time.Now().UnixNano())
	if _, err = db.Exec("CREATE SCHEMA " + schema); err != nil {
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
	for _, sql := range []string{"CREATE TABLE fugue_platform_artifacts(artifact_kind text NOT NULL,status text NOT NULL)", "CREATE TABLE fugue_platform_artifact_releases(artifact_kind text NOT NULL)"} {
		if _, err = local.Exec(sql); err != nil {
			t.Fatal(err)
		}
	}
	busy, err := local.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Rollback()
	if _, err = busy.Exec("SELECT * FROM fugue_platform_artifact_releases"); err != nil {
		t.Fatal(err)
	}
	if err = MigrateHostedProposalGuards(context.Background(), u.String()); err == nil {
		t.Fatal("waited for or ignored busy publication table")
	}
	busy.Rollback()
	for i := 0; i < 2; i++ {
		if err = MigrateHostedProposalGuards(context.Background(), u.String()); err != nil {
			t.Fatal(err)
		}
	}
	for _, sql := range []string{"INSERT INTO fugue_platform_artifacts VALUES ('hosted_path_proposal','draft')", "INSERT INTO fugue_platform_artifacts VALUES ('edge_route_bundle','validated')", "INSERT INTO fugue_platform_artifact_releases VALUES ('edge_route_bundle')"} {
		if _, err = local.Exec(sql); err != nil {
			t.Fatal(err)
		}
	}
	for _, sql := range []string{"UPDATE fugue_platform_artifacts SET status='validated' WHERE artifact_kind='hosted_path_proposal'", "INSERT INTO fugue_platform_artifact_releases VALUES ('hosted_path_proposal')"} {
		if _, err = local.Exec(sql); err == nil {
			t.Fatal("legacy publication path admitted proposal")
		}
	}
}
