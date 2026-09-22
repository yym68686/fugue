package schemamigrate

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAppReleaseWorkloadMigrationPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL for disposable PostgreSQL integration")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires a disposable loopback fugue_test database")
	}
	db, err := sql.Open("pgx", address)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := fmt.Sprintf("revision_workload_%d", time.Now().UnixNano())
	if _, err = db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	defer db.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	isolated, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer isolated.Close()
	if _, err = isolated.Exec(`CREATE TABLE fugue_app_releases(id text PRIMARY KEY,app_id text NOT NULL,tenant_id text NOT NULL,deployment_name text NOT NULL,status text NOT NULL,
source_ref text NOT NULL DEFAULT '',resolved_image_ref text NOT NULL DEFAULT '',runtime_id text NOT NULL DEFAULT '',spec_snapshot_json jsonb);
INSERT INTO fugue_app_releases(id,app_id,tenant_id,deployment_name,status,spec_snapshot_json) VALUES ('release','app','tenant','candidate','ready','{"env":{"CONFIG_VERSION":"original"}}')`); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := MigrateAppReleaseWorkload(ctx, u.String()); err != nil {
			t.Fatal(err)
		}
	}
	// Old writers omit the new column. Their canonical-target and status
	// updates remain valid, while the binding survives repeated migrations.
	if _, err = isolated.Exec(`UPDATE fugue_app_releases SET revision_workload_json='{"deployment_uid":"original","service_uid":"service","operation_id":"operation"}'`); err != nil {
		t.Fatal(err)
	}
	if err := MigrateAppReleaseWorkload(ctx, u.String()); err != nil {
		t.Fatal(err)
	}
	// Reapplying the older migration replaces the identity function but
	// cannot remove the independently added executable-intent guard.
	legacy := strings.Split(appReleaseWorkloadSQL, "-- A separate additive guard")[0]
	if _, err := isolated.Exec(legacy); err != nil {
		t.Fatal(err)
	}
	if _, err = isolated.Exec(`UPDATE fugue_app_releases SET deployment_name='canonical',status='serving'`); err != nil {
		t.Fatal(err)
	}
	var value string
	if err = isolated.QueryRow(`SELECT revision_workload_json->>'deployment_uid' FROM fugue_app_releases`).Scan(&value); err != nil || value != "original" {
		t.Fatalf("binding lost: %q %v", value, err)
	}
	for _, query := range []string{
		`UPDATE fugue_app_releases SET revision_workload_json=NULL`,
		`UPDATE fugue_app_releases SET revision_workload_json='{"deployment_uid":"replacement"}'`,
		`UPDATE fugue_app_releases SET app_id='another-app'`,
		`UPDATE fugue_app_releases SET tenant_id='another-tenant'`,
		`UPDATE fugue_app_releases SET source_ref='other-source'`,
		`UPDATE fugue_app_releases SET resolved_image_ref='other-image'`,
		`UPDATE fugue_app_releases SET runtime_id='other-runtime'`,
		`UPDATE fugue_app_releases SET spec_snapshot_json='{"env":{"CONFIG_VERSION":"replacement"}}'`,
		`UPDATE fugue_app_releases SET spec_snapshot_json=NULL`,
		`INSERT INTO fugue_app_releases(id,app_id,tenant_id,deployment_name,status,revision_workload_json) VALUES ('bad','app','tenant','candidate','ready','null')`,
		`INSERT INTO fugue_app_releases(id,app_id,tenant_id,deployment_name,status,revision_workload_json) VALUES ('bad','app','tenant','candidate','ready','[]')`,
	} {
		if _, err := isolated.Exec(query); err == nil {
			t.Fatalf("immutable identity guard accepted %s", query)
		}
	}
	if err = isolated.QueryRow(`SELECT spec_snapshot_json->'env'->>'CONFIG_VERSION' FROM fugue_app_releases WHERE id='release'`).Scan(&value); err != nil || value != "original" {
		t.Fatalf("original intent lost: %q %v", value, err)
	}
	if _, err = isolated.Exec(`INSERT INTO fugue_app_releases(id,app_id,tenant_id,deployment_name,status) VALUES ('concurrent','app','tenant','candidate','ready')`); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan int64, 2)
	for _, id := range []string{"first", "second"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			r, err := isolated.Exec(`UPDATE fugue_app_releases SET revision_workload_json=jsonb_build_object('deployment_uid',$1::text) WHERE id='concurrent' AND revision_workload_json IS NULL`, id)
			if err != nil {
				t.Error(err)
				return
			}
			n, _ := r.RowsAffected()
			results <- n
		}(id)
	}
	wg.Wait()
	close(results)
	var writes int64
	for n := range results {
		writes += n
	}
	if writes != 1 {
		t.Fatalf("expected exactly one immutable binding, got %d", writes)
	}
}
