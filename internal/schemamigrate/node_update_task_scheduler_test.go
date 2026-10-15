package schemamigrate

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNodeUpdateTaskSchedulerIndexPostgres(t *testing.T) {
	dsn := os.Getenv("FUGUE_TASK_INDEX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable task scheduler index database not configured")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.HasPrefix(u.Path, "/fugue_test_task_index") {
		t.Fatal("disposable loopback task index database required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `CREATE TABLE fugue_node_update_tasks (id text PRIMARY KEY,task_type text NOT NULL,node_updater_id text NOT NULL,created_at timestamptz NOT NULL,metadata_json jsonb)`); err != nil {
		t.Fatal(err)
	}
	defer db.Exec(`DROP TABLE fugue_node_update_tasks`)
	if _, err := db.ExecContext(ctx, `INSERT INTO fugue_node_update_tasks SELECT n::text,'platform_consumer','consumer-'||n,'2026-01-01'::timestamptz+n*interval '1 second',jsonb_build_object('release_set_id','release-'||(n/3)) FROM generate_series(1,50000)n`); err != nil {
		t.Fatal(err)
	}
	writer, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	if _, err := writer.ExecContext(ctx, `UPDATE fugue_node_update_tasks SET metadata_json=metadata_json WHERE id='1'`); err != nil {
		t.Fatal(err)
	}
	buildCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- MigrateNodeUpdateTaskScheduler(buildCtx, dsn) }()
	ready := false
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		var count int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_progress_create_index WHERE relid='fugue_node_update_tasks'::regclass`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			ready = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		t.Fatal("online builder did not start")
	}
	if _, err := writer.ExecContext(ctx, `UPDATE fugue_node_update_tasks SET metadata_json=metadata_json WHERE id='2'`); err != nil {
		t.Fatal("online index blocked writer", err)
	}
	stop()
	if err := <-done; err == nil {
		t.Fatal("canceled builder succeeded")
	}
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := MigrateNodeUpdateTaskScheduler(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	oids := func() []int64 {
		out := []int64{}
		for _, i := range nodeTaskSchedulerIndexes {
			var oid int64
			if err := db.QueryRowContext(ctx, `SELECT to_regclass($1)::oid`, i.name).Scan(&oid); err != nil {
				t.Fatal(err)
			}
			out = append(out, oid)
		}
		return out
	}
	before := oids()
	if err := MigrateNodeUpdateTaskScheduler(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	after := oids()
	if before[0] != after[0] {
		t.Fatal("idempotent migration rebuilt indexes")
	}
	if _, err := db.ExecContext(ctx, `ANALYZE fugue_node_update_tasks`); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, `EXPLAIN SELECT DISTINCT ON(node_updater_id) id FROM fugue_node_update_tasks WHERE task_type='platform_consumer' ORDER BY node_updater_id,created_at DESC,id DESC`)
	if err != nil {
		t.Fatal(err)
	}
	plan := ""
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan += line + "\n"
	}
	rows.Close()
	if !strings.Contains(plan, nodeTaskSchedulerIndexes[0].name) {
		t.Fatal("scheduler index not used", plan)
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM fugue_node_update_tasks`).Scan(&count); err != nil || count != 50000 {
		t.Fatal("migration changed audit history", count, err)
	}
	if _, err := db.ExecContext(ctx, `DROP INDEX idx_fugue_node_update_tasks_type_updater_created`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX idx_fugue_node_update_tasks_type_updater_created ON fugue_node_update_tasks(id)`); err != nil {
		t.Fatal(err)
	}
	if err := MigrateNodeUpdateTaskScheduler(ctx, dsn); err == nil || !strings.Contains(err.Error(), "unexpected definition") {
		t.Fatal("wrong index replaced", err)
	}
}
