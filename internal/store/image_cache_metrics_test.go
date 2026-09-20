package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func metricsProjectionTestDB(tb testing.TB) *Store {
	tb.Helper()
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		tb.Skip("requires disposable local PostgreSQL")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		tb.Fatal("requires disposable loopback fugue_test database")
	}
	db, err := sql.Open("pgx", address)
	if err != nil {
		tb.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	tb.Cleanup(func() { db.Close() })
	for _, ddl := range []string{
		`CREATE TEMPORARY TABLE fugue_node_update_tasks (
 id text,tenant_id text default '',node_updater_id text default '',machine_id text default '',runtime_id text default '',node_key_id text default '',cluster_node_name text default '',
 task_type text,status text,payload_json jsonb default '{}',result_message text default '',error_message text default '',logs_json jsonb default '[]',requested_by_type text default '',requested_by_id text default '',
 created_at timestamptz,updated_at timestamptz,claimed_at timestamptz,completed_at timestamptz)`,
		`CREATE TEMPORARY TABLE fugue_image_cache_prune_plans (
 id text,node_id text,cluster_node_name text,runtime_id text,mode text,candidate_manifest_count integer,protected_manifest_count integer,candidate_blob_count integer,candidate_blob_bytes bigint,protected_blob_count integer default 0,planned_delete_bytes bigint,max_delete_bytes bigint default 0,min_manifest_age text default '',protection_summary_json jsonb,candidate_summary_json jsonb default '{}',candidates_json jsonb default '[]',protected_manifests_json jsonb default '[]',skipped_manifests_json jsonb default '[]',unreferenced_blobs_json jsonb default '[]',node_pressure bool default false,budget_exhausted bool default false,created_at timestamptz,executed_at timestamptz,status text,error text default '')`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			tb.Fatal(err)
		}
	}
	return &Store{db: db, databaseURL: address, dbReady: true}
}

func TestImageCacheMetricProjectionsMatchFullPostgresReads(t *testing.T) {
	s := metricsProjectionTestDB(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	payloads := []map[string]string{{"prune_reason": "image-cache-orphan", "dry_run": "false", "allow_delete": "true", "private": "ignored"}, {"prune_reason": "manual", "dry_run": "false", "allow_delete": "true"}, {}, nil}
	for i := 0; i < 8; i++ {
		typ, status := model.NodeUpdateTaskTypePruneImageCache, model.NodeUpdateTaskStatusCompleted
		if i == 6 {
			typ = model.NodeUpdateTaskTypeReportImageCache
		}
		if i == 7 {
			status = model.NodeUpdateTaskStatusFailed
		}
		raw, _ := json.Marshal(payloads[i%len(payloads)])
		logs, _ := json.Marshal([]model.NodeUpdateTaskLog{{Message: strings.Repeat("unneeded task evidence ", 1024)}})
		_, err := s.db.Exec(`INSERT INTO fugue_node_update_tasks(id,machine_id,cluster_node_name,runtime_id,task_type,status,payload_json,result_message,logs_json,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, fmt.Sprint(i), "node", "worker", "runtime", typ, status, raw, "deleted_bytes=123", logs, now.Add(time.Duration(i)*time.Second), now.Add(time.Duration(8-i)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
	}
	all, err := s.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusCompleted)
	if err != nil {
		t.Fatal(err)
	}
	var want []model.NodeUpdateTask
	for _, x := range all {
		if x.Type == model.NodeUpdateTaskTypePruneImageCache {
			want = append(want, imageCachePruneTaskMetrics(x))
		}
	}
	got, err := s.ListImageCachePruneTaskMetrics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("task metric fields/order differ: got %+v want %+v", got, want)
	}
	if len(all[0].Logs) == 0 {
		t.Fatal("full task read lost evidence")
	}
	for i := 0; i < 4; i++ {
		_, err := s.db.Exec(`INSERT INTO fugue_image_cache_prune_plans(id,node_id,cluster_node_name,runtime_id,mode,status,created_at,candidate_manifest_count,candidate_blob_count,candidate_blob_bytes,planned_delete_bytes,protected_manifest_count,protection_summary_json,error) VALUES($1,'node','worker','runtime','observe','planned',$2,3,4,500,600,2,'{"pinned":2}',$3)`, fmt.Sprint(i), now.Add(time.Duration(i)*time.Second), strings.Repeat("unneeded diagnostic detail ", 400))
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, limit := range []int{1, 3, 10} {
		full, err := s.ListImageCachePrunePlans(model.ImageCachePrunePlanFilter{Limit: limit})
		if err != nil {
			t.Fatal(err)
		}
		want := make([]model.ImageCachePrunePlan, len(full))
		for i := range full {
			want[i] = imageCachePrunePlanMetrics(full[i])
		}
		got, err := s.ListImageCachePrunePlanMetrics(context.Background(), limit)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("plan metric window/order differs at limit=%d", limit)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ListImageCachePruneTaskMetrics(ctx); err == nil {
		t.Fatal("ignored canceled metric collection")
	}
	if _, err := s.ListImageCachePrunePlanMetrics(ctx, 200); err == nil {
		t.Fatal("ignored canceled plan collection")
	}
}

func BenchmarkImageCachePruneTaskMetricRead(b *testing.B) {
	s := metricsProjectionTestDB(b)
	_, err := s.db.Exec(`INSERT INTO fugue_node_update_tasks(id,machine_id,cluster_node_name,runtime_id,task_type,status,payload_json,result_message,logs_json,created_at,updated_at)
 SELECT i::text,'node','worker','runtime',CASE WHEN i<=200 THEN 'prune-image-cache' ELSE 'report-image-cache-inventory' END,'completed',
 '{"prune_reason":"image-cache-orphan","dry_run":"false","allow_delete":"true"}','deleted_bytes=123',jsonb_build_array(jsonb_build_object('message',repeat('evidence ',1000))),now()+(i||' seconds')::interval,now() FROM generate_series(1,5000) AS i`)
	if err != nil {
		b.Fatal(err)
	}
	for name, fn := range map[string]func() error{
		"full_history": func() error {
			_, err := s.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusCompleted)
			return err
		},
		"metric_projection": func() error { _, err := s.ListImageCachePruneTaskMetrics(context.Background()); return err },
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := fn(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
