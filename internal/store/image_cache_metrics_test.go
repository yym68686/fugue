package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
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
		if x.ID == "0" || x.ID == "2" {
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
		"projected_history": func() error {
			rows, err := s.db.QueryContext(context.Background(), `SELECT id, machine_id, cluster_node_name, runtime_id, created_at, updated_at, result_message,
 payload_json->>'dry_run', payload_json->>'allow_delete', payload_json->>'prune_reason'
 FROM fugue_node_update_tasks WHERE status = $1 AND task_type = $2 ORDER BY created_at DESC`, model.NodeUpdateTaskStatusCompleted, model.NodeUpdateTaskTypePruneImageCache)
			if err != nil {
				return err
			}
			defer rows.Close()
			var tasks []model.NodeUpdateTask
			for rows.Next() {
				var task model.NodeUpdateTask
				var dryRun, allowDelete, reason *string
				if err := rows.Scan(&task.ID, &task.MachineID, &task.ClusterNodeName, &task.RuntimeID, &task.CreatedAt, &task.UpdatedAt, &task.ResultMessage, &dryRun, &allowDelete, &reason); err != nil {
					return err
				}
				task.Type = model.NodeUpdateTaskTypePruneImageCache
				task.Status = model.NodeUpdateTaskStatusCompleted
				task.Payload = map[string]string{}
				for k, v := range map[string]*string{"dry_run": dryRun, "allow_delete": allowDelete, "prune_reason": reason} {
					if v != nil {
						task.Payload[k] = *v
					}
				}
				tasks = append(tasks, task)
			}
			if len(tasks) != 200 {
				return fmt.Errorf("projected history: got %d rows, want 200", len(tasks))
			}
			return rows.Err()
		},
		"metric_projection": func() error {
			tasks, err := s.ListImageCachePruneTaskMetrics(context.Background())
			if err == nil && len(tasks) != 1 {
				return fmt.Errorf("latest projection: got %d rows, want 1", len(tasks))
			}
			return err
		},
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

func TestImageCacheTaskMetricReadBoundsHistoryWithoutChangingLatestResults(t *testing.T) {
	for _, backend := range []string{"file", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var s *Store
			if backend == "postgres" {
				s = metricsProjectionTestDB(t)
			} else {
				s = New(filepath.Join(t.TempDir(), "state.json"))
				if err := s.Init(); err != nil {
					t.Fatal(err)
				}
			}
			now := time.Now().UTC().Truncate(time.Microsecond)
			var history []model.NodeUpdateTask
			add := func(id, node, runtime, dryRun, reason string, created, updated int) {
				history = append(history, model.NodeUpdateTask{ID: id, MachineID: node, ClusterNodeName: "worker", RuntimeID: runtime,
					Type: model.NodeUpdateTaskTypePruneImageCache, Status: model.NodeUpdateTaskStatusCompleted,
					Payload:       map[string]string{"dry_run": dryRun, "allow_delete": "true", "prune_reason": reason},
					ResultMessage: "deleted_bytes=" + fmt.Sprint(updated), CreatedAt: now.Add(time.Duration(created) * time.Second), UpdatedAt: now.Add(time.Duration(updated) * time.Second)})
			}
			for i := 0; i < 2000; i++ {
				add(fmt.Sprintf("history-%04d", i), "node", "runtime", "false", "image-cache-orphan", i, i)
			}
			// UpdatedAt, not CreatedAt, chooses the current result. A manual prune
			// must suppress all older orphan-prune results for these same labels.
			add("manual-latest", "node", "runtime", "false", "manual", -1, 3000)
			add("dry-run", "node", "runtime", "true", "image-cache-orphan", 2001, 2001)
			add("new-runtime", "node", "runtime-2", "false", "image-cache-orphan", 2002, 2002)
			add("other-node", "node-2", "runtime", "false", "image-cache-orphan", 2003, 2003)
			// Preserve raw whitespace variants in the store. The existing controller
			// reducer must still pick the newer normalized label across those groups.
			add("whitespace", " node ", "runtime", " false ", "image-cache-orphan", 2004, 4000)
			add("nil-payload", "node", "runtime", "", "", 2005, 2005)
			history[len(history)-1].Payload = nil
			add("missing-payload", "node", "runtime", "", "", 2006, 2006)
			history[len(history)-1].Payload = map[string]string{}
			add("failed", "node", "runtime", "false", "image-cache-orphan", 5000, 5000)
			history[len(history)-1].Status = model.NodeUpdateTaskStatusFailed
			add("other-type", "node", "runtime", "false", "image-cache-orphan", 6000, 6000)
			history[len(history)-1].Type = model.NodeUpdateTaskTypeReportImageCache
			if backend == "postgres" {
				tx, err := s.db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				for _, task := range history {
					payload, _ := json.Marshal(task.Payload)
					if _, err = tx.Exec(`INSERT INTO fugue_node_update_tasks(id,machine_id,cluster_node_name,runtime_id,task_type,status,payload_json,result_message,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, task.ID, task.MachineID, task.ClusterNodeName, task.RuntimeID, task.Type, task.Status, payload, task.ResultMessage, task.CreatedAt, task.UpdatedAt); err != nil {
						_ = tx.Rollback()
						t.Fatal(err)
					}
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			} else if err := s.withLockedState(true, func(state *model.State) error { state.NodeUpdateTasks = history; return nil }); err != nil {
				t.Fatal(err)
			}
			got, err := s.ListImageCachePruneTaskMetrics(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, 0, len(got))
			for _, task := range got {
				ids = append(ids, task.ID)
			}
			sort.Strings(ids)
			want := []string{"dry-run", "manual-latest", "missing-payload", "new-runtime", "other-node", "whitespace"}
			if !reflect.DeepEqual(ids, want) {
				t.Fatalf("history was not reduced to latest raw label groups: got %v want %v", ids, want)
			}
			// Reproduce the consumer's trim-and-select behavior independently, using
			// the full history as an oracle (including its post-selection reason filter).
			selected := func(tasks []model.NodeUpdateTask) map[[5]string]string {
				latest := map[[5]string]model.NodeUpdateTask{}
				for _, task := range tasks {
					if task.Type != model.NodeUpdateTaskTypePruneImageCache || task.Status != model.NodeUpdateTaskStatusCompleted {
						continue
					}
					key := [5]string{strings.TrimSpace(task.MachineID), strings.TrimSpace(task.ClusterNodeName), strings.TrimSpace(task.RuntimeID), strings.TrimSpace(task.Payload["dry_run"]), strings.TrimSpace(task.Payload["allow_delete"])}
					old, ok := latest[key]
					if !ok || task.UpdatedAt.After(old.UpdatedAt) {
						latest[key] = task
					}
				}
				out := map[[5]string]string{}
				for key, task := range latest {
					if strings.TrimSpace(task.Payload["prune_reason"]) == "image-cache-orphan" {
						out[key] = task.ResultMessage
					}
				}
				return out
			}
			if !reflect.DeepEqual(selected(got), selected(history)) {
				t.Fatalf("metric values changed: got %v want %v", selected(got), selected(history))
			}
			if backend == "file" {
				got[0].Payload["prune_reason"] = "mutated"
				again, err := s.ListImageCachePruneTaskMetrics(context.Background())
				if err != nil || again[0].Payload["prune_reason"] == "mutated" {
					t.Fatal("projection mutated durable state", err)
				}
			}
		})
	}
}
