package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
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
		 id text primary key,node_id text,cluster_node_name text,runtime_id text,mode text,candidate_manifest_count integer,protected_manifest_count integer,candidate_blob_count integer,candidate_blob_bytes bigint,protected_blob_count integer default 0,planned_delete_bytes bigint,max_delete_bytes bigint default 0,min_manifest_age text default '',protection_summary_json jsonb,candidate_summary_json jsonb default '{}',candidates_json jsonb default '[]',protected_manifests_json jsonb default '[]',skipped_manifests_json jsonb default '[]',unreferenced_blobs_json jsonb default '[]',node_pressure bool default false,budget_exhausted bool default false,created_at timestamptz,executed_at timestamptz,status text,error text default '')`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			tb.Fatal(err)
		}
	}
	// Exercise the actual additive bootstrap against the predecessor's table
	// shape, including an idempotent second pass. The temp table isolates this
	// check from all other tests and from durable local database contents.
	for pass := 0; pass < 2; pass++ {
		for _, ddl := range postgresSchemaStatements {
			if strings.HasPrefix(ddl, "ALTER TABLE fugue_image_cache_prune_plans ADD COLUMN IF NOT EXISTS policy_version ") || strings.HasPrefix(ddl, "ALTER TABLE fugue_image_cache_prune_plans ADD COLUMN IF NOT EXISTS plan_hash ") {
				if _, err := db.Exec(ddl); err != nil {
					tb.Fatal(err)
				}
			}
		}
	}
	return &Store{db: db, databaseURL: address, dbReady: true}
}

func TestImageCacheAutomaticPlanSummaryPreservesFullPlanHashInPostgres(t *testing.T) {
	s := metricsProjectionTestDB(t)
	plan, err := s.UpsertImageCachePrunePlan(model.ImageCachePrunePlan{
		ID:                     "automatic-summary",
		NodeID:                 "node",
		ClusterNodeName:        "worker",
		RuntimeID:              "runtime",
		Mode:                   model.ImageCachePruneModeDryRun,
		CandidateManifestCount: 1,
		CandidateSummary:       map[string]int{"stale_replica": 1},
		Candidates:             []model.ImageCachePruneCandidate{{Repo: "fugue-apps/demo", Target: "release", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Reason: "stale_replica"}},
		CreatedAt:              time.Now().UTC(),
		Status:                 model.ImageCachePrunePlanStatusPlanned,
	})
	if err != nil {
		t.Fatal(err)
	}
	fullPlanHash := plan.PlanHash
	if fullPlanHash == "" {
		t.Fatal("full plan hash was not sealed")
	}
	// Older writers omit the additive fields. Their complete stored arrays
	// must remain readable and retain the same derived policy/hash.
	if _, err := s.db.Exec(`UPDATE fugue_image_cache_prune_plans SET plan_hash='',policy_version='' WHERE id=$1`, plan.ID); err != nil {
		t.Fatal(err)
	}
	summary, err := s.UpsertImageCachePrunePlanSummary(plan)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := plan
	duplicate.ID = "another-scheduler-tick"
	duplicate.CreatedAt = duplicate.CreatedAt.Add(time.Minute)
	repeated, err := s.UpsertImageCachePrunePlanSummary(duplicate)
	if err != nil || repeated.ID != summary.ID {
		t.Fatalf("unchanged automatic projection did not reuse identity: %s %s %v", summary.ID, repeated.ID, err)
	}
	got, err := s.ListImageCachePrunePlans(model.ImageCachePrunePlanFilter{Mode: model.ImageCachePruneModeDryRun, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("manual preview plus deduplicated summary should be two rows, got %d", len(got))
	}
	var foundSummary, foundManual bool
	for _, item := range got {
		if item.ID == summary.ID {
			foundSummary = item.PlanHash == fullPlanHash && item.CandidateManifestCount == 1 && item.CandidateSummary["stale_replica"] == 1 && len(item.Candidates) == 0
		}
		if item.ID == plan.ID {
			foundManual = len(item.Candidates) == 1 && item.PlanHash == fullPlanHash
		}
	}
	if !foundSummary || !foundManual {
		t.Fatalf("summary identity/detail or independent manual preview changed: %+v", got)
	}
	var details []byte
	if err = s.db.QueryRow(`SELECT candidates_json FROM fugue_image_cache_prune_plans WHERE id=$1`, summary.ID).Scan(&details); err != nil {
		t.Fatal(err)
	}
	if string(details) != "null" {
		t.Fatalf("automatic plan detail remained stored: %d bytes", len(details))
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
	for _, backend := range []string{"postgres"} {
		t.Run(backend, func(t *testing.T) {
			var s *Store
			s = metricsProjectionTestDB(t)

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
			{
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

		})
	}
}
