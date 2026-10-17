package store

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

func artifactReadTestStore(t *testing.T) *Store {
	t.Helper()
	s := metricsProjectionTestDB(t)
	_, err := s.db.Exec(`CREATE TEMP TABLE fugue_platform_artifacts (
 id text PRIMARY KEY, artifact_kind text DEFAULT 'dns-answer-bundle', scope_key text DEFAULT 'global', scope_json jsonb DEFAULT '{}', schema_version text DEFAULT '', generation text, generation_sequence bigint DEFAULT 1, status text DEFAULT 'validated', content_hash text DEFAULT 'unchanged-claimed-hash', content_json jsonb NOT NULL, validation_results_json jsonb DEFAULT '[]', compatibility_floor text DEFAULT '', metadata_json jsonb DEFAULT '{}', created_by_type text DEFAULT '', created_by_id text DEFAULT '', provenance_json jsonb DEFAULT '{}', created_at timestamptz DEFAULT now(), updated_at timestamptz DEFAULT now())`)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestConditionalArtifactReadPostgresPreservesFreshAuthority(t *testing.T) {
	s := artifactReadTestStore(t)
	payload := strings.Repeat("old", 1024)
	raw, _ := json.Marshal(map[string]any{"payload": payload})
	if _, err := s.db.Exec(`INSERT INTO fugue_platform_artifacts(id,generation,content_json) VALUES ('artifact','generation',$1)`, raw); err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"artifact", "generation"} {
		for i := 0; i < 2; i++ {
			got, err := s.pgGetPlatformArtifact(alias)
			if err != nil || got.Content["payload"] != payload {
				t.Fatalf("read %s: %v", alias, err)
			}
			got.Content["payload"] = "caller mutation"
		}
	}
	if s.readMetrics[artifactReadMetric].bodyElisions.Load() != 2 {
		t.Fatal("warm bodies were transferred")
	}
	before := s.readMetrics[artifactReadMetric].bodyBytes.Load()
	if _, err := s.db.Exec(`UPDATE fugue_platform_artifacts SET status='draft', metadata_json='{"policy_digest":"changed"}', provenance_json='{"signature":"revoked"}' WHERE id='artifact'`); err != nil {
		t.Fatal(err)
	}
	got, err := s.pgGetPlatformArtifact("artifact")
	if err != nil || got.Status != "draft" || got.Metadata["policy_digest"] != "changed" || got.Provenance.Signature != "revoked" || got.Content["payload"] != payload {
		t.Fatalf("stale authority: %+v %v", got, err)
	}
	if s.readMetrics[artifactReadMetric].bodyBytes.Load() != before {
		t.Fatal("metadata change retransmitted identical content")
	}
	changed, _ := json.Marshal(map[string]any{"payload": strings.Repeat("new", 1024)})
	if _, err := s.db.Exec(`UPDATE fugue_platform_artifacts SET content_json=$1 WHERE id='artifact'`, changed); err != nil {
		t.Fatal(err)
	}
	got, err = s.pgGetPlatformArtifact("artifact")
	if err != nil || got.Content["payload"] != strings.Repeat("new", 1024) || got.ContentHash != "unchanged-claimed-hash" {
		t.Fatal("actual content change hidden by claimed hash", err)
	}
	// Eviction of all decoded content must trigger a complete read.
	for i := 0; i < 70; i++ {
		raw, _ := json.Marshal(map[string]any{"payload": strings.Repeat(fmt.Sprint(i), 2048)})
		if _, err := platformContentDecodings.decode(raw); err != nil {
			t.Fatal(err)
		}
	}
	before = s.readMetrics[artifactReadMetric].bodyBytes.Load()
	if _, err = s.pgGetPlatformArtifact("artifact"); err != nil || s.readMetrics[artifactReadMetric].bodyBytes.Load() <= before {
		t.Fatal("eviction did not fetch missing body", err)
	}
	if _, err := s.db.Exec(`DELETE FROM fugue_platform_artifacts WHERE id='artifact'`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.pgGetPlatformArtifact("artifact"); err != ErrNotFound {
		t.Fatal("deleted artifact survived cache", err)
	}
}

func TestConditionalArtifactReadPostgresTransferReduction(t *testing.T) {
	s := artifactReadTestStore(t)
	raw, _ := json.Marshal(map[string]any{"payload": strings.Repeat("x", 1<<20)})
	if _, err := s.db.Exec(`INSERT INTO fugue_platform_artifacts(id,generation,content_json) VALUES('large','large-gen',$1)`, raw); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	for i := 0; i < 30; i++ {
		if _, err := s.pgGetPlatformArtifact("large"); err != nil {
			t.Fatal(err)
		}
	}
	bodyBytes := s.readMetrics[artifactReadMetric].bodyBytes.Load()
	if bodyBytes > uint64(len(raw)+100) || s.readMetrics[artifactReadMetric].bodyElisions.Load() != 29 {
		t.Fatalf("unexpected body bytes=%d", bodyBytes)
	}
	t.Logf("30 reads: body=%d bytes vs ~%d baseline; duration=%s", bodyBytes, 30*len(raw), time.Since(started))
}

func TestHostJournaldStatePostgresIgnoresUnrelatedHistory(t *testing.T) {
	s := metricsProjectionTestDB(t)
	_, err := s.db.Exec(`INSERT INTO fugue_node_update_tasks(id,node_updater_id,task_type,status,payload_json,logs_json,created_at,updated_at) SELECT 'history-'||n,'worker','report-image-cache-inventory','completed','{}','[]',now(),now() FROM generate_series(1,200000) n`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(`CREATE INDEX ON fugue_node_update_tasks(task_type,node_updater_id,created_at DESC,id DESC)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(`INSERT INTO fugue_node_update_tasks(id,node_updater_id,task_type,status,payload_json,requested_by_id,created_at,updated_at) VALUES
 ('old-pending','removed-worker',$1,'pending','{}','',now()-interval '2 days',now()),
 ('latest','removed-worker',$1,'completed','{"policy_hash":"new","unused":"discard"}','scheduler',now(),now()),
 ('tie-a','worker',$1,'failed','{"policy_hash":"old"}','scheduler',now(),now()),
 ('tie-z','worker',$1,'completed','{"policy_hash":"new"}','scheduler',now(),now())`, model.NodeUpdateTaskTypeReconcileHostJournaldPolicy)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	latest, busy, err := s.ReadHostJournaldPolicyTaskState(context.Background())
	if err != nil || !busy || len(latest) != 2 || latest["worker"].ID != "tie-z" || latest["removed-worker"].ID != "latest" || len(latest["removed-worker"].Payload) != 1 {
		t.Fatalf("scheduler guard/projection changed: %+v %t %v", latest, busy, err)
	}
	t.Logf("200004 tasks -> %d projected rows in %s", len(latest), time.Since(started))
	// Updating task metadata must not renew the scheduler's terminal-time
	// interval. Keep both same-updater creation times tied to also preserve
	// deterministic selection while checking every timing/ownership input.
	terminal := time.Now().UTC().Truncate(time.Microsecond).Add(-25 * time.Hour)
	if _, err := s.db.Exec(`UPDATE fugue_node_update_tasks
SET created_at=$1::timestamptz-interval '1 hour', updated_at=now(),
    completed_at=CASE WHEN id='tie-z' THEN $1::timestamptz ELSE NULL END
WHERE id IN ('tie-a','tie-z')`, terminal); err != nil {
		t.Fatal(err)
	}
	latest, busy, err = s.ReadHostJournaldPolicyTaskState(context.Background())
	task := latest["worker"]
	if err != nil || !busy || task.ID != "tie-z" || task.CompletedAt == nil || !task.CompletedAt.Equal(terminal) || !task.UpdatedAt.After(terminal) || task.RequestedByID != "scheduler" || task.Payload["policy_hash"] != "new" {
		t.Fatalf("terminal time or scheduler ownership lost in projection: %+v busy=%t error=%v", task, busy, err)
	}
}

func TestHostJournaldStateFilePreservesOldInflightAndTieOrder(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	err := s.withLockedState(true, func(state *model.State) error {
		state.NodeUpdateTasks = []model.NodeUpdateTask{
			{ID: "a", NodeUpdaterID: "worker", Type: model.NodeUpdateTaskTypeReconcileHostJournaldPolicy, Status: "completed", CreatedAt: now, Payload: map[string]string{"policy_hash": "a"}},
			{ID: "z", NodeUpdaterID: "worker", Type: model.NodeUpdateTaskTypeReconcileHostJournaldPolicy, Status: "failed", CreatedAt: now, Payload: map[string]string{"policy_hash": "z"}},
			{ID: "old", NodeUpdaterID: "missing", Type: model.NodeUpdateTaskTypeReconcileHostJournaldPolicy, Status: "running", CreatedAt: now.Add(-time.Hour)},
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	latest, busy, err := s.ReadHostJournaldPolicyTaskState(context.Background())
	if err != nil || !busy || latest["worker"].ID != "z" {
		t.Fatal("file semantics differ", err)
	}
	latest["worker"].Payload["policy_hash"] = "mutation"
	again, _, err := s.ReadHostJournaldPolicyTaskState(context.Background())
	if err != nil || again["worker"].Payload["policy_hash"] != "z" {
		t.Fatal("state alias escaped", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := s.ReadHostJournaldPolicyTaskState(ctx); err == nil {
		t.Fatal("ignored cancellation")
	}
}

func TestConditionalArtifactReadPostgresConcurrentReaders(t *testing.T) {
	s := artifactReadTestStore(t)
	raw, _ := json.Marshal(map[string]any{"payload": strings.Repeat("你好", 2048), "nested": map[string]any{"items": []any{1.5, "é", true, nil}}})
	if _, err := s.db.Exec(`INSERT INTO fugue_platform_artifacts(id,generation,content_json) VALUES('concurrent','concurrent-gen',$1)`, raw); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				got, err := s.pgGetPlatformArtifact("concurrent")
				if err != nil {
					t.Error(err)
					return
				}
				if got.Content["payload"] != strings.Repeat("你好", 2048) {
					t.Error("caller mutation leaked")
				}
				got.Content["payload"] = "mutation"
			}
		}()
	}
	wg.Wait()
}

func TestRouteBusinessReleaseProjectionPostgresPreservesReadinessIdentity(t *testing.T) {
	s := metricsProjectionTestDB(t)
	_, err := s.db.Exec(`CREATE TEMP TABLE fugue_app_releases(id text,tenant_id text DEFAULT '',app_id text DEFAULT '',role text DEFAULT '',source_ref text DEFAULT '',resolved_image_ref text DEFAULT '',upstream_url text DEFAULT '',runtime_id text DEFAULT '',deployment_name text DEFAULT '',service_name text DEFAULT '',status text DEFAULT '',status_reason text DEFAULT '',rollback_target_release_id text DEFAULT '',release_message text DEFAULT '',spec_snapshot_json jsonb,ready_at timestamptz,promoted_at timestamptz,retired_at timestamptz,retention_until timestamptz,created_at timestamptz DEFAULT now(),updated_at timestamptz DEFAULT now(),revision_workload_json jsonb)`)
	if err != nil {
		t.Fatal(err)
	}
	spec := model.AppSpec{Image: "registry.example/image:v1", RuntimeID: "runtime", Env: map[string]string{"UNUSED": strings.Repeat("x", 1<<20)}}
	raw, _ := json.Marshal(spec)
	_, err = s.db.Exec(`INSERT INTO fugue_app_releases(id,spec_snapshot_json,revision_workload_json) VALUES('full',$1,'{"deployment_uid":"proof"}'),('nil',NULL,NULL),('empty','{}',NULL)`, raw)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := s.db.Query(`SELECT ` + routeBusinessReleaseSelectColumns + ` FROM fugue_app_releases ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	releases, err := scanAppReleaseRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(releases) != 3 || releases[0].SpecSnapshot == nil || releases[1].SpecSnapshot == nil || releases[1].SpecSnapshot.Image != spec.Image || releases[1].SpecSnapshot.RuntimeID != spec.RuntimeID || len(releases[1].SpecSnapshot.Env) != 0 || releases[1].RevisionWorkload == nil || releases[1].RevisionWorkload.DeploymentUID != "proof" || releases[2].SpecSnapshot != nil {
		t.Fatalf("readiness identity changed: %+v", releases)
	}
	projected, _ := json.Marshal(releases[1])
	if len(projected) > 4096 {
		t.Fatalf("unneeded spec retained: %d bytes", len(projected))
	}
}

func TestHostnameLineagePostgresBoundsAndProjectsContent(t *testing.T) {
	s := artifactReadTestStore(t)
	raw, _ := json.Marshal(map[string]any{"payload": strings.Repeat("x", 1<<20), "rows": []any{map[string]any{"origin_HOSTNAME": " .Target.example. "}, map[string]any{"hostname": 3}}})
	if _, err := s.db.Exec(`INSERT INTO fugue_platform_artifacts(id,generation,content_json) VALUES('match','match',$1),('miss','miss','{"hostname":"other.example"}')`, raw); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListPlatformHostnameArtifacts(context.Background(), "dns-answer-bundle", "target.example", 20, false)
	if err != nil || len(got) != 1 || got[0].ID != "match" || got[0].Content != nil {
		t.Fatalf("projection: %+v %v", got, err)
	}
	got, err = s.ListPlatformHostnameArtifacts(context.Background(), "dns-answer-bundle", "target.example", 5, true)
	if err != nil || len(got) != 1 || len(got[0].Content) == 0 {
		t.Fatal("explicit content unavailable", err)
	}
	if _, err := s.db.Exec(`UPDATE fugue_platform_artifacts SET updated_at=now()+interval '1 minute' WHERE id='miss'`); err != nil {
		t.Fatal(err)
	}
	got, err = s.ListPlatformHostnameArtifacts(context.Background(), "dns-answer-bundle", "target.example", 1, false)
	if err != nil || len(got) != 0 {
		t.Fatal("candidate boundary was applied after matching", err)
	}
	if _, err = s.ListPlatformHostnameArtifacts(context.Background(), "dns-answer-bundle", "target.example", 6, true); err != ErrInvalidInput {
		t.Fatal("unbounded body option", err)
	}
}
