package store

import (
	"encoding/json"
	"fugue/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestNodeUpdateTaskSummaryAndExactLookupPreserveTenantIsolation(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := s.withLockedState(true, func(state *model.State) error {
		for _, id := range []string{"a", "b", "c"} {
			tenant := "tenant-a"
			if id == "c" {
				tenant = "tenant-b"
			}
			state.NodeUpdateTasks = append(state.NodeUpdateTasks, model.NodeUpdateTask{ID: id, TenantID: tenant, NodeUpdaterID: "worker", Status: model.NodeUpdateTaskStatusCompleted, CreatedAt: now, Payload: map[string]string{"data": strings.Repeat("large", 10000)}, Logs: []model.NodeUpdateTaskLog{{Message: strings.Repeat("log", 10000)}}})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	items, err := s.ListNodeUpdateTasksWithOptions("tenant-a", false, "worker", "completed", NodeUpdateTaskListOptions{Summary: true, Limit: 1})
	if err != nil || len(items) != 1 || items[0].ID != "b" || items[0].Payload != nil || items[0].Logs != nil {
		t.Fatalf("summary=%+v err=%v", items, err)
	}
	raw, _ := json.Marshal(items)
	if len(raw) > 2048 {
		t.Fatalf("summary response retained large details: %d bytes", len(raw))
	}
	items, err = s.ListNodeUpdateTasksWithOptions("tenant-a", false, "", "", NodeUpdateTaskListOptions{TaskID: "c", Limit: 1})
	if err != nil || len(items) != 0 {
		t.Fatalf("cross-tenant exact lookup leaked data: %+v %v", items, err)
	}
	items, err = s.ListNodeUpdateTasksWithOptions("tenant-a", false, "", "", NodeUpdateTaskListOptions{TaskID: "a", Limit: 1})
	if err != nil || len(items) != 1 || len(items[0].Payload) == 0 || len(items[0].Logs) == 0 {
		t.Fatalf("exact details missing: %+v %v", items, err)
	}
}

func TestPostgresNodeUpdateTaskSummaryProjectsOutLargeFields(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{db: db, databaseURL: "postgres://test", dbReady: true}
	query := `SELECT id, tenant_id, node_updater_id, machine_id, runtime_id, node_key_id, cluster_node_name, task_type, status, '{}'::jsonb AS payload_json, result_message, error_message, '[]'::jsonb AS logs_json, requested_by_type, requested_by_id, created_at, updated_at, claimed_at, completed_at FROM fugue_node_update_tasks WHERE id = $1 AND tenant_id = $2 AND node_updater_id = $3 AND status = $4 ORDER BY created_at DESC, id DESC LIMIT $5`
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs("task", "tenant", "worker", "completed", 1).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	if _, err := s.ListNodeUpdateTasksWithOptions("tenant", false, "worker", "completed", NodeUpdateTaskListOptions{TaskID: "task", Summary: true, Limit: 1}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresNodeUpdateTaskSummaryRealDatabase(t *testing.T) {
	s := metricsProjectionTestDB(t)
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		_, err := s.db.Exec(`INSERT INTO fugue_node_update_tasks(id,tenant_id,node_updater_id,task_type,status,payload_json,logs_json,created_at,updated_at) VALUES($1,$1,'worker','diagnose-node','completed',jsonb_build_object('detail',repeat('x',100000)),jsonb_build_array(jsonb_build_object('message',repeat('y',100000))),now(),now())`, tenant)
		if err != nil {
			t.Fatal(err)
		}
	}
	full, err := s.ListNodeUpdateTasks("tenant-a", false, "worker", "completed")
	if err != nil {
		t.Fatal(err)
	}
	summary, err := s.ListNodeUpdateTasksWithOptions("tenant-a", false, "worker", "completed", NodeUpdateTaskListOptions{Limit: 1, Summary: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(full) != 1 || len(summary) != 1 || full[0].ID != summary[0].ID || len(summary[0].Payload) != 0 || len(summary[0].Logs) != 0 {
		t.Fatalf("projection mismatch: full=%d summary=%+v", len(full), summary)
	}
	f, _ := json.Marshal(full)
	b, _ := json.Marshal(summary)
	if len(f) < 200000 || len(b) > 2048 {
		t.Fatalf("unexpected serialized sizes: full=%d summary=%d", len(f), len(b))
	}
	t.Logf("same authorized task: full=%d bytes summary=%d bytes", len(f), len(b))
	other, err := s.ListNodeUpdateTasksWithOptions("tenant-a", false, "", "", NodeUpdateTaskListOptions{TaskID: "tenant-b", Limit: 1})
	if err != nil || len(other) != 0 {
		t.Fatalf("cross-tenant lookup: %v %v", other, err)
	}
}
