package store

import (
	"regexp"
	"slices"
	"testing"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresStorageRescueDeliveryOrder(t *testing.T) {
	s := newStoreTestDatabase(t, "")
	if _, err := s.db.ExecContext(t.Context(), `
INSERT INTO fugue_node_updaters(id,status,token_prefix,token_hash,created_at,updated_at)
VALUES ('updater','active','test','test',now(),now());
INSERT INTO fugue_node_update_tasks(id,node_updater_id,task_type,status,payload_json,created_at,updated_at)
SELECT id,'updater',kind,'pending',payload::jsonb,now()-age::interval,now()
FROM (VALUES
 ('inventory','report-lvm-localpv-inventory','{}','1 minute'),
 ('overdue','report-image-cache-inventory','{}','31 minutes'),
 ('rescue','expand-lvm-localpv','{}','1 second'),
 ('prune','prune-image-cache','{}','11 minutes'),
 ('upgrade','upgrade-node-updater','{}','1 second'),
 ('deploy','replicate-app-image','{"priority":"deploy_blocking"}','1 second'),
 ('capacity','refresh-join-config','{"pod_capacity_mode":"resources","dry_run":"true"}','2 seconds'),
 ('refresh','refresh-join-config','{}','1 second'),
 ('policy','reconcile-host-inotify-policy','{}','11 minutes'),
 ('journald','reconcile-host-journald-policy','{}','11 minutes'),
 ('fresh-policy','reconcile-host-inotify-policy','{}','1 minute')) AS f(id,kind,payload,age)`); err != nil {
		t.Fatal(err)
	}
	want := []string{"upgrade", "overdue", "capacity", "rescue", "deploy", "journald", "policy", "prune", "inventory", "fresh-policy", "refresh"}
	for _, limit := range []int{1, 5, 11} {
		tasks, err := s.ListPendingNodeUpdateTasks("updater", limit)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]string, 0, len(tasks))
		for _, task := range tasks {
			got = append(got, task.ID)
		}
		if !slices.Equal(got, want[:limit]) {
			t.Fatalf("PostgreSQL delivery with limit %d: got %v, want %v", limit, got, want[:limit])
		}
	}

	// Poll with another fresh inventory task as each waiting policy and prune
	// completes. New periodic inventory must not starve either recovery class.
	for i, expected := range want[5:8] {
		if _, err := s.db.ExecContext(t.Context(),
			`UPDATE fugue_node_update_tasks SET status='completed' WHERE id=ANY($1)`, want[:5+i]); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.ExecContext(t.Context(), `INSERT INTO fugue_node_update_tasks
(id,node_updater_id,task_type,status,created_at,updated_at)
VALUES ($1,'updater','report-image-cache-inventory','pending',now(),now())`, "inventory-"+expected); err != nil {
			t.Fatal(err)
		}
		tasks, err := s.ListPendingNodeUpdateTasks("updater", 1)
		if err != nil || len(tasks) != 1 || tasks[0].ID != expected {
			t.Fatalf("delivery after replenishing inventory: got %v, want %s: %v", tasks, expected, err)
		}
	}
}

func TestPGListPendingNodeUpdateTasksPromotesOverdueInventory(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()
	stateStore := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	query := regexp.QuoteMeta(`
SELECT id, tenant_id, node_updater_id, machine_id, runtime_id, node_key_id, cluster_node_name, task_type, status, payload_json, result_message, error_message, logs_json, requested_by_type, requested_by_id, created_at, updated_at, claimed_at, completed_at
FROM fugue_node_update_tasks
WHERE node_updater_id = $1 AND status = $2
ORDER BY CASE
	WHEN task_type = 'upgrade-node-updater' THEN 0
	WHEN task_type = 'refresh-join-config' AND COALESCE(payload_json->>'pod_capacity_mode', '') <> '' THEN 1
	WHEN task_type = 'expand-lvm-localpv' THEN 1
	WHEN task_type IN ('report-image-cache-inventory', 'report-lvm-localpv-inventory') AND created_at <= $3 THEN 1
	WHEN task_type = 'replicate-app-image' AND COALESCE(payload_json->>'priority', '') = 'deploy_blocking' THEN 2
	WHEN task_type = 'prune-image-cache' AND created_at <= $5 THEN 3
	WHEN task_type IN ('reconcile-host-journald-policy', 'reconcile-host-inotify-policy') AND created_at <= $6 THEN 3
	WHEN task_type IN ('report-image-cache-inventory', 'report-lvm-localpv-inventory') THEN 3
	WHEN task_type IN ('reconcile-host-journald-policy', 'reconcile-host-inotify-policy') THEN 4
	WHEN task_type IN ('prune-image-cache', 'decommission-lvm-localpv') THEN 4
	ELSE 5
END, created_at ASC, id ASC
LIMIT $4
`)
	mock.ExpectQuery(query).
		WithArgs("nodeupdater_test", model.NodeUpdateTaskStatusPending, sqlmock.AnyArg(), 1, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "node_updater_id", "machine_id", "runtime_id", "node_key_id",
			"cluster_node_name", "task_type", "status", "payload_json", "result_message",
			"error_message", "logs_json", "requested_by_type", "requested_by_id", "created_at",
			"updated_at", "claimed_at", "completed_at",
		}))

	tasks, err := stateStore.ListPendingNodeUpdateTasks("nodeupdater_test", 1)
	if err != nil {
		t.Fatalf("list pending node update tasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("expected no mocked tasks, got %+v", tasks)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}
