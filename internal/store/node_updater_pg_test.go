package store

import (
	"regexp"
	"testing"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

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
	WHEN task_type IN ('report-image-cache-inventory', 'report-lvm-localpv-inventory') AND created_at <= $3 THEN 1
	WHEN task_type = 'replicate-app-image' AND COALESCE(payload_json->>'priority', '') = 'deploy_blocking' THEN 2
	WHEN task_type IN ('report-image-cache-inventory', 'report-lvm-localpv-inventory') THEN 3
	WHEN task_type IN ('prune-image-cache', 'decommission-lvm-localpv') THEN 4
	ELSE 5
END, created_at ASC, id ASC
LIMIT $4
`)
	mock.ExpectQuery(query).
		WithArgs("nodeupdater_test", model.NodeUpdateTaskStatusPending, sqlmock.AnyArg(), 1).
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
