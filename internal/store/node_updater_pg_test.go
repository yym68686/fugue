package store

import (
	"database/sql"
	"net/url"
	"os"
	"regexp"
	"testing"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresStorageRescueDeliveryOrder(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("requires local PostgreSQL")
	}
	parsed, err := url.Parse(address)
	if err != nil || (parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost") {
		t.Fatal("requires local PostgreSQL")
	}
	db, err := sql.Open("pgx", address)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`create temporary table fugue_node_update_tasks (
 id text, tenant_id text default '', node_updater_id text default 'test', machine_id text default '', runtime_id text default '', node_key_id text default '',
 cluster_node_name text default '',task_type text,status text default 'pending',payload_json jsonb default '{}',result_message text default '',error_message text default '',logs_json jsonb default '[]',
 requested_by_type text default '',requested_by_id text default '',created_at timestamptz default now(),updated_at timestamptz default now(),claimed_at timestamptz,completed_at timestamptz)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`insert into fugue_node_update_tasks(id,task_type) values ('inventory','report-lvm-localpv-inventory'),('rescue','expand-lvm-localpv'),('prune','prune-image-cache'),('upgrade','upgrade-node-updater')`)
	if err != nil {
		t.Fatal(err)
	}
	s := &Store{databaseURL: address, db: db, dbReady: true}
	tasks, err := s.ListPendingNodeUpdateTasks("test", 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 4 {
		t.Fatalf("missing tasks: %+v", tasks)
	}
	for i, want := range []string{"upgrade", "rescue", "inventory", "prune"} {
		if tasks[i].ID != want {
			t.Fatalf("Postgres order %d: got %s want %s", i, tasks[i].ID, want)
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
	WHEN task_type = 'expand-lvm-localpv' THEN 1
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
