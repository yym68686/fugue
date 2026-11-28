package store

import (
	"context"
	"errors"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestUnclaimedBackupExecutionQueueFiltersBeforeLimit(t *testing.T) {
	s, mock := newBackupSchedulePGTestStore(t)
	now := time.Date(2026, 11, 28, 5, 0, 0, 0, time.FixedZone("fixture", 8*3600))
	for _, limit := range []int{0, -1, maxUnclaimedBackupExecutionScan + 1} {
		if _, err := s.ListUnclaimedBackupExecutionIDs(context.Background(), now, limit); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("limit %d: %v", limit, err)
		}
	}
	if _, err := s.ListUnclaimedBackupExecutionIDs(context.Background(), time.Time{}, 1); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("missing due cutoff: %v", err)
	}
	mock.ExpectQuery(`(?s)SELECT id FROM fugue_backup_runs WHERE status = 'pending'.*trigger <> 'retry'.*lease_owner = ''.*locked_until IS NULL.*started_at IS NULL.*target_type <> \$1.*target_json->>'engine' IS DISTINCT FROM \$2.*next_retry_at <= \$3.*ORDER BY created_at, id LIMIT \$4`).
		WithArgs(model.BackupTargetControlPlaneDatabase, model.BackupEngineLonghornSnapshot, now.UTC(), 1).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("durably-requested"))
	ids, err := s.ListUnclaimedBackupExecutionIDs(context.Background(), now, 1)
	if err != nil || !reflect.DeepEqual(ids, []string{"durably-requested"}) {
		t.Fatalf("unexpected initial queue: %v (%v)", ids, err)
	}
	assertBackupSchedulePGExpectations(t, mock)
}

func TestExpiredBackupExecutionQueueOnlyConsidersPreviouslyClaimedRuns(t *testing.T) {
	s, mock := newBackupSchedulePGTestStore(t)
	now := time.Date(2026, 11, 28, 5, 0, 0, 0, time.UTC)
	ttl := 2 * time.Minute
	claimed := model.NormalizeBackupRun(model.BackupRun{
		ID: "old-claim", Status: model.BackupRunStatusRunning, Trigger: model.BackupRunTriggerManual,
		LeaseOwner: "previous-executor", CreatedAt: now.Add(-2 * ttl), UpdatedAt: now.Add(-2 * ttl),
		Target: model.BackupTarget{Type: model.BackupTargetRegistry},
	})
	mock.ExpectQuery(`(?s)SELECT .* FROM fugue_backup_runs WHERE status = 'running' AND COALESCE\(locked_until, COALESCE\(heartbeat_at, updated_at\) \+ \(\$2 \* INTERVAL '1 microsecond'\)\) < \$1 ORDER BY .*created_at, id LIMIT \$3`).
		WithArgs(now, ttl.Microseconds(), 1).
		WillReturnRows(backupScheduleRunRows(claimed))
	runs, err := s.ListExpiredBackupExecutions(context.Background(), now, ttl, 1)
	if err != nil || len(runs) != 1 || runs[0].ID != claimed.ID || runs[0].LeaseOwner != claimed.LeaseOwner {
		t.Fatalf("invalid expired claimed execution: %v (%v)", runs, err)
	}
	assertBackupSchedulePGExpectations(t, mock)
}

func TestPGUnclaimedBackupExecutionDiscoveryDoesNotMutateOrReplay(t *testing.T) {
	dsn := os.Getenv("FUGUE_STORE_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("requires a disposable PostgreSQL instance")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost") || !strings.HasPrefix(u.Path, "/fugue_test") {
		t.Fatal("only a disposable loopback PostgreSQL database may be used")
	}
	db, err := openPostgresDatabase(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	// A connection-local shadow table traps writes and does not require (or
	// modify) any persistent backup run in the test database.
	if _, err := db.Exec(`
CREATE TEMP TABLE fugue_backup_runs (
    id text, status text, trigger text, lease_owner text DEFAULT '', locked_until timestamptz,
    started_at timestamptz, target_type text, target_json jsonb DEFAULT '{}',
    next_retry_at timestamptz, created_at timestamptz DEFAULT '2026-11-27 17:00:00Z'
);
INSERT INTO fugue_backup_runs(id,status,trigger,target_type,target_json,next_retry_at,created_at) VALUES
    ('ordinary-b','pending','manual','registry','{}',NULL,'2026-11-27 19:59:00Z'),
    ('ordinary-a','pending','scheduled','app-database','{"engine":"pg_dump"}','2026-11-27 20:00:00Z','2026-11-27 19:59:00Z'),
    ('coordinated-control-plane','pending','manual','control-plane-db','{}',NULL,'2026-11-27 18:00:00Z'),
    ('coordinated-tenant','pending','manual','app-database','{"engine":"longhorn-snapshot"}',NULL,'2026-11-27 18:00:00Z'),
    ('retry','pending','retry','registry','{}','2026-11-27 18:00:00Z','2026-11-27 18:00:00Z'),
    ('future','pending','manual','registry','{}','2026-11-27 21:00:00Z','2026-11-27 18:00:00Z'),
    ('finished','succeeded','manual','registry','{}',NULL,'2026-11-27 18:00:00Z');
INSERT INTO fugue_backup_runs(id,status,trigger,target_type,started_at) VALUES
    ('prior-side-effect','pending','manual','registry','2026-11-27 17:00:00Z');
INSERT INTO fugue_backup_runs(id,status,trigger,target_type,lease_owner,locked_until) VALUES
    ('already-claimed','pending','manual','registry','another-worker','2026-11-27 21:00:00Z');
CREATE FUNCTION pg_temp.refuse_backup_queue_write() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'discovery attempted to mutate a backup'; END;
$$;
CREATE TRIGGER backup_queue_read_only BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE
ON fugue_backup_runs FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.refuse_backup_queue_write();
SET default_transaction_read_only = on;`); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Date(2026, 11, 27, 20, 0, 0, 0, time.UTC)
	s := &Store{db: db}
	for limit, want := range map[int][]string{1: {"ordinary-a"}, 10: {"ordinary-a", "ordinary-b"}} {
		ids, err := s.ListUnclaimedBackupExecutionIDs(ctx, now, limit)
		if err != nil || !reflect.DeepEqual(ids, want) {
			t.Fatalf("limit %d: got %v (%v), want %v", limit, ids, err, want)
		}
	}
}
