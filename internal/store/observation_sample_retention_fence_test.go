package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
	"fugue/internal/testpostgres"
)

func TestPostgresSampleRetentionFenceAllowsIngestAndTenantCascade(t *testing.T) {
	address := testpostgres.Database(t, "")
	db, err := openPostgresDatabase(address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := t.Context()
	adminExec := func(query string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	adminExec(schemamigrate.PlatformQualityEvidenceRetentionSQL)
	adminExec(schemamigrate.ObservationSampleRetentionFenceSQL)
	adminExec(schemamigrate.ObservationSampleRetentionSQL)
	role := quotePostgresIntegrationIdentifier(t, model.NewID("sample_fence_test"))
	adminExec(`CREATE ROLE ` + role + ` NOLOGIN`)
	const restoreOwners = `
ALTER TABLE public.fugue_edge_network_samples OWNER TO CURRENT_USER;
ALTER TABLE public.fugue_edge_performance_samples OWNER TO CURRENT_USER;
ALTER TABLE public.fugue_resource_usage_samples OWNER TO CURRENT_USER;`
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := db.ExecContext(cleanup, restoreOwners+`DROP OWNED BY `+role+`; DROP ROLE `+role); err != nil {
			t.Error("clean up disposable test role: ", err)
		}
	})
	// The current API login owns these tables in production. Model that
	// effective identity, including the role used by the resource-usage FK.
	for _, table := range retainedObservationTables {
		adminExec(fmt.Sprintf(`ALTER TABLE public.%s OWNER TO %s`, table.table, role))
	}
	adminExec(schemamigrate.ObservationSampleRetentionActivateSQL)
	adminExec(fmt.Sprintf(`
GRANT SELECT, DELETE ON public.fugue_tenants TO %s;
INSERT INTO public.fugue_tenants (id,name,slug,status,created_at,updated_at)
    VALUES ('synthetic','Synthetic','synthetic','active',now(),now());
INSERT INTO public.fugue_resource_usage_samples (id,tenant_id,target_kind,target_id,observed_at)
    VALUES ('tenant-sample','synthetic','app','synthetic',now()-interval '10 days'),
           ('anonymous-sample',NULL,'app','synthetic',now()-interval '10 days');
INSERT INTO public.fugue_edge_network_samples (edge_id,id,hostname,observed_at,sample_json)
    VALUES ('synthetic','old','example.test',now()-interval '2 hours','{}');
INSERT INTO public.fugue_edge_performance_samples (id,sampled_at)
    VALUES ('old',now()-interval '10 days');`, role))
	legacy, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	if _, err := legacy.ExecContext(ctx, `SET ROLE `+role); err != nil {
		t.Fatal(err)
	}
	defer legacy.ExecContext(ctx, `RESET ROLE`)
	foreground, err := legacy.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer foreground.Rollback()
	for _, table := range retainedObservationTables {
		result, err := foreground.ExecContext(ctx, fmt.Sprintf(`DELETE FROM public.%s WHERE %s<clock_timestamp()`, table.table, table.timestamp))
		if err != nil {
			t.Fatal(err)
		}
		if rows, err := result.RowsAffected(); err != nil || rows != 0 {
			t.Fatalf("legacy delete affected %d rows in %s: %v", rows, table.table, err)
		}
	}
	if _, err := foreground.ExecContext(ctx, `INSERT INTO public.fugue_edge_network_samples (edge_id,id,hostname,observed_at,sample_json)
        VALUES ('synthetic','fresh','example.test',now(),'{}')`); err != nil {
		t.Fatalf("fenced legacy delete broke same-transaction sample ingestion: %v", err)
	}
	if err := foreground.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.ExecContext(ctx, `DELETE FROM public.fugue_tenants WHERE id='synthetic'`); err != nil {
		t.Fatalf("fence prohibited a legitimate tenant cascade: %v", err)
	}
	var tenantSample, anonymousSample, freshSample bool
	if err := db.QueryRowContext(ctx, `SELECT
          EXISTS(SELECT 1 FROM public.fugue_resource_usage_samples WHERE id='tenant-sample'),
          EXISTS(SELECT 1 FROM public.fugue_resource_usage_samples WHERE id='anonymous-sample'),
          EXISTS(SELECT 1 FROM public.fugue_edge_network_samples WHERE id='fresh')`).Scan(&tenantSample, &anonymousSample, &freshSample); err != nil {
		t.Fatal(err)
	}
	if tenantSample || !anonymousSample || !freshSample {
		t.Fatalf("wrong rows after tenant cascade and foreground insert: tenant=%t anonymous=%t fresh=%t", tenantSample, anonymousSample, freshSample)
	}
}

func TestPostgresSampleRetentionWorkerUsesDurableFencedBatches(t *testing.T) {
	address := testpostgres.Database(t, "")
	db, err := openPostgresDatabase(address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := t.Context()
	adminExec := func(query string, args ...any) {
		t.Helper()
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	adminExec(schemamigrate.PlatformQualityEvidenceRetentionSQL)
	adminExec(schemamigrate.ObservationSampleRetentionFenceSQL)
	adminExec(schemamigrate.ObservationSampleRetentionSQL)
	role := quotePostgresIntegrationIdentifier(t, model.NewID("sample_worker_test"))
	adminExec(`CREATE ROLE ` + role + ` NOLOGIN`)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := db.ExecContext(cleanup, `DROP OWNED BY `+role+`; DROP ROLE `+role); err != nil {
			t.Error("clean up disposable test role: ", err)
		}
	})
	adminExec(fmt.Sprintf(`
GRANT USAGE ON SCHEMA fugue_observation_maintenance TO %[1]s;
GRANT EXECUTE ON FUNCTION fugue_observation_maintenance.prune_samples_v1(text, boolean, int, int, int) TO %[1]s;
GRANT SELECT ON fugue_observation_maintenance.sample_worker_progress TO %[1]s;`, role))
	openWorker := func() *sql.Conn {
		t.Helper()
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = conn.ExecContext(ctx, `SET ROLE `+role); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		return conn
	}
	closeWorker := func(conn *sql.Conn) {
		t.Helper()
		_, err := conn.ExecContext(ctx, `RESET ROLE`)
		if closeErr := conn.Close(); err != nil || closeErr != nil {
			t.Errorf("close synthetic worker: %v / %v", err, closeErr)
		}
	}
	const invoke = `SELECT * FROM fugue_observation_maintenance.prune_samples_v1('network',$1,$2,10000,1)`
	pass := func(run interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	}, periodic bool, state string, n int64) {
		t.Helper()
		var got string
		var deleted int64
		var at sql.NullTime
		if err := run.QueryRowContext(ctx, invoke, periodic, 7).Scan(&got, &deleted, &at); err != nil || got != state || deleted != n || at.Valid != (state == "completed") {
			t.Fatalf("sample pass state=%q deleted=%d timestamp=%v: %v", got, deleted, at, err)
		}
	}
	worker := openWorker()
	defer func() {
		if worker != nil {
			closeWorker(worker)
		}
	}()
	for _, operation := range []string{
		`SELECT value FROM public.fugue_meta`,
		`DELETE FROM public.fugue_edge_network_samples WHERE false`,
		`UPDATE fugue_observation_maintenance.sample_retention_progress SET total_deleted=0`,
	} {
		if _, err := worker.ExecContext(ctx, operation); err == nil {
			t.Fatal("sample worker obtained excess permissions")
		}
	}
	adminExec(`INSERT INTO public.fugue_edge_network_samples (edge_id,id,hostname,observed_at,sample_json)
    VALUES ('synthetic','older','example.test',now()-interval '9 days','{}'),
           ('synthetic','later','example.test',now()-interval '2 hours','{}'),
           ('synthetic','protected','example.test',now()-interval '30 minutes','{}')`)
	pass(worker, true, "standby", 0)
	key := observationCheckpointKey(observationSampleRetentionOwnerKey)
	adminExec(`INSERT INTO public.fugue_meta (key,value) VALUES($1,'worker')`, key)
	var ignoredState string
	var ignoredRows int64
	var ignoredAt sql.NullTime
	if err := worker.QueryRowContext(ctx, invoke, true, 7).Scan(&ignoredState, &ignoredRows, &ignoredAt); err == nil || !strings.Contains(err.Error(), "fence is missing") {
		t.Fatalf("worker must refuse to delete without the durable fence: %v", err)
	}
	adminExec(`DELETE FROM public.fugue_meta WHERE key=$1`, key)
	activation, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer activation.Close()
	for _, q := range []string{
		`SELECT pg_advisory_xact_lock(315609238744311::bigint)`,
		`SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`,
	} {
		func() {
			inFlight, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer inFlight.Rollback()
			var args []any
			if strings.Contains(q, "$1") {
				args = append(args, key)
			}
			if _, err := inFlight.ExecContext(ctx, q, args...); err != nil {
				t.Fatal(err)
			}
			_, err = activation.ExecContext(ctx, schemamigrate.ObservationSampleRetentionActivateSQL)
			if _, resetErr := activation.ExecContext(ctx, `ROLLBACK`); resetErr != nil {
				t.Fatal(resetErr)
			}
			if err == nil || !strings.Contains(err.Error(), "ingestion transaction is active") {
				t.Fatalf("handoff bypassed an in-flight legacy operation: %v", err)
			}
			pass(worker, true, "standby", 0)
		}()
	}
	for range 2 {
		adminExec(schemamigrate.ObservationSampleRetentionActivateSQL)
	}
	pass(worker, false, "completed", 0) // A quiet producer did not advance its demand.
	tx, err := worker.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	pass(tx, true, "completed", 1)
	competing := openWorker()
	defer closeWorker(competing)
	pass(competing, true, "busy", 0)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	pass(competing, true, "completed", 1)
	pass(competing, true, "completed", 0) // Without demand, the two-hour row remains.
	demand := observationCheckpointKey(observationSampleRetentionDemand + networkSampleRetention)
	adminExec(`INSERT INTO public.fugue_meta (key,value) VALUES($1,(extract(epoch FROM now()+interval '1 day')::bigint)::text)`, demand)
	pass(competing, false, "completed", 1) // Clamp even a future demand to the one-hour floor.
	closeWorker(worker)
	worker = openWorker()
	var owner string
	var passes, deleted int64
	var at time.Time
	if err := worker.QueryRowContext(ctx, `SELECT owner,completed_passes,total_deleted,completed_at
      FROM fugue_observation_maintenance.sample_worker_progress WHERE target='network'`).Scan(&owner, &passes, &deleted, &at); err != nil ||
		owner != "worker" || passes != 4 || deleted != 2 || at.IsZero() {
		t.Fatalf("reopened worker did not see atomic progress: %q %d %d %v: %v", owner, passes, deleted, at, err)
	}
	var protected bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM public.fugue_edge_network_samples WHERE id='protected')`).Scan(&protected); err != nil || !protected {
		t.Fatalf("worker deleted protected recent history: %v", err)
	}
}
